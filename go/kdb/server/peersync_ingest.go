package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
)

// serverLocalNode makes peer sync an ordinary writer of this runtime: it queues on the same
// writeGate as Commit/Upsert/Replay, is refused for the same reasons (draining, read-only,
// fenced), and runs the same post-commit hooks - index maintenance, the unique-key registry and
// CommitListener. Before it existed a peer's commits moved the head outside the gate, racing
// local writers, and never reached any of those hooks (docs/kdb-distributed-plan.md D2/D3/D8).
type serverLocalNode struct {
	rt *KdbServerRuntime
}

// PeerSyncNode returns the peersync.LocalNode that feeds this runtime.
func (s *KdbServerRuntime) PeerSyncNode() peersync.LocalNode { return serverLocalNode{rt: s} }

func (n serverLocalNode) Exclusive(fn func() error) error {
	rt := n.rt
	if rt.draining.Load() {
		return &UnavailableError{Reason: "server is shutting down"}
	}
	if err := rt.Runtime.AssertWritable(); err != nil {
		return err
	}
	if err := rt.fenceErr(); err != nil {
		return err
	}
	timeout := rt.WriteTimeout
	if timeout <= 0 {
		timeout = DefaultWriteTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	release, err := rt.writeGate.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return fn()
}

func (n serverLocalNode) Advanced(step peersync.AdvanceStep) error {
	rt := n.rt
	last := step.Commits[len(step.Commits)-1]
	var firstErr error
	keep := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if p, ok := rt.SQLIndexProvider().(*RegistryIndexProvider); ok && len(p.registry.Descriptors()) > 0 && len(step.Applied) > 0 {
		prepared, err := p.prepareCommitForIndexes(step.Applied)
		if err == nil {
			_, err = p.commitToIndexes(prepared, last.Hash)
		}
		// An index that cannot take a replicated value is not a reason to refuse history the peer
		// already accepted; it is rebuilt from the documents, which are the truth.
		if err != nil {
			keep(fmt.Errorf("peer sync: updating indexes: %w", err))
			if rerr := p.rebuild(nil); rerr != nil {
				keep(rerr)
			}
		}
	}
	if rt.UniqueKeys != nil {
		sch := rt.Schema()
		if sch.HasUniqueConstraints() {
			dups, err := rt.UniqueKeys.RebuildLenient(rt.Runtime.DefaultNamespace, rt.Runtime.Storage, last.DocumentTreeHash, sch)
			keep(err)
			for _, d := range dups {
				rt.replication.record(ReplicationIssue{
					Kind:      ReplicationIssueUniqueDuplicate,
					Namespace: rt.Runtime.DefaultNamespace,
					Detail:    d.Error(),
					CommitHex: last.Hash.Hex(),
				})
			}
		}
	}
	localHost := rt.namespaceSet().Coordinator().LocalHostID()
	for _, c := range step.Commits {
		if m, ok := embed.ParseGroupMarker(c.Message); ok && m.Host != localHost {
			rt.replication.record(ReplicationIssue{
				Kind:      ReplicationIssueForeignGroupPart,
				Namespace: rt.Runtime.DefaultNamespace,
				Detail: fmt.Sprintf("commit is part of cross-namespace group %s (namespaces %v) decided on host %q; "+
					"its atomicity with the other parts is not verified on this node", m.Group, m.Parts, m.Host),
				CommitHex: c.Hash.Hex(),
			})
		}
	}
	if rt.CommitListener != nil {
		for _, c := range step.Commits {
			rt.CommitListener(rt.Runtime.DefaultNamespace, c)
		}
	}
	return firstErr
}

// ReplicationIssueKind classifies something peer sync accepted but could not make consistent.
type ReplicationIssueKind string

const (
	// ReplicationIssueUniqueDuplicate: replicated documents claim a value a unique constraint
	// says only one document may hold.
	ReplicationIssueUniqueDuplicate ReplicationIssueKind = "unique-duplicate"
	// ReplicationIssueForeignGroupPart: a replicated commit is one part of a cross-namespace
	// group decided on another host; nothing here checks the other parts arrived with it.
	ReplicationIssueForeignGroupPart ReplicationIssueKind = "foreign-group-part"
)

// ReplicationIssue is one such thing, kept for an operator to act on.
type ReplicationIssue struct {
	Kind      ReplicationIssueKind
	Namespace string
	Detail    string
	CommitHex string
	At        time.Time
}

// replicationIssues is the in-memory record of ReplicationIssue. Bounded: it is a diagnostic,
// and the durable conflict queue is where anything that needs resolving is kept.
type replicationIssues struct {
	mu     sync.Mutex
	issues []ReplicationIssue
}

const maxReplicationIssues = 1000

func (r *replicationIssues) record(i ReplicationIssue) {
	if i.At.IsZero() {
		i.At = time.Now()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.issues = append(r.issues, i)
	if len(r.issues) > maxReplicationIssues {
		r.issues = r.issues[len(r.issues)-maxReplicationIssues:]
	}
}

// ReplicationIssues returns what peer sync has recorded, oldest first.
func (s *KdbServerRuntime) ReplicationIssues() []ReplicationIssue {
	s.replication.mu.Lock()
	defer s.replication.mu.Unlock()
	return append([]ReplicationIssue(nil), s.replication.issues...)
}
