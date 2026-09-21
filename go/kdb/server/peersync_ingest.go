package server

import (
	"context"
	"fmt"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// serverLocalNode makes peer sync an ordinary writer of this runtime: it queues on the same
// writeGate as Commit/Upsert/Replay, is refused for the same reasons (draining, read-only,
// fenced), and runs the same post-commit hooks - index maintenance, the unique-key registry and
// CommitListener. Before it existed a peer's commits moved the head outside the gate, racing
// local writers, and never reached any of those hooks (docs/kdb-distributed-plan.md D2/D3/D8).
type serverLocalNode struct {
	rt *KdbServerRuntime
}

// peerPersistAsync is how peer sync logs commits it adopts into this runtime, or nil for a
// runtime with no log.
func (s *KdbServerRuntime) peerPersistAsync() func(document.Commit) (func() error, error) {
	if s.persister == nil {
		return nil
	}
	return s.persister.PersistAsync
}

// PeerIngestEnv describes this runtime's namespace to peer sync: its DAG and storage, its write
// serialization and hooks, its log, and its conflict policy.
func (s *KdbServerRuntime) PeerIngestEnv() peersync.IngestEnv {
	env := peersync.IngestEnv{
		DAG:                s.dag,
		Storage:            s.Runtime.Storage,
		NamespaceID:        s.Runtime.DefaultNamespace,
		Node:               s.PeerSyncNode(),
		PersistAsync:       s.peerPersistAsync(),
		ApplyToStorage:     true,
		Resolution:         peersync.ResolutionOptions{Policy: s.PeerSyncConflictPolicy},
		Conflicts:          s.Conflicts,
		CanInstallSnapshot: s.Runtime.CanInstallSnapshot,
		SnapshotInstalled: func(root document.Commit) error {
			if err := s.Runtime.PersistSnapshot(root); err != nil {
				return err
			}
			// The documents arrived without commits the unique registry could be told about.
			return s.RebuildUniqueKeys()
		},
	}
	if _, ok := s.HomeOf(); ok {
		// Only a single-home namespace has fences to check; everything else skips the walk.
		env.CheckCommit = s.checkReplicatedCommit
	}
	return env
}

// PeerNamespaces is the set of namespaces this runtime's process serves to peers, and feeds
// from them: every namespace in its NamespaceSet, or just its own when it has none.
func (s *KdbServerRuntime) PeerNamespaces() peersync.NamespaceProvider {
	return peerNamespaceProvider{set: s.namespaceSet()}
}

type peerNamespaceProvider struct{ set *NamespaceSet }

func (p peerNamespaceProvider) List() []string {
	return append(p.set.Namespaces(), p.set.systemNames()...)
}

func (p peerNamespaceProvider) Env(ns string, create bool) (peersync.IngestEnv, error) {
	rt, ok := p.set.systemRuntime(ns)
	if !ok {
		var err error
		if rt, err = p.set.Resolve(ns, create); err != nil {
			return peersync.IngestEnv{}, err
		}
	}
	if rt.dag == nil {
		return peersync.IngestEnv{}, fmt.Errorf("kdb server: namespace %s has no commit DAG peer sync can use", ns)
	}
	return rt.PeerIngestEnv(), nil
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
	if rt.ProjectionOf != "" {
		return &ProjectionReadOnlyError{Namespace: rt.Runtime.DefaultNamespace, Source: rt.ProjectionOf}
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
	// Schema migrations carried inside replicated commits, in commit order - the storage-level
	// apply skips them (they change no document), so without this a peer's migration would
	// reach the history but never the schema.
	for _, c := range step.Commits {
		for _, op := range c.Operations {
			m, ok := op.(document.SchemaMigrationOp)
			if !ok {
				continue
			}
			mig, err := transaction.DecodeMigration(m.MigrationPayload)
			if err != nil {
				keep(fmt.Errorf("peer sync: commit %s carries an undecodable schema migration: %w", c.Hash.Hex(), err))
				continue
			}
			res := schema.ApplyMigration(rt.Schema(), mig)
			if res.IsFailure() {
				keep(fmt.Errorf("peer sync: commit %s's schema migration does not apply here: %w", c.Hash.Hex(), res.Exception()))
				continue
			}
			sch, _ := res.Value()
			rt.SetSchema(sch)
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
			if err == nil {
				keep(rt.recordUniqueDuplicates(dups, last))
			}
		}
	}
	localHost := rt.namespaceSet().Coordinator().LocalHostID()
	for _, c := range step.Commits {
		if m, ok := embed.ParseGroupMarker(c.Message); ok && m.Host != localHost {
			keep(rt.trackForeignGroupPart(m, c))
		}
	}
	if rt.CommitListener != nil {
		for _, c := range step.Commits {
			rt.CommitListener(rt.Runtime.DefaultNamespace, c)
		}
	}
	return firstErr
}

// recordUniqueDuplicates brings the conflict queue's unique-duplicate entries in line with the
// duplicates a rebuild just found: new ones are recorded, and ones no longer present - an
// operator fixed the data, or a later replicated write did - are cleared.
func (s *KdbServerRuntime) recordUniqueDuplicates(dups []transaction.UniqueConstraintError, at document.Commit) error {
	ns := s.Runtime.DefaultNamespace
	current := map[string]bool{}
	for _, d := range dups {
		id := peersync.ConflictID(peersync.ConflictUniqueDuplicate, ns, d.Key.String(), d.DocID.String())
		current[id] = true
		if _, err := s.Conflicts.Record(peersync.ConflictEntry{
			ID: id, Kind: peersync.ConflictUniqueDuplicate, Namespace: ns,
			Detail: d.Error(), IncomingHex: at.Hash.Hex(),
		}); err != nil {
			return err
		}
	}
	for _, e := range s.Conflicts.List() {
		if e.Kind == peersync.ConflictUniqueDuplicate && !current[e.ID] {
			if err := s.Conflicts.Remove(e.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

// ResolveConflict settles a queued divergence of main by merging the peer's side with the
// documents chosen as choices say - see peersync.ResolveConflict. It is an ordinary write: the
// principal needs commit rights, and the merge replicates like any other commit.
func (s *KdbServerRuntime) ResolveConflict(id string, choices map[codec.UUID]peersync.Choice, principal auth.Principal) (document.Commit, error) {
	if err := s.AuthEngine.Authorizer().Authorize(context.Background(), principal, auth.TxCommitAction{Namespace: s.Runtime.DefaultNamespace}); err != nil {
		return document.Commit{}, &AuthorizationError{Cause: err}
	}
	res, err := peersync.ResolveConflict(s.PeerIngestEnv(), id, choices)
	if err != nil {
		return document.Commit{}, err
	}
	return s.dag.GetCommitOrThrow(res.Head)
}

// DismissConflict drops a queued conflict without acting on it - for kinds nothing can merge
// away (a unique duplicate an operator fixed by hand, a foreign group part accepted as is), and
// for a divergence the operator has decided to leave unmerged. A divergence's tracking branch
// goes with it.
func (s *KdbServerRuntime) DismissConflict(id string, principal auth.Principal) error {
	if err := s.AuthEngine.Authorizer().Authorize(context.Background(), principal, auth.TxCommitAction{Namespace: s.Runtime.DefaultNamespace}); err != nil {
		return &AuthorizationError{Cause: err}
	}
	e, ok := s.Conflicts.Get(id)
	if !ok {
		return peersync.ErrConflictNotFound
	}
	if e.TrackingRef != "" {
		_ = s.dag.DeleteBranch(e.TrackingRef)
	}
	return s.Conflicts.Remove(id)
}

// trackForeignGroupPart follows a cross-namespace group decided on another host as its parts
// arrive by replication, one namespace at a time and possibly in separate syncs. While any part
// is missing - its namespace is not replicated here, or has not received the part yet - every
// part that has arrived is flagged in its namespace's conflict queue, because a reader here can
// see some of the group's effects and not the rest. When the last part arrives, the flags on all
// of them are cleared: the group is whole on this node.
//
// Parts are not held back from readers until the group is complete. That would make one
// namespace's availability depend on another's replication, which multi-leader replication is
// specifically meant not to do; the flag says what a reader may be looking at instead.
func (s *KdbServerRuntime) trackForeignGroupPart(m embed.GroupMarker, c document.Commit) error {
	s.foreignGroups.Store(m.Group, c.Hash)
	set := s.namespaceSet()
	id := func(ns string) string {
		return peersync.ConflictID(peersync.ConflictForeignGroupPart, ns, m.Group.String())
	}
	var missing, pending []string
	for _, part := range m.Parts {
		prt, ok := set.Get(part)
		if !ok {
			missing = append(missing, part)
			continue
		}
		if _, arrived := prt.foreignGroups.Load(m.Group); arrived {
			continue
		}
		// foreignGroups is memory only; a part that arrived before a restart is still flagged
		// in its namespace's (durable) conflict queue, and that flag is what says it is here.
		if prt.Conflicts != nil {
			if _, flagged := prt.Conflicts.Get(id(part)); flagged {
				continue
			}
		}
		pending = append(pending, part)
	}
	if len(missing) == 0 && len(pending) == 0 {
		for _, part := range m.Parts {
			if prt, ok := set.Get(part); ok {
				if err := prt.Conflicts.Remove(id(part)); err != nil {
					return err
				}
			}
		}
		return nil
	}
	detail := fmt.Sprintf("commit %s is one part of cross-namespace group %s (namespaces %v, decided on host %q); ",
		c.Hash.Hex(), m.Group, m.Parts, m.Host)
	if len(missing) > 0 {
		detail += fmt.Sprintf("namespaces %v are not replicated to this node, so the group can never be whole here", missing)
	} else {
		detail += fmt.Sprintf("waiting for the parts in %v to arrive", pending)
	}
	_, err := s.Conflicts.Record(peersync.ConflictEntry{
		ID: id(s.Runtime.DefaultNamespace), Kind: peersync.ConflictForeignGroupPart,
		Namespace: s.Runtime.DefaultNamespace, Detail: detail, IncomingHex: c.Hash.Hex(),
	})
	return err
}
