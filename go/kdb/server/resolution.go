package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/script"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// Conflict resolution chains (docs/kdb-distributed-self-healing-research.md, Phase 10.5): a
// namespace's ordered rules for settling a same-document conflict when peer sync merges. The chain
// is a replicated definition in the metadata namespace, because merges are deterministic only when
// every node decides them alike.

// SetResolutionChain records ns's chain on this runtime. Called by the metadata store as the
// definition arrives or changes; nil clears it.
func (s *KdbServerRuntime) SetResolutionChain(c *peersync.ResolutionChain) {
	if c != nil && len(c.Rules) == 0 && !c.AllowUnrelated {
		c = nil
	}
	s.resolution.Store(c)
}

// ResolutionChainOf returns this namespace's chain, or nil.
func (s *KdbServerRuntime) ResolutionChainOf() *peersync.ResolutionChain { return s.resolution.Load() }

// maxProcedureCallsPerSync bounds how much script one sync may run. A merge with a very large
// number of conflicting documents starts one sandbox per document; past this many the merge is
// held rather than left to run for minutes. Holding is safe - the other node may still merge and
// this one fast-forwards - which is why a limit that two nodes could hit at different points is
// no threat to convergence.
const maxProcedureCallsPerSync = 10_000

// peerResolution is the resolution options peer sync merges this namespace with.
func (s *KdbServerRuntime) peerResolution() peersync.ResolutionOptions {
	calls := 0
	return peersync.ResolutionOptions{
		Policy: s.PeerSyncConflictPolicy,
		Chain:  s.ResolutionChainOf(),
		Valid: func(body string) bool {
			// Every node validates against the namespace's replicated schema. Until a schema
			// change has reached both nodes they can judge differently - the chain hash does not
			// cover the schema - which is why the rule only ever picks between two present values.
			return schema.Validate(document.Document{JSON: body}, s.Schema()).IsSuccess()
		},
		ProcedureHash: s.ProcedureHash,
		Procedure: func(name, sourceHash string, c peersync.ProcedureConflict) (peersync.Settlement, bool, error) {
			calls++
			if calls > maxProcedureCallsPerSync {
				return peersync.Settlement{}, false, fmt.Errorf("more than %d conflicting documents in one sync", maxProcedureCallsPerSync)
			}
			return s.runConflictProcedure(name, sourceHash, c)
		},
	}
}

// runConflictProcedure runs a stored procedure over one conflicting document for RuleProcedure.
//
// Everything that can go wrong returns an error, which holds the merge. That includes this node
// not holding the procedure, or holding a different revision of it: peers compare resolution
// hashes before merging and so should never reach this, but a merge is not the place to discover
// that the rules are not what they were.
func (s *KdbServerRuntime) runConflictProcedure(name, sourceHash string, c peersync.ProcedureConflict) (peersync.Settlement, bool, error) {
	ns := s.Runtime.DefaultNamespace
	src, ok := s.ProcedureSource(name)
	if !ok {
		return peersync.Settlement{}, false, fmt.Errorf("this node holds no procedure %q in %s", name, ns)
	}
	if have := script.SourceHash(src); have != sourceHash {
		return peersync.Settlement{}, false, fmt.Errorf("this node holds revision %s of %q, the chain was set against %s", short(have), name, short(sourceHash))
	}
	dec, err := procRuntime.ResolveConflict(context.Background(), src, script.ModePure, nil, script.ConflictInput{
		DocID:        c.DocID.String(),
		Base:         c.Base,
		Local:        c.Side0,
		Remote:       c.Side1,
		LocalOrigin:  procedureOrigin(c.Origin0),
		RemoteOrigin: procedureOrigin(c.Origin1),
	})
	if err != nil {
		return peersync.Settlement{}, false, err
	}
	sides := [2]*string{c.Side0, c.Side1}
	origins := [2]transaction.ConflictOrigin{c.Origin0, c.Origin1}
	switch dec.Kind {
	case script.DecideDefer:
		return peersync.Settlement{}, false, nil
	case script.DecideTake:
		return peersync.Settlement{Body: sides[dec.Side]}, true, nil
	case script.DecideDoc:
		body := dec.Doc
		return peersync.Settlement{Body: &body}, true, nil
	case script.DecideDelete:
		return peersync.Settlement{}, true, nil
	case script.DecideFork:
		losing := 1 - dec.Side
		f, ok := peersync.ForkOf(c.DocID, sides[losing], origins[losing].Commit)
		if !ok {
			// Nothing to fork: that side deleted the document, or its body is not an object.
			return peersync.Settlement{}, false, fmt.Errorf("procedure asked to keep both sides of %s, but the other side cannot be forked", c.DocID)
		}
		return peersync.Settlement{Body: sides[dec.Side], Fork: f}, true, nil
	default:
		return peersync.Settlement{}, false, fmt.Errorf("procedure returned an unknown decision")
	}
}

func procedureOrigin(o transaction.ConflictOrigin) script.Origin {
	return script.Origin{NodeID: o.NodeID.String(), Commit: o.Commit.Hex(), TimestampMicros: o.TimestampMicros}
}

// short abbreviates a hash for a message an operator reads.
func short(hash string) string {
	if len(hash) > 12 {
		return hash[:12]
	}
	if hash == "" {
		return "(none)"
	}
	return hash
}

// authorizeResolve checks principal may settle this namespace's conflicts: "resolve" when its
// chain names a resolver authority, commit rights otherwise.
func (s *KdbServerRuntime) authorizeResolve(principal auth.Principal) error {
	ns := s.Runtime.DefaultNamespace
	var action auth.Action = auth.TxCommitAction{Namespace: ns}
	if s.ResolutionChainOf().Authority() != nil {
		action = auth.ConflictResolveAction{Namespace: ns}
	}
	if err := s.AuthEngine.Authorizer().Authorize(context.Background(), principal, action); err != nil {
		return &AuthorizationError{Cause: err}
	}
	return nil
}

// ErrResolutionStale refuses to settle a provisional decision over a document that has changed
// since the merge made it: the choice was made against a value that is no longer there.
type ErrResolutionStale struct {
	Documents []string
}

func (e *ErrResolutionStale) Error() string {
	return "conflict resolution: document(s) changed since the provisional merge, decide again against their current values: " + strings.Join(e.Documents, ", ")
}

// resolveProvisional settles a provisional decision: each document takes "local" (the value the
// merge kept), "remote" (the value it displaced) or an explicit body, in one ordinary commit as
// principal whose message names the entry - so every node that adopts it closes its own copy.
// Every document needs a choice, and each must still hold the value the merge kept.
func (s *KdbServerRuntime) resolveProvisional(e peersync.ConflictEntry, choices map[codec.UUID]peersync.Choice, principal auth.Principal) (document.Commit, error) {
	var ops []document.Op
	var undecided, stale []string
	for _, it := range e.Items {
		id, err := codec.ParseUUID(it.DocumentID)
		if err != nil {
			return document.Commit{}, err
		}
		c, ok := choices[id]
		var body *string
		var fork *peersync.ForkDoc
		switch {
		case !ok:
			undecided = append(undecided, it.DocumentID)
			continue
		case c.Body != nil:
			body = c.Body
		case c.Delete:
			body = nil
		case c.Fork != nil:
			keep, losing := it.LocalDoc, it.IncomingDoc
			if c.Fork.Keep == "remote" {
				keep, losing = it.IncomingDoc, it.LocalDoc
			} else if c.Fork.Keep != "local" {
				undecided = append(undecided, it.DocumentID)
				continue
			}
			f, ok := peersync.ForkOf(id, losing, codec.Hash{})
			if !ok {
				// Nothing to fork: the losing side deleted the document, or is not an object.
				undecided = append(undecided, it.DocumentID)
				continue
			}
			body, fork = keep, f
		case c.Take == "local":
			body = it.LocalDoc
		case c.Take == "remote":
			body = it.IncomingDoc
		default:
			undecided = append(undecided, it.DocumentID)
			continue
		}
		cur, _, found, err := s.GetDocument(s.Runtime.DefaultNamespace, id)
		if err != nil {
			return document.Commit{}, err
		}
		if found != (it.LocalDoc != nil) || (found && cur != *it.LocalDoc) {
			stale = append(stale, it.DocumentID)
			continue
		}
		if body == nil {
			ops = append(ops, document.DeleteOp{DocID: id})
		} else {
			ops = append(ops, document.DeleteOp{DocID: id}, document.WriteOp{DocID: id, Patch: *body})
		}
		if fork != nil {
			ops = append(ops, document.WriteOp{DocID: fork.ID, Patch: fork.Body})
		}
	}
	if len(undecided) > 0 {
		return document.Commit{}, fmt.Errorf("peer sync: conflict %s still has documents without a choice: %s", e.ID, strings.Join(undecided, ", "))
	}
	if len(stale) > 0 {
		return document.Commit{}, &ErrResolutionStale{Documents: stale}
	}
	head, err := s.Runtime.DAG.Head()
	if err != nil {
		return document.Commit{}, err
	}
	txID, err := codec.RandomUUID()
	if err != nil {
		return document.Commit{}, err
	}
	defer s.pinBaseTree(head)()
	tx := s.authored(document.Transaction{ID: txID, BaseVersion: head, Timestamp: codec.TimestampNow(), Operations: ops})
	c, err := s.runTransaction(tx, principal, txOptions{}, func() (transactionResult, error) {
		return s.UpsertEngine.Commit(tx, s.dag, s.Runtime.Storage, s.Schema(), nil, peersync.ResolveMessage(e.ID))
	})
	if err != nil {
		return document.Commit{}, err
	}
	return c, s.Conflicts.Remove(e.ID)
}

// ConflictFilter selects queued conflicts for a bulk action. Zero fields match everything.
type ConflictFilter struct {
	Kind peersync.ConflictKind `json:"kind,omitempty"`
	// Peer matches a divergence recorded against that peer node.
	Peer string `json:"peer,omitempty"`
	// Origin matches a conflict in which a document's incoming side was written by that node.
	Origin string `json:"origin,omitempty"`
}

func (f ConflictFilter) matches(e peersync.ConflictEntry) bool {
	if f.Kind != "" && e.Kind != f.Kind {
		return false
	}
	if f.Peer != "" && e.Peer != f.Peer {
		return false
	}
	if f.Origin != "" {
		for _, d := range e.Details {
			if d.IncomingOrigin.NodeID.String() == f.Origin {
				return true
			}
		}
		return false
	}
	return true
}

// BulkResolution is what a bulk action did, or would do, to one entry.
type BulkResolution struct {
	ID        string   `json:"id"`
	Kind      string   `json:"kind"`
	Documents []string `json:"documents"`
	CommitHex string   `json:"commitHex,omitempty"`
	Error     string   `json:"error,omitempty"`
}

// ResolveAll settles every queued conflict filter matches by taking one side ("local" or
// "remote") for all its documents - "the AWS node is right about everything it sent". With dryRun
// it only lists what it would settle. Entries with no documents to choose between (a unique
// duplicate, a foreign group part) are skipped; an entry that fails is reported and the rest go on.
func (s *KdbServerRuntime) ResolveAll(filter ConflictFilter, take string, dryRun bool, principal auth.Principal) ([]BulkResolution, error) {
	if take != "local" && take != "remote" {
		return nil, fmt.Errorf("take must be \"local\" or \"remote\", not %q", take)
	}
	if err := s.authorizeResolve(principal); err != nil {
		return nil, err
	}
	out := []BulkResolution{}
	for _, e := range s.Conflicts.List() {
		if len(e.Items) == 0 || !filter.matches(e) {
			continue
		}
		r := BulkResolution{ID: e.ID, Kind: string(e.Kind)}
		choices := map[codec.UUID]peersync.Choice{}
		for _, it := range e.Items {
			r.Documents = append(r.Documents, it.DocumentID)
			if id, err := codec.ParseUUID(it.DocumentID); err == nil {
				choices[id] = peersync.Choice{Take: take}
			}
		}
		if !dryRun {
			c, err := s.ResolveConflict(e.ID, choices, principal)
			if err != nil {
				r.Error = err.Error()
			} else {
				r.CommitHex = c.Hash.Hex()
			}
		}
		out = append(out, r)
	}
	return out, nil
}

// AckConflict records that a resolver authority polling the queue has received entry id as it
// is now, so it is not delivered again unless its report changes.
func (s *KdbServerRuntime) AckConflict(id string, principal auth.Principal) error {
	if err := s.authorizeResolve(principal); err != nil {
		return err
	}
	e, ok := s.Conflicts.Get(id)
	if !ok {
		return peersync.ErrConflictNotFound
	}
	return s.Conflicts.MarkDelivered(e)
}

// ExpireAuthorityConflicts makes the fallback final for every conflict that has waited for the
// namespace's resolver authority longer than its rule's timeout: a held divergence of main is
// merged by last write - every node computes the same merge, so nodes expiring independently
// converge - and a provisional decision simply stands, its entry dropped. It acts as the runtime,
// not as any principal: the timeout is the namespace's own policy. It returns how many it settled.
func (s *KdbServerRuntime) ExpireAuthorityConflicts(now time.Time) (int, error) {
	rule := s.ResolutionChainOf().Authority()
	timeout := rule.TimeoutDuration()
	if timeout == 0 {
		return 0, nil
	}
	settled := 0
	for _, e := range s.Conflicts.List() {
		if !e.Authority || now.Sub(e.FirstSeen) < timeout {
			continue
		}
		switch e.Kind {
		case peersync.ConflictProvisional:
			if err := s.Conflicts.Remove(e.ID); err != nil {
				return settled, err
			}
		case peersync.ConflictDivergence:
			choices := map[codec.UUID]peersync.Choice{}
			for _, d := range e.Details {
				id, err := codec.ParseUUID(d.DocumentID)
				if err != nil {
					return settled, err
				}
				take := "remote"
				if laterOrigin(d.LocalOrigin, d.IncomingOrigin) {
					take = "local"
				}
				choices[id] = peersync.Choice{Take: take}
			}
			if _, err := peersync.ResolveConflict(s.PeerIngestEnv(), e.ID, choices); err != nil {
				return settled, fmt.Errorf("conflict %s: settling by last write after %s: %w", e.ID, timeout, err)
			}
		default:
			continue
		}
		settled++
	}
	return settled, nil
}

// laterOrigin mirrors peersync's last-write order: the later commit timestamp, then the higher
// commit hash - so every node picks the same side.
func laterOrigin(a, b transaction.ConflictOrigin) bool {
	if a.TimestampMicros != b.TimestampMicros {
		return a.TimestampMicros > b.TimestampMicros
	}
	return a.Commit.Hex() > b.Commit.Hex()
}

// StartAuthorityExpiry runs ExpireAuthorityConflicts over set's namespaces every interval until
// stop is closed.
func StartAuthorityExpiry(set *NamespaceSet, interval time.Duration, stop <-chan struct{}) {
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case now := <-t.C:
				for _, rt := range set.Runtimes() {
					_, _ = rt.ExpireAuthorityConflicts(now)
				}
			}
		}
	}()
}
