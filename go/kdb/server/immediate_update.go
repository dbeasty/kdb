package server

import (
	"context"
	"fmt"
	"sync"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/storage"
)

// An immediate UPDATE (sql.UpdateStatement.Immediate: one with ORDER BY, LIMIT or RETURNING) is a
// work-queue claim - MongoDB's findOneAndUpdate:
//
//	UPDATE docs SET status = 'processing', attempts = attempts + 1
//	 WHERE _c = 'jobs' AND status = 'pending' AND attempts < 3
//	 ORDER BY createdAt LIMIT 1 RETURNING kdb_id, _doc
//
// It is not buffered on the session. It selects its documents, builds its writes and commits them
// in one step at the front of the write gate, against the live head, so two concurrent claims can
// never take the same document and a claim never fails with a version conflict because another
// committed first: there is no base version older than the head to conflict with.
//
// What runs under the gate is kept small. Before the gate, the statement's best k candidates are
// found at the current head (sql.ImmediateUpdater.UpdateCandidates, top-k or index-driven). At
// the front of the gate the documents that changed since then are found by diffing the two
// document trees, and the update is decided among the first LIMIT unchanged candidates plus every
// changed document, all re-read and re-checked at the live head. That is exact: an unchanged
// document that was not a candidate ranked behind every candidate before and still does, so it
// cannot displace the LIMIT unchanged candidates kept. When too few candidates survive, when the
// trees cannot be diffed cheaply, or when there is no LIMIT, the statement is evaluated in full at
// the live head instead - correct, only slower.

// maxImmediateDiff bounds how many changed documents the under-gate step will re-check before
// falling back to evaluating the statement in full.
const maxImmediateDiff = 4096

// maxImmediateCandidates bounds the candidates found before the gate.
const maxImmediateCandidates = 1024

// maxCandidateRetries is how many times the candidate search is run again at a newer head when an
// index walk could not serve the one it read (sql.ExecStats.IndexBehind).
const maxCandidateRetries = 2

// addExecStats adds one statement part's counters into the statement's. Nil-safe.
func addExecStats(into, from *sql.ExecStats) {
	if into == nil || from == nil {
		return
	}
	into.RowsExamined += from.RowsExamined
	into.DocsRead += from.DocsRead
	into.DocBytesRead += from.DocBytesRead
	if from.RetainedBytes > into.RetainedBytes {
		into.RetainedBytes = from.RetainedBytes
	}
}

// ImmediateUpdateResult is what an immediate UPDATE did.
type ImmediateUpdateResult struct {
	// DML holds the RETURNING columns and rows and the number of documents updated.
	DML sql.DMLResult
	// Commit is the commit made; Committed is false when nothing matched and nothing was written.
	Commit    document.Commit
	Committed bool
	// Head is the commit the statement was decided at: Commit's hash when it committed, else the
	// live head it read.
	Head codec.Hash
	// Narrowed reports that the update was decided among candidates rather than by evaluating
	// the statement in full under the gate. For tests and diagnostics.
	Narrowed bool
}

// ExecImmediateUpdate runs an immediate UPDATE on this runtime's namespace as one auto-committed
// statement (see the comment above). qctx carries the namespace, schema, parameters and bounds;
// its AtCommit is ignored. check, when non-nil, runs on the result before anything is committed -
// a reply that would be too large is refused there rather than after the write landed.
func (s *KdbServerRuntime) ExecImmediateUpdate(upd sql.UpdateStatement, qctx sql.QueryContext, principal auth.Principal, sessionID string, check func(sql.DMLResult) error) (ImmediateUpdateResult, error) {
	iu, ok := s.SQLEngine().(sql.ImmediateUpdater)
	if !ok {
		return ImmediateUpdateResult{}, sql.NewPlanningError("UPDATE with ORDER BY, LIMIT or RETURNING is not supported by this engine", "")
	}
	at := func(h codec.Hash) sql.QueryContext {
		c := qctx
		c.AtCommit = &h
		return c
	}
	// Before the gate: the best candidates at the head as it is now.
	var (
		readHead   codec.Hash
		candidates []codec.UUID
		complete   bool
		narrow     = upd.Limit != nil && *upd.Limit > 0
	)
	if narrow {
		// Any recent head is a correct place to find candidates - the gate re-checks whatever
		// changed after it - so when an index walk could not serve the head read, because claims
		// committed in between and the index no longer serves that commit exactly, the search is
		// run again at the newer head rather than falling back to reading every candidate: 8-14%
		// of qtask's claims did, under contention, and each such fallback read every pending job.
		for attempt := 0; ; attempt++ {
			h, err := s.Runtime.DAG.Head()
			if err != nil {
				return ImmediateUpdateResult{}, err
			}
			unpin := s.pinBaseTree(h)
			c := at(h)
			stats := &sql.ExecStats{}
			c.Stats = stats
			cands, comp, err := iu.UpdateCandidates(upd, c, s.immediateCandidateCount(*upd.Limit))
			addExecStats(qctx.Stats, stats)
			if err != nil {
				unpin()
				return ImmediateUpdateResult{}, err
			}
			if stats.IndexBehind && attempt < maxCandidateRetries {
				if latest, herr := s.Runtime.DAG.Head(); herr == nil && latest != h {
					unpin()
					continue
				}
			}
			readHead, candidates, complete = h, cands, comp
			defer unpin()
			break
		}
	}
	var out ImmediateUpdateResult
	commit, committed, err := s.CommitImmediate(principal, sessionID, func(live codec.Hash) ([]document.Op, error) {
		out.Head = live
		var among []codec.UUID
		if narrow {
			among = s.immediateAmong(readHead, live, candidates, complete, *upd.Limit)
		}
		out.Narrowed = among != nil
		res, err := iu.ExecuteUpdateAmong(upd, at(live), among)
		if err != nil {
			return nil, err
		}
		if check != nil {
			if err := check(res); err != nil {
				return nil, err
			}
		}
		out.DML = res
		return res.Operations, nil
	})
	if err != nil {
		return ImmediateUpdateResult{}, err
	}
	out.Commit, out.Committed = commit, committed
	if committed {
		out.Head = commit.Hash
	}
	return out, nil
}

// immediateCandidateCount is how many candidates to find before the gate: enough for this claim
// and for every claim queued ahead of it in the gate to take its LIMIT, with some slack.
func (s *KdbServerRuntime) immediateCandidateCount(limit int) int {
	k := limit*(s.WriteQueueDepth()+2) + 8
	if k > limit+maxImmediateCandidates {
		k = limit + maxImmediateCandidates
	}
	return k
}

// immediateAmong is the set an immediate UPDATE is decided among at live, given the candidates
// found at readHead (see the comment at the top of this file), or nil to evaluate in full.
// Called under the write gate.
func (s *KdbServerRuntime) immediateAmong(readHead, live codec.Hash, candidates []codec.UUID, complete bool, limit int) []codec.UUID {
	var changed []codec.UUID
	if live != readHead {
		var ok bool
		changed, ok = s.changedSince(readHead, live, maxImmediateDiff)
		if !ok {
			return nil
		}
	}
	isChanged := make(map[codec.UUID]struct{}, len(changed))
	for _, id := range changed {
		isChanged[id] = struct{}{}
	}
	among := make([]codec.UUID, 0, limit+len(changed))
	kept := 0
	for _, id := range candidates {
		if kept == limit {
			break
		}
		if _, ch := isChanged[id]; ch {
			continue
		}
		among = append(among, id)
		kept++
	}
	if kept < limit && !complete {
		// The documents that were next in line were not looked at: evaluate in full.
		return nil
	}
	return append(among, changed...)
}

// changedSince lists the documents whose content differs between the trees of commits from and
// to, and false when that cannot be told cheaply: a tree not at hand, or more than max changes.
func (s *KdbServerRuntime) changedSince(from, to codec.Hash, max int) ([]codec.UUID, bool) {
	if s.dag == nil {
		return nil, false
	}
	fromTree, ok := s.treeOfCommit(from)
	if !ok {
		return nil, false
	}
	toTree, ok := s.treeOfCommit(to)
	if !ok {
		return nil, false
	}
	diff, err := document.DiffTrees(document.LocalNodes(fromTree, 32), document.LocalNodes(toTree, 32))
	if err != nil || len(diff) > max {
		return nil, false
	}
	ids := make([]codec.UUID, len(diff))
	for i, d := range diff {
		ids[i] = d.DocID
	}
	return ids, true
}

// treeOfCommit resolves a commit's document tree: the DAG's resident trees first, then the
// storage engine's resolver.
func (s *KdbServerRuntime) treeOfCommit(h codec.Hash) (document.DocumentTree, bool) {
	c, ok := s.dag.GetCommit(h)
	if !ok {
		return document.DocumentTree{}, false
	}
	if t, ok := s.dag.GetDocumentTree(c.DocumentTreeHash); ok {
		return t, true
	}
	if r, ok := s.Runtime.Storage.(storage.TreeResolver); ok {
		if t, ok, err := r.TreeAt(c.DocumentTreeHash); err == nil && ok {
			return t, true
		}
	}
	return document.DocumentTree{}, false
}

// immediateWriteEstimate is the memory reserved for an immediate write before its size is known.
const immediateWriteEstimate = 64 << 10

// CommitImmediate commits one transaction whose operations are decided at the front of the
// write gate: build is called there, with the live head, and nothing else can move the head
// until the commit lands, so whatever build read is still true when it commits. build returning
// no operations commits nothing (committed=false). The commit is made by TransactionEngine on the
// live head, so it cannot conflict with an earlier commit.
func (s *KdbServerRuntime) CommitImmediate(principal auth.Principal, sessionID string, build func(head codec.Hash) ([]document.Op, error)) (commit document.Commit, committed bool, err error) {
	// The cheap refusals first, as runTransaction makes them; authorization of the operations
	// themselves waits until they are known.
	if err := s.admitWrite(document.Transaction{}, principal, false); err != nil {
		return document.Commit{}, false, err
	}
	if s.dag == nil {
		return document.Commit{}, false, fmt.Errorf("kdb server: commit requires an InMemoryCommitDag (or a wrapper exposing one), got %T", s.Runtime.DAG)
	}
	_, err = s.clientWrite(document.Transaction{}, func() (document.Commit, error) {
		timeout := s.WriteTimeout
		if timeout <= 0 {
			timeout = DefaultWriteTimeout
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		grant, err := s.admission.Acquire(ctx, ClassWrite, immediateWriteEstimate)
		if err != nil {
			return document.Commit{}, err
		}
		defer grant.Release()
		release, err := s.writeGate.acquire(ctx)
		if err != nil {
			return document.Commit{}, err
		}
		var once sync.Once
		releaseOnce := func() { once.Do(release) }
		defer releaseOnce()

		head, err := s.Runtime.DAG.Head()
		if err != nil {
			return document.Commit{}, err
		}
		ops, err := build(head)
		if err != nil {
			return document.Commit{}, err
		}
		if len(ops) == 0 {
			return document.Commit{}, nil
		}
		txID, err := codec.RandomUUID()
		if err != nil {
			return document.Commit{}, err
		}
		tx := s.authored(document.Transaction{ID: txID, BaseVersion: head, Operations: ops, Timestamp: codec.TimestampNow()})
		if err := s.admitWrite(tx, principal, false); err != nil {
			return document.Commit{}, err
		}
		if s.DocumentLocks != nil {
			if err := s.DocumentLocks.AssertUnheldByOthers(s.Runtime.DefaultNamespace, sessionID, tx); err != nil {
				return document.Commit{}, err
			}
		}
		c, err := s.commitAtGateFront(tx, func() (transactionResult, error) {
			return s.TransactionEngine.Commit(tx, s.dag, s.Runtime.Storage, s.Schema(), nil, s.clientMessage(tx))
		}, releaseOnce)
		if err != nil {
			return document.Commit{}, err
		}
		commit, committed = c, true
		return c, nil
	})
	if err != nil {
		return document.Commit{}, false, err
	}
	return commit, committed, nil
}
