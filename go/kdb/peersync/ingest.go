package peersync

import (
	"fmt"
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
)

// LocalNode is what peer sync needs from the runtime it feeds. A peer is just another writer:
// anything that moves a namespace's head on a peer's behalf must be serialized against the
// runtime's own writers and must run the same post-commit hooks they do (indexes, unique keys,
// commit listeners). Without this, sync raced local commits and left every derived structure
// behind the documents it describes (docs/kdb-distributed-plan.md, defects D2/D3/D8).
type LocalNode interface {
	// Exclusive runs fn under the runtime's write serialization - for the server, its
	// writeGate. It may refuse (draining, fenced, queue full) without running fn.
	Exclusive(fn func() error) error
	// Advanced is called inside Exclusive, once the head has moved, describing what moved it.
	// An error is returned to the ingest caller but does not undo the head move: the commits are
	// already durable and reachable, and derived state is rebuildable.
	Advanced(step AdvanceStep) error
}

// AdvanceStep describes one head move performed by Ingest.
type AdvanceStep struct {
	// Commits are the commits that became reachable from main, parents first. The last one is
	// the new head.
	Commits []document.Commit
	// Applied is the net change written to live document storage for this move: one final
	// operation per document, the delta from the previous head's tree to the new one.
	Applied []document.Op
}

// IngestEnv is everything Ingest needs about the namespace it feeds.
type IngestEnv struct {
	DAG         *dag.InMemoryCommitDag
	Storage     storage.Adapter
	NamespaceID string
	// Node serializes the head decision against local writers and receives post-commit
	// notifications. nil serializes only against other ingests of the same namespace in this
	// process (the embedded / test default).
	Node LocalNode
	// Persist durably logs a commit stored from a peer, or a merge commit created here. nil
	// means peer sync has no durability of its own (memory runtimes).
	Persist func(document.Commit) error
	// ApplyToStorage writes the documents a head move introduces into Storage. false leaves
	// storage untouched on fast-forward - DAG-only callers (tests of the decision logic). The
	// auto-merge path always writes storage, since the merge tree is built there.
	ApplyToStorage bool
	Resolution     ResolutionOptions
}

// IngestResult reports what Ingest did.
type IngestResult struct {
	// Stored counts commits that were new to this node's DAG.
	Stored  int
	Outcome CommitPushOutcome
	// Head is main's head after the decision.
	Head codec.Hash
}

// Ingest stores commits received from a peer and decides what main should point at, performing
// that decision - the only function that moves a namespace's head on a peer's behalf. Both the
// host's CommitPush handler and the client's pull loop call it, so push and pull cannot drift.
//
// Storing happens first and outside the node's serialization: history is never lost, and adding
// a commit to the DAG changes nothing any reader can see. Everything that can - the head, live
// storage, indexes - happens inside it.
//
// On a fast-forward the net effect of every newly reachable commit is applied to storage in one
// step and verified against the incoming head's declared tree, rather than replaying commit by
// commit: replay can only follow first parents, and a fast-forward across a merge whose first
// parent is not on this node's side would check side-branch commits against a tree that already
// contains the other side, failing a perfectly valid history.
func Ingest(env IngestEnv, commits []document.Commit, incomingHead codec.Hash) (IngestResult, error) {
	stored, err := StoreCommits(env, commits, nil)
	if err != nil {
		return IngestResult{Stored: stored}, err
	}
	res, err := Adopt(env, incomingHead)
	res.Stored = stored
	return res, err
}

// StoreCommits adds commits (parents first) and stubs to the DAG, persisting each new commit,
// and decides nothing: no head moves, no storage changes. It is the first half of Ingest, used
// alone for every page of a multi-page transfer but the last. Returns how many commits were new;
// on error, the commits before the failing one stay stored (history is never lost) and the count
// says how many.
func StoreCommits(env IngestEnv, commits []document.Commit, stubs []document.CommitStub) (int, error) {
	for _, s := range stubs {
		env.DAG.PutStub(s)
	}
	stored := 0
	for _, c := range commits {
		if env.DAG.HasCommit(c.Hash) {
			continue
		}
		if err := env.DAG.PutCommit(c, true); err != nil {
			return stored, fmt.Errorf("peer sync: storing commit %s: %w", c.Hash.Hex(), err)
		}
		if env.Persist != nil {
			if err := env.Persist(c); err != nil {
				return stored, fmt.Errorf("peer sync: persisting commit %s: %w", c.Hash.Hex(), err)
			}
		}
		stored++
	}
	return stored, nil
}

// Adopt decides what main should point at given incomingHead, a commit already stored, and
// performs that decision under the node's serialization - the second half of Ingest.
func Adopt(env IngestEnv, incomingHead codec.Hash) (IngestResult, error) {
	var res IngestResult
	if !env.DAG.HasCommit(incomingHead) {
		head, err := env.DAG.Head()
		res.Head = head
		return res, err
	}
	node := env.Node
	if node == nil {
		node = lockOnlyNode{}
	}
	err := node.Exclusive(func() error {
		lock := divergenceLockFor(env.NamespaceID)
		lock.Lock()
		defer lock.Unlock()
		// Read inside the serialization: a head read before it is a hint at best, and planning
		// against it is exactly how a concurrent local commit used to be dropped from main.
		localHead, err := env.DAG.Head()
		if err != nil {
			return err
		}
		switch ResolveHeadUpdate(env.DAG, localHead, incomingHead) {
		case HeadAlreadyAncestor:
			res.Outcome = CommitPushOutcome{Kind: OutcomeNoOp}
			return nil
		case HeadFastForward:
			step, err := fastForward(env, localHead, incomingHead)
			if err != nil {
				return err
			}
			res.Outcome = CommitPushOutcome{Kind: OutcomeFastForwarded}
			return advance(node, step)
		default:
			outcome, step, err := resolveDivergedLocked(env.DAG, env.Storage, env.NamespaceID, localHead, incomingHead, env.Resolution)
			if err != nil {
				return err
			}
			res.Outcome = outcome
			if outcome.MergeCommit == nil {
				return nil
			}
			if env.Persist != nil {
				if err := env.Persist(*outcome.MergeCommit); err != nil {
					return err
				}
			}
			return advance(node, step)
		}
	})
	head, headErr := env.DAG.Head()
	res.Head = head
	if err != nil {
		return res, err
	}
	return res, headErr
}

func advance(node LocalNode, step AdvanceStep) error {
	if len(step.Commits) == 0 {
		return nil
	}
	return node.Advanced(step)
}

// fastForward moves main from localHead to incomingHead, a descendant of it.
func fastForward(env IngestEnv, localHead, incomingHead codec.Hash) (AdvanceStep, error) {
	commits, err := commitsBetween(env.DAG, incomingHead, localHead)
	if err != nil {
		return AdvanceStep{}, err
	}
	applied := netEffect(commits)
	if env.ApplyToStorage {
		if err := applyAndVerify(env, localHead, incomingHead, applied); err != nil {
			return AdvanceStep{}, err
		}
	}
	if err := env.DAG.SetHead(mainBranch, incomingHead); err != nil {
		return AdvanceStep{}, err
	}
	return AdvanceStep{Commits: commits, Applied: applied}, nil
}

// applyAndVerify writes ops on top of fromHead's tree and requires the result to be exactly
// toHead's declared tree. Any failure discards the staged writes, so storage never runs ahead
// of (or disagrees with) the head.
func applyAndVerify(env IngestEnv, fromHead, toHead codec.Hash, ops []document.Op) error {
	from, err := env.DAG.GetCommitOrThrow(fromHead)
	if err != nil {
		return err
	}
	to, err := env.DAG.GetCommitOrThrow(toHead)
	if err != nil {
		return err
	}
	fail := func(err error) error {
		_ = env.Storage.DiscardPending(env.NamespaceID)
		return err
	}
	// Pre-images, read before anything is staged: if the built tree turns out not to be the one
	// the peer declared, they are what puts storage back on fromHead's tree. Ops carry no
	// pre-image of their own (kdb-history-not-invertible), so this is the only place to get one.
	ids := make([]codec.UUID, len(ops))
	for i, op := range ops {
		ids[i] = opDocID(op)
	}
	before, err := env.Storage.GetDocuments(env.NamespaceID, ids, from.DocumentTreeHash)
	if err != nil {
		return err
	}
	for _, op := range ops {
		switch o := op.(type) {
		case document.WriteOp:
			if err := env.Storage.PutDocument(env.NamespaceID, document.Document{ID: o.DocID, JSON: o.Patch}); err != nil {
				return fail(err)
			}
		case document.DeleteOp:
			if err := env.Storage.DeleteDocument(env.NamespaceID, o.DocID); err != nil {
				return fail(err)
			}
		}
	}
	built, err := env.Storage.CommitTree(env.NamespaceID, from.DocumentTreeHash)
	if err != nil {
		return fail(err)
	}
	if built.TreeHash != to.DocumentTreeHash {
		mismatch := &TreeMismatchError{CommitHash: toHead, Declared: to.DocumentTreeHash, Built: built.TreeHash}
		if err := restore(env, built.TreeHash, ids, before); err != nil {
			return fmt.Errorf("%w (and restoring the previous tree failed: %v)", mismatch, err)
		}
		return mismatch
	}
	env.DAG.PutDocumentTree(built)
	return nil
}

// restore writes pre-images back on top of the tree a failed apply left behind, returning live
// storage to the tree it started from.
func restore(env IngestEnv, onTree codec.Hash, ids []codec.UUID, before []*document.Document) error {
	for i, id := range ids {
		var err error
		if before[i] == nil {
			err = env.Storage.DeleteDocument(env.NamespaceID, id)
		} else {
			err = env.Storage.PutDocument(env.NamespaceID, *before[i])
		}
		if err != nil {
			_ = env.Storage.DiscardPending(env.NamespaceID)
			return err
		}
	}
	_, err := env.Storage.CommitTree(env.NamespaceID, onTree)
	return err
}

func opDocID(op document.Op) codec.UUID {
	switch o := op.(type) {
	case document.WriteOp:
		return o.DocID
	case document.DeleteOp:
		return o.DocID
	}
	return codec.UUID{}
}

// TreeMismatchError reports that applying a peer's commits produced a different tree than the
// one the incoming head declares - the peer's history and its claimed state disagree.
type TreeMismatchError struct {
	CommitHash codec.Hash
	Declared   codec.Hash
	Built      codec.Hash
}

func (e *TreeMismatchError) Error() string {
	return fmt.Sprintf("peer sync: commit %s declares tree %s but its history builds %s",
		e.CommitHash.Hex(), e.Declared.Hex(), e.Built.Hex())
}

// commitsBetween returns every commit reachable from head that is not base and not an ancestor
// of base, parents first, with operations loaded. Unlike Walk(head, &base), which prunes only at
// base's exact hash, this excludes all of base's history - including history reachable around
// base through a merge's other parent, which Walk would revisit.
func commitsBetween(d *dag.InMemoryCommitDag, head, base codec.Hash) ([]document.Commit, error) {
	if head == base {
		return nil, nil
	}
	inRange := map[codec.Hash]document.Commit{}
	queue := []codec.Hash{head}
	for len(queue) > 0 {
		h := queue[0]
		queue = queue[1:]
		if _, seen := inRange[h]; seen {
			continue
		}
		if h == base || d.IsAncestor(h, base) {
			continue
		}
		if d.HasStub(h) || !d.HasCommit(h) {
			continue
		}
		ops, err := d.CommitOperations(h)
		if err != nil {
			return nil, err
		}
		c, err := d.GetCommitOrThrow(h)
		if err != nil {
			return nil, err
		}
		c.Operations = ops
		inRange[h] = c
		queue = append(queue, c.ParentHashes...)
	}
	return topoSort(inRange), nil
}

// topoSort orders commits parents first (Kahn's algorithm over edges inside the set), breaking
// ties by hash so the order is identical on every node for the same set.
func topoSort(set map[codec.Hash]document.Commit) []document.Commit {
	pending := make(map[codec.Hash]int, len(set))
	children := make(map[codec.Hash][]codec.Hash, len(set))
	for h, c := range set {
		n := 0
		for _, p := range c.ParentHashes {
			if _, ok := set[p]; ok {
				n++
				children[p] = append(children[p], h)
			}
		}
		pending[h] = n
	}
	var ready []codec.Hash
	for h, n := range pending {
		if n == 0 {
			ready = append(ready, h)
		}
	}
	out := make([]document.Commit, 0, len(set))
	for len(ready) > 0 {
		sort.Slice(ready, func(i, j int) bool { return ready[i].Hex() < ready[j].Hex() })
		h := ready[0]
		ready = ready[1:]
		out = append(out, set[h])
		for _, ch := range children[h] {
			pending[ch]--
			if pending[ch] == 0 {
				ready = append(ready, ch)
			}
		}
	}
	return out
}

// netEffect folds commits (parents first) into one final operation per document, sorted by id.
// Topological order is what makes "last write" well defined: a merge commit comes after both of
// its sides, so its resolution of a document both sides touched overrides either side's write.
func netEffect(commits []document.Commit) []document.Op {
	final := map[codec.UUID]document.Op{}
	for _, c := range commits {
		for _, op := range c.Operations {
			switch o := op.(type) {
			case document.WriteOp:
				final[o.DocID] = o
			case document.DeleteOp:
				final[o.DocID] = o
			}
		}
	}
	out := make([]document.Op, 0, len(final))
	for _, id := range sortedDocIDs(final) {
		out = append(out, final[id])
	}
	return out
}

// lockOnlyNode is the LocalNode used when the caller supplies none: Ingest's own per-namespace
// divergence lock is the only serialization, and there are no hooks to run.
type lockOnlyNode struct{}

func (lockOnlyNode) Exclusive(fn func() error) error { return fn() }
func (lockOnlyNode) Advanced(AdvanceStep) error      { return nil }
