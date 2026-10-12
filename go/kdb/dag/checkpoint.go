package dag

import (
	"errors"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// CheckpointCommit is one commit as a checkpoint records it: everything
// that makes the commit graph navigable, and nothing that costs bulk.
//
// Operations are deliberately absent. They are the whole text of every
// document the commit wrote, they are already durable in the delta log,
// and the DAG can read them back on demand (see SetOperationsLoader) - so
// a checkpoint that carried them would be as large as the history it
// exists to avoid re-reading. OperationCount is kept so restore can tell
// a commit whose operations were left out from one that genuinely wrote
// nothing, and only mark the former as needing a load.
type CheckpointCommit struct {
	Commit         document.Commit
	OperationCount int
}

// CheckpointState is the DAG state a checkpoint captures and restores.
type CheckpointState struct {
	Commits  []CheckpointCommit
	Branches []document.Branch
	Tags     []document.Tag
}

// CheckpointSnapshot captures the commit graph and refs as of now.
//
// Cheap relative to what it replaces: the commits it copies carry no
// operations, so this is proportional to the number of commits rather
// than to the bytes they wrote.
func (d *InMemoryCommitDag) CheckpointSnapshot() CheckpointState {
	return d.CheckpointSnapshotRetaining(nil)
}

// CheckpointSnapshotRetaining is CheckpointSnapshot with a filter: a
// commit is written out only when keep accepts it, or when a branch or tag
// names it. A nil keep writes every commit, which is what a namespace that
// reclaims nothing wants.
//
// This is the commit graph's half of a retention window. The delta log's
// half deletes segments; without this the graph kept every commit those
// segments ever held, so the checkpoint - the one artifact a
// history=none namespace must reload in full at open - went on growing
// with history even as the log stopped. Measured before this filter
// existed: 1.1 KB after one session, 4.5 KB after eight, on three
// documents.
//
// Branch heads and tags are kept whatever keep says. A ref naming a commit
// the checkpoint omitted would restore a graph whose own head is missing,
// which is not a smaller database but a broken one.
//
// Commits received from a peer and never logged are left out whatever keep
// says, and so is every ref other than main that names one (a conflict's
// tracking branch, say). A checkpoint carries no operations for them and the
// log has none to load, so restoring them would bring back commits nothing
// can read - and, because the DAG then holds them, that no sync would fetch
// again. Leaving them out reopens the namespace as a crash would have: without
// them, and with the next sync fetching them afresh. See unlogged.go.
//
// So is every commit descending from one of them: replay cannot admit a commit whose parent it
// never stored, so a log frame for such a commit is no better than none. See
// CheckpointSnapshotLogged for the case where main itself is among them.
func (d *InMemoryCommitDag) CheckpointSnapshotRetaining(keep func(document.Commit) bool) CheckpointState {
	state, _ := d.checkpointSnapshot(keep)
	return state
}

// ErrHeadNotLogged is a checkpoint refused because main names a commit the delta log does not
// hold: one received from a peer and never logged, or one built on such a commit.
var ErrHeadNotLogged = errors.New("kdb: main names a commit the delta log does not hold")

// CheckpointSnapshotLogged is CheckpointSnapshotRetaining for a checkpoint that will be written,
// refusing (ErrHeadNotLogged) when main names a commit it would have to leave out.
//
// Main moves before its new commits are logged - adopting a peer's head, or a merge made here,
// is decided and applied under the write serialization, and the log write can still fail after
// that. A checkpoint cannot drop main the way it drops other refs, and keeping it would describe
// a head whose history the log does not have. Writing nothing is what leaves the namespace
// correct: the next open uses the previous checkpoint and replays the log after it, which is
// exactly what a crash would have left.
func (d *InMemoryCommitDag) CheckpointSnapshotLogged(keep func(document.Commit) bool) (CheckpointState, error) {
	state, mainUnlogged := d.checkpointSnapshot(keep)
	if mainUnlogged {
		return CheckpointState{}, ErrHeadNotLogged
	}
	return state, nil
}

// checkpointSnapshot is CheckpointSnapshotRetaining, also reporting whether main names a commit
// it left out for want of a log frame.
func (d *InMemoryCommitDag) checkpointSnapshot(keep func(document.Commit) bool) (CheckpointState, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	excluded := d.unloggedClosureLocked()
	unlogged := func(h codec.Hash) bool {
		_, ok := excluded[h]
		return ok
	}
	mainUnlogged := false
	if b, ok := d.branches[mainBranch]; ok {
		mainUnlogged = unlogged(b.HeadHash)
	}
	pinned := make(map[codec.Hash]struct{}, len(d.branches)+len(d.tags))
	for _, b := range d.branches {
		pinned[b.HeadHash] = struct{}{}
	}
	for _, t := range d.tags {
		pinned[t.CommitHash] = struct{}{}
	}
	state := CheckpointState{
		Commits:  make([]CheckpointCommit, 0, len(d.nodes)),
		Branches: make([]document.Branch, 0, len(d.branches)),
		Tags:     make([]document.Tag, 0, len(d.tags)),
	}
	for _, n := range d.nodes {
		c := n.commit
		if unlogged(c.Hash) {
			continue
		}
		if keep != nil && !keep(c) {
			if _, isRef := pinned[c.Hash]; !isRef {
				continue
			}
		}
		count := len(c.Operations)
		if count == 0 && d.opsEvictedFor(c.Hash) {
			// Already evicted from memory, so len() understates it. The
			// exact count does not matter downstream, only whether there
			// were any - see CheckpointCommit.
			count = 1
		}
		c.Operations = nil
		state.Commits = append(state.Commits, CheckpointCommit{Commit: c, OperationCount: count})
	}
	for _, b := range d.branches {
		// main never names an unlogged commit - adopting one is what logs it - and is kept
		// whatever happens, since a checkpoint without it does not open.
		if b.Name != mainBranch && unlogged(b.HeadHash) {
			continue
		}
		state.Branches = append(state.Branches, b)
	}
	for _, t := range d.tags {
		if unlogged(t.CommitHash) {
			continue
		}
		state.Tags = append(state.Tags, t)
	}
	return state, mainUnlogged
}

// unloggedClosureLocked is every resident commit that is unlogged or descends from one, walking
// parents the way ancestry does (expandParentsLocked): a shallow root's parents and a stub's are
// not followed, so the history a deepen is still logging below a root does not take the root -
// which its snapshot made durable - out with it. nil when nothing is unlogged.
func (d *InMemoryCommitDag) unloggedClosureLocked() map[codec.Hash]struct{} {
	if len(d.unlogged) == 0 {
		return nil
	}
	const (
		unknown = iota
		visiting
		clean
		tainted
	)
	state := make(map[codec.Hash]uint8, len(d.nodes))
	out := make(map[codec.Hash]struct{}, len(d.unlogged))
	for h := range d.unlogged {
		state[h] = tainted
		out[h] = struct{}{}
	}
	// Iterative post-order: the history behind a commit is as deep as the namespace's log.
	var stack []codec.Hash
	for start := range d.nodes {
		if state[start] != unknown {
			continue
		}
		stack = append(stack[:0], start)
		for len(stack) > 0 {
			h := stack[len(stack)-1]
			if state[h] == unknown {
				state[h] = visiting
				for _, p := range d.expandParentsLocked(h) {
					if _, resident := d.nodes[p]; resident && state[p] == unknown {
						stack = append(stack, p)
					}
				}
				continue
			}
			stack = stack[:len(stack)-1]
			if state[h] != visiting {
				continue
			}
			state[h] = clean
			for _, p := range d.expandParentsLocked(h) {
				if state[p] == tainted {
					state[h] = tainted
					out[h] = struct{}{}
					break
				}
			}
		}
	}
	return out
}

// RestoreCheckpoint rebuilds the commit graph and refs from a checkpoint,
// in place of replaying them out of the delta log.
//
// Commits are stored without hash verification, which is safe only because
// of what is being skipped and why: a commit hash covers its operations,
// and a checkpoint deliberately omits them, so recomputing here would fail
// for every commit that wrote anything. The verification is not lost, only
// deferred - hydrate checks the loaded operations against the commit's
// hash at the moment they are read back, which is the point at which the
// bytes actually matter. Parents are not required to be present either,
// because a checkpoint's commits arrive in map order rather than in
// dependency order; the graph is complete by the time the loop ends.
func (d *InMemoryCommitDag) RestoreCheckpoint(state CheckpointState) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, cc := range state.Commits {
		if err := d.putCommitLocked(cc.Commit, false, false); err != nil {
			return err
		}
		if cc.OperationCount > 0 {
			// The commit is in the graph with no operations attached, so
			// anything that reads them has to fetch them. Without this it
			// would look like a commit that wrote nothing.
			if d.opsEvicted == nil {
				d.opsEvicted = make(map[codec.Hash]struct{})
			}
			d.opsEvicted[cc.Commit.Hash] = struct{}{}
		}
	}
	for _, b := range state.Branches {
		d.branches[b.Name] = b
	}
	for _, t := range state.Tags {
		d.tags[t.Name] = t
	}
	d.publishHeadLocked()
	return nil
}

// RestoreCommitOperations puts operations back onto commits a checkpoint
// restored without them, for the few commits that carry theirs inside the
// checkpoint itself.
//
// Branch heads are the case this exists for. They must have operations
// resident - the head snapshot is served lock-free by HeadCommit and
// GetCommit, neither of which can fetch anything - and loading them from
// the delta log instead would mean indexing the log at open, which is the
// entire cost a checkpoint exists to avoid paying.
//
// Each commit's hash is checked against its contents, so operations that
// do not belong to the commit they are filed under are rejected rather
// than silently installed.
func (d *InMemoryCommitDag) RestoreCommitOperations(commits []document.Commit) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, c := range commits {
		recomputed, err := document.ComputeCommitHash(c)
		if err != nil {
			return err
		}
		if recomputed != c.Hash {
			return NewConsistencyError(
				"checkpointed operations do not reproduce their commit's hash", d.NamespaceID, &c.Hash)
		}
		if !d.isResidentLocked(c.Hash) {
			continue
		}
		d.replaceCommitLocked(c)
		delete(d.opsEvicted, c.Hash)
		d.trackOpsLocked(c)
	}
	d.publishHeadLocked()
	return nil
}

// HydrateBranchHeads loads operations back onto every branch head that a
// checkpoint restored without them, and re-admits them into the graph.
//
// Needed because the head snapshot is served lock-free (see HeadCommit and
// GetCommit's fast path), and neither of those can perform I/O or report
// that it failed - so a head whose operations were missing would be handed
// out as a commit that wrote nothing, with no way for the caller to tell.
// evictOpsLocked already refuses to evict a branch head for the same
// reason; this establishes that invariant at restore rather than assuming
// it.
//
// Call after RestoreCheckpoint, with no DAG lock held. Cheap: one frame
// read per branch, not per commit.
func (d *InMemoryCommitDag) HydrateBranchHeads() error {
	d.mu.RLock()
	var heads []codec.Hash
	for _, b := range d.branches {
		if d.opsEvictedFor(b.HeadHash) {
			heads = append(heads, b.HeadHash)
		}
	}
	d.mu.RUnlock()

	for _, h := range heads {
		d.mu.RLock()
		c, ok := d.commitLocked(h)
		d.mu.RUnlock()
		if !ok {
			continue
		}
		full, err := d.hydrate(c)
		if err != nil {
			return err
		}
		d.mu.Lock()
		d.replaceCommitLocked(full)
		delete(d.opsEvicted, h)
		d.trackOpsLocked(full)
		d.publishHeadLocked()
		d.mu.Unlock()
	}
	return nil
}
