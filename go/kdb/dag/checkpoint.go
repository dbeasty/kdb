package dag

import (
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
	d.mu.RLock()
	defer d.mu.RUnlock()
	state := CheckpointState{
		Commits:  make([]CheckpointCommit, 0, len(d.commits)),
		Branches: make([]document.Branch, 0, len(d.branches)),
		Tags:     make([]document.Tag, 0, len(d.tags)),
	}
	for _, c := range d.commits {
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
		state.Branches = append(state.Branches, b)
	}
	for _, t := range d.tags {
		state.Tags = append(state.Tags, t)
	}
	return state
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
		if _, ok := d.commits[c.Hash]; !ok {
			continue
		}
		d.commits[c.Hash] = c
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
		c, ok := d.commits[h]
		d.mu.RUnlock()
		if !ok {
			continue
		}
		full, err := d.hydrate(c)
		if err != nil {
			return err
		}
		d.mu.Lock()
		d.commits[h] = full
		delete(d.opsEvicted, h)
		d.trackOpsLocked(full)
		d.publishHeadLocked()
		d.mu.Unlock()
	}
	return nil
}
