package dag

import (
	"errors"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// Commits received from a peer and not (yet) written to this namespace's delta log.
//
// Peer sync stores what a peer sends before deciding whether main adopts it, and logs a commit
// only once main has (peersync.IngestEnv.Persist explains why: replay applies every logged
// commit onto main). A commit can stay stored-but-unadopted for as long as the process lives - a
// conflicting side, a page of a transfer still in flight, a push that arrived as the node
// started draining - and for all that time the DAG is the only place it exists here.
//
// Two things in the DAG assume otherwise and have to be told:
//
//   - A checkpoint describes the commit graph without operations, on the understanding that the
//     log can supply them (CheckpointCommit). Checkpointing an unlogged commit restores it on
//     the next open with no operations and nowhere to read them from, and every later sync that
//     reaches it fails - while its presence tells peer sync it needs nothing more from the peers
//     that could supply it. CheckpointSnapshotRetaining leaves these commits out: on the next
//     open the namespace simply does not have them, exactly as after a crash, and the next sync
//     fetches them again.
//   - The operations budget evicts on the understanding that the log can load them back
//     (opsPinnedLocked). An unlogged commit's operations are pinned until it is logged.
//
// Peer sync marks what it stores (StoreCommits, and Deepen for the history below a shallow root),
// and puts the mark back on a commit whose log write failed - including one it made itself, a
// merge or a join, whose adoption the failure undid. Whatever logs them (peersync's persistAll,
// the one place adopted commits are logged) clears the mark with MarkLogged before queueing the
// frame.
//
// A commit built on an unlogged one is no better off than it is, whatever the log holds: replay
// cannot admit a commit whose parent it never stored. Checkpoints treat both alike, and refuse to
// be written at all while main names either (CheckpointSnapshotLogged).

// ErrOperationsNotLogged is matched (errors.Is) by an operations loader's error for a commit the
// delta log holds no frame for - as opposed to one it could not read. See RefillUnloggedCommit.
var ErrOperationsNotLogged = errors.New("kdb: commit has no frame in the delta log")

// PutUnloggedCommit is PutCommit(c, true) for a commit received from a peer that is not going to
// be logged until main adopts it. A commit already resident is left as it is, mark and all.
func (d *InMemoryCommitDag) PutUnloggedCommit(c document.Commit) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.isResidentLocked(c.Hash) {
		return nil
	}
	// Marked before it is stored, so the operations budget leaves it out (trackOpsLocked).
	d.markUnloggedLocked(c.Hash)
	if err := d.putCommitLocked(c, true, true); err != nil {
		delete(d.unlogged, c.Hash)
		return err
	}
	return nil
}

// PutUnloggedShallowCommit is PutShallowCommit for a commit, received from a peer, whose parents
// this namespace does not have and that is not logged yet - a deepen's new horizon. A commit
// already resident is left as it is, mark and all.
func (d *InMemoryCommitDag) PutUnloggedShallowCommit(c document.Commit) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.isResidentLocked(c.Hash) {
		return nil
	}
	d.markUnloggedLocked(c.Hash)
	if err := d.putCommitLocked(c, false, true); err != nil {
		delete(d.unlogged, c.Hash)
		return err
	}
	d.shallow[c.Hash] = struct{}{}
	d.ancestryVersion++
	d.ancestryEpoch++
	return nil
}

func (d *InMemoryCommitDag) markUnloggedLocked(h codec.Hash) {
	if d.unlogged == nil {
		d.unlogged = make(map[codec.Hash]struct{})
	}
	d.unlogged[h] = struct{}{}
}

// MarkUnlogged puts a commit's mark back - for a caller whose attempt to log it failed after
// MarkLogged - or marks one this node made (a merge, a join) that failed to reach the log. A
// commit that is not resident is ignored.
func (d *InMemoryCommitDag) MarkUnlogged(h codec.Hash) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.isResidentLocked(h) {
		d.markUnloggedLocked(h)
	}
}

// MarkLogged records that h is being written to the delta log. Call it before queueing the
// frame, not after: a checkpoint that claims the segment the frame lands in must describe the
// commit, or the next open skips the frame and loses it.
func (d *InMemoryCommitDag) MarkLogged(h codec.Hash) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.unlogged[h]; !ok {
		return
	}
	delete(d.unlogged, h)
	// Its operations can be loaded back from the log from now on: they join the budget.
	if c, ok := d.commitLocked(h); ok && !d.opsEvictedFor(h) {
		d.trackOpsLocked(c)
	}
}

// IsUnlogged reports whether h was received from a peer and has not been logged.
func (d *InMemoryCommitDag) IsUnlogged(h codec.Hash) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	_, ok := d.unlogged[h]
	return ok
}

// OperationsEvicted reports whether h is resident with its operations dropped, so reading them
// means going to the loader.
func (d *InMemoryCommitDag) OperationsEvicted(h codec.Hash) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.isResidentLocked(h) && d.opsEvictedFor(h)
}

// RefillUnloggedCommit puts back the operations of a resident commit whose operations the delta
// log cannot supply, from a full copy of it - a peer sending it again - and marks it unlogged,
// which is what it is.
//
// The repair for a namespace an earlier version left holding such a commit: its checkpoint
// described a commit received from a peer and never logged, and every sync that reached it
// failed for want of its operations. The copy is accepted only if it reproduces the commit's
// hash, which covers the operations, so it cannot install anything but the commit's own.
func (d *InMemoryCommitDag) RefillUnloggedCommit(c document.Commit) error {
	recomputed, err := document.ComputeCommitHash(c)
	if err != nil {
		return err
	}
	if recomputed != c.Hash {
		return NewConsistencyError("a peer's copy of a commit does not reproduce its hash", d.NamespaceID, &c.Hash)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.isResidentLocked(c.Hash) || !d.opsEvictedFor(c.Hash) {
		return nil
	}
	d.markUnloggedLocked(c.Hash) // before tracking, so the budget cannot evict them straight back
	d.replaceCommitLocked(c)
	delete(d.opsEvicted, c.Hash)
	d.trackOpsLocked(c)
	d.publishHeadLocked()
	return nil
}
