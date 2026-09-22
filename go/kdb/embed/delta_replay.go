package embed

import (
	"errors"
	"fmt"
	"log"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/delta"
)

// replayDeltaNamespace rebuilds d and store from every commit durably
// logged for this namespace. Two properties make this safe to run on
// every single start, including after an unclean shutdown or a
// kill -9 - see kdb-spec-layer13 Component 47:
//
//  1. Segments are read in sequence (commit) order (DeltaSegmentReader.
//     ListSegments' own contract - see DefaultReader.ListSegments), but
//     commits are *applied* in dependency order, not file order (see
//     applyCommitsTopologically below). File order is a fast path, not a
//     correctness requirement, so a bug in segment ordering elsewhere
//     degrades to a slower replay, not a permanently unopenable
//     namespace - which is exactly the failure this replaces (§2.1).
//  2. Corruption in the most recently written segment is tolerated as an
//     expected torn tail; corruption anywhere else is not (§4.3).
//
// Commits are applied as they are read rather than collected first. The
// collect-then-apply version held every commit in the namespace's history
// in one slice, so opening a store cost memory proportional to the sum of
// every version of every document it had ever held, not to the data that
// was actually live: one 1.4MB document rewritten 463 times opened with
// 382MB resident (docs/benchmarks/open-cost.md). Only commits that arrive
// before their parents are held back now, and in the ordinary case -
// segments in commit order, which Component 47 §4.1 guarantees - that set
// is empty.
func replayDeltaNamespace(d *dag.InMemoryCommitDag, store storage.Adapter, r storage.DeltaSegmentReader) error {
	return replayDeltaNamespaceFrom(d, store, r, -1, nil)
}

// replayDeltaNamespaceFrom is replayDeltaNamespace restricted to segments
// newer than afterSequence, which is how a checkpointed namespace opens:
// the checkpoint already accounts for everything up to and including that
// sequence, so only the tail has to be read. Pass -1 to replay everything.
//
// Segment granularity is the right unit here, and it is sound because
// delta.Factory.OpenWriter always starts a fresh segment rather than
// resuming the previous run's last one - so a segment that existed when a
// checkpoint was taken is never appended to afterwards, and "this sequence
// is fully accounted for" cannot go stale.
//
// coord decides the parts of cross-namespace groups the log contains: a part whose group never
// committed is dropped, along with everything descending from it (see groupGate). nil keeps every
// part, which is right only for namespaces no group can have touched.
func replayDeltaNamespaceFrom(
	d *dag.InMemoryCommitDag,
	store storage.Adapter,
	r storage.DeltaSegmentReader,
	afterSequence int64,
	coord *TxnCoordinator,
) error {
	if r == nil {
		return nil
	}
	all, err := r.ListSegments()
	if err != nil {
		return err
	}
	segments := all
	if afterSequence >= 0 {
		segments = make([]storage.DeltaSegmentRef, 0, len(all))
		for _, seg := range all {
			if seg.SequenceNumber > afterSequence {
				segments = append(segments, seg)
			}
		}
	}

	gate := newGroupGate(d, coord)
	defer gate.report()
	// deferred holds only commits whose parents had not been applied when
	// they were read - see applyCommitsTopologically, which drains it.
	var deferred []document.Commit
	for i, seg := range segments {
		read := 0
		readErr := streamSegmentCommits(r, seg, func(c document.Commit) error {
			read++
			if gate.skip(c) {
				return nil
			}
			applied, err := applyIfParentsReady(d, store, c)
			if err != nil {
				return &replayApplyError{err: err}
			}
			if !applied {
				deferred = append(deferred, c)
			}
			return nil
		})
		var corrupt *delta.CorruptFrameError
		var applyFailed *replayApplyError
		if errors.As(readErr, &applyFailed) {
			// Applying a commit failed - a storage or DAG fault, nothing to
			// do with the bytes on disk. Surfaced as itself rather than
			// being run through the torn-tail classification below, which
			// would report a healthy segment as corrupt and send the
			// operator to repair-segments for a problem it cannot fix.
			return applyFailed.err
		}
		if readErr != nil {
			isMostRecent := i == len(segments)-1
			if errors.As(readErr, &corrupt) && isMostRecent {
				// Torn tail on the most recently written segment: the
				// expected shape of an unclean shutdown (the last frame's
				// write never completed, or completed but wasn't fsynced
				// before the process died). Keep every commit applied
				// before it, log it, and continue - this segment is never
				// appended to again (OpenWriter always starts a fresh one),
				// so this decision is stable across future restarts too.
				log.Printf(
					"kdb: namespace %s: delta segment (sequence %d) has a torn tail at byte offset %d (%s) - "+
						"treating as an incomplete write from an unclean shutdown and continuing with the %d "+
						"commit(s) read cleanly before it",
					d.NamespaceID, seg.SequenceNumber, corrupt.Offset, corrupt.Reason, read)
			} else {
				return fmt.Errorf(
					"kdb: namespace %s: delta segment (sequence %d) is corrupt and is not the most recently "+
						"written segment, so this is not an expected torn tail - data may be unrecoverable; "+
						"run kdb-inspect repair-segments before retrying: %w",
					d.NamespaceID, seg.SequenceNumber, readErr)
			}
		}
	}

	return applyCommitsTopologically(d, store, deferred, gate)
}

// groupGate is replay's view of cross-namespace groups: which commits to leave out, and why.
//
// A part whose group did not commit is dropped, and so is every commit descending from a dropped
// commit. That is safe, and not just convenient, because nothing descending from an undecided part
// was ever acknowledged: a commit queued behind a part waits for the part's group to be decided
// before its own acknowledgement (see chainOnBarrier). Dropping by ancestry rather than by position
// in the log is what keeps the answer the same on every later open - a commit made after this
// recovery descends from the head recovery chose, not from the dropped part, however far along
// the log it lands.
//
// A held part - undecided, with a live writer that may yet decide it, which only a read-only
// follower sees - is left out with its descendants in the same way, but without prejudice: the
// next replay asks again.
type groupGate struct {
	d       *dag.InMemoryCommitDag
	coord   *TxnCoordinator
	dropped map[codec.Hash]struct{}
	held    map[codec.Hash]struct{}
	// droppedGroups counts distinct groups rolled back, for the one log line replay emits.
	droppedGroups map[codec.UUID]struct{}
}

func newGroupGate(d *dag.InMemoryCommitDag, coord *TxnCoordinator) *groupGate {
	if coord == nil {
		return nil
	}
	return &groupGate{
		d:             d,
		coord:         coord,
		dropped:       make(map[codec.Hash]struct{}),
		held:          make(map[codec.Hash]struct{}),
		droppedGroups: make(map[codec.UUID]struct{}),
	}
}

// skip reports whether c must be left out of this replay.
func (g *groupGate) skip(c document.Commit) bool {
	if g == nil || g.d.HasCommit(c.Hash) {
		return false
	}
	for _, p := range c.ParentHashes {
		if _, ok := g.dropped[p]; ok {
			g.dropped[c.Hash] = struct{}{}
			return true
		}
		if _, ok := g.held[p]; ok {
			g.held[c.Hash] = struct{}{}
			return true
		}
	}
	decision, isPart := g.coord.resolvePart(c)
	if !isPart {
		return false
	}
	switch decision {
	case partAborted:
		g.dropped[c.Hash] = struct{}{}
		g.droppedGroups[c.TransactionID] = struct{}{}
		return true
	case partHeld:
		g.held[c.Hash] = struct{}{}
		return true
	}
	return false
}

func (g *groupGate) report() {
	if g == nil || len(g.dropped) == 0 {
		return
	}
	log.Printf("kdb: namespace %s: rolled back %d commit(s) belonging to or built on %d cross-namespace "+
		"transaction(s) that never committed (cut off by an unclean shutdown before their decision was durable; "+
		"none of them was ever acknowledged)", g.d.NamespaceID, len(g.dropped), len(g.droppedGroups))
}

// replayApplyError distinguishes "applying this commit failed" from
// "reading this segment failed" as they travel back out through the
// streaming callback, which can only return a plain error. Only the latter
// is a candidate for torn-tail tolerance.
type replayApplyError struct{ err error }

func (e *replayApplyError) Error() string { return e.err.Error() }
func (e *replayApplyError) Unwrap() error { return e.err }

// streamSegmentCommits hands seg's commits to fn one at a time, using the
// reader's own streaming path when it has one (see
// storage.DeltaCommitStreamer for why that is so much cheaper) and falling
// back to ReadAll otherwise, so a reader that cannot stream still replays.
//
// The fallback keeps ReadAll's partial-result contract: on a
// *CorruptFrameError the records read before the fault are returned
// alongside the error, and those still reach fn before it is reported.
func streamSegmentCommits(r storage.DeltaSegmentReader, seg storage.DeltaSegmentRef, fn func(document.Commit) error) error {
	if streamer, ok := r.(storage.DeltaCommitStreamer); ok {
		return streamer.StreamCommits(seg, func(c document.Commit, _ int64) error { return fn(c) })
	}
	records, readErr := r.ReadAll(seg)
	for _, rec := range records {
		c, err := document.FromPayloadBytes(rec.CommitPayload)
		if err != nil {
			return err
		}
		if err := fn(c); err != nil {
			return err
		}
	}
	return readErr
}

// applyIfParentsReady applies c when every parent it names is already in
// d, and reports whether it did. A false is not an error: it means c
// arrived before an ancestor did, and the caller should hold it for
// applyCommitsTopologically to place once the rest of the log has been
// read. A commit already present in d counts as applied.
func applyIfParentsReady(d *dag.InMemoryCommitDag, store storage.Adapter, c document.Commit) (bool, error) {
	if d.HasCommit(c.Hash) {
		return true, nil
	}
	for _, p := range c.ParentHashes {
		if !d.HasCommit(p) {
			return false, nil
		}
	}
	if err := applyReplayedCommit(d, store, c); err != nil {
		return false, err
	}
	return true, nil
}

// applyCommitsTopologically applies commits in dependency order: a commit
// is applied only once every one of its parents has already been applied
// (or was already present in d, e.g. from a prior replay). This is what
// makes replay correct independent of the order commits are handed to it
// in - see replayDeltaNamespace's doc comment point 1.
//
// Runs in rounds rather than a single indexed topological sort: the
// common case (segments already in commit order, which is now guaranteed
// - see Component 47 §4.1) resolves everything in one round, and later
// rounds only run at all if something upstream still got the order
// wrong, so this stays cheap in the case it exists to protect against
// being rare.
func applyCommitsTopologically(d *dag.InMemoryCommitDag, store storage.Adapter, commits []document.Commit, gate *groupGate) error {
	pending := make([]document.Commit, 0, len(commits))
	for _, c := range commits {
		if !d.HasCommit(c.Hash) {
			pending = append(pending, c)
		}
	}
	for len(pending) > 0 {
		var next []document.Commit
		progressed := false
		for _, c := range pending {
			// Re-asked here rather than trusted from the streaming pass: a parent that was still
			// unread then may since have been dropped, and its descendants go with it.
			if gate.skip(c) {
				progressed = true
				continue
			}
			ready := true
			for _, p := range c.ParentHashes {
				if !d.HasCommit(p) {
					ready = false
					break
				}
			}
			if !ready {
				next = append(next, c)
				continue
			}
			if err := applyReplayedCommit(d, store, c); err != nil {
				return err
			}
			progressed = true
		}
		if !progressed {
			return fmt.Errorf(
				"kdb: namespace %s: delta replay: %d commit(s) reference parent commits never found in "+
					"the log - the log is missing data (first unresolved: %s)",
				d.NamespaceID, len(next), next[0].Hash.Hex())
		}
		pending = next
	}
	return nil
}

// applyReplayedCommit applies one already-durable commit's operations to
// store and records it in d - the exact per-commit body the pre-Component-47
// replay ran inline, unchanged, just factored out so
// applyCommitsTopologically can call it once a commit's parents are known
// ready rather than only in file order.
func applyReplayedCommit(d *dag.InMemoryCommitDag, store storage.Adapter, c document.Commit) error {
	// A commit extends main only if main is one of its parents. Anything else in the log is a
	// peer's commit that main reached through a merge (logged just before that merge), or one a
	// crash cut off before its merge was logged: stored, so the history is whole, but neither
	// applied to the live tree nor made the head. The merge that adopts it writes every document
	// its parents disagree on, so applying the merge on top of whichever parent main is at builds
	// the merged tree. Applying every commit in log order instead - what replay did before peer
	// sync logged side commits - moved main onto a peer's branch whenever a crash separated the
	// branch from its merge, orphaning local commits.
	if head, err := d.Head(); err == nil && len(c.ParentHashes) > 0 {
		extends := false
		for _, p := range c.ParentHashes {
			if p == head {
				extends = true
				break
			}
		}
		if !extends {
			return d.PutCommit(c, true)
		}
	}
	for _, op := range c.Operations {
		switch o := op.(type) {
		case document.WriteOp:
			doc, err := document.FromJSONWithID(o.DocID, o.Patch)
			if err != nil {
				doc = document.Document{ID: o.DocID, JSON: o.Patch}
			}
			if err := store.PutDocument(d.NamespaceID, doc); err != nil {
				return err
			}
		case document.DeleteOp:
			if err := store.DeleteDocument(d.NamespaceID, o.DocID); err != nil {
				return err
			}
		default:
			// ignore v1 ops not yet ported
		}
	}
	parentTree := document.EmptyDocumentTree().TreeHash
	if len(c.ParentHashes) > 0 {
		parentTree = c.ParentHashes[0]
	}
	tree, err := store.CommitTree(d.NamespaceID, parentTree)
	if err != nil {
		return err
	}
	d.PutDocumentTree(tree)
	if err := d.PutCommit(c, true); err != nil {
		return err
	}
	return d.SetHead("main", c.Hash)
}
