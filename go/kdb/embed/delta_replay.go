package embed

import (
	"errors"
	"fmt"
	"log"

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
	return replayDeltaNamespaceFrom(d, store, r, -1)
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
func replayDeltaNamespaceFrom(
	d *dag.InMemoryCommitDag,
	store storage.Adapter,
	r storage.DeltaSegmentReader,
	afterSequence int64,
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

	// deferred holds only commits whose parents had not been applied when
	// they were read - see applyCommitsTopologically, which drains it.
	var deferred []document.Commit
	for i, seg := range segments {
		read := 0
		readErr := streamSegmentCommits(r, seg, func(c document.Commit) error {
			read++
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

	return applyCommitsTopologically(d, store, deferred)
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
func applyCommitsTopologically(d *dag.InMemoryCommitDag, store storage.Adapter, commits []document.Commit) error {
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
