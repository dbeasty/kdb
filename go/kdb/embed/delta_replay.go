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
func replayDeltaNamespace(d *dag.InMemoryCommitDag, store storage.Adapter, r storage.DeltaSegmentReader) error {
	if r == nil {
		return nil
	}
	segments, err := r.ListSegments()
	if err != nil {
		return err
	}

	var allCommits []document.Commit
	for i, seg := range segments {
		records, readErr := r.ReadAll(seg)
		var corrupt *delta.CorruptFrameError
		if readErr != nil {
			isMostRecent := i == len(segments)-1
			if errors.As(readErr, &corrupt) && isMostRecent {
				// Torn tail on the most recently written segment: the
				// expected shape of an unclean shutdown (the last frame's
				// write never completed, or completed but wasn't fsynced
				// before the process died). Keep every commit scanned
				// before it, log it, and continue - this segment is never
				// appended to again (OpenWriter always starts a fresh one),
				// so this decision is stable across future restarts too.
				log.Printf(
					"kdb: namespace %s: delta segment (sequence %d) has a torn tail at byte offset %d (%s) - "+
						"treating as an incomplete write from an unclean shutdown and continuing with the %d "+
						"commit(s) read cleanly before it",
					d.NamespaceID, seg.SequenceNumber, corrupt.Offset, corrupt.Reason, len(records))
			} else {
				return fmt.Errorf(
					"kdb: namespace %s: delta segment (sequence %d) is corrupt and is not the most recently "+
						"written segment, so this is not an expected torn tail - data may be unrecoverable; "+
						"run kdb-inspect repair-segments before retrying: %w",
					d.NamespaceID, seg.SequenceNumber, readErr)
			}
		}
		for _, rec := range records {
			c, err := document.FromPayloadBytes(rec.CommitPayload)
			if err != nil {
				return err
			}
			allCommits = append(allCommits, c)
		}
	}

	return applyCommitsTopologically(d, store, allCommits)
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
