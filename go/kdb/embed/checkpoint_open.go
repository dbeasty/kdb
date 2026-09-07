package embed

import (
	"log"

	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
)

// restoreNamespace brings d and store up to the namespace's durable state,
// preferring a checkpoint over reading the whole delta log, and reports
// whether it ended up replaying the log in full anyway.
//
// The checkpoint is a cache and never an authority. Anything wrong with it
// - absent, unreadable, written by a different format version, describing
// another namespace, or naming a segment that is no longer there - falls
// back to a full replay, which is always correct because the delta log is
// the record of truth. That is what makes it safe to add a second
// representation of the same state at all.
func restoreNamespace(
	d *dag.InMemoryCommitDag,
	store storage.Adapter,
	r storage.DeltaSegmentReader,
	shim storage.PlatformIOShim,
	namespaceID string,
	disabled bool,
) (replayedInFull bool, err error) {
	eng, _ := store.(*engine.ServerEngine)
	if disabled {
		// Not just "do not write one": a namespace with checkpoints turned
		// off must not read a stale one left from when they were on.
		return true, replayDeltaNamespace(d, store, r)
	}
	cp, ok := readCheckpoint(shim, namespaceID)
	if !ok || eng == nil || r == nil {
		return true, replayDeltaNamespace(d, store, r)
	}
	if ok, why := checkpointMatchesLog(cp, r); !ok {
		// The log no longer matches what the checkpoint was written
		// against. Trusting it here would skip reading exactly the
		// segments that changed, turning damage or a rolled-back data
		// directory into a namespace that opens cleanly and is quietly
		// missing history. Replaying instead both rebuilds from the
		// authority and surfaces the damage.
		log.Printf("kdb: namespace %s: ignoring the checkpoint and replaying the log in full - %s", namespaceID, why)
		return true, replayDeltaNamespace(d, store, r)
	}
	if cp.ThroughSequence > highestSegmentSequence(r) {
		// The checkpoint claims to cover segments that are not there. The
		// data directory was truncated, restored from an older backup, or
		// otherwise moved underneath us; the log wins.
		log.Printf(
			"kdb: namespace %s: checkpoint covers delta segments through sequence %d but the newest on disk is %d - "+
				"ignoring it and replaying the log in full",
			namespaceID, cp.ThroughSequence, highestSegmentSequence(r))
		return true, replayDeltaNamespace(d, store, r)
	}

	if err := d.RestoreCheckpoint(cp.State); err != nil {
		log.Printf("kdb: namespace %s: checkpoint could not be restored (%v) - replaying the log in full", namespaceID, err)
		return true, replayDeltaNamespace(d, store, r)
	}
	eng.RestoreLiveTree(namespaceID, cp.LiveTree)
	// Historical trees are the one thing a checkpoint does not carry. They
	// stay derivable from the log and are built only if something reads at
	// a historical commit - see engine.SetTreeRebuilder.
	if eng.HistoryStrategy() == storage.HistoryStrategyReplay {
		// Under the replay strategy nothing wrote tree objects, so the only
		// way back to a historical tree is the log. Under the objects
		// strategy this is deliberately left unset: falling back to a scan
		// there would quietly reintroduce the cost the strategy was chosen
		// to avoid, at the moment an operator least expects it.
		eng.SetTreeRebuilder(func() error { return rebuildHistoricalTrees(eng, r) })
	}

	// Branch-head operations come out of the checkpoint itself rather than
	// out of the log: reading them from the log would mean indexing every
	// segment at open, which is the whole cost being avoided here.
	heads := make([]document.Commit, 0, len(cp.HeadCommits))
	for _, payload := range cp.HeadCommits {
		c, err := document.FromPayloadBytes(payload)
		if err != nil {
			log.Printf("kdb: namespace %s: a checkpointed head commit would not decode (%v) - replaying the log in full", namespaceID, err)
			return true, replayDeltaNamespace(d, store, r)
		}
		heads = append(heads, c)
	}
	if err := d.RestoreCommitOperations(heads); err != nil {
		log.Printf("kdb: namespace %s: checkpointed head operations were rejected (%v) - replaying the log in full", namespaceID, err)
		return true, replayDeltaNamespace(d, store, r)
	}
	if err := d.HydrateBranchHeads(); err != nil {
		log.Printf("kdb: namespace %s: could not load branch-head operations back (%v) - replaying the log in full", namespaceID, err)
		return true, replayDeltaNamespace(d, store, r)
	}

	// Only the tail: everything up to cp.ThroughSequence is already here.
	if err := replayDeltaNamespaceFrom(d, store, r, cp.ThroughSequence); err != nil {
		return false, err
	}
	return false, nil
}

// saveCheckpoint records the namespace's current graph, refs and live tree
// so the next open does not have to read the log to rebuild them.
//
// through names the newest delta segment the checkpoint accounts for, and
// getting it wrong in the generous direction loses commits, so the two
// callers derive it differently and neither guesses:
//
//   - Mid-session (checkpointAfterFullReplay), the writer's own segment is
//     still open and will receive this session's commits, so the
//     checkpoint can only claim everything strictly below it.
//   - At close (checkpointOnClose), the writer has been flushed and
//     sealed, nothing more can be appended anywhere, and the newest
//     segment on disk is covered.
//
// Best-effort by design, and callers treat a failure as a log line rather
// than an error: failing to write a cache must never fail an operation
// whose real work already succeeded.
func saveCheckpoint(
	d *dag.InMemoryCommitDag,
	store storage.Adapter,
	r storage.DeltaSegmentReader,
	shim storage.PlatformIOShim,
	namespaceID string,
	through int64,
	disabled bool,
) error {
	if disabled {
		return nil
	}
	eng, ok := store.(*engine.ServerEngine)
	if !ok || shim == nil || through < 0 {
		return nil
	}
	segments, err := segmentFingerprints(r, through)
	if err != nil {
		return err
	}
	state := d.CheckpointSnapshot()
	var headPayloads [][]byte
	for _, b := range state.Branches {
		c, err := d.GetCommitOrThrow(b.HeadHash)
		if err != nil {
			// A head the graph cannot resolve is not something to guess
			// at; leave it out and the next open loads it from the log.
			continue
		}
		payload, err := c.ToPayloadBytes()
		if err != nil {
			continue
		}
		headPayloads = append(headPayloads, payload)
	}
	return writeCheckpoint(shim, namespaceCheckpoint{
		NamespaceID:     namespaceID,
		ThroughSequence: through,
		State:           state,
		LiveTree:        eng.LiveTree(),
		Segments:        segments,
		HeadCommits:     headPayloads,
	})
}

// checkpointAfterFullReplay writes a checkpoint immediately after an open
// that had to read the whole log.
//
// Without this, only a clean shutdown would ever produce a checkpoint, and
// a process that is killed - the ordinary end of a container's life -
// would replay the whole log on every single start, forever. Writing one
// here means the expensive open pays for the next one even if this process
// never gets to shut down.
func checkpointAfterFullReplay(
	d *dag.InMemoryCommitDag,
	store storage.Adapter,
	r storage.DeltaSegmentReader,
	w storage.DeltaSegmentWriter,
	shim storage.PlatformIOShim,
	namespaceID string,
	disabled bool,
) {
	seq, ok := w.(storage.DeltaSegmentSequencer)
	if !ok {
		return
	}
	// Strictly below the writer's own segment: that one is open for
	// appends for the rest of this session.
	if err := saveCheckpoint(d, store, r, shim, namespaceID, seq.SequenceNumber()-1, disabled); err != nil {
		log.Printf("kdb: namespace %s: could not write a checkpoint after replay (%v) - the next open will replay the log again", namespaceID, err)
	}
}

// checkpointOnClose writes a checkpoint during an orderly shutdown, after
// the delta writer has been sealed so no segment can gain another commit.
func checkpointOnClose(
	d *dag.InMemoryCommitDag,
	store storage.Adapter,
	r storage.DeltaSegmentReader,
	shim storage.PlatformIOShim,
	namespaceID string,
	disabled bool,
) {
	if r == nil {
		return
	}
	if err := saveCheckpoint(d, store, r, shim, namespaceID, highestSegmentSequence(r), disabled); err != nil {
		log.Printf("kdb: namespace %s: could not write a checkpoint on close (%v) - the next open will replay the log", namespaceID, err)
	}
}
