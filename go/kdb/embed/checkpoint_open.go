package embed

import (
	"fmt"
	"log"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
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
	if eng != nil {
		eng.SetReplaying(true)
		defer eng.SetReplaying(false)
	}
	// replayIsComplete reports whether reading every segment on disk would
	// actually reconstruct this namespace. It is true for every namespace
	// that has never truncated, which is every namespace under
	// history=full - and false the moment one has, because the segments
	// that are gone carried commits that a replay would simply not see.
	//
	// This is what replaces the old unconditional "fall back to a full
	// replay" for a truncated namespace. That fallback is correct only
	// while the log is the whole record; once it is not, falling back to
	// it turns a bad checkpoint into a database that opens cleanly and is
	// quietly missing everything below the floor. There is no safe
	// automatic recovery from that, so it is an error.
	replayIsComplete := lowestSegmentSequence(r) <= 0
	mustReplay := func() (bool, error) {
		if !replayIsComplete {
			return true, &TruncatedLogError{
				NamespaceID: namespaceID,
				Floor:       lowestSegmentSequence(r),
			}
		}
		return true, replayDeltaNamespace(d, store, r)
	}

	if disabled {
		// Not just "do not write one": a namespace with checkpoints turned
		// off must not read a stale one left from when they were on.
		return mustReplay()
	}
	cp, ok := readCheckpoint(shim, namespaceID)
	if !ok || eng == nil || r == nil {
		return mustReplay()
	}
	if ok, why := checkpointMatchesLog(cp, r); !ok {
		// The log no longer matches what the checkpoint was written
		// against. Trusting it here would skip reading exactly the
		// segments that changed, turning damage or a rolled-back data
		// directory into a namespace that opens cleanly and is quietly
		// missing history. Replaying instead both rebuilds from the
		// authority and surfaces the damage.
		log.Printf("kdb: namespace %s: ignoring the checkpoint and replaying the log in full - %s", namespaceID, why)
		return mustReplay()
	}
	if cp.ThroughSequence > highestSegmentSequence(r) {
		// The checkpoint claims to cover segments that are not there. The
		// data directory was truncated, restored from an older backup, or
		// otherwise moved underneath us; the log wins.
		log.Printf(
			"kdb: namespace %s: checkpoint covers delta segments through sequence %d but the newest on disk is %d - "+
				"ignoring it and replaying the log in full",
			namespaceID, cp.ThroughSequence, highestSegmentSequence(r))
		return mustReplay()
	}

	if err := d.RestoreCheckpoint(cp.State); err != nil {
		log.Printf("kdb: namespace %s: checkpoint could not be restored (%v) - replaying the log in full", namespaceID, err)
		return mustReplay()
	}
	eng.RestoreLiveTree(namespaceID, cp.LiveTree)
	// Historical trees are the one thing a checkpoint does not carry. They
	// stay derivable from the log and are built only if something reads at
	// a historical commit - see engine.SetTreeRebuilder.
	if eng.HistoryStrategy() == storage.HistoryStrategyReplay {
		// Under the replay strategy nothing wrote tree objects, so the only
		// way back to a historical tree is to fold it out of the log. Under
		// the objects strategy this is deliberately left unset: falling
		// back to folding there would quietly reintroduce the cost the
		// strategy was chosen to avoid.
		installTreeRebuilder(d, eng)
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
		return mustReplay()
	}
	if err := d.HydrateBranchHeads(); err != nil {
		log.Printf("kdb: namespace %s: could not load branch-head operations back (%v) - replaying the log in full", namespaceID, err)
		return mustReplay()
	}

	// Only the tail: everything up to cp.ThroughSequence is already here.
	if err := replayDeltaNamespaceFrom(d, store, r, cp.ThroughSequence); err != nil {
		return false, err
	}
	return false, nil
}

// TruncatedLogError reports a namespace whose delta log has had segments
// deleted and whose checkpoint cannot be used, so neither source can
// reconstruct it. Detect with errors.As.
//
// This is the failure mode history=none trades for its smaller footprint,
// and it is deliberately loud. Under history=full the log is always the
// whole record and this can never happen; under history=none the
// checkpoint is an authority rather than a cache, and losing an authority
// is not something to paper over by opening with less data than the
// operator has.
type TruncatedLogError struct {
	NamespaceID string
	Floor       int64
}

func (e *TruncatedLogError) Error() string {
	return fmt.Sprintf(
		"kdb: namespace %q cannot be opened: it runs with history=none and its delta log has been "+
			"truncated (oldest segment is %d), so the checkpoint is the only record of everything "+
			"below that - and the checkpoint is missing or unusable. Restore this namespace from a backup",
		e.NamespaceID, e.Floor)
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
	floor int64,
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
	state := d.CheckpointSnapshotRetaining(commitRetentionFilter(eng))
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
		FloorSequence:   floor,
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
	if err := saveCheckpoint(d, store, r, shim, namespaceID, seq.SequenceNumber()-1, currentFloor(r), disabled); err != nil {
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
	window storage.RetentionWindow,
	disabled bool,
) {
	if r == nil {
		return
	}
	// Close is the natural moment to reclaim: the writer is sealed, so the
	// checkpoint can claim the newest segment, which is the widest floor
	// any truncation will ever get. Under history=full this is a plain
	// checkpoint and deletes nothing.
	res, err := checkpointAndTruncate(
		d, store, r, shim, namespaceID, highestSegmentSequence(r), window, time.Now(), disabled)
	if err != nil {
		log.Printf("kdb: namespace %s: could not write a checkpoint on close (%v) - the next open will replay the log", namespaceID, err)
		return
	}
	if res.Removed > 0 {
		log.Printf("kdb: namespace %s: reclaimed %d delta segment(s), %d bytes; oldest retained segment is now %d",
			namespaceID, res.Removed, res.ReclaimedBytes, res.FloorSequence)
	}
}

// installTreeRebuilder wires the replay strategy's on-demand tree fold and
// hands the engine ownership of the DAG's trees.
//
// The two go together. Bounding the engine's tree store only reclaims
// memory if nothing else is holding the same trees, and the DAG held a
// second map of exactly them - so the store has to become the single owner
// at the same moment it becomes able to evict.
func installTreeRebuilder(d *dag.InMemoryCommitDag, eng *engine.ServerEngine) {
	eng.SetTreeRebuilder(func(want codec.Hash) (document.DocumentTree, bool, error) {
		return rebuildTreeByFolding(d, eng, want)
	})
}

// commitRetentionFilter decides which commits a checkpoint writes out.
//
// nil under history=full, which keeps every commit forever and whose
// checkpoint must therefore describe every commit.
//
// Under history=none it is the retention window, applied to the commit's
// own timestamp - the same window and the same clock the delta log's
// truncation uses, so the two halves of "how much past is there" agree.
// They agree only approximately: truncation works at segment granularity
// and so over-retains, which means some commits older than the window
// still have their frames on disk while the graph has forgotten them.
// That asymmetry is in the safe direction. The promise is "everything
// inside the window is readable", and this filter keeps exactly that; the
// extra frames are unreachable rather than missing.
//
// A zero window keeps nothing but the refs, which is what "keep nothing
// beyond the checkpoint" has to mean for the graph as well as the log.
func commitRetentionFilter(eng *engine.ServerEngine) func(document.Commit) bool {
	if eng == nil || eng.HistoryMode() != storage.HistoryModeNone {
		return nil
	}
	window := eng.RetentionWindow().Resolve()
	if window.Duration <= 0 {
		// Refs only. CheckpointSnapshotRetaining keeps those whatever this
		// says, so a graph of exactly the branch heads is the result.
		return func(document.Commit) bool { return false }
	}
	cutoff := time.Now().Add(-window.Duration).UnixMicro()
	return func(c document.Commit) bool {
		return c.Timestamp.EpochMicros() >= cutoff
	}
}

// currentFloor is the floor to record when nothing is being truncated: the
// oldest segment actually on disk. Zero for a namespace that has never
// truncated, which keeps the recorded floor honest without any special
// case for history=full.
func currentFloor(r storage.DeltaSegmentReader) int64 {
	low := lowestSegmentSequence(r)
	if low < 0 {
		return 0
	}
	return low
}

// checkpointAndTruncate is the maintenance pass for a history=none
// namespace: write a checkpoint, then delete the delta segments that are
// past both it and the retention window.
//
// The two halves are one operation because neither is useful alone. A
// checkpoint without truncation is what history=full does and reclaims
// nothing; truncation without a checkpoint deletes the only record of the
// commits it removes.
//
// The order within it is load-bearing and spelled out on applyTruncation:
// plan, then checkpoint claiming the resulting floor, then delete. A crash
// at any point leaves either the previous state or a checkpoint whose
// floor is ahead of the deletions - which reads as "there are some extra
// old segments", not as damage.
func checkpointAndTruncate(
	d *dag.InMemoryCommitDag,
	store storage.Adapter,
	r storage.DeltaSegmentReader,
	shim storage.PlatformIOShim,
	namespaceID string,
	through int64,
	window storage.RetentionWindow,
	now time.Time,
	disabled bool,
) (TruncationResult, error) {
	if disabled || r == nil || shim == nil || through < 0 {
		return TruncationResult{}, nil
	}
	eng, ok := store.(*engine.ServerEngine)
	if !ok {
		return TruncationResult{}, saveCheckpoint(d, store, r, shim, namespaceID, through, currentFloor(r), disabled)
	}
	if eng.HistoryMode() != storage.HistoryModeNone {
		// history=full: checkpoint, and compact the blob store. Nothing in
		// the delta log is ever reclaimed, so the floor stays where it is -
		// but SSTable duplication is not a retention question and a
		// full-history namespace accumulates exactly as much of it.
		if err := saveCheckpoint(d, store, r, shim, namespaceID, through, currentFloor(r), disabled); err != nil {
			return TruncationResult{}, err
		}
		return withCompaction(TruncationResult{FloorSequence: currentFloor(r)}, eng, namespaceID), nil
	}

	plan, err := planTruncation(d, store, r, through, window, now)
	if err != nil {
		return TruncationResult{}, err
	}
	if err := saveCheckpoint(d, store, r, shim, namespaceID, through, plan.Floor, disabled); err != nil {
		return TruncationResult{}, err
	}
	if plan.IsEmpty() {
		return withCompaction(TruncationResult{FloorSequence: plan.Floor}, eng, namespaceID), nil
	}
	res, err := applyTruncation(store, r, shim, namespaceID, plan)
	if err != nil {
		return res, err
	}
	return withCompaction(res, eng, namespaceID), nil
}

// withCompaction runs the SSTable merge and folds what it reclaimed into
// the result.
//
// After the delta-log work rather than before it: truncation's own
// PrepareForTruncation writes the live dataset into the memtable, and
// compacting before that flush would merge a picture that is about to gain
// another table anyway.
//
// A compaction failure is logged rather than returned. The checkpoint and
// any truncation have already succeeded and are the operations the caller
// asked for; failing them because a space optimization did not run would
// turn a partial success into a reported failure.
func withCompaction(res TruncationResult, eng *engine.ServerEngine, namespaceID string) TruncationResult {
	c, err := eng.CompactBlobStore()
	if err != nil {
		log.Printf("kdb: namespace %s: could not compact the blob store (%v) - it keeps its current tables", namespaceID, err)
		return res
	}
	res.TablesMerged, res.TablesRemoved, res.VersionsDropped = c.Merged, c.Removed, c.Dropped
	if c.Removed > 0 {
		log.Printf("kdb: namespace %s: merged %d sstables into one, removed %d, dropped %d unreachable version(s)",
			namespaceID, c.Merged, c.Removed, c.Dropped)
	}
	return res
}
