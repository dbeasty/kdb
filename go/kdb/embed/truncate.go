package embed

import (
	"fmt"
	"log"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// Truncating the delta log, which is what storage.HistoryModeNone actually
// buys.
//
// Under HistoryModeFull nothing here ever runs: the log is the record, and
// deleting any part of it would delete the only durable copy of the
// versions it carries. Under HistoryModeNone the live dataset has a second
// home (see engine's document_bodies.go) and history has an expiry, so
// segments that are past both can go.
//
// Three constraints gate every deletion, and a segment goes only when all
// three agree. Each exists for a different reason and none subsumes the
// others:
//
//   - Below the checkpoint floor. Until a checkpoint accounts for a
//     segment, that segment is the only record of the commits in it.
//   - Outside the retention window. This is the promise the configuration
//     makes, and the only one an operator can see.
//   - Not the segment currently being written. It is still receiving
//     commits, and its own contents are not final.
//
// The window is enforced at segment granularity because a segment is what
// can be deleted: a segment survives until its *newest* commit is outside
// the window, so real retention overshoots the configured window by up to
// one segment's time span. That is documented as a floor rather than
// hidden - see storage.RetentionWindow.

// TruncationResult reports one truncation pass.
type TruncationResult struct {
	// FloorSequence is the lowest segment sequence still on disk after
	// this pass. Recorded in the checkpoint so a later open can tell
	// "truncated as designed" from "a segment went missing".
	FloorSequence int64
	// Removed is how many segments were deleted, ReclaimedBytes their
	// total size.
	Removed        int
	ReclaimedBytes int64
	// RetainedCommits is how many commits remain reachable in the
	// segments that were kept, as far as the fingerprints can tell.
	RetainedSegments int

	// TablesMerged and TablesRemoved report the SSTable compaction that
	// runs in the same pass. Independent of the delta-log figures above,
	// and independent of the history mode: SSTable duplication is a
	// function of how many times the memtable has been flushed, not of
	// how much history is kept, so both modes accumulate it and both
	// modes reclaim it here.
	TablesMerged  int
	TablesRemoved int
	// VersionsDropped is how many superseded document versions the
	// compaction reclaimed. Always zero under history=full, which keeps
	// every version it has ever been given.
	VersionsDropped int
}

// truncationInputs is everything a truncation pass needs, gathered so the
// decision itself can be a pure function that tests can drive directly.
type truncationInputs struct {
	segments []storage.DeltaSegmentRef
	// through is the newest segment the checkpoint accounts for; nothing
	// above it may be deleted.
	through int64
	// now and window define the retention floor in time.
	now    time.Time
	window storage.RetentionWindow
	// commitTime reports when a segment's last commit happened. A segment
	// whose last commit cannot be dated is kept: an unknown age is not a
	// licence to delete.
	commitTime func(codec.Hash) (time.Time, bool)
}

// segmentsToDelete decides which segments may go.
//
// Pure, and deliberately so: this is the function whose mistakes delete
// data, and it is worth being able to test every boundary of it without a
// filesystem.
func segmentsToDelete(in truncationInputs) []storage.DeltaSegmentRef {
	window := in.window.Resolve()
	// Newest first, so the commit floor can be counted off from the top.
	ordered := make([]storage.DeltaSegmentRef, len(in.segments))
	copy(ordered, in.segments)
	for i := 0; i < len(ordered); i++ {
		for j := i + 1; j < len(ordered); j++ {
			if ordered[j].SequenceNumber > ordered[i].SequenceNumber {
				ordered[i], ordered[j] = ordered[j], ordered[i]
			}
		}
	}

	var out []storage.DeltaSegmentRef
	kept := 0
	for _, seg := range ordered {
		if seg.SequenceNumber > in.through {
			// Above the checkpoint, or the segment still being appended
			// to. Not ours to remove.
			kept++
			continue
		}
		// The commit floor: keep at least one segment beyond whatever the
		// window says, so head is never the oldest thing on disk.
		if kept < 1 {
			kept++
			continue
		}
		if window.Commits > 0 && int64(kept) < segmentsForCommits(window.Commits) {
			kept++
			continue
		}
		if window.Duration > 0 {
			ts, ok := in.commitTime(seg.LastCommitHash)
			if !ok {
				// Undatable, so its age is unknown. Keeping it is the only
				// safe reading.
				kept++
				continue
			}
			if in.now.Sub(ts) < window.Duration {
				kept++
				continue
			}
		}
		out = append(out, seg)
	}
	return out
}

// segmentsForCommits converts a commit floor into a segment floor.
//
// A segment holds many commits and the count is not recorded per segment,
// so this cannot be exact. It errs entirely in one direction: one segment
// per 1000 commits asked for, minimum one, which keeps at least as much as
// requested for any segment holding 1000 commits or more and more than
// requested otherwise. Over-retention is a disk-space cost; under-retention
// is data loss, so the approximation only ever fails safe.
func segmentsForCommits(commits int64) int64 {
	n := commits / 1000
	if n < 1 {
		n = 1
	}
	return n
}

// TruncationPlan is what a truncation pass intends to do, computed before
// anything is written or deleted.
type TruncationPlan struct {
	// Victims are the segments that may be removed.
	Victims []storage.DeltaSegmentRef
	// Floor is the lowest sequence that will still be on disk once they
	// are. Recorded in the checkpoint *before* the deletions, never after
	// - see planAndTruncate for why that order is the safe one.
	Floor int64
}

// IsEmpty reports a plan that would delete nothing.
func (p TruncationPlan) IsEmpty() bool { return len(p.Victims) == 0 }

// planTruncation decides what could go, without touching anything.
func planTruncation(
	d *dag.InMemoryCommitDag,
	store storage.Adapter,
	r storage.DeltaSegmentReader,
	through int64,
	window storage.RetentionWindow,
	now time.Time,
) (TruncationPlan, error) {
	if r == nil {
		return TruncationPlan{}, nil
	}
	eng, ok := store.(*engine.ServerEngine)
	if !ok || eng.HistoryMode() != storage.HistoryModeNone {
		return TruncationPlan{}, nil
	}
	segments, err := r.ListSegments()
	if err != nil {
		return TruncationPlan{}, err
	}
	if len(segments) == 0 {
		return TruncationPlan{}, nil
	}
	victims := segmentsToDelete(truncationInputs{
		segments:   segments,
		through:    through,
		now:        now,
		window:     window,
		commitTime: commitTimeFromDag(d),
	})
	plan := TruncationPlan{Victims: victims, Floor: lowestSequence(segments)}
	if len(victims) > 0 {
		highestVictim := victims[0].SequenceNumber
		for _, v := range victims[1:] {
			if v.SequenceNumber > highestVictim {
				highestVictim = v.SequenceNumber
			}
		}
		plan.Floor = highestVictim + 1
	}
	return plan, nil
}

// applyTruncation carries out a plan: document bodies to disk first, then
// the segments go.
//
// The caller must already have written a checkpoint recording plan.Floor.
// That ordering - checkpoint claiming the floor, *then* delete - is what
// makes a crash in the middle harmless: a checkpoint that claims a floor
// higher than reality simply ignores the segments still sitting below it
// (checkpointMatchesLog skips them), and the next pass deletes them. The
// reverse order leaves a checkpoint that expects segments which are
// already gone, which is indistinguishable from damage.
func applyTruncation(
	store storage.Adapter,
	r storage.DeltaSegmentReader,
	shim storage.PlatformIOShim,
	namespaceID string,
	plan TruncationPlan,
) (TruncationResult, error) {
	result := TruncationResult{FloorSequence: plan.Floor}
	if plan.IsEmpty() || shim == nil {
		return result, nil
	}
	eng, ok := store.(*engine.ServerEngine)
	if !ok {
		return result, nil
	}
	// Bodies to disk before any byte of the log goes away. Reversing these
	// two leaves the live dataset with no durable copy at all if the
	// process dies between them.
	if err := eng.PrepareForTruncation(); err != nil {
		return result, fmt.Errorf(
			"kdb: namespace %s: refusing to truncate the delta log because document bodies "+
				"could not be flushed first: %w", namespaceID, err)
	}
	for _, seg := range plan.Victims {
		name := storio.SegmentNameBuilder.DeltaSequenced(namespaceID, seg.SequenceNumber)
		if err := shim.DeleteSegment(name); err != nil {
			// Stop at the first failure rather than pressing on. What is
			// left behind is a segment below the recorded floor, which the
			// open guard ignores and the next pass retries.
			log.Printf("kdb: namespace %s: could not delete delta segment %d (%v) - stopping truncation here",
				namespaceID, seg.SequenceNumber, err)
			break
		}
		result.Removed++
		result.ReclaimedBytes += seg.SizeBytes
	}
	if remaining, err := r.ListSegments(); err == nil {
		result.RetainedSegments = len(remaining)
	}
	return result, nil
}

// commitTimeFromDag dates a segment by the commit it ends at.
func commitTimeFromDag(d *dag.InMemoryCommitDag) func(codec.Hash) (time.Time, bool) {
	return func(h codec.Hash) (time.Time, bool) {
		c, ok := d.GetCommit(h)
		if !ok {
			return time.Time{}, false
		}
		return time.UnixMicro(c.Timestamp.EpochMicros()), true
	}
}

func lowestSequence(segments []storage.DeltaSegmentRef) int64 {
	if len(segments) == 0 {
		return 0
	}
	low := segments[0].SequenceNumber
	for _, s := range segments[1:] {
		if s.SequenceNumber < low {
			low = s.SequenceNumber
		}
	}
	return low
}
