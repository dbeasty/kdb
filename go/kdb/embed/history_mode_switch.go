package embed

import (
	"fmt"

	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
)

// Switching a namespace's history mode on a running runtime.
//
// The thing to understand about this operation is how little it does. It
// writes a marker and changes what the engine will permit; it deletes
// nothing, folds nothing, and rewrites nothing. A namespace switched from
// full to none holds exactly the bytes it held a moment earlier, and
// switching back gets all of them, because none were lost.
//
// That is deliberate, and it is what separates this from the offline
// migration in migrate_history.go. Reclamation is storage.ReclaimMode's
// question and CompactHistory's job - the destructive one-way door - and
// keeping the two apart is what lets an operator turn none on, look at what
// it *would* reclaim, and turn it off again. A switch that started deleting
// would make evaluating the mode a commitment to it.
//
// So: this lands a namespace on storage.ReclaimManual by default. Turning a
// mode on must not, by itself, start destroying data.

// HistoryModeSwitch reports what a switch did, and what it did not.
type HistoryModeSwitch struct {
	From, To storage.HistoryMode
	// Reclaim is the mode the namespace is on afterwards. ReclaimManual
	// unless the caller asked otherwise, which is what keeps the switch
	// reversible.
	Reclaim storage.ReclaimMode
	// EligibleSegments and EligibleBytes are what a compaction would free
	// if one were run now - zero under full, which reclaims nothing.
	// Reported because a namespace on none+manual has unbounded disk and an
	// operator should see the number that says how much that is costing.
	EligibleSegments int
	EligibleBytes    int64
	// HistoryLost is true when this switch cannot restore what an earlier
	// compaction already reclaimed - i.e. going none -> full on a namespace
	// whose log has already been truncated. The mode changes either way;
	// what this says is that "full" from here means "keeps everything from
	// now on", not "the past is back".
	HistoryLost bool
}

// SetHistoryMode switches this namespace between storage.HistoryModeFull and
// storage.HistoryModeNone, without reclaiming anything.
//
// The switch is metadata: the marker on disk is updated so a restart honours
// it, and the running engine is told, and that is all. Nothing is deleted
// here or as a consequence of this call - the namespace lands on
// storage.ReclaimManual, so it will not start reclaiming on its own either.
// Pass a reclaim mode explicitly to opt into that in the same breath.
//
// Going full -> none is completely reversible until something compacts.
// Going none -> full cannot restore segments an earlier compaction already
// deleted; the returned HistoryLost says whether that applies, and a caller
// showing this to a human should say so rather than letting "full history"
// be read as "the history is back".
func (rt *EmbeddedKdbRuntime) SetHistoryMode(to storage.HistoryMode, reclaim storage.ReclaimMode) (HistoryModeSwitch, error) {
	if err := rt.AssertWritable(); err != nil {
		return HistoryModeSwitch{}, err
	}
	if to != storage.HistoryModeFull && to != storage.HistoryModeNone {
		return HistoryModeSwitch{}, fmt.Errorf("kdb: history mode must be \"full\" or \"none\"")
	}
	eng, ok := rt.Storage.(*engine.ServerEngine)
	if !ok {
		return HistoryModeSwitch{}, fmt.Errorf("kdb: this runtime has no engine that enforces a history mode")
	}
	if rt.DataRoot == "" {
		return HistoryModeSwitch{}, fmt.Errorf(
			"kdb: a memory-backed namespace has no marker to record a history mode in; " +
				"it is whatever it was opened as for as long as it exists")
	}
	from := eng.HistoryMode()
	// An unspecified reclaim mode means *manual* here, not the global
	// default. This is the line that makes a switch to none reversible:
	// resolving to the ordinary default would start the namespace
	// reclaiming as a side effect of changing its mode, which is precisely
	// what this operation is arranged not to do. Switching to full leaves
	// the mode alone instead - full reclaims no segments whatever it says,
	// and quietly moving a namespace onto manual would also stop its
	// SSTable compaction, which is not a retention question and was not
	// asked about.
	landing := reclaim
	if landing == storage.ReclaimUnset {
		if to == storage.HistoryModeNone {
			landing = storage.ReclaimManual
		} else {
			landing = eng.ReclaimMode()
		}
	}
	result := HistoryModeSwitch{From: from, To: to, Reclaim: landing}

	// A namespace whose log has already been truncated cannot be made whole
	// by calling it full again. Detected from the checkpoint's floor rather
	// than guessed from the mode: a none namespace that has never compacted
	// still has everything.
	if to == storage.HistoryModeFull && from == storage.HistoryModeNone {
		result.HistoryLost = rt.logHasBeenTruncated()
	}

	// Marker first, engine second. The marker is what a restart reads, and
	// the ordering matters in only one direction: a crash between them
	// leaves a namespace whose marker says the new mode and whose engine
	// says the old one, which the next open resolves to the marker - the
	// state the caller asked for. The reverse would leave a running engine
	// permitting something the marker does not, and the marker is the
	// authority.
	meta, found := readNamespaceMeta(rt.DataRoot, rt.DefaultNamespace)
	if !found {
		return result, fmt.Errorf(
			"kdb: namespace %q has no meta.json under %s to record a history mode in",
			rt.DefaultNamespace, rt.DataRoot)
	}
	meta.HistoryMode = to.String()
	if to == storage.HistoryModeNone {
		// none serves historical reads by replay, so tree objects would be
		// written for reads that will not resolve. Existing ones stay as
		// dead weight, exactly as the offline migration leaves them.
		meta.HistoryStrategy = storage.HistoryStrategyReplay.String()
	}
	if err := writeNamespaceMeta(rt.DataRoot, rt.DefaultNamespace, meta); err != nil {
		return result, fmt.Errorf("kdb: recording the new history mode: %w", err)
	}

	eng.SetHistoryMode(to)
	eng.SetReclaimMode(result.Reclaim)

	// Say what is now reclaimable, so "armed but not reclaiming" arrives
	// with the number that makes it an informed state to be in.
	if to == storage.HistoryModeNone {
		if res, err := rt.EligibleForCompaction(); err == nil {
			result.EligibleSegments, result.EligibleBytes = res.EligibleSegments, res.EligibleBytes
		}
	}
	return result, nil
}

// EligibleForCompaction reports what a compaction would free right now,
// without freeing any of it.
//
// A held maintenance pass already computes this (see TruncationResult's
// EligibleSegments), so this is that pass run deliberately: it writes a
// checkpoint, which deletes nothing, and reports the plan it did not carry
// out.
func (rt *EmbeddedKdbRuntime) EligibleForCompaction() (TruncationResult, error) {
	if err := rt.AssertWritable(); err != nil {
		return TruncationResult{}, err
	}
	if rt.maintain == nil {
		return TruncationResult{}, nil
	}
	eng, ok := rt.Storage.(*engine.ServerEngine)
	if !ok {
		return rt.maintain()
	}
	previous := eng.ReclaimMode()
	eng.SetReclaimMode(storage.ReclaimManual)
	defer eng.SetReclaimMode(previous)
	return rt.maintain()
}

// logHasBeenTruncated reports whether anything has already been reclaimed
// from this namespace's delta log, which is what decides whether switching
// back to full can restore the past or only stop losing more of it.
//
// Read from the segments on disk rather than from the mode: a namespace on
// none that has never compacted still holds every commit it ever made, and
// telling its operator that history is lost would be false.
func (rt *EmbeddedKdbRuntime) logHasBeenTruncated() bool {
	if rt.deltaReader == nil {
		return false
	}
	segments, err := rt.deltaReader.ListSegments()
	if err != nil || len(segments) == 0 {
		return false
	}
	lowest := segments[0].SequenceNumber
	for _, s := range segments[1:] {
		if s.SequenceNumber < lowest {
			lowest = s.SequenceNumber
		}
	}
	// Sequence 0 is the first segment a namespace ever writes. Anything
	// above it as the floor means the ones below were deleted.
	return lowest > 0
}
