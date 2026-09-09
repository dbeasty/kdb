package embed

import (
	"fmt"
	"time"

	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// Bringing reclaimed segments back from the archive.
//
// A compaction under history=none deletes segments from local disk. With an
// archive configured (see storio.SinkArchive) they still exist, so the
// deletion was eviction rather than destruction - and this is the operation
// that makes that claim mean something, by fetching them back and lowering
// the checkpoint floor so the namespace will read them again.
//
// Deliberately explicit, rather than a fetch-on-read inside the cold loader.
// The loader indexes the segments that are locally present, so a version in
// an evicted segment is not in its index at all: making the read path fall
// back would mean, on every miss, listing the archive and pulling whole
// segments from object storage in the middle of a query, with no bound on
// how much. That turns a query into an unpredictable multi-second fetch and
// hides a large cost behind an ordinary read. An operator restoring a named
// range gets the same data, when they choose, at a cost they can see.
//
// The ordering is the part to get right, and it is the mirror of
// truncation's. Truncation flushes bodies, then claims a higher floor, then
// deletes. A restore does the reverse: fetch the segments, verify they are
// actually there, and only then lower the floor to admit them. Lowering it
// first would leave a checkpoint claiming segments that are not present,
// which the open guard reads as damage rather than as a restore in progress
// - a clean namespace turned into one that refuses to open.

// RestoreResult reports what came back from the archive.
type RestoreResult struct {
	// Restored is how many segments were fetched, and RestoredBytes their total size.
	Restored      int
	RestoredBytes int64
	// FloorSequence is the lowest sequence the namespace will now read from, after being lowered
	// to admit what was restored.
	FloorSequence int64
	// Missing names the segments the archive could not produce. A restore with any of these
	// lowers the floor only as far as the contiguous run it did get: a floor below a hole would
	// leave the namespace expecting a segment nothing can supply.
	Missing []int64
	// Took is how long the fetch ran. Object storage is slow and the number is worth reporting.
	Took time.Duration
}

// RestoreFromArchive fetches delta segments from this namespace's archive tier and lowers the
// checkpoint floor so they are read again.
//
// from and to are inclusive segment sequences. A restore stops at the first sequence the archive
// cannot produce and reports the rest as Missing rather than pressing on: the floor may only be
// lowered to a point above which every segment is present, so a hole bounds what the restore can
// achieve however many segments lie beyond it.
//
// Refused without an archive, which is the honest answer - a replica followed the deletion and
// does not have what was reclaimed, so there would be nothing to restore from.
func (rt *EmbeddedKdbRuntime) RestoreFromArchive(from, to int64) (RestoreResult, error) {
	if err := rt.AssertWritable(); err != nil {
		return RestoreResult{}, err
	}
	if from < 0 || to < from {
		return RestoreResult{}, fmt.Errorf(
			"kdb: restore range %d-%d is not a range; give a first and last segment sequence", from, to)
	}
	restorer, ok := rt.segmentRestorer()
	if !ok {
		return RestoreResult{}, fmt.Errorf(
			"kdb: namespace %s has no archive tier, so nothing it reclaimed can be restored; "+
				"an archive is configured separately from a replica (KDB_S3_ARCHIVE_BUCKET) "+
				"because a replica follows deletions and does not keep what was reclaimed",
			rt.DefaultNamespace)
	}

	started := time.Now()
	result := RestoreResult{}
	// Highest first, so the contiguous run that ends at the existing floor is what gets
	// admitted: restoring 5..9 under a floor of 10 is useful, restoring 5 and 9 with 6-8 missing
	// is not, and the floor must land above any hole.
	lowest := to + 1
	for seq := to; seq >= from; seq-- {
		name := storio.SegmentNameBuilder.DeltaSequenced(rt.DefaultNamespace, seq)
		if err := restorer.RestoreSegment(name); err != nil {
			result.Missing = append(result.Missing, seq)
			// Stop descending: everything below this is behind a hole, and admitting it would
			// need a floor that also admits the hole.
			break
		}
		result.Restored++
		lowest = seq
	}
	result.Took = time.Since(started)
	if result.Restored == 0 {
		result.FloorSequence = currentFloorOf(rt)
		return result, fmt.Errorf(
			"kdb: the archive produced none of segments %d-%d for namespace %s",
			from, to, rt.DefaultNamespace)
	}

	// Size what came back, now that it is local.
	if r := rt.deltaReader; r != nil {
		if segments, err := r.ListSegments(); err == nil {
			for _, s := range segments {
				if s.SequenceNumber >= lowest && s.SequenceNumber <= to {
					result.RestoredBytes += s.SizeBytes
				}
			}
		}
	}

	// Segments are on disk and verified present; only now is it safe to claim them.
	floor, err := rt.lowerCheckpointFloor(lowest)
	if err != nil {
		return result, fmt.Errorf(
			"kdb: segments %d-%d were restored to disk but the checkpoint floor could not be "+
				"lowered to admit them, so they will still be ignored: %w", lowest, to, err)
	}
	result.FloorSequence = floor
	return result, nil
}

// ArchiveAvailable reports whether this namespace keeps what it reclaims, which is what decides
// whether a compaction is eviction or destruction. Worth showing next to any offer to compact.
func (rt *EmbeddedKdbRuntime) ArchiveAvailable() bool {
	_, ok := rt.segmentRestorer()
	return ok
}

// segmentRestorer is this namespace's archive, if it has one.
func (rt *EmbeddedKdbRuntime) segmentRestorer() (interface{ RestoreSegment(string) error }, bool) {
	type archiveAware interface {
		HasArchive() bool
		RestoreSegment(string) error
	}
	shim, ok := rt.shim.(archiveAware)
	if !ok || !shim.HasArchive() {
		return nil, false
	}
	return shim, true
}

// lowerCheckpointFloor rewrites the checkpoint with a lower floor, so segments that were
// restored below the old one are read again.
//
// Only ever lowers. Raising the floor is truncation's job and has a whole ordering built around
// doing it safely; a restore that could also raise it would be a second, unguarded path to
// discarding segments.
func (rt *EmbeddedKdbRuntime) lowerCheckpointFloor(to int64) (int64, error) {
	d, ok := rt.DAG.(*PersistingCommitDAG)
	var graph *dag.InMemoryCommitDag
	if ok {
		graph = d.Delegate()
	} else if plain, isPlain := rt.DAG.(*dag.InMemoryCommitDag); isPlain {
		graph = plain
	}
	if graph == nil || rt.deltaReader == nil || rt.shim == nil {
		return 0, fmt.Errorf("this runtime has no checkpoint to lower")
	}
	current := currentFloorOf(rt)
	if to >= current {
		// Already admitted; nothing to do, and saying so beats rewriting an identical checkpoint.
		return current, nil
	}
	through := highestSegmentSequence(rt.deltaReader)
	if err := saveCheckpoint(graph, rt.Storage, rt.deltaReader, rt.shim, rt.DefaultNamespace,
		through, to, false); err != nil {
		return current, err
	}
	return to, nil
}

// currentFloorOf is the lowest segment sequence this namespace currently has on disk.
func currentFloorOf(rt *EmbeddedKdbRuntime) int64 {
	if rt.deltaReader == nil {
		return 0
	}
	return currentFloor(rt.deltaReader)
}

var _ = storage.HistoryModeNone
