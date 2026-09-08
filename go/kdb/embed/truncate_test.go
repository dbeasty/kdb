package embed

import (
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
)

// segmentsToDelete is the function whose mistakes delete data, so it is
// tested directly, at every boundary, without a filesystem in the way.

func seg(seq int64, hexByte byte) storage.DeltaSegmentRef {
	var h codec.Hash
	h.Bytes[0] = hexByte
	return storage.DeltaSegmentRef{SequenceNumber: seq, LastCommitHash: h, SizeBytes: 1000}
}

// datedAt gives every segment the same age, offset back from now by the
// segment's own sequence in hours: segment 0 is oldest.
func datedAt(now time.Time, count int64) func(codec.Hash) (time.Time, bool) {
	return func(h codec.Hash) (time.Time, bool) {
		seq := int64(h.Bytes[0])
		return now.Add(-time.Duration(count-seq) * time.Hour), true
	}
}

func sequences(refs []storage.DeltaSegmentRef) []int64 {
	out := make([]int64, len(refs))
	for i, r := range refs {
		out[i] = r.SequenceNumber
	}
	return out
}

func TestNothingAboveTheCheckpointIsEverDeleted(t *testing.T) {
	now := time.Now()
	segs := []storage.DeltaSegmentRef{seg(0, 0), seg(1, 1), seg(2, 2), seg(3, 3), seg(4, 4)}
	victims := segmentsToDelete(truncationInputs{
		segments:   segs,
		through:    2,
		now:        now,
		window:     storage.RetentionWindow{Duration: storage.RetainNothing},
		commitTime: datedAt(now, 5),
	})
	for _, v := range victims {
		if v.SequenceNumber > 2 {
			t.Fatalf("segment %d is above the checkpoint and must not be deleted", v.SequenceNumber)
		}
	}
	// Segments 3 and 4 are above the checkpoint and count as retained, so
	// with a zero window everything at or below it may go.
	if got := sequences(victims); len(got) != 3 {
		t.Fatalf("want segments 0-2 deleted, got %v", got)
	}
}

func TestAtLeastOneSegmentSurvivesAZeroWindow(t *testing.T) {
	now := time.Now()
	segs := []storage.DeltaSegmentRef{seg(0, 0), seg(1, 1), seg(2, 2)}
	victims := segmentsToDelete(truncationInputs{
		segments:   segs,
		through:    2, // everything is checkpointed, including the newest
		now:        now,
		window:     storage.RetentionWindow{Duration: storage.RetainNothing},
		commitTime: datedAt(now, 3),
	})
	if len(victims) != len(segs)-1 {
		t.Fatalf("want all but one deleted, got %v", sequences(victims))
	}
	for _, v := range victims {
		if v.SequenceNumber == 2 {
			t.Fatal("the newest segment must survive even a zero window")
		}
	}
}

func TestTheDurationWindowKeepsRecentSegments(t *testing.T) {
	now := time.Now()
	// Five segments, one hour apart: segment 4 is one hour old, segment 0
	// is five.
	segs := []storage.DeltaSegmentRef{seg(0, 0), seg(1, 1), seg(2, 2), seg(3, 3), seg(4, 4)}
	victims := segmentsToDelete(truncationInputs{
		segments:   segs,
		through:    4,
		now:        now,
		window:     storage.RetentionWindow{Duration: 3 * time.Hour},
		commitTime: datedAt(now, 5),
	})
	// Segments 4 (1h), 3 (2h) and 2 (3h... exactly at the boundary, which
	// is not younger than the window) - so 2, 1 and 0 go.
	got := sequences(victims)
	for _, v := range got {
		if v > 2 {
			t.Fatalf("segment %d is inside a 3h window and must be kept, got victims %v", v, got)
		}
	}
	if len(got) == 0 {
		t.Fatal("a 3h window over 5h of segments should reclaim something")
	}
}

func TestADefaultWindowKeepsEverythingRecent(t *testing.T) {
	now := time.Now()
	segs := []storage.DeltaSegmentRef{seg(0, 0), seg(1, 1), seg(2, 2)}
	victims := segmentsToDelete(truncationInputs{
		segments:   segs,
		through:    2,
		now:        now,
		window:     storage.RetentionWindow{}, // resolves to 24h
		commitTime: datedAt(now, 3),
	})
	if len(victims) != 0 {
		t.Fatalf("segments hours old must survive the 24h default, got %v", sequences(victims))
	}
}

// An undatable segment is one whose last commit the graph no longer knows.
// Its age is unknown, and an unknown age is not a licence to delete.
func TestAnUndatableSegmentIsKept(t *testing.T) {
	now := time.Now()
	segs := []storage.DeltaSegmentRef{seg(0, 0), seg(1, 1), seg(2, 2)}
	victims := segmentsToDelete(truncationInputs{
		segments:   segs,
		through:    2,
		now:        now,
		window:     storage.RetentionWindow{Duration: time.Nanosecond},
		commitTime: func(codec.Hash) (time.Time, bool) { return time.Time{}, false },
	})
	if len(victims) != 0 {
		t.Fatalf("segments of unknown age must be kept, got %v", sequences(victims))
	}
}

func TestTheCommitFloorKeepsSegmentsTheDurationWouldDrop(t *testing.T) {
	now := time.Now()
	segs := []storage.DeltaSegmentRef{seg(0, 0), seg(1, 1), seg(2, 2), seg(3, 3), seg(4, 4)}
	// Everything is old enough to go by duration, but a commit floor of
	// 5000 asks for five segments' worth to be kept.
	victims := segmentsToDelete(truncationInputs{
		segments:   segs,
		through:    4,
		now:        now,
		window:     storage.RetentionWindow{Duration: time.Nanosecond, Commits: 5000},
		commitTime: datedAt(now, 5),
	})
	if len(victims) != 0 {
		t.Fatalf("the commit floor should have kept everything, got %v", sequences(victims))
	}
}

func TestSegmentsForCommitsOnlyEverOverRetains(t *testing.T) {
	// The conversion is approximate; it must never round down to zero and
	// must never ask for fewer segments than a strict reading would.
	for _, commits := range []int64{1, 999, 1000, 1001, 10_000} {
		if got := segmentsForCommits(commits); got < 1 {
			t.Fatalf("%d commits asked for %d segments", commits, got)
		}
	}
	if segmentsForCommits(10_000) != 10 {
		t.Fatal("10k commits should ask for 10 segments at 1000 per segment")
	}
}
