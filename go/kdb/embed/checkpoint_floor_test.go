package embed

import (
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
)

// fakeReader lists exactly the segments it is given.
type fakeReader struct{ segs []storage.DeltaSegmentRef }

func (f fakeReader) NamespaceID() string { return "ns" }
func (f fakeReader) ReadAll(storage.DeltaSegmentRef) ([]storage.DeltaRecord, error) {
	return nil, nil
}
func (f fakeReader) ReadRange(storage.DeltaSegmentRef, codec.Hash, codec.Hash) ([]storage.DeltaRecord, error) {
	return nil, nil
}
func (f fakeReader) ListSegments() ([]storage.DeltaSegmentRef, error) { return f.segs, nil }

func ref(seq int64, size int64, last byte) storage.DeltaSegmentRef {
	var h codec.Hash
	h.Bytes[0] = last
	return storage.DeltaSegmentRef{SequenceNumber: seq, SizeBytes: size, LastCommitHash: h}
}

func cpSeg(seq int64, size int64, last byte) checkpointSegment {
	var h codec.Hash
	h.Bytes[0] = last
	return checkpointSegment{Sequence: seq, SizeBytes: size, LastCommitHash: h}
}

// The guard has to tell a deliberately truncated log from a damaged one.
// Too strict and a truncated namespace replays a log it no longer has in
// full; too loose and real damage opens cleanly with data silently gone.
// These are the four cases that distinction consists of.

func TestSegmentsMissingBelowTheFloorAreExpected(t *testing.T) {
	cp := namespaceCheckpoint{
		ThroughSequence: 4,
		FloorSequence:   3,
		Segments: []checkpointSegment{
			cpSeg(0, 100, 0), cpSeg(1, 100, 1), cpSeg(2, 100, 2),
			cpSeg(3, 100, 3), cpSeg(4, 100, 4),
		},
	}
	// Only 3 and 4 remain, which is exactly what truncating to floor 3
	// leaves behind.
	r := fakeReader{segs: []storage.DeltaSegmentRef{ref(3, 100, 3), ref(4, 100, 4)}}
	if ok, why := checkpointMatchesLog(cp, r); !ok {
		t.Fatalf("a correctly truncated log was rejected: %s", why)
	}
}

func TestASegmentMissingAtOrAboveTheFloorIsDamage(t *testing.T) {
	cp := namespaceCheckpoint{
		ThroughSequence: 4,
		FloorSequence:   3,
		Segments:        []checkpointSegment{cpSeg(3, 100, 3), cpSeg(4, 100, 4)},
	}
	// Segment 3 is at the floor and gone: nothing deleted it on purpose.
	r := fakeReader{segs: []storage.DeltaSegmentRef{ref(4, 100, 4)}}
	ok, why := checkpointMatchesLog(cp, r)
	if ok {
		t.Fatal("a segment missing at the floor was accepted as a deliberate truncation")
	}
	if !strings.Contains(why, "3") {
		t.Fatalf("the reason should name the missing segment: %q", why)
	}
}

// The crash window: the checkpoint is written claiming a floor, and the
// process dies before the deletions happen. The extra old segments are
// harmless and the namespace must open.
func TestExtraSegmentsBelowTheFloorAreHarmless(t *testing.T) {
	cp := namespaceCheckpoint{
		ThroughSequence: 4,
		FloorSequence:   3,
		Segments: []checkpointSegment{
			cpSeg(0, 100, 0), cpSeg(1, 100, 1), cpSeg(2, 100, 2),
			cpSeg(3, 100, 3), cpSeg(4, 100, 4),
		},
	}
	// Nothing was actually deleted yet.
	r := fakeReader{segs: []storage.DeltaSegmentRef{
		ref(0, 100, 0), ref(1, 100, 1), ref(2, 100, 2), ref(3, 100, 3), ref(4, 100, 4),
	}}
	if ok, why := checkpointMatchesLog(cp, r); !ok {
		t.Fatalf("a crash between checkpoint and deletion must be harmless: %s", why)
	}
	// And so is a partial deletion, which is where a crash mid-delete
	// leaves things.
	r = fakeReader{segs: []storage.DeltaSegmentRef{ref(1, 100, 1), ref(3, 100, 3), ref(4, 100, 4)}}
	if ok, why := checkpointMatchesLog(cp, r); !ok {
		t.Fatalf("a partial deletion below the floor must be harmless: %s", why)
	}
}

// A namespace that has never truncated has floor 0, and every one of its
// segments must still be checked - which is the behaviour every
// history=full namespace depends on and the one this change could most
// easily have broken.
func TestAZeroFloorChecksEverySegment(t *testing.T) {
	cp := namespaceCheckpoint{
		ThroughSequence: 2,
		FloorSequence:   0,
		Segments:        []checkpointSegment{cpSeg(0, 100, 0), cpSeg(1, 100, 1), cpSeg(2, 100, 2)},
	}
	r := fakeReader{segs: []storage.DeltaSegmentRef{ref(1, 100, 1), ref(2, 100, 2)}}
	if ok, _ := checkpointMatchesLog(cp, r); ok {
		t.Fatal("with floor 0 a missing segment 0 is damage and must be reported")
	}
	// A changed segment is damage too, floor or no floor.
	r = fakeReader{segs: []storage.DeltaSegmentRef{ref(0, 999, 0), ref(1, 100, 1), ref(2, 100, 2)}}
	if ok, why := checkpointMatchesLog(cp, r); ok {
		t.Fatal("a resized segment must be reported as damage")
	} else if !strings.Contains(why, "bytes") {
		t.Fatalf("the reason should describe the size change: %q", why)
	}
	r = fakeReader{segs: []storage.DeltaSegmentRef{ref(0, 100, 9), ref(1, 100, 1), ref(2, 100, 2)}}
	if ok, _ := checkpointMatchesLog(cp, r); ok {
		t.Fatal("a segment ending at a different commit must be reported as damage")
	}
}
