package integrity

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

func TestRepairTornTailTruncatesAndQuarantines(t *testing.T) {
	shim := newTestShim(t)
	const ns = "ns1"
	commits := buildChain(t, 2, ns)
	full := rawFrame(t, commits[0])
	torn := rawFrame(t, commits[1])[:10]
	appendSegment(t, shim, ns, 0, full, torn)

	opts := Options{Level: L1, Compression: storage.CompressionNone}
	report, err := Verify(shim, ns, opts)
	if err != nil {
		t.Fatal(err)
	}
	result, err := Repair(shim, report, opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Steps) != 1 || result.Steps[0].Action != ActionTruncatedTornTail {
		t.Fatalf("expected one truncate step, got %+v", result.Steps)
	}
	if result.Steps[0].QuarantineName == "" {
		t.Fatal("expected a quarantine file name")
	}

	quarantined, err := shim.ReadFromSegment(result.Steps[0].QuarantineName, 0, 1<<20)
	if err != nil {
		t.Fatalf("quarantine file not readable: %v", err)
	}
	if string(quarantined) != string(torn) {
		t.Fatal("quarantined bytes must be exactly the removed torn tail")
	}

	report2, err := Verify(shim, ns, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !report2.Clean() {
		t.Fatalf("expected clean report after repair, got %+v", report2.Findings)
	}
	if report2.Segments[0].FrameCount != 1 {
		t.Fatalf("expected 1 frame surviving repair, got %+v", report2.Segments[0])
	}
}

func TestRepairIsIdempotent(t *testing.T) {
	shim := newTestShim(t)
	const ns = "ns1"
	commits := buildChain(t, 2, ns)
	appendSegment(t, shim, ns, 0, rawFrame(t, commits[0]), rawFrame(t, commits[1])[:10])

	opts := Options{Level: L1, Compression: storage.CompressionNone}
	report, err := Verify(shim, ns, opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Repair(shim, report, opts); err != nil {
		t.Fatal(err)
	}

	report2, err := Verify(shim, ns, opts)
	if err != nil {
		t.Fatal(err)
	}
	result2, err := Repair(shim, report2, opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(result2.Steps) != 0 {
		t.Fatalf("expected no-op on an already-repaired report, got %+v", result2.Steps)
	}
}

func TestRepairMidLogRemovesOnlyProvenSafeFrame(t *testing.T) {
	shim := newTestShim(t)
	const ns = "ns1"
	c0 := buildCommit(t, ns, nil)
	c1 := buildCommit(t, ns, &c0.Hash)  // kept: precedes the corrupt frame in its segment
	c1b := buildCommit(t, ns, &c0.Hash) // corrupted: a dead end nothing else references
	c2 := buildCommit(t, ns, &c0.Hash)  // independent, in a later segment

	appendSegment(t, shim, ns, 0, rawFrame(t, c0))
	appendSegment(t, shim, ns, 1, rawFrame(t, c1), flippedFrame(t, c1b))
	appendSegment(t, shim, ns, 2, rawFrame(t, c2))

	opts := Options{Level: L1, Compression: storage.CompressionNone}
	report, err := Verify(shim, ns, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !hasFinding(report, MidLogCorruption, 1) {
		t.Fatalf("expected mid_log_corruption at segment 1, got %+v", report.Findings)
	}

	result, err := Repair(shim, report, opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Steps) != 1 || result.Steps[0].Action != ActionRewroteSegmentPrefix {
		t.Fatalf("expected one prefix-rewrite step, got %+v", result.Steps)
	}

	report2, err := Verify(shim, ns, Options{Level: L2, Compression: storage.CompressionNone})
	if err != nil {
		t.Fatal(err)
	}
	if !report2.Clean() {
		t.Fatalf("expected clean report after safe repair, got %+v", report2.Findings)
	}
}

func TestRepairRefusesWhenClosureBreaks(t *testing.T) {
	shim := newTestShim(t)
	const ns = "ns1"
	c0 := buildCommit(t, ns, nil)
	c1 := buildCommit(t, ns, &c0.Hash) // corrupted, alone in its segment
	c2 := buildCommit(t, ns, &c1.Hash) // depends on the corrupted commit

	appendSegment(t, shim, ns, 0, rawFrame(t, c0))
	appendSegment(t, shim, ns, 1, flippedFrame(t, c1))
	appendSegment(t, shim, ns, 2, rawFrame(t, c2))

	opts := Options{Level: L1, Compression: storage.CompressionNone}
	report, err := Verify(shim, ns, opts)
	if err != nil {
		t.Fatal(err)
	}

	name := storio.SegmentNameBuilder.DeltaSequenced(ns, 1)
	before, err := shim.ReadFromSegment(name, 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}

	result, err := Repair(shim, report, opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Steps) != 1 || result.Steps[0].Action != ActionRefused {
		t.Fatalf("expected a refused step, got %+v", result.Steps)
	}
	if len(result.Steps[0].MissingHashes) != 1 || result.Steps[0].MissingHashes[0] != c1.Hash.Hex() {
		t.Fatalf("expected missing hash %s, got %+v", c1.Hash.Hex(), result.Steps[0].MissingHashes)
	}

	after, err := shim.ReadFromSegment(name, 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("a refused repair must not touch the segment")
	}
}
