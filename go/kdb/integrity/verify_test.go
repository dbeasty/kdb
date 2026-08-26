package integrity

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/storage"
)

func TestVerifyCleanLogPassesAllLevels(t *testing.T) {
	shim := newTestShim(t)
	const ns = "ns1"
	commits := buildChain(t, 3, ns)
	appendSegment(t, shim, ns, 0, rawFrame(t, commits[0]), rawFrame(t, commits[1]), rawFrame(t, commits[2]))

	report, err := Verify(shim, ns, Options{Level: L2, Compression: storage.CompressionNone})
	if err != nil {
		t.Fatal(err)
	}
	if !report.Clean() {
		t.Fatalf("expected clean report, got findings: %+v", report.Findings)
	}
	if len(report.Segments) != 1 || report.Segments[0].FrameCount != 3 {
		t.Fatalf("expected 1 segment with 3 frames, got %+v", report.Segments)
	}
}

func TestVerifyDetectsMidLogCorruption(t *testing.T) {
	shim := newTestShim(t)
	const ns = "ns1"
	commits := buildChain(t, 3, ns) // c0 <- c1 <- c2
	appendSegment(t, shim, ns, 0, rawFrame(t, commits[0]), flippedFrame(t, commits[1]))
	appendSegment(t, shim, ns, 1, rawFrame(t, commits[2]))

	report, err := Verify(shim, ns, Options{Level: L2, Compression: storage.CompressionNone})
	if err != nil {
		t.Fatal(err)
	}
	if report.Clean() {
		t.Fatal("expected findings, got a clean report")
	}
	if !hasFinding(report, MidLogCorruption, 0) {
		t.Errorf("expected mid_log_corruption at segment 0, got %+v", report.Findings)
	}
	if !hasFindingWithHash(report, MissingParent, commits[1].Hash.Hex()) {
		t.Errorf("expected missing_parent for the corrupted commit %s, got %+v", commits[1].Hash.Hex(), report.Findings)
	}
}

func TestVerifyCrcMismatchOnLastSegmentIsTornTail(t *testing.T) {
	shim := newTestShim(t)
	const ns = "ns1"
	commits := buildChain(t, 2, ns)
	// Single segment, so any corruption in it is corruption in the last
	// segment - kdb-spec-layer13 §4.3 classifies this as a torn tail
	// regardless of *why* the CRC failed, not just outright truncation.
	appendSegment(t, shim, ns, 0, rawFrame(t, commits[0]), flippedFrame(t, commits[1]))

	report, err := Verify(shim, ns, Options{Level: L1, Compression: storage.CompressionNone})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Findings) != 1 || report.Findings[0].Classification != TornTail {
		t.Fatalf("expected exactly one torn_tail finding, got %+v", report.Findings)
	}
}

func TestVerifyDetectsTornTail(t *testing.T) {
	shim := newTestShim(t)
	const ns = "ns1"
	commits := buildChain(t, 2, ns)
	full := rawFrame(t, commits[0])
	torn := rawFrame(t, commits[1])
	torn = torn[:len(torn)-5] // cut short: declared length no longer fits
	appendSegment(t, shim, ns, 0, full, torn)

	report, err := Verify(shim, ns, Options{Level: L2, Compression: storage.CompressionNone})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Findings) != 1 {
		t.Fatalf("expected exactly one finding, got %+v", report.Findings)
	}
	f := report.Findings[0]
	if f.Classification != TornTail || f.Offset != len(full) {
		t.Fatalf("expected torn_tail at offset %d, got %+v", len(full), f)
	}
	if report.Segments[0].FrameCount != 1 {
		t.Fatalf("expected only the clean frame counted, got %+v", report.Segments[0])
	}
}

func TestVerifyDetectsSequenceGap(t *testing.T) {
	shim := newTestShim(t)
	const ns = "ns1"
	commits := buildChain(t, 2, ns)
	appendSegment(t, shim, ns, 0, rawFrame(t, commits[0]))
	appendSegment(t, shim, ns, 2, rawFrame(t, commits[1])) // sequence 1 is missing

	report, err := Verify(shim, ns, Options{Level: L1, Compression: storage.CompressionNone})
	if err != nil {
		t.Fatal(err)
	}
	if !hasFinding(report, SequenceGap, 2) {
		t.Fatalf("expected sequence_gap at segment 2, got %+v", report.Findings)
	}
}

func TestVerifyNeverMutatesOnDisk(t *testing.T) {
	shim := newTestShim(t)
	const ns = "ns1"
	commits := buildChain(t, 2, ns)
	appendSegment(t, shim, ns, 0, rawFrame(t, commits[0]), rawFrame(t, commits[1]))

	before, err := shim.ReadFromSegment("ns/"+ns+"/delta/00000000000000000000.seg", 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(shim, ns, Options{Level: L2, Compression: storage.CompressionNone}); err != nil {
		t.Fatal(err)
	}
	after, err := shim.ReadFromSegment("ns/"+ns+"/delta/00000000000000000000.seg", 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("Verify must not mutate segment bytes")
	}
}

func hasFinding(report *Report, class Classification, segment int64) bool {
	for _, f := range report.Findings {
		if f.Classification == class && f.Segment == segment {
			return true
		}
	}
	return false
}

func hasFindingWithHash(report *Report, class Classification, hash string) bool {
	for _, f := range report.Findings {
		if f.Classification == class && f.CommitHash == hash {
			return true
		}
	}
	return false
}
