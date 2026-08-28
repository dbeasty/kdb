package wal_test

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/io"
	"github.com/limidus/kdb/go/kdb/storage/wal"
)

func TestAppendAndRecover_roundTrip(t *testing.T) {
	shim := io.NewInMemoryPlatformIO()
	cfg := storage.StorageEngineConfig{GlobalMemoryBudgetBytes: 1_000_000, IOShim: shim}
	w, err := (&wal.DefaultFactory{}).OpenOrCreate("ns1", cfg, shim)
	if err != nil {
		t.Fatal(err)
	}
	sum := document.SHA256Digest([]byte{1, 2, 3})
	hash, err := codec.HashFromBytes(sum)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(wal.Record{
		Timestamp: codec.TimestampNow(),
		Kind:      wal.RecordKindPutBlob,
		Payload:   wal.EncodePutBlob(wal.PutBlob{ContentHash: hash, Bytes: []byte{1, 2, 3}}),
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	count := 0
	summary, err := w.Recover(func(wal.Record) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if summary.RecordsReplayed != 1 {
		t.Fatalf("recordsReplayed=%d want 1", summary.RecordsReplayed)
	}
	if count != 1 {
		t.Fatalf("handler count=%d want 1", count)
	}
}

// putRecord builds a PutBlob record whose payload is padded out to roughly payloadBytes, so a
// test can drive segment rotation with a handful of appends.
func putRecord(t *testing.T, payloadBytes int) wal.Record {
	t.Helper()
	body := make([]byte, payloadBytes)
	for i := range body {
		body[i] = byte(i)
	}
	hash, err := codec.HashFromBytes(document.SHA256Digest(body))
	if err != nil {
		t.Fatal(err)
	}
	return wal.Record{
		Timestamp: codec.TimestampNow(),
		Kind:      wal.RecordKindPutBlob,
		Payload:   wal.EncodePutBlob(wal.PutBlob{ContentHash: hash, Bytes: body}),
	}
}

func openWAL(t *testing.T, shim storage.PlatformIOShim, maxSegmentBytes int64) wal.WriteAheadLog {
	t.Helper()
	cfg := storage.StorageEngineConfig{GlobalMemoryBudgetBytes: 1_000_000, IOShim: shim}
	w, err := (&wal.DefaultFactory{WalMaxSegmentBytes: maxSegmentBytes}).OpenOrCreate("ns-rot", cfg, shim)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// TestSegmentRotationAtSizeCap is TC-05 from docs/kdb-spec-layer4a-component10a-wal.md:
// appending past walMaxSegmentBytes opens a new active segment, and recovery replays every
// segment in sequence order. Before rotation existed, walMaxSegmentBytes was never read and
// one segment grew forever.
func TestSegmentRotationAtSizeCap(t *testing.T) {
	shim := io.NewInMemoryPlatformIO()
	w := openWAL(t, shim, 4096)

	const appends = 12
	for i := 0; i < appends; i++ {
		if _, err := w.Append(putRecord(t, 1024)); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if got := w.ActiveSegmentSizeBytes(); got > 4096 {
		t.Fatalf("active segment grew past the cap: %d > 4096", got)
	}
	names, err := shim.ListSegments("ns-rot")
	if err != nil {
		t.Fatal(err)
	}
	if len(names) < 2 {
		t.Fatalf("expected the WAL to have rotated into several segments, got %v", names)
	}

	var seen []int64
	summary, err := w.Recover(func(r wal.Record) error {
		seen = append(seen, r.Sequence)
		return nil
	})
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if summary.RecordsReplayed != appends {
		t.Fatalf("recordsReplayed=%d want %d", summary.RecordsReplayed, appends)
	}
	if summary.SegmentsScanned != len(names) {
		t.Fatalf("segmentsScanned=%d want %d", summary.SegmentsScanned, len(names))
	}
	for i, seq := range seen {
		if seq != int64(i+1) {
			t.Fatalf("replay order: record %d had sequence %d, want %d", i, seq, i+1)
		}
	}
	if summary.LastSequence != appends {
		t.Fatalf("lastSequence=%d want %d", summary.LastSequence, appends)
	}
}

// TestReopenFindsTheWholeSegmentChain confirms a re-opened WAL picks up every rotated segment
// (and its active segment's real size), rather than only the one file it happened to name.
func TestReopenFindsTheWholeSegmentChain(t *testing.T) {
	shim := io.NewInMemoryPlatformIO()
	w := openWAL(t, shim, 4096)
	for i := 0; i < 12; i++ {
		if _, err := w.Append(putRecord(t, 1024)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	sizeBefore := w.ActiveSegmentSizeBytes()

	reopened := openWAL(t, shim, 4096)
	if got := reopened.ActiveSegmentSizeBytes(); got != sizeBefore {
		t.Fatalf("re-opened active segment size = %d, want %d", got, sizeBefore)
	}
	count := 0
	summary, err := reopened.Recover(func(wal.Record) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 12 || summary.LastSequence != 12 {
		t.Fatalf("recovered %d records (lastSequence=%d), want 12", count, summary.LastSequence)
	}
	// Appends continue from the recovered sequence, in the segment the chain ended on.
	res, err := reopened.Append(putRecord(t, 16))
	if err != nil {
		t.Fatal(err)
	}
	if res.Sequence != 13 {
		t.Fatalf("append after reopen got sequence %d, want 13", res.Sequence)
	}
}

// TestTruncateDropsCoveredSegments is TC-04: truncating through the last sequence clears the
// WAL's bytes - including the sealed segments rotation left behind - while sequence numbering
// continues from where it was.
func TestTruncateDropsCoveredSegments(t *testing.T) {
	shim := io.NewInMemoryPlatformIO()
	w := openWAL(t, shim, 4096)
	for i := 0; i < 12; i++ {
		if _, err := w.Append(putRecord(t, 1024)); err != nil {
			t.Fatal(err)
		}
	}
	before, err := shim.ListSegments("ns-rot")
	if err != nil {
		t.Fatal(err)
	}
	if len(before) < 2 {
		t.Fatalf("expected rotation before truncate, got %v", before)
	}

	if err := w.Truncate(w.LastSequence()); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	after, err := shim.ListSegments("ns-rot")
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 1 {
		t.Fatalf("expected one empty segment after a full truncate, got %v", after)
	}
	replayed := 0
	if _, err := w.Recover(func(wal.Record) error {
		replayed++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if replayed != 0 {
		t.Fatalf("expected nothing left to replay, got %d records", replayed)
	}
	res, err := w.Append(putRecord(t, 16))
	if err != nil {
		t.Fatal(err)
	}
	if res.Sequence != 13 {
		t.Fatalf("append after truncate got sequence %d, want 13 (numbering must not restart)", res.Sequence)
	}
}

// TestTruncateKeepsUncoveredSegments confirms a partial truncate only removes segments whose
// records are wholly below the truncate point.
func TestTruncateKeepsUncoveredSegments(t *testing.T) {
	shim := io.NewInMemoryPlatformIO()
	w := openWAL(t, shim, 4096)
	for i := 0; i < 12; i++ {
		if _, err := w.Append(putRecord(t, 1024)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Truncate(2); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	var seen []int64
	if _, err := w.Recover(func(r wal.Record) error {
		seen = append(seen, r.Sequence)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(seen) == 0 {
		t.Fatal("a truncate below the last sequence must not empty the WAL")
	}
	for _, seq := range seen {
		if seq > 12 {
			t.Fatalf("unexpected sequence %d", seq)
		}
	}
	// Records 1..2 may or may not survive depending on where the first rotation fell, but
	// nothing above the truncate point may be missing.
	for want := int64(4); want <= 12; want++ {
		found := false
		for _, seq := range seen {
			if seq == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("sequence %d was above the truncate point but is gone; replayed %v", want, seen)
		}
	}
}

// TestBatchStaysInOneSegment covers the atomicity requirement in the WAL spec's segment
// section: a batch is replayed all-or-nothing, so it must not straddle a rotation.
func TestBatchStaysInOneSegment(t *testing.T) {
	shim := io.NewInMemoryPlatformIO()
	w := openWAL(t, shim, 4096)
	if _, err := w.Append(putRecord(t, 3000)); err != nil {
		t.Fatal(err)
	}
	batch := []wal.Record{putRecord(t, 512), putRecord(t, 512), putRecord(t, 512)}
	res, err := w.AppendBatch(batch)
	if err != nil {
		t.Fatalf("appendBatch: %v", err)
	}
	if res.Sequence != 4 {
		t.Fatalf("batch last sequence = %d, want 4", res.Sequence)
	}
	// The batch (~1.6 KiB) could not fit behind the 3 KiB record under a 4 KiB cap, so it
	// rotated first and now sits alone in the active segment.
	if got := w.ActiveSegmentSizeBytes(); got > 4096 {
		t.Fatalf("active segment past the cap after a batch: %d", got)
	}
	count := 0
	if _, err := w.Recover(func(wal.Record) error {
		count++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 4 {
		t.Fatalf("recovered %d records, want 4", count)
	}
}
