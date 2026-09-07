package interop

import (
	"bytes"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/compression"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/io"
	"github.com/limidus/kdb/go/kdb/storage/wal"
)

// WAL half of the physical-layer conformance suite - test plan §4.1-§4.3, cases W1-W19. The
// Kotlin counterpart is kdb-storage-wal's WalPhysicalGoldenTest, and the two exchange fixtures
// through go/testdata/golden/physical/.

func fixtureWalRecords(t *testing.T) []wal.Record {
	t.Helper()
	h7 := fixtureHash(t, 7)
	return []wal.Record{
		{Sequence: 1, Timestamp: codec.TimestampFromEpochMicros(fixtureEpochMicros), Kind: wal.RecordKindPutBlob, Payload: fixturePayload},
		{Sequence: 2, Timestamp: codec.TimestampFromEpochMicros(fixtureEpochMicros), Kind: wal.RecordKindDeleteBlob, Payload: h7.Bytes[:]},
		{Sequence: 3, Timestamp: codec.TimestampFromEpochMicros(fixtureEpochMicros), Kind: wal.RecordKindFlushCheckpoint, Payload: []byte{}},
		{Sequence: 4, Timestamp: codec.TimestampFromEpochMicros(fixtureNegEpochMicros), Kind: wal.RecordKindMarker, Payload: []byte{0x4B, 0x44, 0x42, 0x50}},
	}
}

func encodeFixtureWalStream(t *testing.T) []byte {
	t.Helper()
	var out []byte
	for _, r := range fixtureWalRecords(t) {
		out = append(out, wal.EncodeRecord(r)...)
	}
	return out
}

// W1: the frame magic is the whole compatibility gate - a mismatch means neither runtime can
// read a single byte of the other's log. Go sat on 0x4B444257 (the pre-timestamp v1 format)
// while Kotlin had already moved to v2.
func TestWalMagicMatchesKotlin(t *testing.T) {
	if wal.Magic != 0x4B444358 {
		t.Fatalf("wal.Magic = %#x, want %#x (Kotlin WalCodec.MAGIC)", wal.Magic, 0x4B444358)
	}
	if wal.BatchMagic != 0x4B444242 {
		t.Fatalf("wal.BatchMagic = %#x, want %#x", wal.BatchMagic, 0x4B444242)
	}
}

// W2/W6: magic(4) + bodyLen(4) + header(21) + crc(4); an empty payload is 33 bytes exactly.
func TestWalEmptyPayloadFrameSize(t *testing.T) {
	frame := wal.EncodeRecord(wal.Record{Sequence: 1, Kind: wal.RecordKindPutBlob})
	if len(frame) != 33 {
		t.Fatalf("empty-payload frame = %d bytes, want 33", len(frame))
	}
}

// W3: publish Go's frames for Kotlin to decode and re-encode.
func TestExportPhysicalGoldenWal(t *testing.T) {
	writePhysicalGolden(t, "wal_records.hex", encodeFixtureWalStream(t))
	h := fixtureHash(t, 7)
	writePhysicalGolden(t, "wal_put_blob_payload.hex", wal.EncodePutBlob(wal.PutBlob{ContentHash: h, Bytes: fixturePayload}))
}

// W3/W4: Kotlin's frames must decode here to the same records - timestamps included, which the
// v1 format could not carry at all (replay fabricated one per record) - and re-encode to the
// identical bytes.
func TestKotlinWalGoldenRoundTrips(t *testing.T) {
	raw := readKotlinGolden(t, "wal_records.hex")
	if raw == nil {
		return
	}
	decoded, err := wal.DecodeRecords(raw, "ns", "seg", false)
	if err != nil {
		t.Fatalf("decoding Kotlin's WAL frames: %v", err)
	}
	if decoded.SkippedCorrupt != 0 {
		t.Fatalf("SkippedCorrupt = %d, want 0", decoded.SkippedCorrupt)
	}
	want := fixtureWalRecords(t)
	if len(decoded.Records) != len(want) {
		t.Fatalf("decoded %d records, want %d", len(decoded.Records), len(want))
	}
	for i, got := range decoded.Records {
		if got.Sequence != want[i].Sequence {
			t.Errorf("record %d sequence = %d, want %d", i, got.Sequence, want[i].Sequence)
		}
		if got.Timestamp.EpochMicros() != want[i].Timestamp.EpochMicros() {
			t.Errorf("record %d epochMicros = %d, want %d", i, got.Timestamp.EpochMicros(), want[i].Timestamp.EpochMicros())
		}
		if got.Kind != want[i].Kind {
			t.Errorf("record %d kind = %v, want %v", i, got.Kind, want[i].Kind)
		}
		if !bytes.Equal(got.Payload, want[i].Payload) {
			t.Errorf("record %d payload = %x, want %x", i, got.Payload, want[i].Payload)
		}
	}
	var re []byte
	for _, r := range decoded.Records {
		re = append(re, wal.EncodeRecord(r)...)
	}
	if !bytes.Equal(raw, re) {
		t.Fatalf("re-encode of Kotlin's frames is not byte-identical\nkotlin %x\ngo     %x", raw, re)
	}
}

// W7: hash(32) || bytes, no length prefix, on both sides.
func TestKotlinPutBlobPayloadLayout(t *testing.T) {
	raw := readKotlinGolden(t, "wal_put_blob_payload.hex")
	if raw == nil {
		return
	}
	want := wal.EncodePutBlob(wal.PutBlob{ContentHash: fixtureHash(t, 7), Bytes: fixturePayload})
	if !bytes.Equal(raw, want) {
		t.Fatalf("PutBlob payload layout differs\nkotlin %x\ngo     %x", raw, want)
	}
}

// W8: junk must be resynced past. Breaking out of the loop here - the old behavior - discarded
// every intact record after the first damaged byte in the segment.
func TestWalResyncsPastCorruptPrefix(t *testing.T) {
	good := wal.EncodeRecord(wal.Record{Sequence: 7, Kind: wal.RecordKindPutBlob, Payload: fixturePayload})
	stream := append(bytes.Repeat([]byte{0x5A}, 12), good...)
	decoded, err := wal.DecodeRecords(stream, "ns", "seg", true)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Records) != 1 || decoded.Records[0].Sequence != 7 {
		t.Fatalf("recovered %d records, want the one following the junk prefix", len(decoded.Records))
	}
	// W13: the skip has to be *counted*, not silently swallowed - RecordsSkippedCorrupt was
	// declared and never assigned, so it read zero however damaged the segment was.
	if decoded.SkippedCorrupt != 1 {
		t.Fatalf("SkippedCorrupt = %d, want 1", decoded.SkippedCorrupt)
	}
}

// W9: recordLen below the 21-byte header makes the payload length negative. Without this bound
// the decoder panicked with an out-of-range slice on any segment that reached it.
func TestWalRejectsRecordLenBelowHeader(t *testing.T) {
	const recordLen = 5
	total := 4 + 4 + recordLen + 4
	b := make([]byte, total)
	b[0], b[1], b[2], b[3] = 0x4B, 0x44, 0x43, 0x58 // wal.Magic
	b[7] = recordLen
	crc := compression.CRC32(b, 0, total-4)
	b[total-4], b[total-3], b[total-2], b[total-1] = byte(crc>>24), byte(crc>>16), byte(crc>>8), byte(crc)
	decoded, err := wal.DecodeRecords(b, "ns", "seg", true)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Records) != 0 {
		t.Fatalf("decoded %d records from a sub-header frame, want 0", len(decoded.Records))
	}
}

// W12: a truncated final frame is the ordinary shape of an unclean shutdown, not corruption.
func TestWalTornTailIsNotCountedAsCorrupt(t *testing.T) {
	a := wal.EncodeRecord(wal.Record{Sequence: 1, Kind: wal.RecordKindPutBlob, Payload: fixturePayload})
	b := wal.EncodeRecord(wal.Record{Sequence: 2, Kind: wal.RecordKindPutBlob, Payload: fixturePayload})
	decoded, err := wal.DecodeRecords(append(a, b[:len(b)-4]...), "ns", "seg", true)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Records) != 1 || decoded.SkippedCorrupt != 0 {
		t.Fatalf("got %d records / %d skipped, want 1 / 0", len(decoded.Records), decoded.SkippedCorrupt)
	}
}

// W14: strict mode reports the offset of the bad frame, and both runtimes report the same one.
func TestWalStrictModeReportsOffset(t *testing.T) {
	good := wal.EncodeRecord(wal.Record{Sequence: 1, Kind: wal.RecordKindPutBlob, Payload: fixturePayload})
	stream := append(good, bytes.Repeat([]byte{0x5A}, 12)...)
	_, err := wal.DecodeRecords(stream, "ns", "seg", false)
	ce, ok := err.(*wal.CorruptionError)
	if !ok {
		t.Fatalf("err = %v (%T), want *wal.CorruptionError", err, err)
	}
	if ce.Offset != int64(len(good)) {
		t.Fatalf("corruption offset = %d, want %d", ce.Offset, len(good))
	}
}

// W15/W16: the name builders must agree string-for-string with Kotlin's, or one runtime's
// segment chain is invisible - or unparseable - to the other.
func TestWalSegmentChainNamesMatchKotlin(t *testing.T) {
	walID, err := codec.UUIDFromString(fixtureWalIDString)
	if err != nil {
		t.Fatal(err)
	}
	f := &wal.DefaultFactory{}
	if got, want := f.ActiveSegmentName("p1", walID), "ns/p1/wal/"+fixtureWalIDString; got != want {
		t.Fatalf("active segment name = %q, want %q", got, want)
	}
	// Exercised through a real rotation rather than the unexported name builder, so the test
	// pins what actually lands on disk.
	shim := io.NewInMemoryPlatformIO()
	w, err := (&wal.DefaultFactory{WalMaxSegmentBytes: 64}).OpenOrCreate("p1", storageConfig(), shim)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		if _, err := w.Append(wal.Record{Kind: wal.RecordKindPutBlob, Payload: fixturePayload}); err != nil {
			t.Fatal(err)
		}
	}
	names := w.(*wal.DefaultWriteAheadLog).SegmentNames()
	if len(names) < 2 {
		t.Fatalf("no rotation happened; names = %v", names)
	}
	for _, n := range names[1:] {
		id, seq, ok := wal.ParseWalFileName(n[len("ns/p1/wal/"):])
		if !ok {
			t.Fatalf("rotated name %q does not parse - Kotlin's parser must accept it too", n)
		}
		if len(n) != len("ns/p1/wal/")+len(id)+1+20 {
			t.Fatalf("rotated name %q: sequence suffix must be zero-padded to 20 digits", n)
		}
		if seq < 2 {
			t.Fatalf("rotated segment %q starts at sequence %d, want >= 2", n, seq)
		}
	}
}

// W18: recovery must replay the whole chain, not just the newest segment.
func TestWalRecoverReplaysWholeChain(t *testing.T) {
	shim := io.NewInMemoryPlatformIO()
	w, err := (&wal.DefaultFactory{WalMaxSegmentBytes: 64}).OpenOrCreate("p1", storageConfig(), shim)
	if err != nil {
		t.Fatal(err)
	}
	const n = 8
	for i := 0; i < n; i++ {
		if _, err := w.Append(wal.Record{Kind: wal.RecordKindPutBlob, Payload: fixturePayload}); err != nil {
			t.Fatal(err)
		}
	}
	reopened, err := (&wal.DefaultFactory{WalMaxSegmentBytes: 64}).OpenOrCreate("p1", storageConfig(), shim)
	if err != nil {
		t.Fatal(err)
	}
	var seen []int64
	summary, err := reopened.Recover(func(r wal.Record) error {
		seen = append(seen, r.Sequence)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(seen) != n {
		t.Fatalf("replayed %d records across %d segments, want all %d", len(seen), summary.SegmentsScanned, n)
	}
	if summary.SegmentsScanned < 2 {
		t.Fatalf("SegmentsScanned = %d, want the whole chain", summary.SegmentsScanned)
	}
}

func storageConfig() storage.StorageEngineConfig {
	return storage.StorageEngineConfig{GlobalMemoryBudgetBytes: 1_000_000}
}
