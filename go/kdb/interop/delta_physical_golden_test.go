package interop

import (
	"bytes"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/compression"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/delta"
	"github.com/limidus/kdb/go/kdb/storage/io"
)

// Delta-segment half of the physical-layer conformance suite - test plan §4.5 (D1-D9) and §4.6
// (L1, L5). Kotlin counterpart: kdb-storage-delta's DeltaPhysicalGoldenTest.

// fixtureCommit is a commit with every field populated except SchemaHash, which stays nil - the
// case where the two encoders could most easily disagree, since Kotlin supplies an explicit null
// that the schema default then omits while Go omits the field outright. If those diverged, the
// commit *hash* would differ and a Go-written commit would lose its identity on the JVM.
func fixtureCommit(t *testing.T) document.Commit {
	t.Helper()
	txID, err := codec.UUIDFromString("11111111-2222-3333-4444-555555555555")
	if err != nil {
		t.Fatal(err)
	}
	author, err := codec.UUIDFromString("66666666-7777-8888-9999-aaaaaaaaaaaa")
	if err != nil {
		t.Fatal(err)
	}
	c, err := document.BuildCommit(
		[]codec.Hash{fixtureHash(t, 1), fixtureHash(t, 2)},
		fixtureNamespaceID,
		txID,
		codec.TimestampFromEpochMicros(fixtureEpochMicros),
		author,
		nil,
		fixtureHash(t, 3),
		nil,
		"fixture commit 〰",
	)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// D1/D2: the KDBP header is 20 fixed-width bytes and, with CODEC_NONE, the whole frame is
// compressor-independent and therefore byte-comparable across languages.
func TestExportPhysicalGoldenDelta(t *testing.T) {
	c := fixtureCommit(t)
	payload, err := c.ToPayloadBytes()
	if err != nil {
		t.Fatal(err)
	}
	// D8: the commit payload itself - Layer 0 bytes, no framing.
	writePhysicalGolden(t, "commit_payload.hex", payload)
	// D9: and its hash, the commit's identity.
	writePhysicalGolden(t, "commit_hash.hex", c.Hash.Bytes[:])

	frame, err := delta.PageCodec{}.Frame(payload, storage.CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	writePhysicalGolden(t, "delta_frame_none.hex", frame)
}

// D8/D9: the commit payload and hash must be byte-identical. This is the strongest single claim
// in the suite - the hash is the commit's identity across the whole DAG.
func TestKotlinCommitPayloadAndHashMatch(t *testing.T) {
	payloadGolden := readKotlinGolden(t, "commit_payload.hex")
	if payloadGolden == nil {
		return
	}
	c := fixtureCommit(t)
	payload, err := c.ToPayloadBytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payloadGolden, payload) {
		t.Fatalf("commit payload differs\nkotlin %x\ngo     %x", payloadGolden, payload)
	}
	hashGolden := readKotlinGolden(t, "commit_hash.hex")
	if hashGolden != nil && !bytes.Equal(hashGolden, c.Hash.Bytes[:]) {
		t.Fatalf("commit hash differs\nkotlin %x\ngo     %x", hashGolden, c.Hash.Bytes[:])
	}
	// And Kotlin's own bytes must decode here to the same commit, hash included.
	decoded, err := document.FromPayloadBytes(payloadGolden)
	if err != nil {
		t.Fatalf("decoding Kotlin's commit payload: %v", err)
	}
	if decoded.Hash != c.Hash {
		t.Fatalf("decoded hash %s, want %s", decoded.Hash.Hex(), c.Hash.Hex())
	}
}

// D2: a CODEC_NONE frame carries no compressor output, so it is byte-comparable end to end.
func TestKotlinDeltaUncompressedFrameMatches(t *testing.T) {
	golden := readKotlinGolden(t, "delta_frame_none.hex")
	if golden == nil {
		return
	}
	c := fixtureCommit(t)
	payload, err := c.ToPayloadBytes()
	if err != nil {
		t.Fatal(err)
	}
	frame, err := delta.PageCodec{}.Frame(payload, storage.CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(golden, frame) {
		t.Fatalf("uncompressed KDBP frame differs\nkotlin %x\ngo     %x", golden, frame)
	}
}

// D1: the header layout itself - magic, version, codec id, both lengths, CRC - at fixed offsets.
func TestDeltaFrameHeaderLayout(t *testing.T) {
	payload := []byte("kdbp frame body")
	frame, err := delta.PageCodec{}.Frame(payload, storage.CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(frame[0:4]); got != "KDBP" {
		t.Fatalf("magic = %q, want KDBP", got)
	}
	if frame[4] != delta.PageFormatVersion {
		t.Fatalf("version = %d, want %d", frame[4], delta.PageFormatVersion)
	}
	if frame[5] != 0 {
		t.Fatalf("codec id = %d, want 0 (none)", frame[5])
	}
	if frame[6] != 0 || frame[7] != 0 {
		t.Fatal("reserved u16 must be zero")
	}
	if got := be32(frame, 8); got != len(payload) {
		t.Fatalf("compressed length = %d, want %d", got, len(payload))
	}
	if got := be32(frame, 12); got != len(payload) {
		t.Fatalf("uncompressed length = %d, want %d", got, len(payload))
	}
	if got, want := uint32(be32(frame, 16)), compression.CRC32All(payload); got != want {
		t.Fatalf("body crc = %08x, want %08x", got, want)
	}
	if len(frame) != delta.PageFrameHeaderSize+len(payload) {
		t.Fatalf("frame = %d bytes, want %d", len(frame), delta.PageFrameHeaderSize+len(payload))
	}
}

// D4: a codec id neither side knows must be an error, never a guess.
func TestDeltaUnknownCodecIsRejected(t *testing.T) {
	frame, err := delta.PageCodec{}.Frame([]byte("x"), storage.CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	frame[5] = 0x7F
	if _, err := (delta.PageCodec{}).Parse(frame); err == nil {
		t.Fatal("an unknown codec id must be rejected")
	}
}

// D6: a truncated trailing frame ends the scan cleanly - the ordinary shape of an unclean
// shutdown, not corruption.
func TestDeltaTornTailStopsCleanly(t *testing.T) {
	frames := fixtureDeltaSegment(t)
	commits, err := delta.ScanSegmentBytes(frames[:len(frames)-4])
	if err != nil {
		t.Fatalf("a torn tail must not be an error: %v", err)
	}
	if len(commits) != 1 {
		t.Fatalf("scanned %d commits before the torn frame, want 1", len(commits))
	}
}

// D7: a CRC mismatch on an otherwise complete frame is corruption, reported at its offset, with
// the commits that scanned cleanly before it still available.
func TestDeltaCrcMismatchReportsOffsetAndPartials(t *testing.T) {
	frames := fixtureDeltaSegment(t)
	first, err := delta.PageCodec{}.Frame(mustPayload(t), storage.CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), frames...)
	corrupt[len(first)+delta.PageFrameHeaderSize] ^= 0xFF
	commits, err := delta.ScanSegmentBytes(corrupt)
	cfe, ok := err.(*delta.CorruptFrameError)
	if !ok {
		t.Fatalf("err = %v (%T), want *delta.CorruptFrameError", err, err)
	}
	if cfe.Offset != len(first) {
		t.Fatalf("corrupt frame reported at offset %d, want %d", cfe.Offset, len(first))
	}
	if len(commits) != 1 {
		t.Fatalf("partial commits = %d, want the 1 that scanned cleanly before the damage", len(commits))
	}
}

// D5/L1: the segment path builders must agree with Kotlin's string-for-string, and the parser
// must reject the pre-Layer-13 random-UUID names rather than guessing at their order.
func TestSegmentNamesMatchKotlin(t *testing.T) {
	nb := io.SegmentNameBuilder
	for _, tc := range []struct{ got, want string }{
		{nb.DeltaSequenced("ns1", 42), "ns/ns1/delta/00000000000000000042.seg"},
		{nb.Delta("ns1", "abc"), "ns/ns1/delta/abc"},
		{nb.WAL("ns1", "abc"), "ns/ns1/wal/abc"},
		{nb.SSTable("ns1", 3, "abc"), "ns/ns1/sstable/L3/abc"},
		{nb.NamespacePrefix("ns1"), "ns/ns1/"},
		{io.SnapshotKeyBuilder.Enlistment("e1"), "kdb:snap:e1"},
		{io.DeltaSequencedFileName(7), "00000000000000000007.seg"},
	} {
		if tc.got != tc.want {
			t.Errorf("segment name = %q, want %q", tc.got, tc.want)
		}
	}
	if _, ok := io.ParseDeltaSequencedFileName("f81d4fae-7dec-11d0-a765-00a0c91e6bf6.seg"); ok {
		t.Error("a legacy random-UUID delta name must not parse as a sequence")
	}
	if seq, ok := io.ParseDeltaSequencedFileName("00000000000000000042.seg"); !ok || seq != 42 {
		t.Errorf("ParseDeltaSequencedFileName = (%d, %v), want (42, true)", seq, ok)
	}
}

// L5: both runtimes reject the same unsafe segment names.
func TestSegmentNameValidationMatchesKotlin(t *testing.T) {
	for _, bad := range []string{"", "ns/../etc/passwd", "etc/passwd", "wal/x"} {
		if err := io.ValidateSegmentName(bad); err == nil {
			t.Errorf("ValidateSegmentName(%q) accepted an unsafe name", bad)
		}
	}
	if err := io.ValidateSegmentName("ns/ns1/wal/abc"); err != nil {
		t.Errorf("ValidateSegmentName rejected a valid name: %v", err)
	}
}

// P1: CRC-32 must agree bit-for-bit, including the canonical check value.
func TestCrc32MatchesKotlin(t *testing.T) {
	if got := compression.CRC32All(nil); got != 0 {
		t.Errorf("CRC32(empty) = %08x, want 0", got)
	}
	if got := compression.CRC32All([]byte("123456789")); got != 0xCBF43926 {
		t.Errorf(`CRC32("123456789") = %08x, want cbf43926 (the canonical CRC-32/ISO-HDLC check value)`, got)
	}
}

// P4: the JVM must decompress a klauspost frame that omits the content-size field, and an empty
// body. Both are shapes Go writes and upstream libzstd does not; the fixture is what lets the
// Kotlin side prove it.
func TestExportPhysicalGoldenZstd(t *testing.T) {
	body, err := compression.Compress([]byte("kdb zstd interop fixture payload, long enough to actually compress"), 3)
	if err != nil {
		t.Fatal(err)
	}
	writePhysicalGolden(t, "zstd_body.hex", body)
	empty, err := compression.Compress(nil, 3)
	if err != nil {
		t.Fatal(err)
	}
	writePhysicalGolden(t, "zstd_empty_body.hex", empty)
}

// C2 in this direction: Go must decompress whatever zstd-jni produced.
func TestKotlinZstdBodyCrossDecodes(t *testing.T) {
	body := readKotlinGolden(t, "zstd_body.hex")
	if body == nil {
		return
	}
	out, err := compression.Decompress(body, 1<<20)
	if err != nil {
		t.Fatalf("decompressing zstd-jni's frame: %v", err)
	}
	if string(out) != "kdb zstd interop fixture payload, long enough to actually compress" {
		t.Fatalf("cross-decoded to %q", out)
	}
	if empty := readKotlinGolden(t, "zstd_empty_body.hex"); empty != nil {
		out, err := compression.Decompress(empty, 1<<20)
		if err != nil {
			t.Fatalf("decompressing zstd-jni's empty frame: %v", err)
		}
		if len(out) != 0 {
			t.Fatalf("empty frame decoded to %d bytes", len(out))
		}
	}
}

func mustPayload(t *testing.T) []byte {
	t.Helper()
	c := fixtureCommit(t)
	p, err := c.ToPayloadBytes()
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// fixtureDeltaSegment is two identical CODEC_NONE frames back to back, so a test can damage or
// truncate the second and still expect the first to scan.
func fixtureDeltaSegment(t *testing.T) []byte {
	t.Helper()
	frame, err := delta.PageCodec{}.Frame(mustPayload(t), storage.CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	return append(append([]byte(nil), frame...), frame...)
}

func be32(b []byte, off int) int {
	return int(b[off])<<24 | int(b[off+1])<<16 | int(b[off+2])<<8 | int(b[off+3])
}
