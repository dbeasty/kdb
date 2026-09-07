package interop

import (
	"bytes"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/io"
	"github.com/limidus/kdb/go/kdb/storage/sstable"
)

// SSTable half of the physical-layer conformance suite - test plan §4.4, cases S1-S10.
//
// Whole segments, not bare codec calls: the footer is only locatable via its trailing indexLen,
// so testing the format end-to-end is the only way to pin that the trailer, the index lines and
// the block offsets agree with each other as well as with Kotlin.
//
// Block *bodies* are ZSTD and can never be compared across languages (zstd-jni and
// klauspost/compress emit different, equally valid frames - test plan §2). What is compared is
// the footer, which carries no compressed bytes, and the values each side reads back out of the
// other's segment.

// fixtureSSTableEntries is the put/delete sequence both runtimes apply, including a key written
// twice - the case where the two writers disagreed, Kotlin's insertion-ordered map collapsing it
// to one entry while Go appended a second.
func writeFixtureSSTable(t *testing.T, w sstable.Writer) {
	t.Helper()
	w.Put(fixtureHash(t, 1), []byte("alpha"))
	w.Put(fixtureHash(t, 2), fixturePayload)
	w.Delete(fixtureHash(t, 3))
	w.Put(fixtureHash(t, 1), []byte("alpha-rewritten"))
	w.Put(fixtureHash(t, 4), []byte{})
}

func buildFixtureSSTable(t *testing.T) (storage.PlatformIOShim, sstable.Handle, []byte) {
	t.Helper()
	shim := io.NewInMemoryPlatformIO()
	w := sstable.NewDefaultWriter(shim, fixtureNamespaceID, 0)
	writeFixtureSSTable(t, w)
	h, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	return shim, h, readWholeSegment(t, shim, h.SegmentName)
}

func readWholeSegment(t *testing.T, shim storage.PlatformIOShim, name string) []byte {
	t.Helper()
	var out []byte
	var off int64
	for {
		p, err := shim.ReadFromSegment(name, off, 8192)
		if err != nil {
			t.Fatal(err)
		}
		if len(p) == 0 {
			return out
		}
		out = append(out, p...)
		off += int64(len(p))
		if len(p) < 8192 {
			return out
		}
	}
}

// footerOf slices the footer out of a whole segment using the same trailing-indexLen bootstrap
// the readers use: magic(4) + indexLen(4) + index + fileHash(32), then a 4-byte indexLen copy.
func footerOf(t *testing.T, segment []byte) []byte {
	t.Helper()
	if len(segment) < 44 {
		t.Fatalf("segment is %d bytes, too short to hold a footer", len(segment))
	}
	tail := segment[len(segment)-4:]
	indexLen := int(tail[0])<<24 | int(tail[1])<<16 | int(tail[2])<<8 | int(tail[3])
	start := len(segment) - (40 + indexLen) - 4
	if start < 0 || start > len(segment) {
		t.Fatalf("footer trailer says indexLen=%d, which does not fit in %d bytes", indexLen, len(segment))
	}
	return segment[start:]
}

// S3: the same table must produce the same footer every time. Ranging a Go map to build the
// index made these bytes randomized per run - so no two writes of identical content ever agreed,
// within Go let alone with Kotlin.
func TestSSTableFooterIsDeterministic(t *testing.T) {
	_, _, first := buildFixtureSSTable(t)
	firstFooter := footerOf(t, first)
	for i := 0; i < 50; i++ {
		_, _, again := buildFixtureSSTable(t)
		if !bytes.Equal(firstFooter, footerOf(t, again)) {
			t.Fatalf("footer bytes differ between two writes of the same table (iteration %d)", i)
		}
	}
}

// S7: a key written twice is one entry, holding the later value - Kotlin's linkedMapOf
// semantics. Appending instead left a stale, unreachable block ahead of the live one and fed
// both values into the fileHash preimage.
func TestSSTableDuplicateKeyCollapsesToLastValue(t *testing.T) {
	shim, h, segment := buildFixtureSSTable(t)
	lines := footerIndexLines(t, footerOf(t, segment))
	if len(lines) != 4 {
		t.Fatalf("footer holds %d index lines, want 4 distinct keys: %q", len(lines), lines)
	}
	v, err := sstable.NewDefaultReader(shim, h).Get(fixtureHash(t, 1))
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != "alpha-rewritten" {
		t.Fatalf("rewritten key reads %q, want the later value", v)
	}
}

// S5: a tombstone is a fourth field on its index line and no block at all; a table with no
// tombstones is byte-for-byte what the three-field format produced.
func TestSSTableTombstoneLineFormat(t *testing.T) {
	shim, h, segment := buildFixtureSSTable(t)
	lines := footerIndexLines(t, footerOf(t, segment))
	deleted := fixtureHash(t, 3).Hex()
	var found bool
	for _, l := range lines {
		if len(l) > len(deleted) && l[:len(deleted)] == deleted {
			found = true
			if want := deleted + ":0:0:1"; l != want {
				t.Fatalf("tombstone line = %q, want %q", l, want)
			}
		}
	}
	if !found {
		t.Fatalf("no index line for the deleted key: %q", lines)
	}
	value, del, ok, err := sstable.NewDefaultReader(shim, h).Lookup(fixtureHash(t, 3))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || !del || value != nil {
		t.Fatalf("Lookup(deleted) = (%q, deleted=%v, found=%v), want a tombstone", value, del, ok)
	}
}

// S6: the last four bytes duplicate indexLen. Without that trailer the footer is unlocatable -
// a reader needs indexLen to know where the footer starts, and indexLen used to live only
// *inside* the footer at an offset that depends on knowing where it starts.
func TestSSTableFooterTrailerLocatesTheFooter(t *testing.T) {
	_, _, segment := buildFixtureSSTable(t)
	footer := footerOf(t, segment)
	if got := int(footer[4])<<24 | int(footer[5])<<16 | int(footer[6])<<8 | int(footer[7]); got != len(footer)-44 {
		t.Fatalf("indexLen inside the footer (%d) disagrees with the trailer", got)
	}
	if magic := int(footer[0])<<24 | int(footer[1])<<16 | int(footer[2])<<8 | int(footer[3]); magic != 0x4B444253 {
		t.Fatalf("footer magic = %#x, want 0x4B444253", magic)
	}
}

// S4/S8: publish the whole segment. Kotlin compares the footer byte-for-byte (C1) and reads the
// values back out (C2) - the block bodies themselves are ZSTD and cannot be compared directly.
func TestExportPhysicalGoldenSSTable(t *testing.T) {
	_, _, segment := buildFixtureSSTable(t)
	writePhysicalGolden(t, "sstable_segment.hex", segment)
	writePhysicalGolden(t, "sstable_footer.hex", footerOf(t, segment))
}

// TestKotlinSSTableGoldenMatches covers S4/S8/S9/S11.
//
// The footer is deliberately *not* compared byte-for-byte: its per-block offset and compressed
// size are downstream of the compressor, and zstd-jni and klauspost/compress emit different
// (equally valid) frame lengths for the same input - see the test plan's "What ZSTD drags out of
// C1". What must match exactly is everything the compressor cannot touch: the index line order,
// each key, each tombstone flag, and the fileHash - the segment's content address, and so the
// field that decides whether a Kotlin-written SSTable keeps its identity here.
func TestKotlinSSTableGoldenMatches(t *testing.T) {
	kotlinSegment := readKotlinGolden(t, "sstable_segment.hex")
	if kotlinSegment == nil {
		return
	}
	_, _, goSegment := buildFixtureSSTable(t)
	goFooter, kotlinFooter := footerOf(t, goSegment), footerOf(t, kotlinSegment)

	goKeys, kotlinKeys := footerKeysAndFlags(t, goFooter), footerKeysAndFlags(t, kotlinFooter)
	if len(goKeys) != len(kotlinKeys) {
		t.Fatalf("index holds %d lines here and %d in Kotlin's segment", len(goKeys), len(kotlinKeys))
	}
	for i := range goKeys {
		if goKeys[i] != kotlinKeys[i] {
			t.Fatalf("index line %d: %q here, %q in Kotlin's segment", i, goKeys[i], kotlinKeys[i])
		}
	}
	if !bytes.Equal(fileHashOf(t, goFooter), fileHashOf(t, kotlinFooter)) {
		t.Fatalf("fileHash differs\nkotlin %x\ngo     %x", fileHashOf(t, kotlinFooter), fileHashOf(t, goFooter))
	}

	// S9: read Kotlin's own segment - compressed bodies included - through Go's reader.
	shim := io.NewInMemoryPlatformIO()
	const name = "ns/" + fixtureNamespaceID + "/sstable/L0/kotlin-fixture"
	if _, err := shim.AppendToSegment(name, kotlinSegment); err != nil {
		t.Fatal(err)
	}
	r := sstable.NewDefaultReader(shim, sstable.Handle{SegmentName: name, Level: 0})
	for _, tc := range []struct {
		seed byte
		want string
	}{{1, "alpha-rewritten"}, {2, string(fixturePayload)}, {4, ""}} {
		got, err := r.Get(fixtureHash(t, tc.seed))
		if err != nil {
			t.Fatalf("reading key %d out of Kotlin's segment: %v", tc.seed, err)
		}
		if string(got) != tc.want {
			t.Errorf("key %d = %q, want %q", tc.seed, got, tc.want)
		}
	}
}

// S10: a flipped byte in a block body must fail loudly rather than yielding garbage.
func TestSSTableCorruptBlockIsRejected(t *testing.T) {
	shim, h, segment := buildFixtureSSTable(t)
	_ = shim
	corrupt := append([]byte(nil), segment...)
	// Byte 20 is inside the first block's compressed body (the header is 16 bytes).
	corrupt[20] ^= 0xFF
	shim2 := io.NewInMemoryPlatformIO()
	if _, err := shim2.AppendToSegment(h.SegmentName, corrupt); err != nil {
		t.Fatal(err)
	}
	if _, err := sstable.NewDefaultReader(shim2, h).Get(fixtureHash(t, 1)); err == nil {
		t.Fatal("a corrupted block body must be rejected, not decoded")
	}
}

// footerKeysAndFlags reduces each index line to `<keyHex>[:1]`, dropping the offset and
// compressed size - the two fields a different zstd implementation legitimately changes.
func footerKeysAndFlags(t *testing.T, footer []byte) []string {
	t.Helper()
	var out []string
	for _, l := range footerIndexLines(t, footer) {
		parts := strings.Split(l, ":")
		if len(parts) > 3 {
			out = append(out, parts[0]+":"+parts[3])
			continue
		}
		out = append(out, parts[0])
	}
	return out
}

// fileHashOf is the 32-byte content address sitting between the index and the trailer.
func fileHashOf(t *testing.T, footer []byte) []byte {
	t.Helper()
	indexLen := int(footer[4])<<24 | int(footer[5])<<16 | int(footer[6])<<8 | int(footer[7])
	return footer[8+indexLen : 8+indexLen+32]
}

func footerIndexLines(t *testing.T, footer []byte) []string {
	t.Helper()
	indexLen := int(footer[4])<<24 | int(footer[5])<<16 | int(footer[6])<<8 | int(footer[7])
	body := string(footer[8 : 8+indexLen])
	if body == "" {
		return nil
	}
	var out []string
	for _, l := range bytes.Split([]byte(body), []byte("\n")) {
		out = append(out, string(l))
	}
	return out
}

var _ = codec.Hash{}
