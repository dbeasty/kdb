package delta

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

func richCommit(t *testing.T) document.Commit {
	t.Helper()
	schemaHash := codec.Hash{Bytes: [32]byte{9, 9, 9}}
	c := document.Commit{
		ParentHashes:  []codec.Hash{{Bytes: [32]byte{1}}, {Bytes: [32]byte{2}}},
		NamespaceID:   testNS,
		TransactionID: codec.UUID{MSB: 3, LSB: 4},
		Timestamp:     codec.Timestamp{EpochMillis: 1234},
		AuthorNodeID:  codec.UUID{MSB: 5, LSB: 6},
		Operations: []document.Op{
			document.WriteOp{DocID: codec.UUID{MSB: 7, LSB: 8}, Patch: `{"a":"` + strings.Repeat("x", 5000) + `"}`},
			document.DeleteOp{DocID: codec.UUID{MSB: 9, LSB: 10}},
			document.FileWriteOp{Path: "dir/file.bin", BlobHash: codec.Hash{Bytes: [32]byte{11}}},
			document.SchemaMigrationOp{MigrationID: codec.UUID{MSB: 12, LSB: 13}, MigrationPayload: `{"m":1}`},
		},
		DocumentTreeHash: codec.Hash{Bytes: [32]byte{14}},
		SchemaHash:       &schemaHash,
		Message:          "rich",
	}
	h, err := document.ComputeCommitHash(c)
	if err != nil {
		t.Fatal(err)
	}
	c.Hash = h
	return c
}

// TestParseCommitDoesNotAliasThePayload is the safety argument for parseCommit's buffer reuse: a
// commit decoded from a payload must not change when the payload's bytes are overwritten
// afterwards - which is exactly what happens once the buffer goes back to the pool, or (for an
// uncompressed frame, decoded in place) when the segment buffer is reused. Every op kind is
// covered, since each decodes different field types.
func TestParseCommitDoesNotAliasThePayload(t *testing.T) {
	want := richCommit(t)
	for _, comp := range []storage.CompressionCodec{storage.CompressionNone, storage.CompressionZSTD} {
		frame := rawFrame(t, want, comp)
		got, err := (PageCodec{}).parseCommit(frame)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("codec %d: decoded %+v, want %+v", comp, got, want)
		}
		// Scribble over the frame and over buffers the pool is likely to hand back.
		for i := range frame {
			frame[i] = 0xA5
		}
		for i := 0; i < 8; i++ {
			other := rawFrame(t, document.Commit{NamespaceID: strings.Repeat("z", 6000)}, storage.CompressionZSTD)
			_, _ = (PageCodec{}).parseCommit(other)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("codec %d: decoded commit changed after its payload was overwritten", comp)
		}
	}

	// The premise itself, without the pool's nondeterminism: FromPayloadBytes keeps nothing.
	payload, err := want.ToPayloadBytes()
	if err != nil {
		t.Fatal(err)
	}
	got, err := document.FromPayloadBytes(payload)
	if err != nil {
		t.Fatal(err)
	}
	for i := range payload {
		payload[i] = 0x5A
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("FromPayloadBytes result aliases its input")
	}
}

// TestParseCommitMatchesParse: the pooled path decodes exactly what Parse + FromPayloadBytes
// decode, and fails the same way on a corrupt frame.
func TestParseCommitMatchesParse(t *testing.T) {
	c := richCommit(t)
	for _, comp := range []storage.CompressionCodec{storage.CompressionNone, storage.CompressionZSTD} {
		frame := rawFrame(t, c, comp)
		payload, err := (PageCodec{}).Parse(frame)
		if err != nil {
			t.Fatal(err)
		}
		viaParse, err := document.FromPayloadBytes(payload)
		if err != nil {
			t.Fatal(err)
		}
		got, err := (PageCodec{}).parseCommit(frame)
		if err != nil || !reflect.DeepEqual(got, viaParse) {
			t.Fatalf("codec %d: parseCommit %+v (%v), Parse path %+v", comp, got, err, viaParse)
		}

		bad := append([]byte(nil), frame...)
		bad[4] = 99
		_, perr := (PageCodec{}).Parse(bad)
		_, cerr := (PageCodec{}).parseCommit(bad)
		if perr == nil || cerr == nil || perr.Error() != cerr.Error() {
			t.Fatalf("bad version: Parse %v, parseCommit %v", perr, cerr)
		}
		bad = append([]byte(nil), frame...)
		bad[5] = 7
		_, perr = (PageCodec{}).Parse(bad)
		_, cerr = (PageCodec{}).parseCommit(bad)
		if perr == nil || cerr == nil || perr.Error() != cerr.Error() {
			t.Fatalf("bad codec: Parse %v, parseCommit %v", perr, cerr)
		}
	}
}

func sizedCommit(t *testing.T, patchBytes int, seed byte) document.Commit {
	t.Helper()
	// Incompressible-ish text so a zstd frame stays about as large as its payload.
	b := make([]byte, patchBytes)
	x := uint32(seed) + 1
	for i := range b {
		x = x*1664525 + 1013904223
		b[i] = 'a' + byte(x>>24)%26
	}
	c := document.Commit{
		NamespaceID: testNS,
		Timestamp:   codec.Timestamp{EpochMillis: int64(seed)},
		Operations:  []document.Op{document.WriteOp{DocID: codec.UUID{MSB: int64(seed), LSB: 1}, Patch: `{"p":"` + string(b) + `"}`}},
	}
	h, err := document.ComputeCommitHash(c)
	if err != nil {
		t.Fatal(err)
	}
	c.Hash = h
	return c
}

// TestReadCommitAtAcrossFrameSizes covers readCommitFrame's three paths through a real
// file-backed shim: a frame inside the first probe (one read), one larger than the probe (a
// second read into a bigger pooled buffer), and one larger than the pool holds (the old
// header-then-frame reads). Each must decode to exactly the commit written, and a frame cut short
// by the end of the segment must still be reported as corrupt.
func TestReadCommitAtAcrossFrameSizes(t *testing.T) {
	for _, comp := range []storage.CompressionCodec{storage.CompressionNone, storage.CompressionZSTD} {
		fb, _ := diskShim(t)
		cfg := newConfig(fb, comp)
		id, _ := codec.RandomUUID()
		w := NewDefaultWriter(testNS, id, 0, fb, cfg)
		sizes := []int{10, 3000, 4100, 50_000, 300_000, 5 << 20}
		var commits []document.Commit
		var offsets []int64
		for i, n := range sizes {
			c := sizedCommit(t, n, byte(i))
			off, err := w.Append(record(t, c))
			if err != nil {
				t.Fatal(err)
			}
			// Compare against the commit as decoding normalizes it (empty vs nil slices).
			payload, err := c.ToPayloadBytes()
			if err != nil {
				t.Fatal(err)
			}
			norm, err := document.FromPayloadBytes(payload)
			if err != nil {
				t.Fatal(err)
			}
			commits = append(commits, norm)
			offsets = append(offsets, off)
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
		r := NewDefaultReader(testNS, fb, cfg)
		ref := storage.DeltaSegmentRef{NamespaceID: testNS, SequenceNumber: 0}
		for i := range sizes {
			got, err := r.ReadCommitAt(ref, offsets[i])
			if err != nil {
				t.Fatalf("codec %d size %d: %v", comp, sizes[i], err)
			}
			if !reflect.DeepEqual(got, commits[i]) {
				t.Fatalf("codec %d size %d: decoded a different commit", comp, sizes[i])
			}
		}
		// A frame header past the last frame: the segment ends inside it.
		end := offsets[len(offsets)-1] + 1
		if _, err := r.ReadCommitAt(ref, end+int64(sizes[len(sizes)-1])*4); err == nil {
			t.Fatalf("codec %d: reading past the end succeeded", comp)
		}
	}

	// A frame whose tail is missing: written whole, then the segment truncated mid-frame.
	fb, root := diskShim(t)
	cfg := newConfig(fb, storage.CompressionNone)
	id, _ := codec.RandomUUID()
	w := NewDefaultWriter(testNS, id, 0, fb, cfg)
	c := sizedCommit(t, 20_000, 1)
	off, err := w.Append(record(t, c))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	truncateSegment(t, root, testNS, 0, off+10_000)
	r := NewDefaultReader(testNS, fb, cfg)
	if _, err := r.ReadCommitAt(storage.DeltaSegmentRef{NamespaceID: testNS}, off); err == nil {
		t.Fatal("a frame cut short by the end of its segment decoded")
	}
}

func truncateSegment(t *testing.T, root, ns string, seq, size int64) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(storio.SegmentNameBuilder.DeltaSequenced(ns, seq)))
	if err := os.Truncate(p, size); err != nil {
		t.Fatal(err)
	}
}
