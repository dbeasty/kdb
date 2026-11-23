package sstable

import (
	"bytes"
	"encoding/hex"
	"errors"
	"math/rand"
	"testing"

	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// A table whose last four bytes (the footer's index-length trailer) decode to a length longer
// than the file is damaged. Opening it and compacting it must say so, promptly, rather than size
// an allocation or a read from that length.
func TestFooterLongerThanFileIsCorrupt(t *testing.T) {
	good := testShim(t)
	h := bigTable(t, good, "app/docs", 50, 64, 1, nil)
	data := readSegment(t, good, h.SegmentName)
	data[len(data)-4], data[len(data)-3], data[len(data)-2], data[len(data)-1] = 0x1B, 0x2C, 0x3D, 0x4E // ~456 million

	shim := testShim(t)
	if _, err := shim.AppendToSegment(h.SegmentName, data); err != nil {
		t.Fatal(err)
	}
	if _, err := NewDefaultReader(shim, h).Index(); !errors.Is(err, ErrCorruptTable) {
		t.Fatalf("Index error = %v, want ErrCorruptTable", err)
	}
	if _, err := Compact(shim, "app/docs", 1, []Handle{h}, false, nil); !errors.Is(err, ErrCorruptTable) {
		t.Fatalf("Compact error = %v, want ErrCorruptTable", err)
	}
}

// corruptTableCopy writes data as h's segment in a fresh disk-backed shim, for reading back a
// damaged table through the read path production uses (pooled positional reads).
func corruptTableCopy(t *testing.T, h Handle, data []byte) storage.PlatformIOShim {
	t.Helper()
	root := t.TempDir()
	cfg := storio.PlatformIOConfig{RootDirectory: &root}
	store, err := storio.NewOSByteStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	shim := storio.NewFileBackedPlatformIO(cfg, store)
	if _, err := shim.AppendToSegment(h.SegmentName, data); err != nil {
		t.Fatal(err)
	}
	return shim
}

// The footer carries its index length twice: in the trailer, which locates the footer, and again
// inside it. A damaged inner copy that runs past the footer used to slice out of range in
// parseFooter - a panic on every lookup of the table - instead of reporting a corrupt table.
func TestFooterInnerIndexLenPastFooterIsCorrupt(t *testing.T) {
	good := testShim(t)
	h := bigTable(t, good, "app/docs", 20, 64, 1, nil)
	data := readSegment(t, good, h.SegmentName)
	indexLen := readInt(data, len(data)-footerTrailerSize)
	footerStart := len(data) - (40 + indexLen) - footerTrailerSize
	writeInt(data, footerStart+4, indexLen+1000)

	shim := corruptTableCopy(t, h, data)
	if _, err := NewDefaultReader(shim, h).Index(); !errors.Is(err, ErrCorruptTable) {
		t.Fatalf("Index error = %v, want ErrCorruptTable", err)
	}
}

// A footer line is text with no checksum of its own, so one damaged byte can turn a block size
// into a negative number. Reading that block used to size a buffer from it and panic; it must be
// reported as a corrupt table, by a lookup and by a compaction alike.
func TestFooterNegativeBlockSizeIsCorrupt(t *testing.T) {
	good := testShim(t)
	w := NewDefaultWriter(good, "app/docs", 0)
	key := keyFor([]byte("the-only-key"))
	value := make([]byte, 300)
	rand.New(rand.NewSource(1)).Read(value) // incompressible: a three-digit block size
	w.Put(key, value)
	h, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	data := readSegment(t, good, h.SegmentName)
	line := []byte(hex.EncodeToString(key.Bytes[:]) + ":0:")
	at := bytes.Index(data, line)
	if at < 0 {
		t.Fatal("footer line not found")
	}
	copy(data[at+len(line):], "-99") // ":0:3xx" -> ":0:-99"

	shim := corruptTableCopy(t, h, data)
	if _, _, _, err := NewDefaultReader(shim, h).Lookup(key); !errors.Is(err, ErrCorruptTable) {
		t.Fatalf("Lookup error = %v, want ErrCorruptTable", err)
	}
	if _, err := Compact(shim, "app/docs", 1, []Handle{h}, false, nil); !errors.Is(err, ErrCorruptTable) {
		t.Fatalf("Compact error = %v, want ErrCorruptTable", err)
	}
}
