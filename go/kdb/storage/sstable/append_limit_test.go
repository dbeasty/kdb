package sstable_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage/io"
	"github.com/limidus/kdb/go/kdb/storage/sstable"
)

// cappedIO returns a file-backed shim with a deliberately small per-append ceiling, which is
// the only way to see this: the in-memory shim has no ceiling at all, so every memory-backed
// test in the repo passed while compaction was failing on disk.
func cappedIO(t *testing.T, maxAppend int) *io.FileBackedPlatformIO {
	t.Helper()
	root := t.TempDir()
	cfg := io.PlatformIOConfig{RootDirectory: &root, FsyncOnFlush: false, MaxAppendBytes: maxAppend}
	store, err := io.NewOSByteStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return io.NewFileBackedPlatformIO(cfg, store)
}

func keyOf(t *testing.T, i int) codec.Hash {
	t.Helper()
	h, err := codec.HashFromBytes(document.SHA256Digest([]byte(fmt.Sprintf("key-%d", i))))
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// TestFooterLargerThanOneAppendStillWrites is the regression test for the compaction failure
// seen at ~108,000 commits: "could not compact the blob store (append size exceeds max ...) -
// it keeps its current tables".
//
// The footer carries one text line per key, about 83 bytes of it, and was written in a single
// append. Against the default 16MB ceiling that caps a table at roughly 200,000 keys - which a
// namespace reaches once its documents, tree objects and version locations are counted. Past
// that, every compaction failed and silently kept the tables it already had, so the store grew
// without ever being merged.
func TestFooterLargerThanOneAppendStillWrites(t *testing.T) {
	const maxAppend = 4096
	const entries = 400 // ~83 bytes of index each: far past the ceiling

	shim := cappedIO(t, maxAppend)
	w := sstable.NewDefaultWriter(shim, "ns", 0)

	want := make(map[codec.Hash][]byte, entries)
	for i := 0; i < entries; i++ {
		k := keyOf(t, i)
		v := []byte(fmt.Sprintf("value-%d", i))
		w.Put(k, v)
		want[k] = v
	}

	handle, err := w.Finish()
	if err != nil {
		t.Fatalf("writing a table whose footer exceeds one append failed: %v", err)
	}

	// Written is not enough - it has to be readable, which is what proves the pieces were
	// concatenated rather than mangled.
	r := sstable.NewDefaultReader(shim, handle)
	for k, v := range want {
		got, err := r.Get(k)
		if err != nil {
			t.Fatalf("reading back %s: %v", k.Hex()[:8], err)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("key %s: got %q want %q", k.Hex()[:8], got, v)
		}
	}
}

// TestValueLargerThanOneAppendStillWrites is the other payload that outgrows a single append: a
// value is written as one block, and a full tree object holds every document in the namespace.
func TestValueLargerThanOneAppendStillWrites(t *testing.T) {
	const maxAppend = 4096

	shim := cappedIO(t, maxAppend)
	w := sstable.NewDefaultWriter(shim, "ns", 0)

	k := keyOf(t, 0)
	// Incompressible, so the block does not shrink under the ceiling on its way to disk.
	big := make([]byte, 64*1024)
	for i := range big {
		big[i] = byte(i*7 + i/251)
	}
	w.Put(k, big)

	handle, err := w.Finish()
	if err != nil {
		t.Fatalf("writing a value larger than one append failed: %v", err)
	}
	got, err := sstable.NewDefaultReader(shim, handle).Get(k)
	if err != nil {
		t.Fatalf("reading back the large value: %v", err)
	}
	if !bytes.Equal(got, big) {
		t.Fatalf("large value round-tripped wrong: got %d bytes want %d", len(got), len(big))
	}
}
