package sstable_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage/io"
	"github.com/limidus/kdb/go/kdb/storage/sstable"
)

// countingIO counts the appends a writer makes.
type countingIO struct {
	*io.FileBackedPlatformIO
	appends int
}

func (c *countingIO) AppendToSegment(name string, b []byte) (int64, error) {
	c.appends++
	return c.FileBackedPlatformIO.AppendToSegment(name, b)
}

// A table's blocks go out in a few large appends, not one per entry: each append is a system
// call, and per-entry appends made a 10k-entry memtable flush take 1.6 s on macOS. Values larger
// than the gathering buffer, and tables mixing them with small ones and deletes, read back exactly.
func TestFinishGathersBlocksIntoFewAppends(t *testing.T) {
	shim := &countingIO{FileBackedPlatformIO: cappedIO(t, 16<<20)}
	w := sstable.NewDefaultWriter(shim, "ns", 0)
	want := map[codec.Hash][]byte{}
	const entries = 10_000
	for i := 0; i < entries; i++ {
		k := keyOf(t, i)
		v := []byte(fmt.Sprintf(`{"i":%d,"pad":"%x"}`, i, keyOf(t, i+entries).Bytes))
		if i%1000 == 7 {
			v = bytes.Repeat([]byte{byte(i)}, 3<<20) // larger than one gathered append
		}
		if i%997 == 3 {
			w.Delete(k)
			continue
		}
		w.Put(k, v)
		// Every large value and a sample of the rest: the wrapper hides the table's sidecar, so
		// each read parses the whole footer.
		if len(v) > 1<<20 || i%37 == 0 {
			want[k] = v
		}
	}
	handle, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if shim.appends > 64 {
		t.Fatalf("%d appends for a %d-entry table; blocks should be gathered", shim.appends, entries)
	}
	r := sstable.NewDefaultReader(shim, handle)
	for k, v := range want {
		got, err := r.Get(k)
		if err != nil {
			t.Fatalf("reading back %s: %v", k.Hex()[:8], err)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("key %s read back %d bytes, want %d", k.Hex()[:8], len(got), len(v))
		}
	}
}

// BenchmarkFinish10kSmallEntries is a memtable flush of 10k ~200-byte bodies to a real file.
func BenchmarkFinish10kSmallEntries(b *testing.B) {
	root := b.TempDir()
	cfg := io.PlatformIOConfig{RootDirectory: &root, MaxAppendBytes: 16 << 20}
	store, err := io.NewOSByteStore(cfg)
	if err != nil {
		b.Fatal(err)
	}
	shim := io.NewFileBackedPlatformIO(cfg, store)
	keys := make([]codec.Hash, 10_000)
	for i := range keys {
		h, _ := codec.HashFromBytes(bytes.Repeat([]byte{byte(i), byte(i >> 8), 1}, 11)[:32])
		keys[i] = h
	}
	value := bytes.Repeat([]byte("abcdefghij"), 20)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		w := sstable.NewDefaultWriter(shim, "ns", 0)
		for _, k := range keys {
			w.Put(k, value)
		}
		if _, err := w.Finish(); err != nil {
			b.Fatal(err)
		}
	}
}
