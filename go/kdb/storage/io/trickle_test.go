package io

import (
	"bytes"
	"os"
	"sync/atomic"
	"testing"
)

// On Linux an SSTable is synced every trickleSyncBytes as it is written, so its final sync - and
// the commit log's syncs running beside it - never find more than that queued. Delta segments,
// whose syncs the commit log already decides, are left alone, as is everything off Linux and a
// store that does not sync.
func TestSSTablesAreTrickleSynced(t *testing.T) {
	shim, store, _ := openPreallocShim(t, 0)
	table := SegmentNameBuilder.SSTablePartial("a/one", "t1")
	log := SegmentNameBuilder.DeltaSequenced("a/one", 0)
	piece := bytes.Repeat([]byte{7}, 64<<10)
	for i := 0; i < 24; i++ { // 1.5 MiB each
		if _, err := shim.AppendToSegment(table, piece); err != nil {
			t.Fatal(err)
		}
		if _, err := shim.AppendToSegment(log, piece); err != nil {
			t.Fatal(err)
		}
	}
	store.mu.Lock()
	tseg, lseg := store.handles[table], store.handles[log]
	store.mu.Unlock()
	if lseg.trickle {
		t.Fatal("a delta segment is trickle-synced")
	}
	if !trickleSupported {
		if tseg.trickle {
			t.Fatal("trickle-synced off Linux")
		}
		return
	}
	if !tseg.trickle {
		t.Fatal("an SSTable is not trickle-synced")
	}
	// 1.5 MiB written: one sync at the first MiB, half a MiB since.
	if got := atomic.LoadInt64(&tseg.unsynced); got != 512<<10 {
		t.Fatalf("%d bytes unsynced after 1.5 MiB, want %d", got, 512<<10)
	}
	if got, want := atomic.LoadInt64(&tseg.size), int64(24*len(piece)); got != want {
		t.Fatalf("SSTable size %d, want %d", got, want)
	}

	root := t.TempDir()
	cfg := DefaultPlatformIOConfig()
	cfg.RootDirectory = &root
	cfg.FsyncOnFlush = false
	nosync, err := NewOSByteStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer nosync.Close()
	if _, err := nosync.Append(table, piece); err != nil {
		t.Fatal(err)
	}
	nosync.mu.Lock()
	defer nosync.mu.Unlock()
	if nosync.handles[table].trickle {
		t.Fatal("a store that never syncs trickle-syncs")
	}
}

// A trickled zero-fill leaves exactly the file a plain one does.
func TestZeroFillTrickleWritesTheSameZeros(t *testing.T) {
	dir := t.TempDir()
	for _, trickle := range []int64{0, 1 << 20, 3 << 19} {
		f, err := os.CreateTemp(dir, "zf-*")
		if err != nil {
			t.Fatal(err)
		}
		const size = 5<<20 + 123
		if err := zeroFillTrickle(f, size, trickle); err != nil {
			t.Fatal(err)
		}
		info, err := f.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() != size {
			t.Fatalf("trickle %d: %d bytes, want %d", trickle, info.Size(), size)
		}
		got := make([]byte, size)
		if _, err := f.ReadAt(got, 0); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, make([]byte, size)) {
			t.Fatalf("trickle %d: not all zeros", trickle)
		}
		_ = f.Close()
	}
}
