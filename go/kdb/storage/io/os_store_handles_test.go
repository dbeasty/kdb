package io

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
)

func openHandleCount(s *OSByteStore) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.handles)
}

// Sealing a segment closes its write handle, so the handles a process holds track the segments it
// is still writing, not every segment it has ever written.
func TestSealedSegmentsDoNotKeepWriteHandles(t *testing.T) {
	shim, store := openOSShim(t)
	const segments = 300
	for i := 0; i < segments; i++ {
		name := SegmentNameBuilder.DeltaSequenced("app/data", int64(i))
		if _, err := shim.AppendToSegment(name, []byte(fmt.Sprintf("frame-%d", i))); err != nil {
			t.Fatal(err)
		}
		if n := openHandleCount(store); n != 1 {
			t.Fatalf("segment %d: %d write handles while one segment is open, want 1", i, n)
		}
		if _, ok, err := shim.SegmentStamp(name); err != nil || ok {
			t.Fatalf("segment %d: stamped while open for appending (ok=%v err=%v)", i, ok, err)
		}
		if err := shim.SealSegment(name); err != nil {
			t.Fatal(err)
		}
		if n := openHandleCount(store); n != 0 {
			t.Fatalf("after sealing %d segments: %d write handles, want 0", i+1, n)
		}
		st, ok, err := shim.SegmentStamp(name)
		if err != nil || !ok {
			t.Fatalf("segment %d: sealed segment not stamped (ok=%v err=%v)", i, ok, err)
		}
		if want := int64(len(fmt.Sprintf("frame-%d", i))); st.Size != want {
			t.Fatalf("segment %d: stamp size %d, want %d", i, st.Size, want)
		}
	}
	// Everything written is still there to read.
	got, err := shim.ReadFromSegment(SegmentNameBuilder.DeltaSequenced("app/data", 7), 0, 64)
	if err != nil || string(got) != "frame-7" {
		t.Fatalf("read back %q, %v", got, err)
	}
}

// A preallocated delta segment keeps its logical size while being written, and once sealed (handle
// dropped) is stamped from the file - stably, and as a later process would stamp it. Its data
// reads back intact either way.
func TestSealedPreallocatedSegmentStampsFromTheFile(t *testing.T) {
	const prealloc = 64 << 10
	root := t.TempDir()
	cfg := DefaultPlatformIOConfig()
	cfg.RootDirectory = &root
	cfg.FsyncOnFlush = false
	cfg.PreallocateBytes = prealloc
	store, err := NewOSByteStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	name := SegmentNameBuilder.DeltaSequenced("app/data", 1)
	data := bytes.Repeat([]byte{0xAB}, 100)
	if _, err := store.Append(name, data); err != nil {
		t.Fatal(err)
	}
	if n, err := store.Size(name); err != nil || n != int64(len(data)) {
		t.Fatalf("open segment Size = %d, %v; want the logical %d", n, err, len(data))
	}
	if err := store.Flush(name, false); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkSealed(name); err != nil {
		t.Fatal(err)
	}
	if n := openHandleCount(store); n != 0 {
		t.Fatalf("%d write handles after sealing, want 0", n)
	}
	size, mod, ok, err := store.Stamp(name)
	if err != nil || !ok {
		t.Fatalf("sealed segment not stamped (ok=%v err=%v)", ok, err)
	}
	if size < int64(len(data)) {
		t.Fatalf("stamp size %d is below the data written (%d)", size, len(data))
	}
	// What a fresh process sees.
	other, err := NewOSByteStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	size2, mod2, ok2, err := other.Stamp(name)
	if err != nil || !ok2 || size2 != size || mod2 != mod {
		t.Fatalf("another process stamps (%d,%d,%v,%v), this one (%d,%d)", size2, mod2, ok2, err, size, mod)
	}
	got, err := store.Read(name, 0, len(data))
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read back %d bytes, %v", len(got), err)
	}
}

// FlushSegment runs concurrently with appends and seals; a flush whose handle is closed by a seal
// under it must still sync rather than fail.
func TestFlushRacingSealDoesNotFail(t *testing.T) {
	root := t.TempDir()
	cfg := DefaultPlatformIOConfig()
	cfg.RootDirectory = &root
	cfg.FsyncOnFlush = true
	cfg.SyncMode = SyncModeFast
	store, err := NewOSByteStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	shim := NewFileBackedPlatformIO(cfg, store)

	var current atomic.Value
	current.Store(SegmentNameBuilder.DeltaSequenced("app/data", 0))
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var flushErr atomic.Value
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := shim.FlushSegment(current.Load().(string)); err != nil {
				flushErr.Store(err)
				return
			}
		}
	}()
	for i := 0; i < 40; i++ {
		name := SegmentNameBuilder.DeltaSequenced("app/data", int64(i))
		current.Store(name)
		for j := 0; j < 5; j++ {
			if _, err := shim.AppendToSegment(name, []byte("x")); err != nil {
				t.Fatal(err)
			}
		}
		if err := shim.SealSegment(name); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
	if err, _ := flushErr.Load().(error); err != nil {
		t.Fatalf("flush racing seal: %v", err)
	}
	if n := openHandleCount(store); n != 0 {
		t.Fatalf("%d write handles after sealing everything, want 0", n)
	}
}

// A flush whose handle was closed between lookup and sync falls back to a fresh handle.
func TestFlushOnClosedHandleFallsBack(t *testing.T) {
	_, store := openOSShim(t)
	name := SegmentNameBuilder.DeltaSequenced("app/data", 1)
	if _, err := store.Append(name, []byte("abc")); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	seg := store.handles[name]
	store.mu.Unlock()
	_ = seg.file.Close() // as a seal racing the flush would
	if err := store.Flush(name, true); err != nil {
		t.Fatalf("flush on a closed handle: %v", err)
	}
	// Sealing still drops the (already closed) handle.
	if err := store.MarkSealed(name); err != nil && !errors.Is(err, os.ErrClosed) {
		t.Fatal(err)
	}
	if n := openHandleCount(store); n != 0 {
		t.Fatalf("%d write handles after sealing, want 0", n)
	}
}
