package io

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/storage"
)

func openOSShim(t *testing.T) (*FileBackedPlatformIO, *OSByteStore) {
	t.Helper()
	root := t.TempDir()
	cfg := DefaultPlatformIOConfig()
	cfg.RootDirectory = &root
	cfg.FsyncOnFlush = false
	store, err := NewOSByteStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return NewFileBackedPlatformIO(cfg, store), store
}

func TestReadFromSegmentIntoMatchesReadFromSegment(t *testing.T) {
	shim, _ := openOSShim(t)
	name := SegmentNameBuilder.DeltaSequenced("app/data", 1)
	data := make([]byte, 100_000)
	for i := range data {
		data[i] = byte(i * 7)
	}
	if _, err := shim.AppendToSegment(name, data); err != nil {
		t.Fatal(err)
	}
	var into storage.SegmentIntoReader = shim
	for _, tc := range []struct {
		off int64
		n   int
	}{{0, 0}, {0, 1}, {0, 24}, {10, 4096}, {99_990, 100}, {100_000, 10}, {100_005, 10}, {0, 100_000}, {0, 200_000}} {
		want, err := shim.ReadFromSegment(name, tc.off, tc.n)
		if err != nil {
			t.Fatal(err)
		}
		dst := bytes.Repeat([]byte{0xEE}, tc.n)
		got, err := into.ReadFromSegmentInto(name, tc.off, dst)
		if err != nil {
			t.Fatalf("ReadFromSegmentInto(%d, %d): %v", tc.off, tc.n, err)
		}
		if !bytes.Equal(dst[:got], want) {
			t.Fatalf("ReadFromSegmentInto(%d, %d) read %d bytes that differ from ReadFromSegment's %d", tc.off, tc.n, got, len(want))
		}
	}
	if _, err := into.ReadFromSegmentInto(SegmentNameBuilder.DeltaSequenced("app/data", 9), 0, make([]byte, 8)); err == nil {
		t.Fatal("reading an unknown segment succeeded")
	}
	if _, err := into.ReadFromSegmentInto(name, -1, make([]byte, 8)); err == nil {
		t.Fatal("a negative offset was accepted")
	}
}

// TestSmallReadsAreClampedWithoutAStat: readAtClamped serves small reads without stat-ing the
// file. What a caller gets back must not change: a read past the end returns what exists, and a
// read mostly past the end does not hand back a buffer sized for the request.
func TestSmallReadsAreClampedWithoutAStat(t *testing.T) {
	shim, _ := openOSShim(t)
	name := SegmentNameBuilder.DeltaSequenced("app/data", 1)
	if _, err := shim.AppendToSegment(name, []byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	got, err := shim.ReadFromSegment(name, 7, 4000)
	if err != nil || string(got) != "789" {
		t.Fatalf("read across the end: %q %v", got, err)
	}
	if cap(got) > 64 {
		t.Fatalf("a 3-byte result kept a %d-byte buffer", cap(got))
	}
	got, err = shim.ReadFromSegment(name, 10, 100)
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("read at the end: %v %v (nil=%v)", got, err, got == nil)
	}
	got, err = shim.ReadFromSegment(name, 2, 6)
	if err != nil || string(got) != "234567" {
		t.Fatalf("read inside: %q %v", got, err)
	}
}

// blockingStore wraps a store so a Read parks until released, counting how many are parked at
// once.
type blockingStore struct {
	SegmentByteStore
	inside  atomic.Int32
	maxSeen atomic.Int32
	release chan struct{}
}

func (b *blockingStore) Read(name string, off int64, n int) ([]byte, error) {
	now := b.inside.Add(1)
	for {
		m := b.maxSeen.Load()
		if now <= m || b.maxSeen.CompareAndSwap(m, now) {
			break
		}
	}
	<-b.release
	defer b.inside.Add(-1)
	return b.SegmentByteStore.Read(name, off, n)
}

// TestReadsOfOneSegmentRunConcurrently: two reads of the same segment are inside the store at
// the same time. ReadFromSegment used to hold the segment's lock exclusively for the whole read,
// so concurrent cold reads of one segment queued behind each other's disk I/O.
func TestReadsOfOneSegmentRunConcurrently(t *testing.T) {
	_, osStore := openOSShim(t)
	bs := &blockingStore{SegmentByteStore: osStore, release: make(chan struct{})}
	shim := NewFileBackedPlatformIO(DefaultPlatformIOConfig(), bs)
	name := SegmentNameBuilder.DeltaSequenced("app/data", 1)
	if _, err := shim.AppendToSegment(name, []byte("hello world")); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := shim.ReadFromSegment(name, 0, 5); err != nil {
				t.Error(err)
			}
		}()
	}
	deadline := time.Now().Add(5 * time.Second)
	for bs.maxSeen.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(bs.release)
	wg.Wait()
	if got := bs.maxSeen.Load(); got < 2 {
		t.Fatalf("at most %d read(s) of one segment ran at once; reads are still serialized", got)
	}
}

// TestReadsNeverSeeAHalfAppliedAppend is the other half of the lock: reads share it, appends do
// not. A reader that reads to the end of a segment while appends land must always see a whole
// number of appends, each intact - never the front of one without its tail.
//
// A contract test more than a detector: on macOS/APFS a pread does not appear to observe a
// write() midway even with no lock at all (checked by removing it: 20 runs passed), so this can
// only catch the lock going missing on a platform where it matters, such as Linux, whose reads
// copy page by page without taking the inode lock a write holds.
func TestReadsNeverSeeAHalfAppliedAppend(t *testing.T) {
	shim, _ := openOSShim(t)
	name := SegmentNameBuilder.DeltaSequenced("app/data", 1)
	const record = 32 << 10
	const records = 64
	stop := make(chan struct{})
	errs := make(chan error, 8)
	var wg sync.WaitGroup
	check := func(b []byte) error {
		if len(b)%record != 0 {
			return fmt.Errorf("read %d bytes: not a whole number of %d-byte appends", len(b), record)
		}
		for r := 0; r < len(b)/record; r++ {
			rec := b[r*record : (r+1)*record]
			if !bytes.Equal(rec, bytes.Repeat([]byte{byte(r + 1)}, record)) {
				return fmt.Errorf("append %d read back damaged", r)
			}
		}
		return nil
	}
	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			buf := make([]byte, record*records)
			for {
				select {
				case <-stop:
					return
				default:
				}
				var got []byte
				if g%2 == 0 {
					b, err := shim.ReadFromSegment(name, 0, record*records)
					if err != nil {
						continue // not created yet
					}
					got = b
				} else {
					n, err := shim.ReadFromSegmentInto(name, 0, buf)
					if err != nil {
						continue
					}
					got = buf[:n]
				}
				if err := check(got); err != nil {
					errs <- err
					return
				}
			}
		}(g)
	}
	for r := 0; r < records; r++ {
		if _, err := shim.AppendToSegment(name, bytes.Repeat([]byte{byte(r + 1)}, record)); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}
