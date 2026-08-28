package embed

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
)

// fakeSegmentWriter records the order records were appended in and how many
// times Flush (the physical fsync in the real writer) was called, which is what
// the group-commit behavior actually has to be asserted on - timing a real disk
// would make these tests flaky and prove nothing.
type fakeSegmentWriter struct {
	mu        sync.Mutex
	appended  []string
	flushes   int
	appendErr error
	flushErr  error
	// beforeAppend runs inside Append, before the record is recorded, so a test
	// can widen the window in which other callers queue up behind a batch.
	beforeAppend func()
}

func (f *fakeSegmentWriter) Append(r storage.DeltaRecord) (int64, error) {
	if f.beforeAppend != nil {
		f.beforeAppend()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.appendErr != nil {
		return 0, f.appendErr
	}
	f.appended = append(f.appended, string(r.CommitPayload))
	return int64(len(f.appended)), nil
}

func (f *fakeSegmentWriter) Flush() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.flushErr != nil {
		return f.flushErr
	}
	f.flushes++
	return nil
}

func (f *fakeSegmentWriter) snapshot() ([]string, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.appended...), f.flushes
}

func (f *fakeSegmentWriter) NamespaceID() string   { return "test/ns" }
func (f *fakeSegmentWriter) SegmentID() codec.UUID { return codec.UUID{} }
func (f *fakeSegmentWriter) CurrentSizeBytes() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return int64(len(f.appended))
}
func (f *fakeSegmentWriter) IsSealed() bool { return false }
func (f *fakeSegmentWriter) Seal() (storage.DeltaSegmentRef, error) {
	return storage.DeltaSegmentRef{}, nil
}

var _ storage.DeltaSegmentWriter = (*fakeSegmentWriter)(nil)

func rec(payload string) storage.DeltaRecord {
	return storage.DeltaRecord{CommitPayload: []byte(payload)}
}

// TestCommitLogPreservesEnqueueOrder is the constraint the whole design hangs
// on: delta replay walks the segment in order, so records must land in the
// order they were queued even though the write happens on another goroutine.
func TestCommitLogPreservesEnqueueOrder(t *testing.T) {
	w := &fakeSegmentWriter{}
	c := newCommitLogWriter(w, storage.DurabilitySync)
	defer c.Close()

	const n = 200
	for i := 0; i < n; i++ {
		if err := c.Enqueue(rec(fmt.Sprintf("commit-%03d", i))); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}
	got, _ := w.snapshot()
	if len(got) != n {
		t.Fatalf("appended %d records, want %d", len(got), n)
	}
	for i, p := range got {
		if want := fmt.Sprintf("commit-%03d", i); p != want {
			t.Fatalf("record %d = %q, want %q - delta replay depends on this order", i, p, want)
		}
	}
}

// TestCommitLogSyncWaitsForFlush: a nil return under DurabilitySync must mean
// the record is on disk, not merely queued.
func TestCommitLogSyncWaitsForFlush(t *testing.T) {
	w := &fakeSegmentWriter{}
	c := newCommitLogWriter(w, storage.DurabilitySync)
	defer c.Close()

	if err := c.Enqueue(rec("durable")); err != nil {
		t.Fatal(err)
	}
	got, flushes := w.snapshot()
	if len(got) != 1 {
		t.Fatalf("appended %d records, want 1 by the time Enqueue returned", len(got))
	}
	if flushes < 1 {
		t.Fatalf("flushes = %d, want at least 1 before Enqueue returns under DurabilitySync", flushes)
	}
}

// TestCommitLogGroupsConcurrentCommits is the actual payoff: concurrent
// commits must share fsyncs rather than each forcing their own. Asserted as
// "fewer flushes than records", not an exact count, since how many batches
// form depends on scheduling.
func TestCommitLogGroupsConcurrentCommits(t *testing.T) {
	// Hold the first append until every writer has queued, so the drain loop
	// is guaranteed to find a full queue when it batches.
	var queued sync.WaitGroup
	const n = 64
	queued.Add(n)
	release := make(chan struct{})
	var once sync.Once
	w := &fakeSegmentWriter{beforeAppend: func() {
		once.Do(func() { <-release })
	}}
	c := newCommitLogWriter(w, storage.DurabilitySync)
	defer c.Close()

	var wg sync.WaitGroup
	var failures int64
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			wait, err := c.EnqueueAsync(rec(fmt.Sprintf("c%02d", i)))
			queued.Done()
			if err != nil {
				atomic.AddInt64(&failures, 1)
				return
			}
			if err := wait(); err != nil {
				atomic.AddInt64(&failures, 1)
			}
		}(i)
	}
	queued.Wait()
	close(release)
	wg.Wait()

	if failures != 0 {
		t.Fatalf("%d writers failed", failures)
	}
	got, flushes := w.snapshot()
	if len(got) != n {
		t.Fatalf("appended %d records, want %d", len(got), n)
	}
	if flushes >= n {
		t.Fatalf("flushes = %d for %d records: concurrent commits are not sharing an fsync", flushes, n)
	}
}

// TestCommitLogAsyncDoesNotWait: under DurabilityAsync the caller returns once
// the record is queued. Close must still get it to disk.
func TestCommitLogAsyncDoesNotWait(t *testing.T) {
	w := &fakeSegmentWriter{}
	c := newCommitLogWriter(w, storage.DurabilityAsync)
	for i := 0; i < 10; i++ {
		if err := c.Enqueue(rec(fmt.Sprintf("a%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got, flushes := w.snapshot()
	if len(got) != 10 {
		t.Fatalf("appended %d records after Close, want 10 - Close must drain the queue", len(got))
	}
	if flushes < 1 {
		t.Fatalf("flushes = %d, want at least one by Close", flushes)
	}
}

// TestCommitLogLatchesFailure: a log with a hole in it must not keep accepting
// writes - replay would stop at the hole and silently lose everything after it.
func TestCommitLogLatchesFailure(t *testing.T) {
	boom := errors.New("disk is gone")
	w := &fakeSegmentWriter{appendErr: boom}
	c := newCommitLogWriter(w, storage.DurabilitySync)
	defer c.Close()

	if err := c.Enqueue(rec("first")); !errors.Is(err, boom) {
		t.Fatalf("first Enqueue error = %v, want %v", err, boom)
	}
	if err := c.Enqueue(rec("second")); !errors.Is(err, boom) {
		t.Fatalf("second Enqueue error = %v, want the latched %v", err, boom)
	}
}

// TestCommitLogEnqueueAfterCloseFails guards the shutdown race directly: Close
// closes the request channel, so an Enqueue racing it must be refused rather
// than panic on a send to a closed channel.
func TestCommitLogEnqueueAfterCloseFails(t *testing.T) {
	w := &fakeSegmentWriter{}
	c := newCommitLogWriter(w, storage.DurabilitySync)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Enqueue(rec("too late")); !errors.Is(err, ErrCommitLogClosed) {
		t.Fatalf("Enqueue after Close = %v, want ErrCommitLogClosed", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestCommitLogConcurrentEnqueueAndClose is the -race counterpart to the test
// above: many writers racing a Close must all either succeed or get
// ErrCommitLogClosed, and none may panic.
func TestCommitLogConcurrentEnqueueAndClose(t *testing.T) {
	w := &fakeSegmentWriter{}
	c := newCommitLogWriter(w, storage.DurabilitySync)

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := c.Enqueue(rec(fmt.Sprintf("r%02d", i)))
			if err != nil && !errors.Is(err, ErrCommitLogClosed) {
				t.Errorf("Enqueue: unexpected error %v", err)
			}
		}(i)
	}
	if err := c.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	wg.Wait()
}
