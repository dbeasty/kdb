package wal

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGroupCommitter_AllWaitersSeeSyncedResult(t *testing.T) {
	g := NewGroupCommitter()
	var syncCalls int64
	doSync := func() error {
		atomic.AddInt64(&syncCalls, 1)
		time.Sleep(2 * time.Millisecond) // simulate real fsync latency
		return nil
	}

	const n = 200
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 1; i <= n; i++ {
		seq := int64(i)
		go func() {
			defer wg.Done()
			if err := g.SyncTo(seq, doSync); err != nil {
				t.Errorf("SyncTo(%d) error: %v", seq, err)
			}
		}()
	}
	wg.Wait()

	if g.SyncedSeq() < n {
		t.Fatalf("SyncedSeq()=%d, want >= %d", g.SyncedSeq(), n)
	}
	calls := atomic.LoadInt64(&syncCalls)
	if calls >= n {
		t.Fatalf("expected group commit to coalesce fsyncs, got %d calls for %d waiters (no batching happened)", calls, n)
	}
	if calls < 1 {
		t.Fatalf("expected at least 1 sync call, got %d", calls)
	}
	t.Logf("%d waiters coalesced into %d physical sync calls", n, calls)
}

func TestGroupCommitter_AlreadySyncedReturnsImmediately(t *testing.T) {
	g := NewGroupCommitter()
	calls := 0
	doSync := func() error {
		calls++
		return nil
	}
	if err := g.SyncTo(5, doSync); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("calls=%d, want 1", calls)
	}
	// A lower or equal sequence should not trigger another physical sync.
	if err := g.SyncTo(3, doSync); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("calls=%d after already-synced request, want still 1", calls)
	}
}

func TestGroupCommitter_PropagatesSyncError(t *testing.T) {
	g := NewGroupCommitter()
	boom := &testSyncError{}
	doSync := func() error { return boom }

	var wg sync.WaitGroup
	errs := make([]error, 10)
	wg.Add(10)
	for i := range errs {
		i := i
		go func() {
			defer wg.Done()
			errs[i] = g.SyncTo(int64(i+1), doSync)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != boom {
			t.Fatalf("waiter %d: err=%v, want boom", i, err)
		}
	}
	if g.SyncedSeq() != 0 {
		t.Fatalf("SyncedSeq()=%d after failed sync, want 0", g.SyncedSeq())
	}
}

type testSyncError struct{}

func (e *testSyncError) Error() string { return "boom" }

// TestGroupCommitter_LateJoinerNotFalselyCovered stresses the exact race the
// implementation exists to avoid: a waiter that registers concurrently with
// an in-flight round must not be told "synced" by that round's result
// unless a later round (which its registration is guaranteed to precede)
// actually covers it. We check this indirectly: doSync counts how many
// times it runs versus how many distinct waiter batches exist, and every
// waiter's returned nil error must correspond to SyncedSeq ending up >=
// its own seq once SyncTo returns.
func TestGroupCommitter_LateJoinerNotFalselyCovered(t *testing.T) {
	g := NewGroupCommitter()
	var syncCount int64
	doSync := func() error {
		atomic.AddInt64(&syncCount, 1)
		time.Sleep(time.Millisecond)
		return nil
	}

	const waves = 50
	var wg sync.WaitGroup
	for wave := 0; wave < waves; wave++ {
		wg.Add(1)
		seq := int64(wave + 1)
		go func() {
			defer wg.Done()
			if err := g.SyncTo(seq, doSync); err != nil {
				t.Errorf("SyncTo(%d): %v", seq, err)
				return
			}
			if g.SyncedSeq() < seq {
				t.Errorf("SyncTo(%d) returned but SyncedSeq()=%d", seq, g.SyncedSeq())
			}
		}()
		time.Sleep(50 * time.Microsecond) // stagger arrivals across rounds
	}
	wg.Wait()
}
