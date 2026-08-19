package engine

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/wal"
)

// fakeWAL counts Sync() calls without doing real I/O, so durability-mode
// tests can assert on sync behavior directly instead of timing disk
// operations.
type fakeWAL struct {
	seq       int64
	syncCalls int64
}

func (f *fakeWAL) WalID() codec.UUID             { return codec.UUID{} }
func (f *fakeWAL) PartitionKey() string          { return "fake" }
func (f *fakeWAL) LastSequence() int64           { return atomic.LoadInt64(&f.seq) }
func (f *fakeWAL) ActiveSegmentSizeBytes() int64 { return 0 }
func (f *fakeWAL) Append(record wal.Record) (wal.AppendResult, error) {
	seq := atomic.AddInt64(&f.seq, 1)
	return wal.AppendResult{Sequence: seq}, nil
}
func (f *fakeWAL) AppendBatch(records []wal.Record) (wal.AppendResult, error) {
	return wal.AppendResult{}, nil
}
func (f *fakeWAL) Sync() error {
	atomic.AddInt64(&f.syncCalls, 1)
	return nil
}
func (f *fakeWAL) Recover(handler func(wal.Record) error) (wal.RecoverySummary, error) {
	return wal.RecoverySummary{}, nil
}
func (f *fakeWAL) Truncate(truncateThroughSequence int64) error { return nil }
func (f *fakeWAL) Close() error                                 { return nil }

func TestDurabilitySync_SyncsEveryWrite(t *testing.T) {
	fw := &fakeWAL{}
	cfg := storage.StorageEngineConfig{GlobalMemoryBudgetBytes: 1 << 20, Durability: storage.DurabilitySync}
	e := NewServerEngine("ns", cfg, fw)
	for i := 0; i < 10; i++ {
		if _, err := e.WriteBlob([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if got := atomic.LoadInt64(&fw.syncCalls); got == 0 {
		t.Fatalf("syncCalls=%d, want > 0 under DurabilitySync", got)
	}
}

func TestDurabilityMemoryOnly_NeverSyncs(t *testing.T) {
	fw := &fakeWAL{}
	cfg := storage.StorageEngineConfig{GlobalMemoryBudgetBytes: 1 << 20, Durability: storage.DurabilityMemoryOnly}
	e := NewServerEngine("ns", cfg, fw)
	for i := 0; i < 10; i++ {
		if _, err := e.WriteBlob([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if got := atomic.LoadInt64(&fw.syncCalls); got != 0 {
		t.Fatalf("syncCalls=%d, want 0 under DurabilityMemoryOnly", got)
	}
	// The write itself must still be visible in-memory even though
	// nothing was synced to disk.
	hash, err := e.WriteBlob([]byte("check"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := e.ReadBlob(hash)
	if err != nil || string(b) != "check" {
		t.Fatalf("ReadBlob after memory-only write: b=%q err=%v", b, err)
	}
}

func TestDurabilityAsync_SyncsOnTimerNotPerWrite(t *testing.T) {
	fw := &fakeWAL{}
	cfg := storage.StorageEngineConfig{
		GlobalMemoryBudgetBytes: 1 << 20,
		Durability:              storage.DurabilityAsync,
		AsyncSyncIntervalMillis: 20,
	}
	e := NewServerEngine("ns", cfg, fw)
	defer e.Close()

	for i := 0; i < 50; i++ {
		if _, err := e.WriteBlob([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	// Writes themselves must not have blocked on a sync.
	if got := atomic.LoadInt64(&fw.syncCalls); got != 0 {
		t.Fatalf("syncCalls=%d immediately after writes, want 0 (async should not sync inline)", got)
	}

	time.Sleep(60 * time.Millisecond) // a few ticker intervals
	if got := atomic.LoadInt64(&fw.syncCalls); got == 0 {
		t.Fatalf("syncCalls=%d after waiting past the async interval, want > 0", got)
	}
	if got := atomic.LoadInt64(&fw.syncCalls); got >= 50 {
		t.Fatalf("syncCalls=%d, want far fewer than the 50 writes (async should batch on a timer)", got)
	}
}

func TestServerEngineClose_FlushesAsyncOnShutdown(t *testing.T) {
	fw := &fakeWAL{}
	cfg := storage.StorageEngineConfig{
		GlobalMemoryBudgetBytes: 1 << 20,
		Durability:              storage.DurabilityAsync,
		AsyncSyncIntervalMillis: time.Hour.Milliseconds(), // effectively never fires on its own
	}
	e := NewServerEngine("ns", cfg, fw)
	if _, err := e.WriteBlob([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt64(&fw.syncCalls); got != 0 {
		t.Fatalf("syncCalls=%d before Close, want 0", got)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt64(&fw.syncCalls); got == 0 {
		t.Fatalf("syncCalls=%d after Close, want > 0 (final flush on shutdown)", got)
	}
}
