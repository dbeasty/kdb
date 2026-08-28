package engine_test

// Phase 0 baseline benchmark for the write path described in
// docs/benchmarks/phase0-baseline.md. It measures ServerEngine.WriteBlob
// throughput under concurrent load against a real, disk-backed WAL, so
// the fsync cost is representative rather than optimistic in-memory
// numbers. See metrics.Default for the accompanying per-stage
// (lock_wait / fsync_wait) breakdown printed after each run.

import (
	"fmt"
	"os"
	"testing"

	"github.com/limidus/kdb/go/kdb/metrics"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
	"github.com/limidus/kdb/go/kdb/storage/io"
	"github.com/limidus/kdb/go/kdb/storage/wal"
)

func newDiskBackedServerEngine(b *testing.B) *engine.ServerEngine {
	b.Helper()
	root, err := os.MkdirTemp("", "kdb-bench-wal")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { os.RemoveAll(root) })

	store, err := io.NewOSByteStore(io.PlatformIOConfig{RootDirectory: &root})
	if err != nil {
		b.Fatal(err)
	}
	shim := io.NewFileBackedPlatformIO(io.PlatformIOConfig{RootDirectory: &root}, store)
	cfg := storage.StorageEngineConfig{GlobalMemoryBudgetBytes: 64 << 20, IOShim: shim}

	w, err := (&wal.DefaultFactory{}).OpenOrCreate("bench-ns", cfg, shim)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = w.Close() })

	return engine.NewServerEngine("bench-ns", cfg, w)
}

// BenchmarkWriteBlobConcurrent_DiskWAL is the Phase 0 baseline for the
// single most important bottleneck: ServerEngine.WriteBlob currently
// holds one engine-wide mutex across a synchronous WAL fsync
// (server_engine.go), so throughput here is expected to be bounded by
// disk fsync latency divided by exactly one concurrent writer at a time,
// regardless of -cpu.
func BenchmarkWriteBlobConcurrent_DiskWAL(b *testing.B) {
	for _, parallelism := range []int{1, 8, 64, 256} {
		b.Run(fmt.Sprintf("parallel-%d", parallelism), func(b *testing.B) {
			e := newDiskBackedServerEngine(b)
			metrics.Default.Reset()
			b.SetParallelism(parallelism)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				payload := make([]byte, 128)
				i := 0
				for pb.Next() {
					payload[0] = byte(i)
					if _, err := e.WriteBlob(payload); err != nil {
						b.Fatal(err)
					}
					i++
				}
			})
			b.StopTimer()
			reportStages(b)
		})
	}
}

func reportStages(b *testing.B) {
	b.Helper()
	for _, s := range metrics.Default.Snapshot() {
		b.Logf("stage=%-14s count=%-8d mean=%-12s p50=%-12s p99=%-12s max=%s",
			s.Stage, s.Count, s.Mean, s.P50, s.P99, s.Max)
	}
}
