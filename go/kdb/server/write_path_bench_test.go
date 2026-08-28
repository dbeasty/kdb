package server

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/metrics"
	"github.com/limidus/kdb/go/kdb/schema"
)

// The production write path had no benchmark until this one, which is why the
// engine's largest allocation went unnoticed for so long: the pre-existing
// commit benchmark (transaction/commit_throughput_bench_test.go) runs against
// mem.InMemoryStorageAdapter, which never persists, so it never reaches
// PersistingCommitDAG.Persist -> delta.PageCodec.Frame -> compression.Compress -
// the ~21MB-per-commit zstd encoder construction. Everything a real
// kdb-service write touches (schema phase, document staging, commit trie, DAG
// append, delta-log framing, segment append, fsync) is in scope here.
//
// Watch B/op and allocs/op as much as ns/op. Run with:
//
//	go test ./kdb/server/ -run '^$' -bench BenchmarkFileBackedUpsert -benchmem
func BenchmarkFileBackedUpsert(b *testing.B) {
	for _, parallelism := range []int{1, 8, 64} {
		b.Run(fmt.Sprintf("parallel-%d", parallelism), func(b *testing.B) {
			ns := "bench/writes"
			rt, err := embed.OpenFileRuntime(b.TempDir(), "bench", ns, schema.None())
			if err != nil {
				b.Fatalf("OpenFileRuntime: %v", err)
			}
			defer rt.Close()
			srv := NewKdbServerRuntime(rt)
			// The default 64-deep write queue is a load-shedding bound, not a
			// throughput one: at parallel-64 (SetParallelism multiplies by
			// GOMAXPROCS) almost every write would be rejected BUSY, and
			// rejections are cheap, so the benchmark would report the cost of
			// saying no rather than the cost of committing. Raise it here so
			// concurrent writers are actually admitted and group commit is
			// what's being measured.
			srv.SetWriteQueueCapacityForTest(4096)

			metrics.Default.Reset()
			var counter, busy int64
			b.SetParallelism(parallelism)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					n := atomic.AddInt64(&counter, 1)
					docID, err := codec.RandomUUID()
					if err != nil {
						b.Fatal(err)
					}
					body := fmt.Sprintf(`{"v":%d,"name":"bench"}`, n)
					_, err = srv.Upsert(ns, docID, body, auth.Principal{})
					// A full write queue is designed backpressure, not a failure
					// (writeGate / kdb-spec-layer13 Component 51). Count it rather
					// than aborting: at high parallelism against a capacity-1 gate
					// it is the expected outcome, and the count is itself the
					// interesting number.
					var busyErr *BusyError
					switch {
					case err == nil:
					case errors.As(err, &busyErr):
						atomic.AddInt64(&busy, 1)
					default:
						b.Fatalf("Upsert: %v", err)
					}
				}
			})
			b.StopTimer()
			if busy > 0 {
				b.Logf("admission: %d/%d writes rejected BUSY (write queue full)", busy, counter)
			}
			reportStagesTo(b)
		})
	}
}

// reportStagesTo prints the per-stage metrics.Default breakdown the storage and
// transaction benchmarks already report (lock_wait / fsync_wait / tree_rebuild),
// so numbers from this benchmark line up with docs/benchmarks/phase0-baseline.md.
func reportStagesTo(b *testing.B) {
	b.Helper()
	for _, s := range metrics.Default.Snapshot() {
		b.Logf("stage=%-14s count=%-8d mean=%-13v p50=%-13v p99=%-13v max=%v",
			s.Stage, s.Count, s.Mean, s.P50, s.P99, s.Max)
	}
}
