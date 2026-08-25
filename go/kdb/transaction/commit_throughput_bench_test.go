package transaction_test

// Phase 0 baseline benchmark for the transaction commit path (see
// docs/benchmarks/phase0-baseline.md). Each goroutine commits writes to
// disjoint document IDs, so the optimistic conflict detector never
// rejects a commit here — this isolates the cost of the InMemoryCommitDag
// mutex and InMemoryStorageAdapter.CommitTree's full-tree rebuild
// (in_memory.go) from conflict-policy overhead.

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/metrics"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage/mem"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// BenchmarkCommitConcurrent_DisjointDocs measures steady-state commit
// throughput as the namespace grows, to surface how commitTree's O(namespace
// size) full-tree rebuild (in_memory.go CommitTree) degrades with document
// count rather than staying proportional to change size.
func BenchmarkCommitConcurrent_DisjointDocs(b *testing.B) {
	for _, parallelism := range []int{1, 8, 64} {
		b.Run(fmt.Sprintf("parallel-%d", parallelism), func(b *testing.B) {
			d, err := dag.NewInMemoryCommitDag("bench/commit")
			if err != nil {
				b.Fatal(err)
			}
			store := mem.NewInMemoryStorageAdapter()
			base, err := d.Head()
			if err != nil {
				b.Fatal(err)
			}
			eng := transaction.NewEngine(transaction.ConflictPolicyStrict, nil)

			metrics.Default.Reset()
			var counter int64
			b.SetParallelism(parallelism)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					n := atomic.AddInt64(&counter, 1)
					docID, _ := codec.RandomUUID()
					tx := document.Transaction{
						ID:          mustUUID(),
						BaseVersion: base,
						Operations: []document.Op{document.WriteOp{
							DocID: docID,
							Patch: fmt.Sprintf(`{"v":%d}`, n),
						}},
						Timestamp: codec.TimestampNow(),
					}
					res, err := eng.Commit(tx, d, store, schema.None(), nil, "")
					if err != nil {
						b.Fatal(err)
					}
					if _, ok := res.(transaction.ResultSuccess); !ok {
						b.Fatalf("unexpected result %T for disjoint-doc commit", res)
					}
				}
			})
			b.StopTimer()
			for _, s := range metrics.Default.Snapshot() {
				b.Logf("stage=%-14s count=%-8d mean=%-12s p50=%-12s p99=%-12s max=%s",
					s.Stage, s.Count, s.Mean, s.P50, s.P99, s.Max)
			}
		})
	}
}

func mustUUID() codec.UUID {
	id, err := codec.RandomUUID()
	if err != nil {
		panic(err)
	}
	return id
}

// BenchmarkCommitScalingWithHistorySize guards findExistingCommit's O(1) dag.
// GetCommitByTransactionID lookup (default_engine.go): before that fix, every single Commit call
// walked up to 8192 commits of history first, regardless of whether a retry was happening, which
// meant commit cost grew with total namespace history rather than staying flat - the dominant
// cause of kdb-service getting OOM-killed under sustained write load once history grew past a
// few thousand commits (docs/benchmarks/lightsail-sim/README.md). ns/op here should stay roughly
// flat across history sizes; a return to the old O(history) behavior would show it growing
// roughly linearly with historySize instead.
func BenchmarkCommitScalingWithHistorySize(b *testing.B) {
	for _, historySize := range []int{10, 1000, 8000} {
		b.Run(fmt.Sprintf("history-%d", historySize), func(b *testing.B) {
			d, err := dag.NewInMemoryCommitDag("bench/history-scaling")
			if err != nil {
				b.Fatal(err)
			}
			store := mem.NewInMemoryStorageAdapter()
			eng := transaction.NewEngine(transaction.ConflictPolicyStrict, nil)

			head, err := d.Head()
			if err != nil {
				b.Fatal(err)
			}
			for i := 0; i < historySize; i++ {
				docID := mustUUID()
				tx := document.Transaction{
					ID:          mustUUID(),
					BaseVersion: head,
					Operations:  []document.Op{document.WriteOp{DocID: docID, Patch: fmt.Sprintf(`{"v":%d}`, i)}},
					Timestamp:   codec.TimestampNow(),
				}
				res, err := eng.Commit(tx, d, store, schema.None(), nil, "")
				if err != nil {
					b.Fatal(err)
				}
				success, ok := res.(transaction.ResultSuccess)
				if !ok {
					b.Fatalf("seed commit %d: unexpected result %T", i, res)
				}
				head = success.Commit.Hash
			}

			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				docID := mustUUID()
				tx := document.Transaction{
					ID:          mustUUID(),
					BaseVersion: head,
					Operations:  []document.Op{document.WriteOp{DocID: docID, Patch: fmt.Sprintf(`{"v":"measured-%d"}`, n)}},
					Timestamp:   codec.TimestampNow(),
				}
				res, err := eng.Commit(tx, d, store, schema.None(), nil, "")
				if err != nil {
					b.Fatal(err)
				}
				success, ok := res.(transaction.ResultSuccess)
				if !ok {
					b.Fatalf("measured commit %d: unexpected result %T", n, res)
				}
				head = success.Commit.Hash
			}
		})
	}
}
