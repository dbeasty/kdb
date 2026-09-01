package server

// Fills the gap left by write_path_bench_test.go (insert-only) and
// storage/engine/{read,write}_throughput_bench_test.go (single-op, disjoint keys, no
// server-layer transactions): those answer "how fast is a fresh insert" but not "how fast is a
// read", "how fast is an update to an existing document", "what happens when reads and writes
// share a workload", or "what happens when concurrent writers actually contend for the same
// documents" (as opposed to the disjoint-ID isolation transaction/commit_throughput_bench_test.go
// deliberately uses to isolate lock/rebuild cost from conflict cost).
//
// "single-user" here means one caller issuing requests strictly one at a time (a plain
// sequential loop, not RunParallel(1) - SetParallelism(1) still spawns GOMAXPROCS goroutines per
// the testing.B docs, which is already concurrency). "heavy-multi-user" is RunParallel at
// heavyMultiUserParallelism. "overlapping" means every worker draws from the same small pool of
// document IDs (real contention); "non-overlapping" partitions the pool so workers rarely touch
// the same document (see keyFor's wraparound caveat at very high goroutine counts).
//
// Run with, e.g.:
//
//	go test ./kdb/server/ -run '^$' -bench BenchmarkWorkload -benchtime 2s -v
import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

const heavyMultiUserParallelism = 64

type keyspaceMode int

const (
	keyspaceNonOverlapping keyspaceMode = iota
	keyspaceOverlapping
)

func (m keyspaceMode) String() string {
	if m == keyspaceOverlapping {
		return "overlapping"
	}
	return "non-overlapping"
}

// newWorkloadServer opens a real disk-backed runtime under the default durability
// (storage.DurabilitySync + storio.SyncModeFull - real fsync per write), same shape as
// BenchmarkFileBackedUpsert. Every benchmark in this file except BenchmarkWorkloadDurability
// runs under this mode; see that benchmark for the sync/async/memory-only comparison.
func newWorkloadServer(b *testing.B, ns string) *KdbServerRuntime {
	b.Helper()
	return newWorkloadServerWithOptions(b, ns, embed.StorageOptions{})
}

func newWorkloadServerWithOptions(b *testing.B, ns string, opts embed.StorageOptions) *KdbServerRuntime {
	b.Helper()
	rt, err := embed.OpenFileRuntimeWithOptions(
		b.TempDir(), "bench", ns, schema.None(),
		embed.FileRuntimeOptions{Storage: opts},
	)
	if err != nil {
		b.Fatalf("OpenFileRuntimeWithOptions: %v", err)
	}
	b.Cleanup(func() { rt.Close() })
	srv := NewKdbServerRuntime(rt)
	srv.SetWriteQueueCapacityForTest(4096)
	return srv
}

// seedDocs pre-populates n documents via Upsert (LastWrite, never conflicts) and returns their IDs.
func seedDocs(b *testing.B, srv *KdbServerRuntime, ns string, n int) []codec.UUID {
	b.Helper()
	ids := make([]codec.UUID, n)
	for i := 0; i < n; i++ {
		id, err := codec.RandomUUID()
		if err != nil {
			b.Fatal(err)
		}
		if _, err := srv.Upsert(ns, id, fmt.Sprintf(`{"v":%d}`, i), auth.Principal{}); err != nil {
			b.Fatal(err)
		}
		ids[i] = id
	}
	return ids
}

// keyFor picks a document ID for iteration i of workerID. Overlapping mode draws from the whole
// pool, so every worker eventually touches every key. Non-overlapping mode buckets workers into
// at most numBuckets private slices of the pool; if the goroutine count exceeds numBuckets on a
// very high-core machine at heavyMultiUserParallelism, buckets wrap and get shared - an
// acceptable approximation for what this benchmark is measuring (contention vs. no contention),
// not a correctness guarantee.
const nonOverlappingBuckets = 128

func keyFor(ids []codec.UUID, mode keyspaceMode, workerID, i int) codec.UUID {
	if mode == keyspaceOverlapping || len(ids) < nonOverlappingBuckets {
		return ids[i%len(ids)]
	}
	partitionSize := len(ids) / nonOverlappingBuckets
	base := (workerID % nonOverlappingBuckets) * partitionSize
	return ids[base+(i%partitionSize)]
}

// runSequential drives fn b.N times on the calling goroutine only - true single-user, no
// concurrency at all - and reports ops/sec.
func runSequential(b *testing.B, fn func(i int)) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fn(i)
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

// runHeavyMultiUser drives fn across heavyMultiUserParallelism*GOMAXPROCS goroutines, each with
// its own workerID and per-goroutine iteration counter, and reports ops/sec.
func runHeavyMultiUser(b *testing.B, fn func(workerID, i int)) {
	b.Helper()
	var workerSeq int64
	b.ReportAllocs()
	b.SetParallelism(heavyMultiUserParallelism)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		workerID := int(atomic.AddInt64(&workerSeq, 1) - 1)
		i := 0
		for pb.Next() {
			fn(workerID, i)
			i++
		}
	})
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

// BenchmarkWorkloadRead: point-read throughput (KdbServerRuntime.GetDocument, full stack -
// schema/DAG/tree lookup - not the storage-engine-only BenchmarkGetDocumentConcurrent).
func BenchmarkWorkloadRead(b *testing.B) {
	const poolSize = 4096
	for _, mode := range []keyspaceMode{keyspaceOverlapping, keyspaceNonOverlapping} {
		ns := "bench/read"
		srv := newWorkloadServer(b, ns)
		ids := seedDocs(b, srv, ns, poolSize)

		b.Run("single-user/"+mode.String(), func(b *testing.B) {
			runSequential(b, func(i int) {
				id := keyFor(ids, mode, 0, i)
				if _, _, _, err := srv.GetDocument(ns, id); err != nil {
					b.Fatal(err)
				}
			})
		})
		b.Run("heavy-multi-user/"+mode.String(), func(b *testing.B) {
			runHeavyMultiUser(b, func(workerID, i int) {
				id := keyFor(ids, mode, workerID, i)
				if _, _, _, err := srv.GetDocument(ns, id); err != nil {
					b.Fatal(err)
				}
			})
		})
	}
}

// BenchmarkWorkloadWriteInsert: pure insert-new-document throughput via Upsert. Every write
// targets a brand-new random UUID, so "overlapping" keys are not meaningful here (see
// BenchmarkWorkloadUpdate for contended writes to existing documents).
func BenchmarkWorkloadWriteInsert(b *testing.B) {
	ns := "bench/insert"

	b.Run("single-user", func(b *testing.B) {
		srv := newWorkloadServer(b, ns)
		runSequential(b, func(i int) {
			id, err := codec.RandomUUID()
			if err != nil {
				b.Fatal(err)
			}
			if _, err := srv.Upsert(ns, id, fmt.Sprintf(`{"v":%d}`, i), auth.Principal{}); err != nil {
				b.Fatal(err)
			}
		})
	})
	b.Run("heavy-multi-user", func(b *testing.B) {
		srv := newWorkloadServer(b, ns)
		runHeavyMultiUser(b, func(workerID, i int) {
			id, err := codec.RandomUUID()
			if err != nil {
				b.Fatal(err)
			}
			if _, err := srv.Upsert(ns, id, fmt.Sprintf(`{"v":"insert-%d-%d"}`, workerID, i), auth.Principal{}); err != nil {
				b.Fatal(err)
			}
		})
	})
}

// BenchmarkWorkloadUpdate: repeated Upsert of existing documents (ConflictPolicyLastWrite - never
// rejects, but still serializes through the same write gate/WAL as every other write), contrasting
// a shared hot pool against private per-worker partitions.
func BenchmarkWorkloadUpdate(b *testing.B) {
	const poolSize = 4096
	for _, mode := range []keyspaceMode{keyspaceOverlapping, keyspaceNonOverlapping} {
		ns := "bench/update"
		srv := newWorkloadServer(b, ns)
		ids := seedDocs(b, srv, ns, poolSize)

		b.Run("single-user/"+mode.String(), func(b *testing.B) {
			runSequential(b, func(i int) {
				id := keyFor(ids, mode, 0, i)
				if _, err := srv.Upsert(ns, id, fmt.Sprintf(`{"v":"updated-%d"}`, i), auth.Principal{}); err != nil {
					b.Fatal(err)
				}
			})
		})
		b.Run("heavy-multi-user/"+mode.String(), func(b *testing.B) {
			runHeavyMultiUser(b, func(workerID, i int) {
				id := keyFor(ids, mode, workerID, i)
				if _, err := srv.Upsert(ns, id, fmt.Sprintf(`{"v":"updated-%d-%d"}`, workerID, i), auth.Principal{}); err != nil {
					b.Fatal(err)
				}
			})
		})
	}
}

// BenchmarkWorkloadMixedReadWrite: an 80/20 read/update mix against the same document pool,
// the shape a real request stream actually looks like (mostly reads, some writes) rather than
// the pure-mode benchmarks above.
func BenchmarkWorkloadMixedReadWrite(b *testing.B) {
	const poolSize = 4096
	const writeEveryNth = 5 // 1-in-5 == 20% writes, 80% reads
	for _, mode := range []keyspaceMode{keyspaceOverlapping, keyspaceNonOverlapping} {
		ns := "bench/mixed"
		srv := newWorkloadServer(b, ns)
		ids := seedDocs(b, srv, ns, poolSize)

		op := func(workerID, i int) {
			id := keyFor(ids, mode, workerID, i)
			if i%writeEveryNth == 0 {
				if _, err := srv.Upsert(ns, id, fmt.Sprintf(`{"v":"mixed-%d-%d"}`, workerID, i), auth.Principal{}); err != nil {
					b.Fatal(err)
				}
				return
			}
			if _, _, _, err := srv.GetDocument(ns, id); err != nil {
				b.Fatal(err)
			}
		}

		b.Run("single-user/"+mode.String(), func(b *testing.B) {
			runSequential(b, func(i int) { op(0, i) })
		})
		b.Run("heavy-multi-user/"+mode.String(), func(b *testing.B) {
			runHeavyMultiUser(b, op)
		})
	}
}

// BenchmarkWorkloadTransaction: explicit multi-op-capable commits through TransactionEngine
// (ConflictPolicyStrict), each anchored on the DAG head it observes. Overlapping mode has every
// worker target the same small document pool, so concurrent commits genuinely race and conflict;
// non-overlapping gives each worker its own partition, matching
// transaction/commit_throughput_bench_test.go's disjoint-ID isolation but through the full
// server stack (write gate, admission, real disk WAL) instead of the bare in-memory engine.
// Reports both ops/sec (successful commits only) and conflicts/op (retries paid per success).
func BenchmarkWorkloadTransaction(b *testing.B) {
	const poolSize = 256 // small on purpose for "overlapping": guarantees real contention
	for _, mode := range []keyspaceMode{keyspaceOverlapping, keyspaceNonOverlapping} {
		ns := "bench/tx"
		srv := newWorkloadServer(b, ns)
		ids := seedDocs(b, srv, ns, poolSize)

		b.Run("single-user/"+mode.String(), func(b *testing.B) {
			var conflicts int64
			runSequential(b, func(i int) {
				id := keyFor(ids, mode, 0, i)
				commitWithRetry(b, srv, ns, id, fmt.Sprintf(`{"v":"tx-%d"}`, i), &conflicts)
			})
			b.ReportMetric(float64(conflicts)/float64(b.N), "conflicts/op")
		})
		b.Run("heavy-multi-user/"+mode.String(), func(b *testing.B) {
			var conflicts int64
			runHeavyMultiUser(b, func(workerID, i int) {
				id := keyFor(ids, mode, workerID, i)
				commitWithRetry(b, srv, ns, id, fmt.Sprintf(`{"v":"tx-%d-%d"}`, workerID, i), &conflicts)
			})
			b.ReportMetric(float64(conflicts)/float64(b.N), "conflicts/op")
		})
	}
}

// commitOnce builds and submits a single-op explicit transaction anchored on the DAG head it
// observes right now. See KdbServerRuntime.Commit (ConflictPolicyStrict).
func commitOnce(srv *KdbServerRuntime, ns string, id codec.UUID, body string) error {
	head, err := srv.Runtime.DAG.Head()
	if err != nil {
		return err
	}
	txID, err := codec.RandomUUID()
	if err != nil {
		return err
	}
	tx := document.Transaction{
		ID:          txID,
		BaseVersion: head,
		Operations:  []document.Op{document.WriteOp{DocID: id, Patch: body}},
		Timestamp:   codec.TimestampNow(),
	}
	_, err = srv.Commit(ns, tx, "", auth.Principal{})
	return err
}

// commitWithRetry retries commitOnce on *ConflictError, counting attempts that were rejected as
// conflicting - the shape a real optimistic-concurrency client would use. See
// docs/benchmarks/workload-matrix.md "Finding 1" for why conflicts/op reads 0 today regardless
// of key overlap: ServerEngine.GetDocument does not honor its atCommit parameter, so
// ConflictPolicyStrict's conflict detection cannot fire against it.
func commitWithRetry(b *testing.B, srv *KdbServerRuntime, ns string, id codec.UUID, body string, conflicts *int64) {
	b.Helper()
	for {
		err := commitOnce(srv, ns, id, body)
		if err == nil {
			return
		}
		var conflictErr *ConflictError
		if errors.As(err, &conflictErr) {
			atomic.AddInt64(conflicts, 1)
			continue
		}
		b.Fatal(err)
	}
}

// BenchmarkWorkloadDurability answers "does async/memory-only durability actually make writes
// faster" directly, crossing insert/update/transaction against the three storage.Durability
// modes (storage/config.go) at both concurrency levels. Key overlap is held at non-overlapping
// throughout: durability affects fsync cost, not document-identity contention (see
// BenchmarkWorkloadUpdate/BenchmarkWorkloadTransaction for that axis).
//
//   - sync-full: storage.DurabilitySync + storio.SyncModeFull (F_FULLFSYNC on darwin) - the
//     default every other benchmark in this file runs under.
//   - async-100ms: storage.DurabilityAsync - acknowledges once appended to the WAL in memory;
//     a background ticker flushes to disk every 100ms (embed/commit_log.go), so a crash can lose
//     up to one interval of acknowledged writes. This is "eventually flushes to persistent
//     storage" - matches server/write_path_bench_test.go's BenchmarkFileBackedUpsertModes.
//   - memory-only: storage.DurabilityMemoryOnly - never syncs the WAL, and per
//     embed/persisting_dag.go's NewPersistingCommitDAGWithAsyncInterval, commits aren't queued
//     for eventual persistence at all. Not "eventually flushes" - genuinely ephemeral unless the
//     caller layers its own checkpointing on top. Included as the theoretical ceiling, not a mode
//     most deployments would run in.
func BenchmarkWorkloadDurability(b *testing.B) {
	durabilityModes := []struct {
		name string
		opts embed.StorageOptions
	}{
		{"sync-full", embed.StorageOptions{Durability: storage.DurabilitySync, SyncMode: storio.SyncModeFull}},
		{"async-100ms", embed.StorageOptions{Durability: storage.DurabilityAsync, SyncMode: storio.SyncModeFast, AsyncSyncIntervalMillis: 100}},
		{"memory-only", embed.StorageOptions{Durability: storage.DurabilityMemoryOnly}},
	}

	for _, mode := range durabilityModes {
		ns := "bench/durability-" + mode.name

		b.Run(mode.name+"/insert/single-user", func(b *testing.B) {
			srv := newWorkloadServerWithOptions(b, ns, mode.opts)
			runSequential(b, func(i int) {
				id, err := codec.RandomUUID()
				if err != nil {
					b.Fatal(err)
				}
				if _, err := srv.Upsert(ns, id, fmt.Sprintf(`{"v":"d-%d"}`, i), auth.Principal{}); err != nil {
					b.Fatal(err)
				}
			})
		})
		b.Run(mode.name+"/insert/heavy-multi-user", func(b *testing.B) {
			srv := newWorkloadServerWithOptions(b, ns, mode.opts)
			runHeavyMultiUser(b, func(workerID, i int) {
				id, err := codec.RandomUUID()
				if err != nil {
					b.Fatal(err)
				}
				if _, err := srv.Upsert(ns, id, fmt.Sprintf(`{"v":"d-%d-%d"}`, workerID, i), auth.Principal{}); err != nil {
					b.Fatal(err)
				}
			})
		})

		const updatePoolSize = 4096
		b.Run(mode.name+"/update/single-user", func(b *testing.B) {
			srv := newWorkloadServerWithOptions(b, ns, mode.opts)
			ids := seedDocs(b, srv, ns, updatePoolSize)
			runSequential(b, func(i int) {
				id := keyFor(ids, keyspaceNonOverlapping, 0, i)
				if _, err := srv.Upsert(ns, id, fmt.Sprintf(`{"v":"u-%d"}`, i), auth.Principal{}); err != nil {
					b.Fatal(err)
				}
			})
		})
		b.Run(mode.name+"/update/heavy-multi-user", func(b *testing.B) {
			srv := newWorkloadServerWithOptions(b, ns, mode.opts)
			ids := seedDocs(b, srv, ns, updatePoolSize)
			runHeavyMultiUser(b, func(workerID, i int) {
				id := keyFor(ids, keyspaceNonOverlapping, workerID, i)
				if _, err := srv.Upsert(ns, id, fmt.Sprintf(`{"v":"u-%d-%d"}`, workerID, i), auth.Principal{}); err != nil {
					b.Fatal(err)
				}
			})
		})

		const txPoolSize = 256
		b.Run(mode.name+"/transaction/single-user", func(b *testing.B) {
			srv := newWorkloadServerWithOptions(b, ns, mode.opts)
			ids := seedDocs(b, srv, ns, txPoolSize)
			var conflicts int64
			runSequential(b, func(i int) {
				id := keyFor(ids, keyspaceNonOverlapping, 0, i)
				commitWithRetry(b, srv, ns, id, fmt.Sprintf(`{"v":"t-%d"}`, i), &conflicts)
			})
		})
		b.Run(mode.name+"/transaction/heavy-multi-user", func(b *testing.B) {
			srv := newWorkloadServerWithOptions(b, ns, mode.opts)
			ids := seedDocs(b, srv, ns, txPoolSize)
			var conflicts int64
			runHeavyMultiUser(b, func(workerID, i int) {
				id := keyFor(ids, keyspaceNonOverlapping, workerID, i)
				commitWithRetry(b, srv, ns, id, fmt.Sprintf(`{"v":"t-%d-%d"}`, workerID, i), &conflicts)
			})
		})
	}
}
