package transaction_test

// Calibration benchmark for kdb-spec-layer13 Component 48 §5.2's cost model. The spec requires
// server.CostModel's base/k constants be *measured, not guessed*; this is the measurement, and
// re-running it is how those constants get re-derived (or a regression in per-commit retention
// gets caught) after any change to the commit path.
//
// What it measures: the bytes a single commit holds *live* - i.e. still reachable after the
// commit returns, retained by the DAG and the storage adapter - as a function of the write
// payload size. That is deliberately not the same number as `go test -benchmem`'s B/op, which
// counts total bytes allocated including garbage the commit produced and immediately dropped.
// Admission control needs the retained figure: it is reserving against the memory a commit will
// still be occupying once it completes, which is precisely what accumulates until the process
// dies.

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage/mem"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// BenchmarkCommitBytesPerOp reports retained-bytes-per-commit at several payload sizes, plus the
// implied per-payload-byte slope. Run with:
//
//	go test ./kdb/transaction/ -run '^$' -bench BenchmarkCommitBytesPerOp -benchtime 2000x
//
// and read the retained_B/op custom metric - the intercept across payload sizes calibrates
// server.costBasePerClass[ClassWrite], the slope calibrates costKPerClass[ClassWrite].
func BenchmarkCommitBytesPerOp(b *testing.B) {
	for _, payloadBytes := range []int{64, 512, 4096, 32768} {
		b.Run(fmt.Sprintf("payload-%d", payloadBytes), func(b *testing.B) {
			d, err := dag.NewInMemoryCommitDag("bench/cost")
			if err != nil {
				b.Fatal(err)
			}
			store := mem.NewInMemoryStorageAdapter()
			base, err := d.Head()
			if err != nil {
				b.Fatal(err)
			}
			eng := transaction.NewEngine(transaction.ConflictPolicyStrict, nil)

			// A patch whose JSON encoding is approximately payloadBytes.
			filler := make([]byte, payloadBytes)
			for i := range filler {
				filler[i] = 'a' + byte(i%26)
			}
			patch := fmt.Sprintf(`{"v":%q}`, string(filler))

			// Retained bytes = heap in use after the run minus before, divided by iterations.
			// Both readings are taken after a forced GC so only genuinely-reachable memory
			// counts; the whole point is to exclude the garbage a commit churns through.
			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				docID, _ := codec.RandomUUID()
				tx := document.Transaction{
					ID:          mustUUID(),
					BaseVersion: base,
					Operations: []document.Op{document.WriteOp{
						DocID: docID,
						Patch: patch,
					}},
					Timestamp: codec.TimestampNow(),
				}
				res, err := eng.Commit(tx, d, store, schema.None(), nil, "")
				if err != nil {
					b.Fatal(err)
				}
				if _, ok := res.(transaction.ResultSuccess); !ok {
					b.Fatalf("unexpected result %T", res)
				}
			}
			b.StopTimer()

			runtime.GC()
			runtime.ReadMemStats(&after)
			// Keep d and store reachable across the ReadMemStats above, so the memory they
			// retain is actually counted rather than collected before the reading.
			runtime.KeepAlive(d)
			runtime.KeepAlive(store)

			retained := int64(after.HeapAlloc) - int64(before.HeapAlloc)
			if retained < 0 {
				retained = 0
			}
			b.ReportMetric(float64(retained)/float64(b.N), "retained_B/op")
			b.ReportMetric(float64(retained)/float64(b.N)/float64(payloadBytes), "retained_B/payload_B")
		})
	}
}
