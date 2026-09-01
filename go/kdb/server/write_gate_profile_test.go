package server

import (
	"context"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
)

// BenchmarkCommitGateBreakdown measures where a commit's wall clock actually goes, so the
// question "is the write gate the bottleneck" is answered with numbers rather than intuition.
//
// Two arms:
//
//	Serial   - one goroutine, so the gate is never contended. This is the per-commit cost of the
//	           engine itself: conflict detection, the schema pass, tree build, DAG append.
//	Parallel - N goroutines all committing, so the gate is saturated.
//
// If the gate were the bottleneck, Parallel's per-op time would approach Serial's multiplied by
// the goroutine count while total throughput stayed flat. If instead the engine work dominates,
// the two arms report similar per-op times and batching would buy nothing - a group-commit layer
// would add cross-transaction conflict-detection complexity for no measurable gain.
func BenchmarkCommitGateBreakdown(b *testing.B) {
	for _, arm := range []struct {
		name     string
		parallel bool
	}{{"Serial", false}, {"Parallel", true}} {
		b.Run(arm.name, func(b *testing.B) {
			rt := benchRuntime(b)
			b.ResetTimer()
			if arm.parallel {
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						commitOne(b, rt)
					}
				})
				return
			}
			for i := 0; i < b.N; i++ {
				commitOne(b, rt)
			}
		})
	}
}

// BenchmarkWriteGateOverheadOnly isolates the gate primitive from the commit it guards: acquire
// and immediately release, with no engine work in between. This is the ceiling on what any
// batching scheme could possibly recover.
func BenchmarkWriteGateOverheadOnly(b *testing.B) {
	rt := benchRuntime(b)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			release, err := rt.AcquireWriteSlotWithContextForTest(ctx)
			if err != nil {
				cancel()
				b.Fatal(err)
			}
			release()
			cancel()
		}
	})
}

// BenchmarkFileBackedCommitGateBreakdown is the same measurement against a real data directory,
// where an fsync per commit could plausibly dominate and make batching worth its complexity.
// This is the arm that decides it: durability grouping already happens (PersistAsync releases
// the gate as soon as a commit's log position is fixed, so concurrent commits share one physical
// sync), so what this shows is whether anything is left on the table beyond that.
func BenchmarkFileBackedCommitGateBreakdown(b *testing.B) {
	for _, arm := range []struct {
		name     string
		parallel bool
	}{{"Serial", false}, {"Parallel", true}} {
		b.Run(arm.name, func(b *testing.B) {
			embedded, err := embed.OpenFileRuntime(b.TempDir(), "demo", "app/data", schema.None())
			if err != nil {
				b.Fatal(err)
			}
			defer embedded.Close()
			rt := NewKdbServerRuntime(embedded)
			b.ResetTimer()
			if arm.parallel {
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						commitOne(b, rt)
					}
				})
				return
			}
			for i := 0; i < b.N; i++ {
				commitOne(b, rt)
			}
		})
	}
}

func benchRuntime(b *testing.B) *KdbServerRuntime {
	b.Helper()
	embedded, err := embed.OpenMemoryRuntime("demo", "app/data", schema.None())
	if err != nil {
		b.Fatal(err)
	}
	return NewKdbServerRuntime(embedded)
}

func commitOne(b *testing.B, rt *KdbServerRuntime) {
	b.Helper()
	head, err := rt.Runtime.DAG.Head()
	if err != nil {
		b.Fatal(err)
	}
	docID, _ := codec.RandomUUID()
	txID, _ := codec.RandomUUID()
	tx := document.Transaction{
		ID:          txID,
		BaseVersion: head,
		Operations:  []document.Op{document.WriteOp{DocID: docID, Patch: `{"v":1}`}},
		Timestamp:   codec.TimestampNow(),
	}
	if _, err := rt.Commit("app/data", tx, "bench", auth.Principal{}); err != nil {
		b.Fatal(err)
	}
}
