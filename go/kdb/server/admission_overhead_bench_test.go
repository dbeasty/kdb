package server

// Branch-only microbenchmark: the exact per-operation cost the adaptive estimator adds to the
// read path - shape fingerprint, tree-size lookup, estimate, grant acquire/release, and the
// feedback observation. This is the "downside" number for the PR: what every governed SELECT
// pays that it did not pay before.

import (
	"context"
	"testing"

	"github.com/limidus/kdb/go/kdb/sql"
)

func BenchmarkScanAdmissionOverhead(b *testing.B) {
	guard := NewMemoryGuard(1<<30, 0.85)
	defer guard.Stop()
	adm := NewAdmission(guard, DefaultRescueReserveBytes, DefaultScanRowBudget)
	stmt, err := sql.DefaultParser{}.Parse("SELECT kdb_id FROM t WHERE kdb_id = 'x' LIMIT 5")
	if err != nil {
		b.Fatal(err)
	}
	sel := stmt.(sql.StmtSelect)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		in := ScanEstimateInput{
			Namespace: "app/data",
			Shape:     sql.ShapeOfSelect(sel.Query),
			TreeSize:  100_000,
			MaxRows:   10_000,
			RowBudget: int(adm.ScanRowBudget()),
		}
		est := adm.Costs().EstimateScan(in)
		grant, err := adm.AcquireBytes(ctx, ClassScan, est)
		if err != nil {
			b.Fatal(err)
		}
		adm.Costs().ObserveScanActual(in, est, 40_000)
		grant.Release()
	}
}

// The write-path grant cycle, for before/after context: the old path made two process-wide
// runtime/metrics.Read calls per grant; this one makes none.
func BenchmarkWriteGrantCycle(b *testing.B) {
	guard := NewMemoryGuard(1<<30, 0.85)
	defer guard.Stop()
	adm := NewAdmission(guard, DefaultRescueReserveBytes, DefaultScanRowBudget)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		grant, err := adm.Acquire(ctx, ClassWrite, 1024)
		if err != nil {
			b.Fatal(err)
		}
		grant.Release()
	}
}
