package server

import (
	"errors"
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/index/stores"
	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/wire"
)

func shapeOf(t *testing.T, q string) sql.QueryShape {
	t.Helper()
	stmt, err := sql.DefaultParser{}.Parse(q)
	if err != nil {
		t.Fatal(err)
	}
	return sql.ShapeOfSelect(stmt.(sql.StmtSelect).Query)
}

// U2: a query is sized by what its plan can hold - the index's candidates, one page, nothing at
// all for a count - not by the namespace's size. A user's 8,136 tasks in a 552k-document
// namespace were refused at "1.79 GB > 1.56 GB".
func TestScanEstimateIsBoundedByThePlan(t *testing.T) {
	m := NewCostModel()
	for i := 0; i < 200; i++ {
		m.ObserveDocSize("ns", 16<<10)
	}
	ordered := ScanEstimateInput{Namespace: "ns", TreeSize: 552_000, MaxRows: 10000, RowBudget: 1_000_000,
		Shape: shapeOf(t, `SELECT kdb_id, _doc FROM docs WHERE userId = ? ORDER BY createdAt`)}
	whole := m.EstimateScan(ordered)
	ordered.Candidates, ordered.CandidatesKnown = 8136, true
	byIndex := m.EstimateScan(ordered)
	if byIndex >= whole/50 {
		t.Fatalf("indexed ORDER BY estimate %d, whole-namespace %d: candidates did not bound it", byIndex, whole)
	}
	if byIndex < 8136*16<<10 {
		t.Fatalf("indexed estimate %d is below what the candidates hold", byIndex)
	}

	lean := ScanEstimateInput{Namespace: "ns", TreeSize: 552_000, MaxRows: 10000, RowBudget: 1_000_000,
		Shape: shapeOf(t, `SELECT kdb_id, status FROM docs WHERE _c = ? AND kdb_id > ? ORDER BY kdb_id LIMIT 5000`)}
	if lean.Shape.HasOrderBy {
		t.Fatal("ORDER BY kdb_id is served as an id-ordered stream, not a sort")
	}
	if got, max := m.EstimateScan(lean), int64(5000*(16<<10+scanRowOverheadBytes)*2)+(1<<20); got > max {
		t.Fatalf("ORDER BY kdb_id LIMIT 5000 estimated %d, above a 5000-row stream (%d)", got, max)
	}

	paged := ScanEstimateInput{Namespace: "ns", TreeSize: 552_000, MaxRows: 10000, PageSize: 200,
		Shape: shapeOf(t, `SELECT kdb_id, _doc FROM docs WHERE _c = ?`)}
	unpaged := paged
	unpaged.PageSize = 0
	if p, u := m.EstimateScan(paged), m.EstimateScan(unpaged); p*10 > u {
		t.Fatalf("a 200-row page estimated %d against %d unpaged", p, u)
	}

	count := ScanEstimateInput{Namespace: "ns", TreeSize: 552_000, MaxRows: 10000, RowBudget: 1_000_000,
		Shape: shapeOf(t, `SELECT COUNT(*) FROM docs WHERE _c = ? AND status = ?`)}
	if got := m.EstimateScan(count); got > 1<<20 {
		t.Fatalf("COUNT(*) estimated %d; it holds no documents", got)
	}
}

// U2: a query that really holds more than the server allows one query fails with a classified
// error instead of growing until the process is killed.
func TestRetainedLimitStopsAQuery(t *testing.T) {
	rt := newTestRuntime(t)
	defer rt.Release()
	if _, err := rt.OpenIndexes(stores.Options{}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 300; i++ {
		upsertDoc(t, rt, fmt.Sprintf(`{"_c":"tasks","n":%d,"pad":"%01000d"}`, i, i))
	}
	stats := &sql.ExecStats{}
	ctx := sql.QueryContext{NamespaceID: rt.Runtime.DefaultNamespace, Schema: rt.Runtime.Schema, Stats: stats, RetainedLimit: 64 << 10}
	_, err := rt.SQLEngine().Execute(`SELECT kdb_id, _doc FROM docs WHERE n >= 0 ORDER BY n`, ctx)
	var limit *sql.RetainedLimitError
	if !errors.As(err, &limit) {
		t.Fatalf("query holding ~300 KB under a 64 KiB limit: err %v", err)
	}
	if stats.RetainedBytes > 128<<10 {
		t.Fatalf("stopped only after holding %d bytes", stats.RetainedBytes)
	}
	if code, _ := classifyError(err); code != wire.ErrorCodeResourceExhausted {
		t.Fatal("unclassified")
	}
	// Under the limit, the same query runs.
	ctx.Stats, ctx.RetainedLimit = &sql.ExecStats{}, 64<<20
	if _, err := rt.SQLEngine().Execute(`SELECT kdb_id, _doc FROM docs WHERE n >= 0 ORDER BY n`, ctx); err != nil {
		t.Fatal(err)
	}
}
