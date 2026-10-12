package server

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/index/stores"
	"github.com/limidus/kdb/go/kdb/policy"
	"github.com/limidus/kdb/go/kdb/sql"
)

func countOf(t *testing.T, srv *KdbServerRuntime, q string, budget int) (string, string) {
	t.Helper()
	ctx := sql.QueryContext{NamespaceID: srv.Runtime.DefaultNamespace, Schema: srv.Runtime.Schema, RowBudget: budget}
	res, err := srv.SQLEngine().Execute(q, ctx)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	if len(res.Rows) != 1 || len(res.Rows[0].Values) != 1 {
		t.Fatalf("%s: unexpected shape %+v", q, res.Rows)
	}
	return cellText(res.Rows[0].Values[0]), res.Plan
}

func cellText(c sql.Cell) string {
	if l, ok := c.(sql.CellLong); ok {
		return itoa(l.Value)
	}
	return "?"
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var b []byte
	for v > 0 {
		b = append([]byte{byte('0' + v%10)}, b...)
		v /= 10
	}
	if neg {
		b = append([]byte{'-'}, b...)
	}
	return string(b)
}

// COUNT(*) is answered from the tree size / index count, without reading documents, so it is
// not subject to the scan row budget - and it declines (reads) whenever it could be wrong.
func TestCountStarIsAnsweredWithoutReadingDocuments(t *testing.T) {
	srv := newTestRuntime(t)
	provider, err := srv.OpenIndexes(stores.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ns := srv.Runtime.DefaultNamespace
	if err := provider.CreateIndex(sql.StmtCreateIndex{
		Name: "by_user", Table: "docs", Fields: []sql.IndexField{{Path: "userId"}}, Using: "HASH",
	}, sql.QueryContext{NamespaceID: ns}); err != nil {
		t.Fatal(err)
	}
	loadQtaskShape(t, srv, 200)

	// A row budget of 10 would abort any scan of 200 documents.
	if n, plan := countOf(t, srv, `SELECT COUNT(*) FROM docs`, 10); n != "200" || plan != "count:tree" {
		t.Fatalf("COUNT(*) = %s via %s, want 200 via count:tree", n, plan)
	}
	if n, plan := countOf(t, srv, `SELECT COUNT(*) FROM docs WHERE userId = 'u3'`, 10); n != "20" || plan != "count:index" {
		t.Fatalf("COUNT(*) WHERE = %s via %s, want 20 via count:index", n, plan)
	}
	// Anything the shortcut cannot vouch for takes the ordinary path and agrees with it.
	if n, plan := countOf(t, srv, `SELECT COUNT(*) FROM docs WHERE userId = 'u3' AND n > 100`, 0); n == "" || plan == "count:index" || plan == "count:tree" {
		t.Fatalf("a compound WHERE must not be pushed down, got %s via %s", n, plan)
	}
	if n, plan := countOf(t, srv, `SELECT COUNT(taskId) FROM docs`, 0); n != "200" || plan == "count:tree" {
		t.Fatalf("COUNT(col) = %s via %s", n, plan)
	}

	// While expiry hides documents the tree size over-counts what a read returns: decline.
	srv.SetDocumentExpiry(&policy.DocumentExpiryPolicy{FieldPath: "expiresAt", GraceMillis: 0, SweepIntervalMillis: 60000})
	if n, plan := countOf(t, srv, `SELECT COUNT(*) FROM docs`, 0); n != "200" || plan == "count:tree" {
		t.Fatalf("with expiry on, COUNT(*) = %s via %s, want 200 by reading", n, plan)
	}
}
