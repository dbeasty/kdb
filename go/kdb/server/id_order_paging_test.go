package server

import (
	"fmt"
	"sort"
	"testing"

	"github.com/limidus/kdb/go/kdb/index/stores"
	"github.com/limidus/kdb/go/kdb/sql"
)

func selectIDs(t *testing.T, rt *KdbServerRuntime, q string, params ...string) ([]string, *sql.ExecStats) {
	t.Helper()
	stats := &sql.ExecStats{}
	ps := make([]sql.Parameter, len(params))
	for i, p := range params {
		ps[i] = sql.ParamString{Value: p}
	}
	ctx := sql.QueryContext{NamespaceID: rt.Runtime.DefaultNamespace, Schema: rt.Runtime.Schema, Stats: stats, Parameters: ps}
	res, err := rt.SQLEngine().Execute(q, ctx)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	out := make([]string, len(res.Rows))
	for i, r := range res.Rows {
		out[i] = r.Values[0].(sql.CellString).Value
	}
	return out, stats
}

// U4/U7: a lean read paged by its last id - `... AND kdb_id > ? ORDER BY kdb_id LIMIT n`, the
// shape qtask's store issues - returns exactly the next n matches in id order, and reads about n
// documents to do it rather than every candidate.
func TestKdbIDOrderedPageReadsOnlyThePage(t *testing.T) {
	rt := newTestRuntime(t)
	defer rt.Release()
	p, err := rt.OpenIndexes(stores.Options{})
	if err != nil {
		t.Fatal(err)
	}
	createHashIndex(t, p, rt, "by_c", "_c", "HASH")
	createHashIndex(t, p, rt, "by_user", "userId", "HASH")
	for i := 0; i < 1200; i++ {
		coll := "tasks"
		if i%4 == 3 {
			coll = "notes"
		}
		upsertDoc(t, rt, fmt.Sprintf(`{"_c":%q,"userId":"u%d","n":%d,"pad":"%0200d"}`, coll, i%3, i, i))
	}
	all, _ := selectIDs(t, rt, `SELECT kdb_id FROM docs WHERE _c = 'tasks' AND userId = 'u1' AND n >= 0`)
	sort.Strings(all)
	if len(all) < 200 {
		t.Fatalf("shape: %d matches", len(all))
	}
	const page = 50
	queries := []string{
		// index candidates, a residual the index cannot serve
		`SELECT kdb_id FROM docs WHERE _c = ? AND (userId = 'u1') AND n >= 0 AND kdb_id > ? ORDER BY kdb_id LIMIT 50`,
		// a full scan (no servable conjunct)
		`SELECT kdb_id FROM docs WHERE n >= 0 AND userId LIKE 'u1' AND _c LIKE ? AND kdb_id > ? ORDER BY kdb_id LIMIT 50`,
	}
	for _, q := range queries {
		var got []string
		after := ""
		for pages := 0; ; pages++ {
			ids, stats := selectIDs(t, rt, q, "tasks", after)
			got = append(got, ids...)
			// A quarter of the documents match, so a scan reads ~4 per row; bound it at twice that.
			if stats.DocsRead > 8*page {
				t.Errorf("%s: page %d read %d documents for %d rows", q, pages, stats.DocsRead, len(ids))
			}
			if len(ids) < page {
				break
			}
			after = ids[len(ids)-1]
		}
		if fmt.Sprint(got) != fmt.Sprint(all) {
			t.Fatalf("%s: paged by id returned %d ids, want the %d matches in id order", q, len(got), len(all))
		}
	}
	// A bound the id order cannot use (not canonical text) still answers correctly.
	ids, _ := selectIDs(t, rt, `SELECT kdb_id FROM docs WHERE _c = 'tasks' AND userId = 'u1' AND n >= 0 AND kdb_id > ? ORDER BY kdb_id LIMIT 5`, "0")
	if fmt.Sprint(ids) != fmt.Sprint(all[:5]) {
		t.Fatalf("non-UUID bound: %v, want %v", ids, all[:5])
	}
}
