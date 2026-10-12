package server

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/index/stores"
	"github.com/limidus/kdb/go/kdb/sql"
)

// qtask keeps several collections in one schemaless namespace, told apart by a `_c` field, and
// filters them by JSON path (userId, projectIds, taskId). These tests pin that an index on such
// a path makes the filtered read independent of how many documents the namespace holds.

func loadQtaskShape(t *testing.T, srv *KdbServerRuntime, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		id, err := codec.RandomUUID()
		if err != nil {
			t.Fatal(err)
		}
		body := fmt.Sprintf(`{"_c":"tasks","userId":"u%d","taskId":"t%d","projectIds":["p%d","p%d"],"n":%d}`,
			i/20, i, i%97, (i+1)%97, i)
		if _, err := srv.Upsert(srv.Runtime.DefaultNamespace, id, body, auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
}

func queryWithin(t *testing.T, srv *KdbServerRuntime, q string, want int) (time.Duration, string) {
	t.Helper()
	ctx := sql.QueryContext{NamespaceID: srv.Runtime.DefaultNamespace, Schema: srv.Runtime.Schema}
	start := time.Now()
	res, err := srv.SQLEngine().Execute(q, ctx)
	d := time.Since(start)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	if len(res.Rows) != want {
		t.Fatalf("%s: %d rows, want %d", q, len(res.Rows), want)
	}
	return d, res.Plan
}

func TestJSONPathIndexKeepsFilteredReadFlat(t *testing.T) {
	sizes := []int{1000, 10000}
	if !testing.Short() {
		sizes = append(sizes, 100000)
	}
	queries := []string{
		// each user owns exactly 20 documents, whatever the namespace size
		`SELECT kdb_id FROM docs WHERE userId = 'u7'`,
		`SELECT kdb_id FROM docs WHERE taskId = 't42'`,
		// the low-selectivity collection tag comes first; the planner must not choose it
		`SELECT kdb_id FROM docs WHERE _c = 'tasks' AND taskId = 't42'`,
		`SELECT kdb_id FROM docs WHERE _c = 'tasks' AND userId = 'u7' AND projectIds = 'p7'`,
	}
	medians := map[string][]time.Duration{}
	for _, n := range sizes {
		srv := newTestRuntime(t)
		provider, err := srv.OpenIndexes(stores.Options{})
		if err != nil {
			t.Fatal(err)
		}
		ns := srv.Runtime.DefaultNamespace
		for _, f := range []string{"userId", "taskId", "projectIds", "_c"} {
			if err := provider.CreateIndex(sql.StmtCreateIndex{
				Name: "by_" + f, Table: "docs", Fields: []sql.IndexField{{Path: f}}, Using: "HASH",
			}, sql.QueryContext{NamespaceID: ns}); err != nil {
				t.Fatalf("CREATE INDEX %s: %v", f, err)
			}
		}
		loadQtaskShape(t, srv, n)
		ctx := sql.QueryContext{NamespaceID: ns, Schema: srv.Runtime.Schema}
		// Seeds the incremental state: the one read that may cost O(documents).
		if _, err := srv.SQLEngine().Execute(queries[0], ctx); err != nil {
			t.Fatal(err)
		}

		for _, q := range queries {
			var samples []time.Duration
			for round := 0; round < 15; round++ {
				// A write between reads is the case that used to replay the whole index.
				loadQtaskShape(t, srv, 1)
				start := time.Now()
				res, err := srv.SQLEngine().Execute(q, ctx)
				samples = append(samples, time.Since(start))
				if err != nil {
					t.Fatalf("%s: %v", q, err)
				}
				if res.Plan == "fullscan" {
					t.Fatalf("n=%d: %q fell back to a full scan", n, q)
				}
			}
			sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
			med := samples[len(samples)/2]
			medians[q] = append(medians[q], med)
			t.Logf("n=%-6d median %-10s %s", n, med.Round(time.Microsecond), q)
		}
	}
	// Flat means a hundredfold more documents does not cost a hundredfold more time. The bound
	// is loose on purpose (timing on a shared machine) yet far below the linear growth that a
	// replay or a scan would show.
	for q, m := range medians {
		first, last := m[0], m[len(m)-1]
		if last > 20*first && last > 20*time.Millisecond {
			t.Errorf("not flat: %s took %s at the smallest size and %s at the largest", q, first, last)
		}
	}
}
