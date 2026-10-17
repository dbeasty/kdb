package sql

import (
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/index"
)

// #318: the ordered BTREE walk gives up for the bounded top-k once it has passed a few index
// entries per candidate, and EXPLAIN shows which of the two a query ran.

// lateCandidatesFixture: 400 candidates (status = 'pending') whose v sorts behind every one of
// 20,000 other documents, so the walk on v passes all of those before reaching any candidate.
// w is spread evenly, so the walk on w finds the first candidate almost at once.
func lateCandidatesFixture(t *testing.T) *orderedFixture {
	f := newOrderedFixture(t, "app/walk-late", map[string]index.IndexType{
		"status": index.IndexTypeHash, "v": index.IndexTypeBTree, "w": index.IndexTypeBTree,
	})
	r := rand.New(rand.NewSource(7))
	writes := map[codec.UUID]string{}
	for i := 0; i < 20_400; i++ {
		id, _ := codec.RandomUUID()
		status, v := "done", r.Intn(1_000_000)
		if i < 400 {
			status, v = "pending", 2_000_000+r.Intn(1000)
		}
		writes[id] = fmt.Sprintf(`{"status":%q,"v":%d,"w":%d,"n":%d}`, status, v, r.Intn(1_000_000), i)
	}
	f.commit(writes, nil)
	return f
}

func TestBTreeWalkIsAbandonedWhenCandidatesSortLate(t *testing.T) {
	f := lateCandidatesFixture(t)
	for _, q := range []string{
		"SELECT n, v FROM t WHERE status = 'pending' ORDER BY v LIMIT 3",
		"SELECT n, v FROM t WHERE status = 'pending' ORDER BY v, n LIMIT 10 OFFSET 2",
	} {
		want, _, err := f.run(true, q, nil)
		if err != nil {
			t.Fatal(err)
		}
		got, st, err := f.run(false, q, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got.Rows, want.Rows) {
			t.Fatalf("%s: rows differ:\n got  %v\n want %v", q, got.Rows, want.Rows)
		}
		if got.Plan != "index:eq(status)+topk(order(v) abandoned)" {
			t.Fatalf("%s: plan %q, want the walk abandoned for the top-k", q, got.Plan)
		}
		// Each candidate is read once: nothing the walk read is read again.
		if st.DocsRead != 400 {
			t.Fatalf("%s: read %d documents, want each of the 400 candidates once", q, st.DocsRead)
		}
	}
	// The negative control: candidates spread through w's order keep the walk.
	got, st, err := f.run(false, "SELECT n FROM t WHERE status = 'pending' ORDER BY w LIMIT 3", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.Plan != "index:eq(status)+order(w)" || st.DocsRead >= 100 {
		t.Fatalf("plan %q read %d documents, want the walk to settle after a few", got.Plan, st.DocsRead)
	}
}

func TestExplainReportsThePlanItRan(t *testing.T) {
	f := lateCandidatesFixture(t)
	for q, plan := range map[string]string{
		"EXPLAIN SELECT n FROM t WHERE status = 'pending' ORDER BY v LIMIT 1":         "index:eq(status)+topk(order(v) abandoned)",
		"explain analyze SELECT n FROM t WHERE status = 'pending' ORDER BY w LIMIT 1": "index:eq(status)+order(w)",
		"EXPLAIN SELECT n FROM t WHERE status = 'pending'":                            "index:eq(status)",
		"EXPLAIN SELECT n FROM t WHERE n = 5":                                         "fullscan",
	} {
		res, st, err := f.run(false, q, nil)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		var names []string
		for _, c := range res.Columns {
			names = append(names, c.Name+":"+c.SQLType)
		}
		if got := strings.Join(names, ","); got != "plan:VARCHAR,rows:BIGINT,rows_examined:BIGINT,docs_read:BIGINT" {
			t.Fatalf("%s: columns %s", q, got)
		}
		if len(res.Rows) != 1 {
			t.Fatalf("%s: %d rows, want one", q, len(res.Rows))
		}
		row := res.Rows[0].Values
		if row[0] != (CellString{Value: plan}) {
			t.Fatalf("%s: plan %v, want %q", q, row[0], plan)
		}
		if row[3] != (CellLong{Value: int64(st.DocsRead)}) {
			t.Fatalf("%s: docs_read %v, stats say %d", q, row[3], st.DocsRead)
		}
	}
	res, _, err := f.run(false, "EXPLAIN SELECT n FROM t WHERE status = 'pending' LIMIT 7", nil)
	if err != nil || res.Rows[0].Values[1] != (CellLong{Value: 7}) {
		t.Fatalf("rows column: %v %v", res.Rows, err)
	}
	for stmt, want := range map[string]string{
		"EXPLAIN":                    "EXPLAIN takes a SELECT",
		"EXPLAIN DELETE FROM t":      "EXPLAIN takes a SELECT",
		"EXPLAIN UPDATE t SET a = 1": "EXPLAIN takes a SELECT",
	} {
		if _, err := (DefaultParser{}).Parse(stmt); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: got %v, want %q", stmt, err, want)
		}
	}
}
