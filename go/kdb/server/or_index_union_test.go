package server

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/index/stores"
	"github.com/limidus/kdb/go/kdb/sql"
)

// An OR conjunct is served as the union of its disjuncts' index paths when every disjunct has
// one. These tests run each query against the same documents before any index exists (a full
// scan, the reference answer) and after, and require identical results - plus that the indexed
// run really avoided the scan.

// loadOrShape writes n documents shaped like qtask's access-control data, with the awkward cases
// spread through them: a missing or null userId, collaborators as an array of objects (a
// multikey path), an empty collaborators array, a missing n, and an unindexed tag.
func loadOrShape(t testing.TB, srv *KdbServerRuntime, n int) []codec.UUID {
	t.Helper()
	var out []codec.UUID
	for i := 0; i < n; i++ {
		id, err := codec.RandomUUID()
		if err != nil {
			t.Fatal(err)
		}
		c := "tasks"
		if i%3 == 0 {
			c = "projects"
		}
		fields := []string{fmt.Sprintf(`"_c":%q`, c), fmt.Sprintf(`"projectId":"p%d"`, i%11), fmt.Sprintf(`"tag":"t%d"`, i%4)}
		switch {
		case i%17 == 0: // no userId at all
		case i%19 == 0:
			fields = append(fields, `"userId":null`)
		default:
			fields = append(fields, fmt.Sprintf(`"userId":"u%d"`, i%30))
		}
		switch {
		case i%5 == 0:
			fields = append(fields, `"collaborators":[]`)
		case i%2 == 0:
			fields = append(fields, fmt.Sprintf(`"collaborators":[{"userId":"u%d"},{"userId":"u%d"}]`, (i+1)%30, (i+7)%30))
		}
		if i%13 != 0 {
			fields = append(fields, fmt.Sprintf(`"n":%d`, i))
		}
		body := "{" + strings.Join(fields, ",") + "}"
		if _, err := srv.Upsert(srv.Runtime.DefaultNamespace, id, body, auth.Principal{}); err != nil {
			t.Fatal(err)
		}
		out = append(out, id)
	}
	return out
}

func createOrIndexes(t testing.TB, srv *KdbServerRuntime) {
	t.Helper()
	provider, err := srv.OpenIndexes(stores.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ns := srv.Runtime.DefaultNamespace
	for _, ix := range []struct{ path, using string }{
		{"_c", "HASH"}, {"userId", "HASH"}, {"collaborators.userId", "HASH"}, {"projectId", "HASH"}, {"n", "BTREE"},
	} {
		if err := provider.CreateIndex(sql.StmtCreateIndex{
			Name: "by_" + strings.ReplaceAll(ix.path, ".", "_"), Table: "docs", Fields: []sql.IndexField{{Path: ix.path}}, Using: ix.using,
		}, sql.QueryContext{NamespaceID: ns}); err != nil {
			t.Fatalf("CREATE INDEX %s: %v", ix.path, err)
		}
	}
}

type orRun struct {
	ids  []string
	plan string
	seen int // rows examined
}

func runOr(t testing.TB, srv *KdbServerRuntime, q string, budget int, params ...sql.Parameter) orRun {
	t.Helper()
	stats := &sql.ExecStats{}
	ctx := sql.QueryContext{NamespaceID: srv.Runtime.DefaultNamespace, Schema: srv.Runtime.Schema, Parameters: params, Stats: stats, RowBudget: budget}
	res, err := srv.SQLEngine().Execute(q, ctx)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	var out []string
	for _, r := range res.Rows {
		out = append(out, cellString(r.Values[0]))
	}
	sort.Strings(out)
	return orRun{ids: out, plan: res.Plan, seen: stats.RowsExamined}
}

func cellString(c sql.Cell) string {
	switch v := c.(type) {
	case sql.CellString:
		return v.Value
	case sql.CellLong:
		return itoa(v.Value)
	}
	return fmt.Sprintf("%v", c)
}

type orCase struct {
	name   string
	where  string
	params []sql.Parameter
	plan   string // wanted plan once indexed ("" = any index plan, "fullscan" = must scan)
}

var orCases = []orCase{
	{"two-way", `userId = 'u3' OR collaborators.userId = 'u3'`, nil, "index:or(eq(userId)|eq(collaborators.userId))"},
	{"qtask shape with parameters", `_c = ? AND ((userId = ?) OR (collaborators.userId = ?))`,
		[]sql.Parameter{sql.ParamString{Value: "tasks"}, sql.ParamString{Value: "u4"}, sql.ParamString{Value: "u4"}},
		"index:or(eq(userId)|eq(collaborators.userId))"},
	{"three-way with IN", `userId = 'u3' OR collaborators.userId = 'u3' OR projectId IN ('p1', 'p2')`, nil,
		"index:or(eq(userId)|eq(collaborators.userId)|in(projectId))"},
	{"ranges", `n < 20 OR n BETWEEN 300 AND 310 OR n >= 590 OR userId = 'u5'`, nil,
		"index:or(range(n)|range(n)|range(n)|eq(userId))"},
	{"strict and inclusive range bounds", `n > 595 OR n <= 3 OR 7 > n`, nil, ""},
	{"OR nested inside AND", `_c = 'projects' AND (userId = 'u6' OR collaborators.userId = 'u6') AND n > 100`, nil, ""},
	{"AND inside OR", `(userId = 'u3' AND n > 100) OR projectId IN ('p4')`, nil, "index:or(eq(userId)|in(projectId))"},
	{"OR inside AND inside OR", `(_c = 'projects' AND (userId = 'u1' OR userId = 'u2')) OR collaborators.userId = 'u9'`, nil,
		"index:or(or(eq(userId)|eq(userId))|eq(collaborators.userId))"},
	{"duplicates across disjuncts", `userId = 'u3' OR userId = 'u3' OR (_c = 'projects' AND userId = 'u3') OR collaborators.userId = 'u3'`, nil, ""},
	{"one disjunct unindexed", `userId = 'u3' OR tag = 't1'`, nil, "fullscan"},
	{"one disjunct IS NULL", `userId IS NULL OR collaborators.userId = 'u2'`, nil, "fullscan"},
	{"one disjunct compares with NULL", `userId = NULL OR userId = 'u2'`, nil, "fullscan"},
	{"NULL in an IN list", `userId IN ('u2', NULL) OR collaborators.userId = 'u8'`, nil, "index:or(in(userId)|eq(collaborators.userId))"},
	{"NOT of an OR", `NOT (userId = 'u3' OR userId = 'u4')`, nil, "fullscan"},
	{"no matches", `userId = 'nobody' OR collaborators.userId = 'nobody'`, nil, "index:or(eq(userId)|eq(collaborators.userId))"},
}

func TestOrIsServedFromAUnionOfIndexLookups(t *testing.T) {
	srv := newTestRuntime(t)
	docs := loadOrShape(t, srv, 600)

	// A key lookup is a disjunct like any other.
	keyCase := orCase{"key lookup disjunct", fmt.Sprintf(`kdb_id = '%s' OR userId = 'u3'`, docs[17]), nil, "index:or(keylookup|eq(userId))"}
	cases := append(append([]orCase{}, orCases...), keyCase)

	want := map[string]orRun{}
	for _, c := range cases {
		r := runOr(t, srv, `SELECT kdb_id FROM docs WHERE `+c.where, 0, c.params...)
		if c.name != keyCase.name && r.plan != "fullscan" {
			t.Fatalf("%s: unindexed plan %q, want fullscan", c.name, r.plan)
		}
		want[c.name] = r
	}

	createOrIndexes(t, srv)
	for _, c := range cases {
		got := runOr(t, srv, `SELECT kdb_id FROM docs WHERE `+c.where, 0, c.params...)
		if strings.Join(got.ids, ",") != strings.Join(want[c.name].ids, ",") {
			t.Errorf("%s: indexed run returned %d rows, scan returned %d", c.name, len(got.ids), len(want[c.name].ids))
		}
		switch {
		case c.plan == "fullscan":
			if got.plan != "fullscan" {
				t.Errorf("%s: plan %q, want fullscan", c.name, got.plan)
			}
		case c.plan != "":
			if got.plan != c.plan {
				t.Errorf("%s: plan %q, want %q", c.name, got.plan, c.plan)
			}
		default:
			if got.plan == "fullscan" {
				t.Errorf("%s: fell back to a full scan", c.name)
			}
		}
		if got.plan != "fullscan" && got.seen >= 600 {
			t.Errorf("%s: plan %q examined %d rows - as many as a scan", c.name, got.plan, got.seen)
		}
		t.Logf("%-36s rows=%-4d plan=%s examined=%d (scan examined %d)", c.name, len(got.ids), got.plan, got.seen, want[c.name].seen)
	}
	// Sanity: the cases are not trivially empty.
	if len(want["two-way"].ids) == 0 || len(want["three-way with IN"].ids) <= len(want["two-way"].ids) {
		t.Fatalf("fixture too thin: two-way %d rows, three-way %d", len(want["two-way"].ids), len(want["three-way with IN"].ids))
	}
}

// The planner still prefers a single cheap conjunct over a large OR: the OR's estimate is the sum
// of its disjuncts'.
func TestOrDoesNotBeatACheaperConjunct(t *testing.T) {
	srv := newTestRuntime(t)
	loadOrShape(t, srv, 600)
	createOrIndexes(t, srv)
	r := runOr(t, srv, `SELECT kdb_id FROM docs WHERE (userId = 'u3' OR collaborators.userId = 'u3' OR projectId IN ('p1', 'p2')) AND n = 63`, 0)
	if r.plan != "index:range(n)" && r.plan != "index:eq(n)" {
		t.Fatalf("plan %q, want the single n lookup", r.plan)
	}
	if len(r.ids) != 1 {
		t.Fatalf("got %d rows, want 1", len(r.ids))
	}
	// And the other way round: a big conjunct loses to a small OR.
	r = runOr(t, srv, `SELECT kdb_id FROM docs WHERE _c = 'tasks' AND (userId = 'u3' OR collaborators.userId = 'u3')`, 0)
	if !strings.HasPrefix(r.plan, "index:or(") {
		t.Fatalf("plan %q, want the OR union over the collection tag", r.plan)
	}
}

func TestCountWithOrMatchesTheScan(t *testing.T) {
	srv := newTestRuntime(t)
	loadOrShape(t, srv, 600)
	queries := []string{
		`SELECT COUNT(*) FROM docs WHERE userId = 'u3' OR collaborators.userId = 'u3'`,
		`SELECT COUNT(*) FROM docs WHERE _c = 'tasks' AND (userId = 'u3' OR collaborators.userId = 'u3' OR projectId IN ('p1', 'p2'))`,
		`SELECT COUNT(*) FROM docs WHERE userId = 'u3' OR tag = 't2'`,
	}
	want := map[string]orRun{}
	for _, q := range queries {
		want[q] = runOr(t, srv, q, 0)
	}
	createOrIndexes(t, srv)
	for i, q := range queries {
		got := runOr(t, srv, q, 0)
		if got.ids[0] != want[q].ids[0] {
			t.Errorf("%s: %s indexed, %s scanned", q, got.ids[0], want[q].ids[0])
		}
		if i < 2 && !strings.HasPrefix(got.plan, "index:or(") {
			t.Errorf("%s: plan %q", q, got.plan)
		}
	}
}

// A paged SELECT whose WHERE is served by the union reads every matching row exactly once, in
// document id order (keyset paging), whatever disjunct found it.
func TestPagedSelectWithOr(t *testing.T) {
	srv := newTestRuntime(t)
	loadOrShape(t, srv, 600)
	q := `SELECT kdb_id FROM docs WHERE userId = 'u3' OR collaborators.userId = 'u3' OR projectId IN ('p1', 'p2')`
	scanned := runOr(t, srv, q, 0)
	createOrIndexes(t, srv)

	head, err := srv.Runtime.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	for _, size := range []int{1, 7, 50} {
		var got []string
		var after *codec.UUID
		offset := 0
		for pages := 0; ; pages++ {
			if pages > 1000 {
				t.Fatal("paging does not terminate")
			}
			ctx := sql.QueryContext{NamespaceID: srv.Runtime.DefaultNamespace, Schema: srv.Runtime.Schema, AtCommit: &head,
				Page: &sql.PageRequest{Offset: offset, Size: size, After: after}}
			res, err := srv.SQLEngine().Execute(q, ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(res.Plan, "index:or(") && len(res.Rows) > 0 {
				t.Fatalf("page plan %q", res.Plan)
			}
			for _, r := range res.Rows {
				got = append(got, cellString(r.Values[0]))
			}
			offset += len(res.Rows)
			if !res.More {
				break
			}
			if len(res.RowDocIDs) == 0 {
				t.Fatal("a keyset page carries no row ids")
			}
			last := res.RowDocIDs[len(res.RowDocIDs)-1]
			after = &last
		}
		for i := 1; i < len(got); i++ {
			a, _ := codec.ParseUUID(got[i-1])
			b, _ := codec.ParseUUID(got[i])
			if bytes.Compare(a.Bytes(), b.Bytes()) >= 0 {
				t.Fatalf("size %d: rows not in document id order at %d", size, i)
			}
		}
		sorted := append([]string{}, got...)
		sort.Strings(sorted)
		if strings.Join(sorted, ",") != strings.Join(scanned.ids, ",") {
			t.Fatalf("size %d: paged %d rows, scan found %d", size, len(got), len(scanned.ids))
		}
	}
}

func dmlTargets(t *testing.T, srv *KdbServerRuntime, q string, budget int) []string {
	t.Helper()
	ctx := sql.QueryContext{NamespaceID: srv.Runtime.DefaultNamespace, Schema: srv.Runtime.Schema, RowBudget: budget}
	res, err := srv.SQLEngine().ExecuteDML(q, ctx)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	var out []string
	for _, op := range res.Operations {
		switch o := op.(type) {
		case document.WriteOp:
			out = append(out, o.DocID.String())
		case document.DeleteOp:
			out = append(out, o.DocID.String())
		}
	}
	sort.Strings(out)
	if len(out) != res.RowsAffected {
		t.Fatalf("%s: %d operations, %d rows affected", q, len(out), res.RowsAffected)
	}
	return out
}

// UPDATE and DELETE resolve their rows through the same access path: identical targets with and
// without indexes, and the indexed run fits a row budget a scan would exceed.
func TestUpdateAndDeleteWithOr(t *testing.T) {
	srv := newTestRuntime(t)
	loadOrShape(t, srv, 600)
	stmts := []string{
		`UPDATE docs SET marked = true WHERE userId = 'u3' OR collaborators.userId = 'u3'`,
		`DELETE FROM docs WHERE (userId = 'u5' AND n > 100) OR projectId IN ('p4') OR collaborators.userId = 'u5'`,
	}
	want := map[string][]string{}
	for _, q := range stmts {
		want[q] = dmlTargets(t, srv, q, 0)
		if len(want[q]) == 0 {
			t.Fatalf("%s: fixture matches nothing", q)
		}
	}
	createOrIndexes(t, srv)
	for _, q := range stmts {
		got := dmlTargets(t, srv, q, 200) // a scan of 600 documents would exceed this
		if strings.Join(got, ",") != strings.Join(want[q], ",") {
			t.Errorf("%s: %d targets indexed, %d scanned", q, len(got), len(want[q]))
		}
	}
}

// TestOrUnionAvoidsTheScanAtScale times qtask's access-control shape on 20k documents, scanned
// and indexed. It reports the numbers; the assertion is only on rows examined, which is exact.
func TestOrUnionAvoidsTheScanAtScale(t *testing.T) {
	if testing.Short() {
		t.Skip("loads 20k documents")
	}
	const n = 20000
	srv := newTestRuntime(t)
	loadOrShape(t, srv, n)
	queries := []string{
		`SELECT kdb_id FROM docs WHERE _c = 'tasks' AND ((userId = 'u3') OR (collaborators.userId = 'u3'))`,
		`SELECT kdb_id FROM docs WHERE _c = 'tasks' AND (userId = 'u3' OR collaborators.userId = 'u3' OR projectId IN ('p1', 'p2'))`,
	}
	median := func(q string) (time.Duration, orRun) {
		var samples []time.Duration
		var r orRun
		for i := 0; i < 7; i++ {
			start := time.Now()
			r = runOr(t, srv, q, 0)
			samples = append(samples, time.Since(start))
		}
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		return samples[len(samples)/2], r
	}
	before := map[string]time.Duration{}
	scans := map[string]orRun{}
	for _, q := range queries {
		before[q], scans[q] = median(q)
	}
	createOrIndexes(t, srv)
	for _, q := range queries {
		after, r := median(q)
		if strings.Join(r.ids, ",") != strings.Join(scans[q].ids, ",") {
			t.Errorf("%s: results differ", q)
		}
		if r.seen >= n/2 {
			t.Errorf("%s: examined %d rows", q, r.seen)
		}
		t.Logf("n=%d rows=%d scan %s (examined %d) -> %s %s (examined %d): %s",
			n, len(r.ids), before[q].Round(time.Microsecond), scans[q].seen, r.plan, after.Round(time.Microsecond), r.seen, q)
	}
}
