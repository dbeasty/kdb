package server

import (
	"fmt"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/index/stores"
	"github.com/limidus/kdb/go/kdb/policy"
	"github.com/limidus/kdb/go/kdb/sql"
)

// expiryShape loads n documents of three kinds - expired, live and never-expiring - across two
// collections, and returns how many of each are in "tasks". Deadlines alternate between epoch
// milliseconds and RFC 3339 strings, the two forms expiry accepts.
func expiryShape(t *testing.T, rt *KdbServerRuntime, now time.Time, n int) (expiredTasks, liveTasks int) {
	t.Helper()
	for i := 0; i < n; i++ {
		coll := "tasks"
		if i%3 == 2 {
			coll = "notes"
		}
		var exp string
		switch i % 5 {
		case 0: // expired, millis
			exp = fmt.Sprintf(`,"_expireAt":%d`, now.Add(-time.Duration(i+1)*time.Second).UnixMilli())
		case 1: // expired, RFC 3339 with an offset
			exp = fmt.Sprintf(`,"_expireAt":%q`, now.Add(-time.Hour).In(time.FixedZone("x", 5*3600)).Format(time.RFC3339))
		case 2: // live
			exp = fmt.Sprintf(`,"_expireAt":%q`, now.Add(time.Hour).Format(time.RFC3339Nano))
		case 3: // an array is not a deadline: never expires, though the index files its elements
			exp = fmt.Sprintf(`,"_expireAt":[%d]`, now.Add(-time.Hour).UnixMilli())
		}
		upsertDoc(t, rt, fmt.Sprintf(`{"_c":%q,"userId":"u%d","n":%d%s}`, coll, i%7, i, exp))
		if coll == "tasks" {
			if i%5 <= 1 {
				expiredTasks++
			} else {
				liveTasks++
			}
		}
	}
	return expiredTasks, liveTasks
}

func countStats(t *testing.T, rt *KdbServerRuntime, q string, budget int) (string, string, *sql.ExecStats) {
	t.Helper()
	stats := &sql.ExecStats{}
	ctx := sql.QueryContext{NamespaceID: rt.Runtime.DefaultNamespace, Schema: rt.Runtime.Schema, RowBudget: budget, Stats: stats}
	res, err := rt.SQLEngine().Execute(q, ctx)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	if len(res.Rows) != 1 || len(res.Rows[0].Values) != 1 {
		t.Fatalf("%s: unexpected shape %+v", q, res.Rows)
	}
	return cellText(res.Rows[0].Values[0]), res.Plan, stats
}

func rowCount(t *testing.T, rt *KdbServerRuntime, q string) int {
	t.Helper()
	ctx := sql.QueryContext{NamespaceID: rt.Runtime.DefaultNamespace, Schema: rt.Runtime.Schema}
	res, err := rt.SQLEngine().Execute(q, ctx)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return len(res.Rows)
}

func createHashIndex(t *testing.T, p *RegistryIndexProvider, rt *KdbServerRuntime, name, field, using string) {
	t.Helper()
	if err := p.CreateIndex(sql.StmtCreateIndex{
		Name: name, Table: "docs", Fields: []sql.IndexField{{Path: field}}, Using: using,
	}, sql.QueryContext{NamespaceID: rt.Runtime.DefaultNamespace}); err != nil {
		t.Fatal(err)
	}
}

// U1: under an expiry policy, COUNT(*) - of the namespace, and of an indexed WHERE - is answered
// without reading the documents it counts. Only the expired candidates the expiry index nominates
// are read, and the answer agrees with what a read returns.
func TestExpiryCountReadsOnlyExpiredCandidates(t *testing.T) {
	rt := newTestRuntime(t)
	defer rt.Release()
	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	rt.SetClockForTest(func() time.Time { return now })
	p, err := rt.OpenIndexes(stores.Options{})
	if err != nil {
		t.Fatal(err)
	}
	createHashIndex(t, p, rt, "by_c", "_c", "HASH")
	createHashIndex(t, p, rt, "by_user", "userId", "HASH")
	const n = 500
	expiredTasks, liveTasks := expiryShape(t, rt, now, n)
	// The policy arrives after the data: building the expiry index scans once, here.
	rt.SetDocumentExpiry(&policy.DocumentExpiryPolicy{FieldPath: "_expireAt", SweepIntervalMillis: 3600000})
	if expiredTasks == 0 || liveTasks == 0 {
		t.Fatal("shape has no expired or no live tasks")
	}
	expiredAll := 2 * n / 5

	cases := []struct {
		q    string
		plan string
	}{
		{`SELECT COUNT(*) FROM docs`, "count:tree"},
		{`SELECT COUNT(*) FROM docs WHERE _c = 'tasks'`, "count:index"},
		{`SELECT COUNT(*) FROM docs WHERE _c = 'tasks' AND userId = 'u3'`, "count:index"},
		{`SELECT COUNT(*) FROM docs WHERE _c = 'tasks' AND userId IN ('u1', 'u2', NULL)`, "count:index"},
		{`SELECT COUNT(*) FROM docs WHERE userId = 'u3' AND _c = 'nothing'`, "count:index"},
	}
	for _, c := range cases {
		where := ""
		if i := len("SELECT COUNT(*) FROM docs"); len(c.q) > i {
			where = c.q[i:]
		}
		want := rowCount(t, rt, "SELECT kdb_id FROM docs"+where)
		// A row budget of 10 would abort any read of the namespace.
		got, plan, stats := countStats(t, rt, c.q, 10)
		if got != itoa(int64(want)) || plan != c.plan {
			t.Errorf("%s = %s via %s, want %d via %s", c.q, got, plan, want, c.plan)
		}
		if stats.DocsRead > expiredAll {
			t.Errorf("%s read %d documents; only the %d expired candidates may be read", c.q, stats.DocsRead, expiredAll)
		}
	}
	if got, _, _ := countStats(t, rt, `SELECT COUNT(*) FROM docs WHERE _c = 'tasks'`, 0); got != itoa(int64(liveTasks)) {
		t.Fatalf("tasks = %s, want %d live", got, liveTasks)
	}

	// A WHERE the indexes cannot serve exactly is counted by streaming: right answer, and nothing
	// retained however many match.
	got, _, stats := countStats(t, rt, `SELECT COUNT(*) FROM docs WHERE _c = 'tasks' AND n >= 0`, 0)
	if got != itoa(int64(liveTasks)) {
		t.Fatalf("streamed count = %s, want %d", got, liveTasks)
	}
	if stats.RetainedBytes != 0 {
		t.Fatalf("streamed count retained %d bytes, want 0", stats.RetainedBytes)
	}
}

// U1: the sweeper finds expired documents through the expiry index - it deletes exactly what has
// expired, leaves the array-valued "deadline" alone, and counts agree before and after.
func TestExpirySweepUsesTheIndex(t *testing.T) {
	rt := newTestRuntime(t)
	defer rt.Release()
	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	rt.SetClockForTest(func() time.Time { return now })
	p, err := rt.OpenIndexes(stores.Options{})
	if err != nil {
		t.Fatal(err)
	}
	createHashIndex(t, p, rt, "by_c", "_c", "HASH")
	rt.SetDocumentExpiry(&policy.DocumentExpiryPolicy{FieldPath: "_expireAt", SweepIntervalMillis: 3600000})
	// Documents written after the policy reach the index through the commit path.
	const n = 200
	expiredTasks, liveTasks := expiryShape(t, rt, now, n)

	tree, ok := rt.headTreeHash()
	if !ok {
		t.Fatal("no head")
	}
	docs, ok, err := rt.expiredDocsAt(tree, 0)
	if err != nil || !ok {
		t.Fatalf("expiry index cannot answer: ok=%v err=%v", ok, err)
	}
	if len(docs) != 2*n/5 {
		t.Fatalf("index found %d expired, want %d", len(docs), 2*n/5)
	}
	before, _, _ := countStats(t, rt, `SELECT COUNT(*) FROM docs WHERE _c = 'tasks'`, 0)
	deleted, err := rt.SweepExpiredNow()
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 2*n/5 {
		t.Fatalf("swept %d, want %d", deleted, 2*n/5)
	}
	after, plan, _ := countStats(t, rt, `SELECT COUNT(*) FROM docs WHERE _c = 'tasks'`, 0)
	if before != after || after != itoa(int64(liveTasks)) || plan != "count:index" {
		t.Fatalf("tasks before sweep %s, after %s via %s; want %d both (expired %d)", before, after, plan, liveTasks, expiredTasks)
	}
	// Time passes: the live hour-out deadlines expire too, found the same way.
	now = now.Add(2 * time.Hour)
	if deleted, err = rt.SweepExpiredNow(); err != nil || deleted != n/5 {
		t.Fatalf("second sweep deleted %d (%v), want %d", deleted, err, n/5)
	}
}

// The expiry index is a system index: a user's own index on the same field is still created, and
// user predicates on the field are served by the user's index with the user's (string) semantics.
func TestExpiryIndexCoexistsWithAUserIndexOnTheField(t *testing.T) {
	rt := newTestRuntime(t)
	defer rt.Release()
	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	rt.SetClockForTest(func() time.Time { return now })
	p, err := rt.OpenIndexes(stores.Options{})
	if err != nil {
		t.Fatal(err)
	}
	rt.SetDocumentExpiry(&policy.DocumentExpiryPolicy{FieldPath: "_expireAt", SweepIntervalMillis: 3600000})
	future := now.Add(time.Hour).Format(time.RFC3339)
	upsertDoc(t, rt, fmt.Sprintf(`{"_expireAt":%q}`, future))
	upsertDoc(t, rt, fmt.Sprintf(`{"_expireAt":%q}`, now.Add(2*time.Hour).Format(time.RFC3339)))
	before := rowCount(t, rt, fmt.Sprintf(`SELECT kdb_id FROM docs WHERE _expireAt = '%s'`, future))
	createHashIndex(t, p, rt, "user_exp", "_expireAt", "BTREE")
	q := fmt.Sprintf(`SELECT kdb_id FROM docs WHERE _expireAt = '%s'`, future)
	if got := rowCount(t, rt, q); got != 1 || before != 1 {
		t.Fatalf("%s: %d rows (scan said %d), want 1", q, got, before)
	}
	// Turning expiry off drops the system index and leaves the user's.
	rt.SetDocumentExpiry(nil)
	if _, ok := p.registry.ByName(expiryIndexName); ok {
		t.Fatal("expiry index still registered after expiry was turned off")
	}
	if _, ok := p.registry.ByName("user_exp"); !ok {
		t.Fatal("user index dropped with the expiry index")
	}
}
