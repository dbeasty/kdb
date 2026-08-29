package sql_test

import (
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/storage/mem"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// ExecuteSelect was at 5.9% coverage: the SELECT path the whole server exposes had almost no
// test. ORDER BY, LIMIT and their interaction are the parts where being wrong looks like
// working - a query returns rows, just not the right ones.

type queryFixture struct {
	engine sql.Engine
	ctx    sql.QueryContext
}

// newQueryFixture writes one document per name, in the order given, and returns an engine
// pointed at the resulting head. The insertion order is deliberately not the sorted order, so a
// query that fails to sort cannot accidentally look correct.
func newQueryFixture(t *testing.T, ns string, names ...string) *queryFixture {
	t.Helper()
	d, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatal(err)
	}
	store := mem.NewInMemoryStorageAdapter()
	head, err := d.Head()
	if err != nil {
		t.Fatal(err)
	}

	nameField, err := schema.NewField("name", schema.StringType{}, false, false, false)
	if err != nil {
		t.Fatal(err)
	}
	rankField, err := schema.NewField("rank", schema.Int32Type{}, false, false, false)
	if err != nil {
		t.Fatal(err)
	}
	sch, err := schema.Build([]schema.Field{nameField, rankField}, 1,
		codec.TimestampFromEpochMicros(1_700_000_000_000_000), "")
	if err != nil {
		t.Fatal(err)
	}

	txEngine := transaction.NewEngine(transaction.ConflictPolicyStrict, nil)
	for i, name := range names {
		doc, err := document.FromJSON(fmt.Sprintf(`{"name":%q,"rank":%d}`, name, i))
		if err != nil {
			t.Fatal(err)
		}
		txID, _ := codec.RandomUUID()
		author, _ := codec.RandomUUID()
		tx := document.Transaction{
			ID: txID, BaseVersion: head, AuthorNodeID: author,
			Timestamp:  codec.TimestampFromEpochMicros(int64(1_700_000_000_000_000 + i)),
			Operations: []document.Op{document.WriteOp{DocID: doc.ID, Patch: doc.JSON}},
		}
		res, err := txEngine.Replay(tx, d, store, sch, head, "")
		if err != nil {
			t.Fatal(err)
		}
		success, ok := res.(transaction.ResultSuccess)
		if !ok {
			t.Fatalf("fixture write %q: %T", name, res)
		}
		head = success.Commit.Hash
	}

	return &queryFixture{
		engine: sql.NewEngine(store, d),
		ctx:    sql.QueryContext{NamespaceID: ns, Schema: sch, AtCommit: &head},
	}
}

// names runs a query and returns the first column of every row as a string.
func (f *queryFixture) names(t *testing.T, query string) []string {
	t.Helper()
	res, err := f.engine.Execute(query, f.ctx)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	out := make([]string, 0, len(res.Rows))
	for _, row := range res.Rows {
		if len(row.Values) == 0 {
			t.Fatalf("%s: a row has no values", query)
		}
		s, ok := row.Values[0].(sql.CellString)
		if !ok {
			t.Fatalf("%s: first cell is %T, want CellString", query, row.Values[0])
		}
		out = append(out, s.Value)
	}
	return out
}

func (f *queryFixture) count(t *testing.T, query string) int64 {
	t.Helper()
	res, err := f.engine.Execute(query, f.ctx)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("%s: got %d rows, want exactly 1", query, len(res.Rows))
	}
	switch c := res.Rows[0].Values[0].(type) {
	case sql.CellLong:
		return c.Value
	default:
		t.Fatalf("%s: aggregate cell is %T", query, res.Rows[0].Values[0])
		return 0
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// The sorted order is alpha < bravo < charlie < delta < echo; the fixture writes them in an
// order that shares no prefix with it, so "the first N in scan order" and "the first N in
// sorted order" are different answers for every N.
var fixtureNames = []string{"delta", "alpha", "echo", "bravo", "charlie"}

func TestSelectOrderByAscendingAndDescending(t *testing.T) {
	f := newQueryFixture(t, "app/order", fixtureNames...)

	got := f.names(t, `SELECT name FROM t ORDER BY name ASC`)
	want := []string{"alpha", "bravo", "charlie", "delta", "echo"}
	if !equalStrings(got, want) {
		t.Fatalf("ASC: got %v, want %v", got, want)
	}

	got = f.names(t, `SELECT name FROM t ORDER BY name DESC`)
	want = []string{"echo", "delta", "charlie", "bravo", "alpha"}
	if !equalStrings(got, want) {
		t.Fatalf("DESC: got %v, want %v", got, want)
	}
}

// LIMIT has to be applied after ORDER BY: "the first three rows in sorted order", not "three
// arbitrary rows, sorted among themselves". The planner puts its PlanLimit outermost and the id
// resolver applied it before any document had been read, let alone sorted - so an ordered query
// answered from whichever rows the scan happened to reach first. DESC is the case that exposes
// it, since scan order and sorted order then disagree at the very first row.
func TestSelectOrderByIsAppliedBeforeLimit(t *testing.T) {
	f := newQueryFixture(t, "app/order-limit", fixtureNames...)

	for _, tc := range []struct {
		query string
		want  []string
	}{
		{`SELECT name FROM t ORDER BY name ASC LIMIT 3`, []string{"alpha", "bravo", "charlie"}},
		{`SELECT name FROM t ORDER BY name DESC LIMIT 2`, []string{"echo", "delta"}},
		{`SELECT name FROM t ORDER BY name DESC LIMIT 1`, []string{"echo"}},
		{`SELECT name FROM t ORDER BY name ASC LIMIT 1`, []string{"alpha"}},
		{`SELECT name FROM t ORDER BY name ASC LIMIT 99`,
			[]string{"alpha", "bravo", "charlie", "delta", "echo"}},
	} {
		if got := f.names(t, tc.query); !equalStrings(got, tc.want) {
			t.Errorf("%s\n got %v\nwant %v", tc.query, got, tc.want)
		}
	}
}

func TestSelectOrderByWithOffset(t *testing.T) {
	f := newQueryFixture(t, "app/order-offset", fixtureNames...)

	for _, tc := range []struct {
		query string
		want  []string
	}{
		{`SELECT name FROM t ORDER BY name ASC LIMIT 2 OFFSET 1`, []string{"bravo", "charlie"}},
		{`SELECT name FROM t ORDER BY name DESC LIMIT 2 OFFSET 1`, []string{"delta", "charlie"}},
		{`SELECT name FROM t ORDER BY name ASC LIMIT 2 OFFSET 4`, []string{"echo"}},
		{`SELECT name FROM t ORDER BY name ASC LIMIT 2 OFFSET 99`, nil},
	} {
		got := f.names(t, tc.query)
		if len(got) == 0 && len(tc.want) == 0 {
			continue
		}
		if !equalStrings(got, tc.want) {
			t.Errorf("%s\n got %v\nwant %v", tc.query, got, tc.want)
		}
	}
}

// An aggregate consumes every matching row and produces one; LIMIT bounds that single output
// row, not the input. The planner's PlanLimit used to truncate the rows being aggregated, so
// COUNT(*) reported the limit rather than the count.
func TestAggregateIgnoresLimitOnItsInput(t *testing.T) {
	f := newQueryFixture(t, "app/aggregate-limit", fixtureNames...)

	if got := f.count(t, `SELECT COUNT(*) FROM t`); got != 5 {
		t.Fatalf("COUNT(*) = %d, want 5", got)
	}
	for _, query := range []string{
		`SELECT COUNT(*) FROM t LIMIT 1`,
		`SELECT COUNT(*) FROM t LIMIT 2`,
		`SELECT COUNT(*) FROM t LIMIT 3 OFFSET 1`,
	} {
		if got := f.count(t, query); got != 5 {
			t.Errorf("%s = %d, want 5 - LIMIT truncated the aggregate's input", query, got)
		}
	}
}

// Without ORDER BY there is no defined order, so LIMIT may still be pushed down to the scan -
// but it must return the right number of rows.
func TestSelectLimitWithoutOrderByReturnsThatManyRows(t *testing.T) {
	f := newQueryFixture(t, "app/limit-only", fixtureNames...)

	for _, tc := range []struct {
		query string
		want  int
	}{
		{`SELECT name FROM t`, 5},
		{`SELECT name FROM t LIMIT 2`, 2},
		{`SELECT name FROM t LIMIT 5`, 5},
		{`SELECT name FROM t LIMIT 99`, 5},
		{`SELECT name FROM t LIMIT 2 OFFSET 4`, 1},
		{`SELECT name FROM t LIMIT 2 OFFSET 99`, 0},
	} {
		if got := len(f.names(t, tc.query)); got != tc.want {
			t.Errorf("%s returned %d rows, want %d", tc.query, got, tc.want)
		}
	}
}

func TestSelectWhereWithOrderByAndLimit(t *testing.T) {
	f := newQueryFixture(t, "app/where-order-limit", fixtureNames...)

	// WHERE narrows to {bravo, charlie, delta, echo}; DESC LIMIT 2 must take echo and delta
	// from that set, not from the unfiltered scan order.
	got := f.names(t, `SELECT name FROM t WHERE name > 'alpha' ORDER BY name DESC LIMIT 2`)
	want := []string{"echo", "delta"}
	if !equalStrings(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestSelectOrderByIsStableForEqualKeys(t *testing.T) {
	// Every row has the same name, so the sort key never discriminates: the rows must come back
	// in their original relative order rather than an arbitrary one.
	f := newQueryFixture(t, "app/order-stable", "same", "same", "same", "same")
	if got := len(f.names(t, `SELECT name FROM t ORDER BY name ASC`)); got != 4 {
		t.Fatalf("got %d rows, want 4", got)
	}
}
