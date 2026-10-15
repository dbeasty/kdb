package sql_test

import (
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/index"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/sql"
)

// An OR conjunct is served as the union of its disjuncts' access paths when every disjunct has
// one (executor.go orLookup). These pin the planner's side against the fake provider; the
// end-to-end equivalence with a scan over real indexes is go/kdb/server/or_index_union_test.go.

// countingProvider is fakeProvider plus exact counts, so the planner can cost conjuncts.
type countingProvider struct {
	*fakeProvider
}

func (p *countingProvider) ExactCount(ctx sql.QueryContext, field string, value sql.Cell) (int, bool, error) {
	byValue, ok := p.exact[field]
	if !ok {
		return 0, false, nil
	}
	return len(byValue[cellString(value)]), true, nil
}

func newOrFixture(t *testing.T, ns string) (*docFixture, *countingProvider) {
	p := &countingProvider{fakeProvider: &fakeProvider{
		exact: map[string]map[string][]codec.UUID{
			// 'open' over-returns bravo (which is 'done'): the residual must drop it.
			"status": {"open": ids(1, 3, 2), "done": ids(2, 5), "blocked": ids(4)},
			"tags":   {"x": ids(1, 3), "y": ids(1, 2, 3), "z": ids(3, 5)},
		},
		ranges:   map[string][]codec.UUID{"priority": ids(1, 2, 3, 4, 5)},
		fulltext: map[string][]index.RankedResult{"tasks_text": ranked(1, 2.5)},
	}}
	return newDocFixture(t, ns, schema.None(), p, corpus...), p
}

func TestOrUnionPlansAndResults(t *testing.T) {
	f, _ := newOrFixture(t, "app/or-union")
	for _, c := range []struct{ q, plan, want string }{
		{`SELECT title FROM tasks WHERE status = 'blocked' OR tags = 'z' ORDER BY title`,
			"index:or(eq(status)|eq(tags))", "Echo,charlie,delta"},
		// The residual drops bravo, which the 'open' lookup over-returns.
		{`SELECT title FROM tasks WHERE status = 'open' OR status = 'blocked' ORDER BY title`,
			"index:or(eq(status)|eq(status))", "alpha,charlie,delta"},
		{`SELECT title FROM tasks WHERE (status = 'open' AND priority > 2) OR tags IN ('z') ORDER BY title`,
			"index:or(eq(status)|in(tags))", "Echo,alpha,charlie"},
		{`SELECT title FROM tasks WHERE status = 'done' AND (tags = 'x' OR (tags = 'z' OR status = 'blocked')) ORDER BY title`,
			"index:eq(status)", "Echo"},
		{`SELECT title FROM tasks WHERE kdb_id = '00000000-0000-4000-8000-000000000004' OR tags = 'x' ORDER BY title`,
			"index:or(keylookup|eq(tags))", "alpha,charlie,delta"},
		// A disjunct with no index path: any document might satisfy it, so only a scan is correct.
		{`SELECT title FROM tasks WHERE status = 'blocked' OR title = 'bravo' ORDER BY title`,
			"fullscan", "bravo,delta"},
		// A ranking is cut at the search depth, so it never serves a disjunct.
		{`SELECT title FROM tasks WHERE MATCH(tasks_text, 'x') OR status = 'blocked' ORDER BY title`,
			"fullscan", "alpha,delta"},
	} {
		rows, res, err := f.query(c.q)
		if err != nil {
			t.Fatalf("%s: %v", c.q, err)
		}
		if res.Plan != c.plan || strings.Join(rows, ",") != c.want {
			t.Errorf("%s:\n got  %q %v\n want %q %s", c.q, res.Plan, rows, c.plan, c.want)
		}
	}
}

// The OR's estimate is the sum of its disjuncts', so a cheaper single conjunct wins and the OR's
// lookups are never made; and a cheap OR beats an expensive conjunct.
func TestOrUnionIsCostedAgainstOtherConjuncts(t *testing.T) {
	f, p := newOrFixture(t, "app/or-cost")
	p.exactCalls = nil
	rows, res, err := f.query(`SELECT title FROM tasks WHERE (status = 'open' OR tags = 'y') AND status = 'blocked'`)
	if err != nil {
		t.Fatal(err)
	}
	if res.Plan != "index:eq(status)" || len(rows) != 0 {
		t.Fatalf("plan %q rows %v", res.Plan, rows)
	}
	if strings.Join(p.exactCalls, ",") != "status=blocked" {
		t.Fatalf("the OR (estimate 6) must not be looked up after a 1-row conjunct: %v", p.exactCalls)
	}

	p.exactCalls = nil
	rows, res, err = f.query(`SELECT title FROM tasks WHERE tags = 'y' AND (status = 'blocked' OR kdb_id = '00000000-0000-4000-8000-000000000003') ORDER BY title`)
	if err != nil {
		t.Fatal(err)
	}
	if res.Plan != "index:or(eq(status)|keylookup)" || strings.Join(rows, ",") != "charlie" {
		t.Fatalf("plan %q rows %v", res.Plan, rows)
	}
	if strings.Join(p.exactCalls, ",") != "status=blocked" {
		t.Fatalf("the 3-row conjunct must not be looked up after a 2-row OR: %v", p.exactCalls)
	}
}

// Without an index provider an OR of document keys is still served by key.
func TestOrOfKeysNeedsNoProvider(t *testing.T) {
	f := newCorpusFixture(t, "app/or-keys")
	f.ctx.RowBudget = 2 // a scan of the five documents would exceed this
	_, res, err := f.query(`SELECT kdb_id FROM tasks WHERE kdb_id = '00000000-0000-4000-8000-000000000001' OR kdb_id IN ('00000000-0000-4000-8000-000000000003')`)
	if err != nil {
		t.Fatal(err)
	}
	if res.Plan != "index:or(keylookup|keylookup)" || len(res.Rows) != 2 {
		t.Fatalf("plan %q rows %d", res.Plan, len(res.Rows))
	}
}
