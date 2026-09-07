package sql_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/index"
	"github.com/limidus/kdb/go/kdb/index/fusion"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/sql"
)

// SQL half of Component 66: the planner's index selection (kdb-spec-layer16 §9.1, §9.3)
// against a fake IndexProvider. The fake records every call so tests can assert both that an
// index was consulted and with what strictness.

type rangeCall struct {
	field     string
	low, high sql.Cell
	lowInc    bool
	highInc   bool
}

type fakeProvider struct {
	exact    map[string]map[string][]codec.UUID // field -> cellString(value) -> ids
	ranges   map[string][]codec.UUID            // field -> ids returned for any range
	fulltext map[string][]index.RankedResult    // index name -> hits
	vectors  map[string][]index.RankedResult    // field -> hits

	exactCalls  []string
	rangeCalls  []rangeCall
	ftDepths    []int
	vecDepths   []int
	lastQuery   string
	lastVector  []float32
	lastIndexOK bool
}

func (p *fakeProvider) FullTextSearch(ctx sql.QueryContext, indexOrField, query string, depth int) ([]index.RankedResult, bool, error) {
	hits, ok := p.fulltext[indexOrField]
	p.ftDepths = append(p.ftDepths, depth)
	p.lastQuery = query
	p.lastIndexOK = ok
	if depth > 0 && len(hits) > depth {
		hits = hits[:depth]
	}
	return hits, ok, nil
}

func (p *fakeProvider) VectorSearch(ctx sql.QueryContext, field string, vector []float32, depth int) ([]index.RankedResult, bool, error) {
	hits, ok := p.vectors[field]
	p.vecDepths = append(p.vecDepths, depth)
	p.lastVector = vector
	return hits, ok, nil
}

func (p *fakeProvider) ExactLookup(ctx sql.QueryContext, field string, value sql.Cell) ([]codec.UUID, bool, error) {
	p.exactCalls = append(p.exactCalls, field+"="+cellString(value))
	byValue, ok := p.exact[field]
	if !ok {
		return nil, false, nil
	}
	return byValue[cellString(value)], true, nil
}

func (p *fakeProvider) RangeLookup(ctx sql.QueryContext, field string, low, high sql.Cell, lowInclusive, highInclusive bool) ([]codec.UUID, bool, error) {
	p.rangeCalls = append(p.rangeCalls, rangeCall{field, low, high, lowInclusive, highInclusive})
	ids, ok := p.ranges[field]
	return ids, ok, nil
}

func ids(ns ...int) []codec.UUID {
	out := make([]codec.UUID, len(ns))
	for i, n := range ns {
		out[i] = corpusID(n)
	}
	return out
}

func ranked(pairs ...any) []index.RankedResult {
	var out []index.RankedResult
	for i := 0; i+1 < len(pairs); i += 2 {
		out = append(out, index.RankedResult{DocID: corpusID(pairs[i].(int)), Score: float32(pairs[i+1].(float64))})
	}
	return out
}

func newIndexedFixture(t *testing.T, ns string) (*docFixture, *fakeProvider) {
	p := &fakeProvider{
		exact: map[string]map[string][]codec.UUID{
			// The status index deliberately returns a superset for 'open' (bravo is 'done') so
			// the test can prove the residual filter still runs over index results.
			"status": {"open": ids(1, 3, 2), "done": ids(2, 5), "blocked": ids(4)},
		},
		ranges:   map[string][]codec.UUID{"priority": ids(1, 2, 3, 4, 5)},
		fulltext: map[string][]index.RankedResult{"tasks_text": ranked(1, 2.5, 3, 1.25, 2, 0.5)},
		vectors:  map[string][]index.RankedResult{"embedding": ranked(5, 0.9, 1, 0.8, 4, 0.7)},
	}
	return newDocFixture(t, ns, schema.None(), p, corpus...), p
}

func TestIndexExactLookupIsUsedAndResidualStillApplies(t *testing.T) {
	f, p := newIndexedFixture(t, "app/idx-eq")
	rows, res, err := f.query(`SELECT title FROM tasks WHERE status = 'open' ORDER BY title`)
	if err != nil {
		t.Fatal(err)
	}
	if res.Plan != "index:eq(status)" {
		t.Fatalf("plan: %q", res.Plan)
	}
	if strings.Join(rows, ",") != "alpha,charlie" {
		t.Fatalf("residual must drop the index's false positive (bravo): %v", rows)
	}
	if len(p.exactCalls) != 1 || p.exactCalls[0] != "status=open" {
		t.Fatalf("calls: %v", p.exactCalls)
	}
	// The other conjunct is residual-only; the constant may sit on either side.
	rows, res, _ = f.query(`SELECT title FROM tasks WHERE title > 'a' AND 'done' = status`)
	if res.Plan != "index:eq(status)" || strings.Join(rows, ",") != "bravo" {
		t.Fatalf("%q %v", res.Plan, rows)
	}
	// Parameters are usable as index keys.
	rows, res, _ = f.query(`SELECT title FROM tasks t WHERE t.status = ?`, sql.ParamString{Value: "blocked"})
	if res.Plan != "index:eq(status)" || strings.Join(rows, ",") != "delta" {
		t.Fatalf("%q %v", res.Plan, rows)
	}
	// An index that says "no index for this field" falls back to a full scan, correctly.
	rows, res, _ = f.query(`SELECT title FROM tasks WHERE title = 'alpha'`)
	if res.Plan != "fullscan" || strings.Join(rows, ",") != "alpha" {
		t.Fatalf("%q %v", res.Plan, rows)
	}
	// OR is not a conjunct; NULL keys and reserved columns are never looked up.
	_, res, _ = f.query(`SELECT title FROM tasks WHERE status = 'open' OR status = 'done'`)
	if res.Plan != "fullscan" {
		t.Fatalf("OR must not use the index: %q", res.Plan)
	}
	_, res, _ = f.query(`SELECT title FROM tasks WHERE status = NULL`)
	if res.Plan != "fullscan" {
		t.Fatalf("NULL key must not use the index: %q", res.Plan)
	}
	_, res, _ = f.query(`SELECT title FROM tasks WHERE kdb_id = 'x'`)
	if res.Plan != "fullscan" {
		t.Fatalf("reserved column must not use the index: %q", res.Plan)
	}
}

func TestIndexRangeLookupHasCorrectStrictness(t *testing.T) {
	f, p := newIndexedFixture(t, "app/idx-range")
	type tc struct {
		q               string
		low, high       string
		lowInc, highInc bool
		want            string
	}
	for _, c := range []tc{
		{`SELECT title FROM tasks WHERE priority > 2 ORDER BY title`, "2", "NULL", false, false, "Echo,alpha"},
		{`SELECT title FROM tasks WHERE priority >= 2 ORDER BY title`, "2", "NULL", true, false, "Echo,alpha,charlie"},
		{`SELECT title FROM tasks WHERE priority < 2 ORDER BY title`, "NULL", "2", false, false, "bravo"},
		{`SELECT title FROM tasks WHERE priority <= 2 ORDER BY title`, "NULL", "2", false, true, "bravo,charlie"},
		{`SELECT title FROM tasks WHERE 2 < priority ORDER BY title`, "2", "NULL", false, false, "Echo,alpha"},
		{`SELECT title FROM tasks WHERE priority BETWEEN 2 AND 3 ORDER BY title`, "2", "3", true, true, "alpha,charlie"},
		{`SELECT title FROM tasks WHERE priority BETWEEN ? AND ? ORDER BY title`, "1", "2", true, true, "bravo,charlie"},
	} {
		p.rangeCalls = nil
		rows, res, err := f.query(c.q, sql.ParamInt{Value: 1}, sql.ParamInt{Value: 2})
		if err != nil {
			t.Fatal(err)
		}
		if res.Plan != "index:range(priority)" {
			t.Errorf("%s: plan %q", c.q, res.Plan)
		}
		if len(p.rangeCalls) != 1 {
			t.Fatalf("%s: %d range calls", c.q, len(p.rangeCalls))
		}
		call := p.rangeCalls[0]
		lowS, highS := "NULL", "NULL"
		if call.low != nil {
			lowS = cellString(call.low)
		}
		if call.high != nil {
			highS = cellString(call.high)
		}
		if lowS != c.low || highS != c.high || call.lowInc != c.lowInc || call.highInc != c.highInc {
			t.Errorf("%s: got [%s,%s] inc=(%v,%v), want [%s,%s] inc=(%v,%v)", c.q, lowS, highS, call.lowInc, call.highInc, c.low, c.high, c.lowInc, c.highInc)
		}
		if strings.Join(rows, ",") != c.want {
			t.Errorf("%s: rows %v, want %s", c.q, rows, c.want)
		}
	}
}

func TestIndexInLookupUnionsPerValue(t *testing.T) {
	f, p := newIndexedFixture(t, "app/idx-in")
	rows, res, err := f.query(`SELECT title FROM tasks WHERE status IN ('done', 'blocked', 'done') ORDER BY title`)
	if err != nil {
		t.Fatal(err)
	}
	if res.Plan != "index:in(status)" || strings.Join(rows, ",") != "Echo,bravo,delta" {
		t.Fatalf("%q %v", res.Plan, rows)
	}
	if len(p.exactCalls) != 3 {
		t.Fatalf("one lookup per value: %v", p.exactCalls)
	}
	// A value the index cannot serve makes the whole IN residual-only.
	_, res, _ = f.query(`SELECT title FROM tasks WHERE title IN ('alpha')`)
	if res.Plan != "fullscan" {
		t.Fatalf("%q", res.Plan)
	}
}

func TestIndexUpdateAndDeleteUseTheIndexPath(t *testing.T) {
	f, p := newIndexedFixture(t, "app/idx-dml")
	res, err := f.commitDML(`UPDATE tasks SET status = 'closed' WHERE status = 'open'`)
	if err != nil || res.RowsAffected != 2 {
		t.Fatalf("%+v %v", res, err)
	}
	if len(p.exactCalls) == 0 {
		t.Fatal("UPDATE should resolve its rows through the index")
	}
	// The fake index is deliberately not maintained by the write, so re-read through a
	// non-indexed predicate: what matters here is that the UPDATE resolved its rows via the
	// index and still wrote the right two documents.
	f.expect(`SELECT title, status FROM tasks WHERE title IN ('alpha', 'charlie') ORDER BY title`, "alpha|closed", "charlie|closed")
}

func TestMatchRequiresFullTextIndex(t *testing.T) {
	f, _ := newIndexedFixture(t, "app/idx-nomatch")
	f.expectPlanningError(`SELECT title FROM tasks WHERE MATCH(nosuch_index, 'deploy')`, "no FULLTEXT index for nosuch_index")
	f.expectPlanningError(`SELECT MATCH(nosuch_index, 'deploy') AS s FROM tasks`, "no FULLTEXT index for nosuch_index")
	f.expectPlanningError(`SELECT SIMILARITY(nosuch, [1, 2]) AS s FROM tasks ORDER BY s DESC LIMIT 1`, "no VECTOR index for nosuch")
	f.expectPlanningError(`SELECT title FROM tasks WHERE MATCH(tasks_text, 5)`, "MATCH query must be a string")
}

func TestMatchPredicateProjectionAndDepth(t *testing.T) {
	f, p := newIndexedFixture(t, "app/idx-match")
	// As a predicate: hits in rank order, with the plan naming the index.
	rows, res, err := f.query(`SELECT title FROM tasks WHERE MATCH(tasks_text, 'deploy staging')`)
	if err != nil {
		t.Fatal(err)
	}
	if res.Plan != "fulltext(tasks_text)" || strings.Join(rows, ",") != "alpha,charlie,bravo" {
		t.Fatalf("%q %v", res.Plan, rows)
	}
	if p.lastQuery != "deploy staging" || p.ftDepths[len(p.ftDepths)-1] != 0 {
		t.Fatalf("query %q depth %v (no LIMIT means every hit)", p.lastQuery, p.ftDepths)
	}
	// Residual conjuncts still apply on top of the hits.
	rows, _, _ = f.query(`SELECT title FROM tasks WHERE MATCH(tasks_text, 'deploy') AND status = 'open'`)
	if strings.Join(rows, ",") != "alpha,charlie" {
		t.Fatalf("%v", rows)
	}
	// As a projection: the score (a DOUBLE cell), 0 for non-hits.
	rows, res, err = f.query(`SELECT title, MATCH(tasks_text, 'deploy') AS score FROM tasks ORDER BY title`)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(rows, ";") != "Echo|0;alpha|2.5;bravo|0.5;charlie|1.25;delta|0" {
		t.Fatalf("%v", rows)
	}
	if res.Columns[1].SQLType != "DOUBLE" {
		t.Fatalf("score column type: %+v", res.Columns[1])
	}
	if _, ok := res.Rows[0].Values[1].(sql.CellDouble); !ok {
		t.Fatalf("score cell: %T", res.Rows[0].Values[1])
	}
	// The canonical spec query. The identical MATCH in the projection and in WHERE is one
	// search expression, so it is resolved exactly once, at depth max(50, 4*(n+m)).
	p.ftDepths = nil
	rows, res, err = f.query(`SELECT kdb_id, _doc, MATCH(tasks_text, 'deploy staging') AS score FROM tasks WHERE MATCH(tasks_text, 'deploy staging') ORDER BY score DESC LIMIT 2`)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.ftDepths) != 1 || p.ftDepths[0] != 50 {
		t.Fatalf("the same MATCH must be resolved once with depth 50: %v", p.ftDepths)
	}
	if res.Plan != "fulltext(tasks_text)" || len(rows) != 2 || !strings.HasPrefix(rows[0], corpusID(1).String()) || !strings.HasSuffix(rows[0], "|2.5") {
		t.Fatalf("%q %v", res.Plan, rows)
	}
	p.ftDepths = nil
	_, _, _ = f.query(`SELECT MATCH(tasks_text, 'x') AS score FROM tasks ORDER BY score DESC LIMIT 100 OFFSET 20`)
	if p.ftDepths[0] != 480 {
		t.Fatalf("depth = 4*(100+20): %v", p.ftDepths)
	}
	p.ftDepths = nil
	_, _, _ = f.query(`SELECT MATCH(tasks_text, 'x') AS score FROM tasks ORDER BY score DESC LIMIT 1000`)
	if p.ftDepths[0] != 1000 {
		t.Fatalf("depth capped at 1000: %v", p.ftDepths)
	}
}

func TestSimilarityWithLiteralAndParameterVectors(t *testing.T) {
	f, p := newIndexedFixture(t, "app/idx-sim")
	rows, res, err := f.query(`SELECT title, SIMILARITY(embedding, [0.1, 0.2, 0.3]) AS score FROM tasks ORDER BY score DESC LIMIT 2`)
	if err != nil {
		t.Fatal(err)
	}
	if res.Plan != "vector(embedding)" || strings.Join(rows, ";") != "Echo|0.9;alpha|0.8" {
		t.Fatalf("%q %v", res.Plan, rows)
	}
	if len(p.lastVector) != 3 || p.lastVector[2] != float32(0.3) || p.vecDepths[0] != 50 {
		t.Fatalf("vector %v depth %v", p.lastVector, p.vecDepths)
	}
	rows, _, err = f.query(`SELECT title FROM tasks ORDER BY SIMILARITY(embedding, ?) DESC`, sql.ParamVector{Value: []float32{1, 2}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(rows, ",") != "Echo,alpha,delta" || len(p.lastVector) != 2 || p.lastVector[1] != 2 {
		t.Fatalf("%v %v", rows, p.lastVector)
	}
	// A document outside the candidate set projects NULL; MATCH non-hits project 0.
	rows, _, _ = f.query(`SELECT title, SIMILARITY(embedding, [1]) AS s FROM tasks WHERE title = 'bravo'`)
	if strings.Join(rows, ",") != "bravo|NULL" {
		t.Fatalf("%v", rows)
	}
	if _, _, err := f.query(`SELECT SIMILARITY(embedding, ?) AS s FROM tasks ORDER BY s DESC`, sql.ParamString{Value: "x"}); err == nil {
		t.Fatal("a non-vector parameter must be rejected")
	}
}

func TestFuseOrdersByRankFusion(t *testing.T) {
	f, p := newIndexedFixture(t, "app/idx-fuse")
	for _, mode := range []string{"rrf", "weighted"} {
		want := fusion.Fuse([]fusion.Arm{
			{Results: p.fulltext["tasks_text"], Weight: 1, Depth: 50},
			{Results: p.vectors["embedding"], Weight: 1, Depth: 50},
		}, map[string]fusion.Mode{"rrf": fusion.ModeRRF, "weighted": fusion.ModeWeightedSum}[mode], 0)
		q := `SELECT kdb_id, FUSE(MATCH(tasks_text, ?), SIMILARITY(embedding, ?), '` + mode + `') AS score FROM tasks ORDER BY score DESC LIMIT 10`
		rows, res, err := f.query(q, sql.ParamString{Value: "deploy"}, sql.ParamVector{Value: []float32{0.5}})
		if err != nil {
			t.Fatal(err)
		}
		if res.Plan != "fuse:"+mode+"(fulltext(tasks_text),vector(embedding))" {
			t.Fatalf("plan: %q", res.Plan)
		}
		if len(rows) != len(want) {
			t.Fatalf("%s: got %d rows, want %d: %v", mode, len(rows), len(want), rows)
		}
		for i, w := range want {
			if !strings.HasPrefix(rows[i], w.DocID.String()+"|") {
				t.Errorf("%s row %d: %s, want doc %s", mode, i, rows[i], w.DocID)
			}
			// Scores widen to double through the float32 shortest round-trip, so 0.9f reads
			// as 0.9 rather than 0.8999999761581421.
			score := rows[i][strings.LastIndex(rows[i], "|")+1:]
			want := strconv.FormatFloat(float64(w.Score), 'g', -1, 32)
			if score != want {
				t.Errorf("%s row %d: score %s, want %s", mode, i, score, want)
			}
		}
	}
	// Default mode is RRF; residual WHERE still filters the fused set.
	rows, _, err := f.query(`SELECT title FROM tasks WHERE status = 'open' ORDER BY FUSE(MATCH(tasks_text, 'x'), SIMILARITY(embedding, [1])) DESC`)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(rows, ",") != "alpha,charlie" {
		t.Fatalf("%v", rows)
	}
}

func TestStatsStillAccountForIndexPaths(t *testing.T) {
	f, _ := newIndexedFixture(t, "app/idx-stats")
	stats := &sql.ExecStats{}
	ctx := f.ctx
	ctx.Stats = stats
	if _, err := f.engine.Execute(`SELECT title FROM tasks WHERE status = 'open'`, ctx); err != nil {
		t.Fatal(err)
	}
	if stats.RowsExamined != 3 || stats.DocsRead != 3 || stats.DocBytesRead == 0 || stats.RetainedBytes == 0 {
		t.Fatalf("%+v", stats)
	}
}
