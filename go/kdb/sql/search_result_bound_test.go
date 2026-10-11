package sql_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/index"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/sql"
)

// A MATCH ranking longer than MaxRows used to be cut at MaxRows before the residual WHERE ran,
// so `MATCH(...) AND status = 'open'` lost every open document that ranked below the cut. With no
// LIMIT the search already returns every hit (depth 0); only the executor then dropped most of
// them. With a LIMIT the candidate depth is the spec's min(1000, max(50, 4*(n+m))) and is
// unchanged here.

func searchBoundFixture(t *testing.T, ns string, docs, maxRows int) (*docFixture, *fakeProvider) {
	t.Helper()
	bodies := make([]string, docs)
	hits := make([]index.RankedResult, docs)
	for i := 0; i < docs; i++ {
		status := "closed"
		if i >= docs-3 { // the three lowest-ranked documents are the only open ones
			status = "open"
		}
		bodies[i] = fmt.Sprintf(`{"title":"t%02d","status":%q}`, i+1, status)
		hits[i] = index.RankedResult{DocID: corpusID(i + 1), Score: float32(docs - i)}
	}
	p := &fakeProvider{fulltext: map[string][]index.RankedResult{"t_text": hits}}
	f := newDocFixture(t, ns, schema.None(), p, bodies...)
	f.ctx.MaxRows = maxRows
	return f, p
}

func TestMatchWithFilterSeesHitsRankedPastMaxRows(t *testing.T) {
	f, _ := searchBoundFixture(t, "app/search-bound1", 12, 5)
	rows, _, err := f.query(`SELECT title FROM tasks WHERE MATCH(t_text, 'x') AND status = 'open' ORDER BY title`)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 || rows[0] != "t10" || rows[2] != "t12" {
		t.Fatalf("the three open documents rank 10th-12th and must be found, got %v", rows)
	}
}

func TestMatchResultLargerThanMaxRowsIsAnErrorNotACut(t *testing.T) {
	f, _ := searchBoundFixture(t, "app/search-bound2", 12, 5)
	_, _, err := f.query(`SELECT title FROM tasks WHERE MATCH(t_text, 'x')`)
	var limit *sql.ResultRowLimitError
	if !errors.As(err, &limit) {
		t.Fatalf("12 hits against a bound of 5 must be a *ResultRowLimitError, got %T: %v", err, err)
	}
}

func TestMatchWithLimitKeepsItsBoundedDepth(t *testing.T) {
	f, p := searchBoundFixture(t, "app/search-bound3", 12, 5)
	rows, _, err := f.query(`SELECT title, MATCH(t_text, 'x') AS s FROM tasks WHERE MATCH(t_text, 'x') ORDER BY s DESC LIMIT 3`)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("LIMIT 3 returned %d rows", len(rows))
	}
	if len(p.ftDepths) == 0 || p.ftDepths[0] != 50 {
		t.Fatalf("a LIMITed ranked query asks the index for the spec's minimum depth of 50, got %v", p.ftDepths)
	}
}
