package sql_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/sql"
)

// MaxRows bounds what a query may return. It used to bound the ids a full scan collected
// *before* the WHERE ran, so on a namespace larger than MaxRows a selective query examined only
// the first MaxRows documents and answered from that prefix with no error: COUNT(*) topped out
// at MaxRows, and a lookup of a later document found nothing. These tests use a small MaxRows
// over a larger namespace; the default (10,000) behaves the same way.
func boundedFixture(t *testing.T, ns string, docs, maxRows int) *queryFixture {
	t.Helper()
	names := make([]string, docs)
	for i := range names {
		names[i] = fmt.Sprintf("n%03d", i)
	}
	f := newQueryFixture(t, ns, names...)
	f.ctx.MaxRows = maxRows
	return f
}

func TestSelectiveWhereSeesDocumentsPastMaxRows(t *testing.T) {
	f := boundedFixture(t, "app/bound1", 50, 10)
	got := f.names(t, "SELECT name FROM t WHERE name = 'n049'")
	if len(got) != 1 || got[0] != "n049" {
		t.Fatalf("a document past MaxRows must be found by a selective query, got %v", got)
	}
	// A range that matches only late documents.
	if got := f.names(t, "SELECT name FROM t WHERE rank >= 45 ORDER BY name"); len(got) != 5 {
		t.Fatalf("want the 5 documents ranked 45..49, got %v", got)
	}
}

func TestAggregateCountsTheWholeNamespaceNotAPrefix(t *testing.T) {
	f := boundedFixture(t, "app/bound2", 50, 10)
	if n := f.count(t, "SELECT COUNT(*) FROM t"); n != 50 {
		t.Fatalf("COUNT(*) = %d, want 50", n)
	}
	if n := f.count(t, "SELECT COUNT(*) FROM t WHERE rank >= 10"); n != 40 {
		t.Fatalf("filtered COUNT(*) = %d, want 40", n)
	}
}

func TestResultLargerThanMaxRowsIsAnErrorNotATruncation(t *testing.T) {
	f := boundedFixture(t, "app/bound3", 50, 10)
	for _, q := range []string{
		"SELECT name FROM t",
		"SELECT name FROM t WHERE rank >= 5",
		"SELECT name FROM t ORDER BY name",
	} {
		_, err := f.engine.Execute(q, f.ctx)
		var limit *sql.ResultRowLimitError
		if !errors.As(err, &limit) {
			t.Fatalf("%q: want *ResultRowLimitError, got %T: %v", q, err, err)
		}
		if limit.Limit != 10 {
			t.Errorf("%q: error should report the limit 10, got %d", q, limit.Limit)
		}
	}
}

func TestLimitWithinMaxRowsStillWorksOverALargerNamespace(t *testing.T) {
	f := boundedFixture(t, "app/bound4", 50, 10)
	if got := f.names(t, "SELECT name FROM t LIMIT 5"); len(got) != 5 {
		t.Fatalf("LIMIT 5 returned %d rows", len(got))
	}
	if got := f.names(t, "SELECT name FROM t LIMIT 5 OFFSET 40"); len(got) != 5 {
		t.Fatalf("LIMIT 5 OFFSET 40 returned %d rows", len(got))
	}
	if got := f.names(t, "SELECT name FROM t ORDER BY name DESC LIMIT 3"); len(got) != 3 || got[0] != "n049" {
		t.Fatalf("ORDER BY .. LIMIT 3 = %v", got)
	}
	// A LIMIT larger than the bound is still an overflow, not a silent cap.
	if _, err := f.engine.Execute("SELECT name FROM t LIMIT 30", f.ctx); err == nil {
		t.Fatal("LIMIT 30 over MaxRows 10 returned rows instead of an error")
	}
}

func TestResultExactlyAtMaxRowsIsReturnedWhole(t *testing.T) {
	f := boundedFixture(t, "app/bound5", 10, 10)
	if got := f.names(t, "SELECT name FROM t"); len(got) != 10 {
		t.Fatalf("10 rows at MaxRows 10 returned %d", len(got))
	}
}
