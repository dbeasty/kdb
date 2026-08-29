package sql_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/sql"
)

// kdb-spec-layer13 §2.8: before this, only MaxRows bounded a scan, and it bounded the rows
// *returned*. A selective predicate over a large namespace returns almost nothing while still
// reading every document into memory to decide that - unbounded work that no admission decision
// could see coming, because the request that triggers it looks tiny.
func TestScanRowBudgetAbortsAnOverlyExpensiveScan(t *testing.T) {
	names := make([]string, 0, 50)
	for i := 0; i < 50; i++ {
		names = append(names, fmt.Sprintf("n%02d", i))
	}
	f := newQueryFixture(t, "app/budget", names...)

	f.ctx.RowBudget = 10
	_, err := f.engine.Execute("SELECT name FROM t", f.ctx)
	if err == nil {
		t.Fatal("expected a scan examining 50 rows against a budget of 10 to be aborted")
	}
	var budgetErr *sql.ScanRowBudgetExceededError
	if !errors.As(err, &budgetErr) {
		t.Fatalf("expected *ScanRowBudgetExceededError, got %T: %v", err, err)
	}
	if budgetErr.Budget != 10 {
		t.Errorf("error should report the budget it exceeded, got %d", budgetErr.Budget)
	}
}

// A budget the scan stays within must not change the answer - the bound is a safety limit, not a
// silent truncation of results.
func TestScanWithinRowBudgetIsUnaffected(t *testing.T) {
	f := newQueryFixture(t, "app/budget2", "alice", "bob", "carol")
	f.ctx.RowBudget = 1000
	got := f.names(t, "SELECT name FROM t ORDER BY name")
	want := []string{"alice", "bob", "carol"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

// Zero means unlimited, so an unconfigured deployment behaves exactly as before.
func TestZeroRowBudgetIsUnlimited(t *testing.T) {
	names := make([]string, 0, 30)
	for i := 0; i < 30; i++ {
		names = append(names, fmt.Sprintf("n%02d", i))
	}
	f := newQueryFixture(t, "app/budget3", names...)
	f.ctx.RowBudget = 0
	if got := f.names(t, "SELECT name FROM t"); len(got) != 30 {
		t.Errorf("an unlimited budget must not bound the scan, got %d rows", len(got))
	}
}
