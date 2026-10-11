package sql_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/sql"
)

// `WHERE kdb_id = ?` names a document by key. It used to be a full scan like any other
// predicate; it is served by key now, so an application that updates or deletes by id is
// O(log n), not O(namespace). The tests prove "not a scan" with a row budget of 1, which any scan
// of the 5-document corpus exceeds.

const keyOf3 = "00000000-0000-4000-8000-000000000003"

func keyFixture(t *testing.T, ns string) *docFixture {
	f := newCorpusFixture(t, ns)
	f.ctx.RowBudget = 1
	return f
}

func TestKdbIDEqualityIsServedByKey(t *testing.T) {
	f := keyFixture(t, "app/key1")
	rows, res, err := f.query(`SELECT title FROM tasks WHERE kdb_id = '` + keyOf3 + `'`)
	if err != nil {
		t.Fatal(err)
	}
	if res.Plan != "keylookup" || len(rows) != 1 || rows[0] != "charlie" {
		t.Fatalf("plan=%q rows=%v, want a keylookup returning charlie", res.Plan, rows)
	}
	// Reversed operands and a bound parameter take the same path.
	if _, res, _ := f.query(`SELECT title FROM tasks WHERE '` + keyOf3 + `' = kdb_id`); res.Plan != "keylookup" {
		t.Errorf("reversed operands: plan=%q", res.Plan)
	}
	rows, res, err = f.query(`SELECT title FROM tasks WHERE kdb_id = ?`, sql.ParamString{Value: keyOf3})
	if err != nil || res.Plan != "keylookup" || len(rows) != 1 {
		t.Fatalf("parameter: plan=%q rows=%v err=%v", res.Plan, rows, err)
	}
}

func TestKdbIDInListIsServedByKey(t *testing.T) {
	f := newCorpusFixture(t, "app/key2")
	f.ctx.RowBudget = 3
	rows, res, err := f.query(`SELECT title FROM tasks WHERE kdb_id IN ('` + keyOf3 + `', '00000000-0000-4000-8000-000000000001', '00000000-0000-4000-8000-000000000099') ORDER BY title`)
	if err != nil {
		t.Fatal(err)
	}
	if res.Plan != "keylookup" || strings.Join(rows, ",") != "alpha,charlie" {
		t.Fatalf("plan=%q rows=%v, want alpha,charlie (the unknown id contributes nothing)", res.Plan, rows)
	}
}

func TestKeyLookupStillAppliesTheRestOfTheWhere(t *testing.T) {
	f := keyFixture(t, "app/key3")
	// charlie is 'open', not 'done': the key narrows, the residual decides.
	rows, _, err := f.query(`SELECT title FROM tasks WHERE kdb_id = '` + keyOf3 + `' AND status = 'done'`)
	if err != nil || len(rows) != 0 {
		t.Fatalf("rows=%v err=%v, want none", rows, err)
	}
	rows, _, err = f.query(`SELECT title FROM tasks WHERE kdb_id = '` + keyOf3 + `' AND status = 'open'`)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%v err=%v, want charlie", rows, err)
	}
}

func TestKeyLookupOfNothingFindsNothing(t *testing.T) {
	f := keyFixture(t, "app/key4")
	for _, where := range []string{
		`kdb_id = '00000000-0000-4000-8000-000000000099'`, // well-formed, absent
		`kdb_id = 'not-a-uuid'`,                           // names nothing
		`kdb_id IN ('x', 'y')`,
	} {
		rows, res, err := f.query(`SELECT title FROM tasks WHERE ` + where)
		if err != nil || len(rows) != 0 {
			t.Errorf("%s: rows=%v err=%v", where, rows, err)
		}
		if res.Plan != "keylookup" {
			t.Errorf("%s: plan=%q", where, res.Plan)
		}
	}
}

// Predicates that are not a plain key match must not take the key path, or they would answer
// from a candidate set that does not contain their matches.
func TestNonKeyPredicatesOnKdbIDStillScan(t *testing.T) {
	f := newCorpusFixture(t, "app/key5")
	for _, where := range []string{
		`kdb_id <> '` + keyOf3 + `'`,
		`NOT (kdb_id = '` + keyOf3 + `')`,
		`kdb_id = '` + keyOf3 + `' OR status = 'done'`,
		`kdb_id LIKE '%3'`,
	} {
		rows, res, err := f.query(`SELECT title FROM tasks WHERE ` + where + ` ORDER BY title`)
		if err != nil {
			t.Fatalf("%s: %v", where, err)
		}
		if res.Plan == "keylookup" {
			t.Errorf("%s was served by key", where)
		}
		_ = rows
	}
	rows, _, _ := f.query(`SELECT title FROM tasks WHERE kdb_id = '` + keyOf3 + `' OR status = 'done' ORDER BY title`)
	if strings.Join(rows, ",") != "Echo,bravo,charlie" && strings.Join(rows, ",") != "bravo,charlie,Echo" {
		t.Errorf("OR across a key and another column must see both sides, got %v", rows)
	}
}

func TestAScanOfTheSameCorpusExceedsTheBudgetUsedAbove(t *testing.T) {
	f := keyFixture(t, "app/key6")
	_, _, err := f.query(`SELECT title FROM tasks WHERE status = 'open'`)
	var budget *sql.ScanRowBudgetExceededError
	if !errors.As(err, &budget) {
		t.Fatalf("a budget of 1 must abort a scan, otherwise the tests above prove nothing: %v", err)
	}
}
