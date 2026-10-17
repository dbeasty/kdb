package sql_test

import (
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/sql"
)

// #317: COALESCE and CASE, so an UPDATE can default a missing or null field in one statement -
// the `$inc` a queue claim needs - instead of storing null or falling back to read-then-write.

func newIncFixture(t *testing.T) *docFixture {
	// a: a number, missing, null, a string.
	return newDocFixture(t, "app/inc", schema.None(), nil,
		`{"k":1,"a":2}`, `{"k":2}`, `{"k":3,"a":null}`, `{"k":4,"a":"x"}`)
}

func TestCoalesceDefaultsMissingAndNull(t *testing.T) {
	f := newIncFixture(t)
	f.expect(`SELECT k, COALESCE(a, 0) FROM docs ORDER BY k`, "1|2", "2|0", "3|0", "4|x")
	f.expect(`SELECT COALESCE(NULL, nosuch, a, 7, 8) FROM docs ORDER BY k`, "2", "7", "7", "x")
	f.expect(`SELECT COALESCE(NULL)`, "NULL")
	f.expect(`SELECT k FROM docs WHERE COALESCE(a, 0) < 1 ORDER BY k`, "2", "3")
	f.expectWithParams(`SELECT k FROM docs WHERE COALESCE(a, ?) = 9 ORDER BY k`,
		[]sql.Parameter{sql.ParamInt{Value: 9}}, "2", "3")
	f.expectPlanningError(`SELECT COALESCE() FROM docs`, "COALESCE takes at least one argument")
	f.expectPlanningError(`SELECT COALESCE(SUM(a), 0) FROM docs`, "aggregate")
}

// TestCoalesceIncrementCountsFromZero is the claim from the issue: one atomic statement whose
// increment treats a missing or null counter as 0, as MongoDB's $inc does.
func TestCoalesceIncrementCountsFromZero(t *testing.T) {
	f := newIncFixture(t)
	res, err := f.commitDML(`UPDATE docs SET a = COALESCE(a, 0) + ? WHERE k < 4 RETURNING k, a`,
		sql.ParamInt{Value: 1})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(returnedRows(res), ","); got != "1|3,2|1,3|1" {
		t.Fatalf("returned %q", got)
	}
	f.expect(`SELECT k, a FROM docs ORDER BY k`, "1|3", "2|1", "3|1", "4|x")
}

func TestCaseExpressions(t *testing.T) {
	f := newIncFixture(t)
	// Searched form; the first matching arm wins; no ELSE is NULL.
	f.expect(`SELECT k, CASE WHEN a IS MISSING THEN 'missing' WHEN a IS NULL THEN 'null' WHEN a > 1 THEN 'big' END FROM docs ORDER BY k`,
		"1|big", "2|missing", "3|null", "4|NULL")
	// Simple form compares with =.
	f.expect(`SELECT CASE k WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'many' END FROM docs ORDER BY k`,
		"one", "two", "many", "many")
	// Nested, with parameters on both sides of the operand (parameter numbering survives the
	// parser's look-ahead for the simple form).
	f.expectWithParams(`SELECT CASE ? WHEN k THEN CASE WHEN ? THEN 'yes' ELSE 'no' END END FROM docs ORDER BY k`,
		[]sql.Parameter{sql.ParamInt{Value: 2}, sql.ParamBool{Value: true}}, "NULL", "yes", "NULL", "NULL")
	// In WHERE and ORDER BY.
	f.expect(`SELECT k FROM docs WHERE CASE WHEN a IS NULL THEN true ELSE false END ORDER BY k`, "2", "3")
	f.expect(`SELECT k FROM docs ORDER BY CASE WHEN k = 3 THEN 0 ELSE k END, k`, "3", "1", "2", "4")
	// The issue's statement: an increment that treats missing or null as 0.
	if _, err := f.commitDML(`UPDATE docs SET a = CASE WHEN a IS MISSING OR a IS NULL THEN ? ELSE a + ? END WHERE k < 4`,
		sql.ParamInt{Value: 1}, sql.ParamInt{Value: 1}); err != nil {
		t.Fatal(err)
	}
	f.expect(`SELECT k, a FROM docs ORDER BY k`, "1|3", "2|1", "3|1", "4|x")
}

func TestCaseIsStillAColumnName(t *testing.T) {
	f := newDocFixture(t, "app/case-col", schema.None(), nil, `{"case":"c1","n":1}`)
	f.expect(`SELECT case FROM docs`, "c1")
	f.expect(`SELECT n FROM docs WHERE case = 'c1'`, "1")
	// As the operand of a simple CASE it must be qualified: `CASE case WHEN` reads as a searched
	// CASE, the way it would in any SQL where CASE is reserved.
	f.expect(`SELECT CASE docs.case WHEN 'c1' THEN 'hit' END FROM docs`, "hit")
}

func TestCaseParseErrors(t *testing.T) {
	for stmt, want := range map[string]string{
		`SELECT CASE WHEN true 1 END`:             "expected THEN",
		`SELECT CASE WHEN true THEN 1`:            "expected END",
		`SELECT CASE WHEN true THEN 1 ELSE 2`:     "expected END",
		`SELECT CASE WHEN THEN 1 END`:             "expected THEN",
		`SELECT CASE WHEN true THEN END`:          "expected END",
		`SELECT k FROM docs WHERE CASE k END = 1`: "trailing input",
	} {
		_, err := sql.DefaultParser{}.Parse(stmt)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: got %v, want an error containing %q", stmt, err, want)
		}
	}
}

func TestCaseAndCoalesceResultTypes(t *testing.T) {
	f := newIncFixture(t)
	_, res, err := f.query(`SELECT CASE WHEN k = 1 THEN 1 ELSE 2 END, CASE WHEN k = 1 THEN 'a' ELSE NULL END, CASE WHEN k = 1 THEN 1 ELSE 'b' END, COALESCE(a, 0), COALESCE(1, 2) FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	var types []string
	for _, c := range res.Columns {
		types = append(types, c.SQLType)
	}
	if got := strings.Join(types, ","); got != "BIGINT,VARCHAR,JSON,JSON,BIGINT" {
		t.Fatalf("column types %s", got)
	}
}
