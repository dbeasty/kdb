package sql_test

import (
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/storage/mem"
)

// newTablelessEngine builds an engine over an empty namespace with no commits at all.
//
// The emptiness is the point rather than convenience: `SELECT 1` is a liveness probe, and it
// has to answer on a namespace that has never been written to. The executor short-circuits
// before resolving a commit for exactly this reason - a probe that failed on a fresh namespace
// would be useless precisely when a caller most wants a plain answer.
func newTablelessEngine(t *testing.T) (sql.Engine, sql.QueryContext) {
	t.Helper()
	const ns = "app/data"
	d, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatal(err)
	}
	return sql.NewEngine(mem.NewInMemoryStorageAdapter(), d), sql.QueryContext{
		NamespaceID: ns,
		Schema:      schema.None(),
	}
}

func TestTablelessSelectReturnsOneRow(t *testing.T) {
	engine, ctx := newTablelessEngine(t)

	result, err := engine.Execute("SELECT 1", ctx)
	if err != nil {
		t.Fatalf("SELECT 1 failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("got %d rows, want exactly 1", len(result.Rows))
	}
	if len(result.Rows[0].Values) != 1 {
		t.Fatalf("got %d values, want 1", len(result.Rows[0].Values))
	}
	cell, ok := result.Rows[0].Values[0].(sql.CellLong)
	if !ok {
		t.Fatalf("got %T, want CellLong", result.Rows[0].Values[0])
	}
	if cell.Value != 1 {
		t.Fatalf("value = %d, want 1", cell.Value)
	}
	if len(result.Columns) != 1 {
		t.Fatalf("got %d columns, want 1", len(result.Columns))
	}
}

func TestTablelessSelectVariants(t *testing.T) {
	engine, ctx := newTablelessEngine(t)

	cases := []struct {
		sql        string
		wantRows   int
		wantColumn string
		check      func(t *testing.T, cell sql.Cell)
	}{
		{
			sql: "SELECT 1", wantRows: 1, wantColumn: "expr",
			check: func(t *testing.T, c sql.Cell) {
				if v, ok := c.(sql.CellLong); !ok || v.Value != 1 {
					t.Fatalf("got %#v, want CellLong{1}", c)
				}
			},
		},
		{
			sql: "SELECT 1 AS one", wantRows: 1, wantColumn: "one",
			check: func(t *testing.T, c sql.Cell) {
				if v, ok := c.(sql.CellLong); !ok || v.Value != 1 {
					t.Fatalf("got %#v, want CellLong{1}", c)
				}
			},
		},
		{
			sql: "SELECT 'ok'", wantRows: 1, wantColumn: "expr",
			check: func(t *testing.T, c sql.Cell) {
				if v, ok := c.(sql.CellString); !ok || v.Value != "ok" {
					t.Fatalf("got %#v, want CellString{ok}", c)
				}
			},
		},
		{
			sql: "SELECT NULL", wantRows: 1, wantColumn: "expr",
			check: func(t *testing.T, c sql.Cell) {
				if _, ok := c.(sql.CellNull); !ok {
					t.Fatalf("got %#v, want CellNull", c)
				}
			},
		},
		// LIMIT and OFFSET are meaningful even over one row: LIMIT 0 genuinely means no rows.
		{sql: "SELECT 1 LIMIT 0", wantRows: 0},
		{sql: "SELECT 1 LIMIT 1", wantRows: 1, wantColumn: "expr"},
		{sql: "SELECT 1 OFFSET 1", wantRows: 0},
	}

	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			result, err := engine.Execute(tc.sql, ctx)
			if err != nil {
				t.Fatalf("%q failed: %v", tc.sql, err)
			}
			if len(result.Rows) != tc.wantRows {
				t.Fatalf("got %d rows, want %d", len(result.Rows), tc.wantRows)
			}
			if tc.wantRows == 0 {
				return
			}
			if tc.wantColumn != "" && result.Columns[0].Name != tc.wantColumn {
				t.Fatalf("column name = %q, want %q", result.Columns[0].Name, tc.wantColumn)
			}
			if tc.check != nil {
				tc.check(t, result.Rows[0].Values[0])
			}
		})
	}
}

func TestTablelessSelectSeveralProjections(t *testing.T) {
	engine, ctx := newTablelessEngine(t)

	result, err := engine.Execute("SELECT 1 AS a, 'two' AS b, NULL AS c", ctx)
	if err != nil {
		t.Fatalf("failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(result.Rows))
	}
	if len(result.Rows[0].Values) != 3 {
		t.Fatalf("got %d values, want 3", len(result.Rows[0].Values))
	}
	names := make([]string, len(result.Columns))
	for i, c := range result.Columns {
		names[i] = c.Name
	}
	if strings.Join(names, ",") != "a,b,c" {
		t.Fatalf("columns = %v, want [a b c]", names)
	}
}

// TestTablelessSelectWithParameter covers the shape a driver actually sends when probing with a
// bound value, and proves parameters reach the synthetic row.
func TestTablelessSelectWithParameter(t *testing.T) {
	engine, ctx := newTablelessEngine(t)
	ctx.Parameters = []sql.Parameter{sql.ParamString{Value: "pong"}}

	result, err := engine.Execute("SELECT ?", ctx)
	if err != nil {
		t.Fatalf("SELECT ? failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(result.Rows))
	}
	cell, ok := result.Rows[0].Values[0].(sql.CellString)
	if !ok || cell.Value != "pong" {
		t.Fatalf("got %#v, want CellString{pong}", result.Rows[0].Values[0])
	}
}

// TestTablelessSelectWhereFiltersTheSyntheticRow checks that WHERE still means something: over
// one row it decides whether that row survives.
func TestTablelessSelectWhereFiltersTheSyntheticRow(t *testing.T) {
	engine, ctx := newTablelessEngine(t)

	kept, err := engine.Execute("SELECT 1 WHERE 1 = 1", ctx)
	if err != nil {
		t.Fatalf("true predicate failed: %v", err)
	}
	if len(kept.Rows) != 1 {
		t.Fatalf("true predicate returned %d rows, want 1", len(kept.Rows))
	}

	dropped, err := engine.Execute("SELECT 1 WHERE 1 = 2", ctx)
	if err != nil {
		t.Fatalf("false predicate failed: %v", err)
	}
	if len(dropped.Rows) != 0 {
		t.Fatalf("false predicate returned %d rows, want 0", len(dropped.Rows))
	}
	// Columns are still reported for an empty result, so a client can describe the shape.
	if len(dropped.Columns) != 1 {
		t.Fatalf("got %d columns on an empty result, want 1", len(dropped.Columns))
	}
}

// TestTablelessSelectRejectsWhatItCannotMean pins the constructs refused rather than silently
// answered with NULL. `SELECT name` with no table is a forgotten FROM clause, and saying so
// beats returning a null.
func TestTablelessSelectRejectsWhatItCannotMean(t *testing.T) {
	engine, ctx := newTablelessEngine(t)

	cases := []struct {
		sql     string
		wantMsg string
	}{
		{"SELECT *", "SELECT * requires a FROM clause"},
		{"SELECT name", "requires a FROM clause"},
		{"SELECT COUNT(*)", "requires a FROM clause"},
		{"SELECT 1 WHERE name = 'x'", "requires a FROM clause"},
		{"SELECT 1 ORDER BY name", "requires a FROM clause"},
		// Not a FROM-clause error: there is no arithmetic operator, so `+ name` is trailing
		// input the parser cannot account for. It used to be silently dropped, making this
		// statement quietly mean `SELECT 1`.
		{"SELECT 1 + name", "unexpected trailing input"},
	}

	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			_, err := engine.Execute(tc.sql, ctx)
			if err == nil {
				t.Fatalf("%q unexpectedly succeeded", tc.sql)
			}
			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Fatalf("%q error = %q, want it to contain %q", tc.sql, err, tc.wantMsg)
			}
		})
	}
}
