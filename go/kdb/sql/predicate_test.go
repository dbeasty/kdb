package sql_test

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/sql"
)

// Component 71 - predicates over nested and array paths (kdb-spec-layer16 §2, §4).

func TestPredicateDottedPathsWithImplicitArrayTraversal(t *testing.T) {
	f := newCorpusFixture(t, "app/pred-paths")
	f.expect(`SELECT title FROM tasks WHERE collaborators.userId = 'u1'`, "alpha")
	f.expect(`SELECT title FROM tasks WHERE collaborators.userId = 'u2'`, "alpha")
	f.expect(`SELECT title FROM tasks WHERE collaborators.userId = 'u3'`, "bravo")
	f.expect(`SELECT title FROM tasks WHERE collaborators.userId <> 'u1' ORDER BY title`, "alpha", "bravo")
	f.expect(`SELECT title FROM tasks WHERE steps.text = 'plan'`, "alpha")
	f.expect(`SELECT title FROM tasks WHERE steps.text LIKE '%staging'`, "alpha")
	// Terminal array: equality is membership.
	f.expect(`SELECT title FROM tasks WHERE projectIds = 'p1' ORDER BY title`, "Echo", "alpha")
	f.expect(`SELECT title FROM tasks WHERE projectIds = 'p2' ORDER BY title`, "alpha", "bravo")
	f.expect(`SELECT title FROM tasks WHERE tags = 'z' ORDER BY title`, "Echo", "charlie")
	f.expect(`SELECT title FROM tasks WHERE tags = 'x' AND tags = 'z'`, "charlie")
	// Ordering against candidates: any tag > 'y'.
	f.expect(`SELECT title FROM tasks WHERE tags > 'y' ORDER BY title`, "Echo", "charlie")
	// Empty array and absent path yield no candidates.
	f.expect(`SELECT title FROM tasks WHERE projectIds = 'none'`)
	f.expect(`SELECT title FROM tasks WHERE nested.deep.path = 1`)
}

func TestPredicateArrayFunctions(t *testing.T) {
	f := newCorpusFixture(t, "app/pred-array")
	f.expect(`SELECT title FROM tasks WHERE ARRAY_CONTAINS(tags, 'x') ORDER BY title`, "alpha", "charlie")
	// Superset test: every listed value must be present.
	f.expect(`SELECT title FROM tasks WHERE ARRAY_CONTAINS(tags, 'x', 'y') ORDER BY title`, "alpha", "charlie")
	f.expect(`SELECT title FROM tasks WHERE ARRAY_CONTAINS(tags, 'x', 'z')`, "charlie")
	f.expect(`SELECT title FROM tasks WHERE ARRAY_CONTAINS(tags, 'x', 'q')`)
	f.expect(`SELECT title FROM tasks WHERE ARRAY_CONTAINS_ANY(tags, 'z', 'q') ORDER BY title`, "Echo", "charlie")
	f.expect(`SELECT title FROM tasks WHERE ARRAY_CONTAINS_ANY(tags, 'q')`)
	f.expect(`SELECT title FROM tasks WHERE NOT ARRAY_CONTAINS(tags, 'y') ORDER BY title`, "Echo", "delta")
	f.expectWithParams(`SELECT title FROM tasks WHERE ARRAY_CONTAINS(projectIds, ?)`, []sql.Parameter{sql.ParamString{Value: "p2"}}, "alpha", "bravo")
	// Deep JSON equality with numeric normalisation.
	g := newDocFixture(t, "app/pred-array-deep", f.sch, nil,
		`{"n":"a","items":[{"k":1},{"k":2}],"nums":[1,2.5]}`,
		`{"n":"b","items":[{"k":2.0}],"nums":[3]}`,
		`{"n":"c","items":"notarray"}`)
	g.expect(`SELECT n FROM t WHERE ARRAY_CONTAINS(nums, 1.0)`, "a")
	g.expect(`SELECT n FROM t WHERE ARRAY_CONTAINS(nums, 2.5, 1)`, "a")
	g.expect(`SELECT n FROM t WHERE ARRAY_CONTAINS(items, '{"k":2}')`)
	// A non-array at the path is false, and ARRAY_LENGTH of it is NULL.
	g.expect(`SELECT n FROM t WHERE ARRAY_CONTAINS(items, 'notarray')`)
	g.expect(`SELECT n, ARRAY_LENGTH(items), ARRAY_LENGTH(nums), ARRAY_LENGTH(missing) FROM t ORDER BY n`, "a|2|2|NULL", "b|1|1|NULL", "c|NULL|NULL|NULL")
	// ARRAY_LENGTH as an operand.
	f.expect(`SELECT title FROM tasks WHERE ARRAY_LENGTH(tags) = 3`, "charlie")
	f.expect(`SELECT title FROM tasks WHERE ARRAY_LENGTH(tags) >= 2 ORDER BY title`, "alpha", "charlie")
	f.expect(`SELECT title FROM tasks WHERE ARRAY_LENGTH(projectIds) IS NULL ORDER BY title`, "delta")
	f.expect(`SELECT title FROM tasks WHERE ARRAY_LENGTH(projectIds) = 0`, "charlie")
	f.expect(`SELECT title, ARRAY_LENGTH(tags) AS n FROM tasks ORDER BY n DESC, title LIMIT 2`, "charlie|3", "alpha|2")
	f.expectPlanningError(`SELECT title FROM tasks WHERE ARRAY_CONTAINS(tags)`, "at least one value")
	f.expectPlanningError(`SELECT title FROM tasks WHERE ARRAY_LENGTH(tags, 1) = 1`, "exactly one argument")
}

func TestPredicateTableAliasStripping(t *testing.T) {
	f := newCorpusFixture(t, "app/pred-alias")
	f.expect(`SELECT t.title FROM tasks t WHERE t.collaborators.userId = 'u3'`, "bravo")
	f.expect(`SELECT title FROM tasks WHERE tasks.tags = 'z' AND tasks.status = 'done'`, "Echo")
	// A prefix that is neither the table nor its alias is an ordinary (absent) nested path.
	f.expect(`SELECT title FROM tasks t WHERE other.title = 'alpha'`)
}

func TestPublicEvalHelpersStillWork(t *testing.T) {
	f := newCorpusFixture(t, "app/pred-public")
	_ = f
	if sql.CompareCells(sql.CellNull{}, sql.CellLong{Value: 1}) >= 0 {
		t.Fatal("NULL must sort first")
	}
	if sql.CompareCells(sql.CellNull{}, sql.CellNull{}) != 0 {
		t.Fatal("NULL vs NULL must be 0")
	}
	if sql.CompareCells(sql.CellString{Value: "a"}, sql.CellLong{Value: 1}) <= 0 {
		t.Fatal("mismatched types must order deterministically (numbers first) rather than panic")
	}
	if sql.CompareCells(sql.CellLong{Value: 2}, sql.CellDouble{Value: 2.5}) >= 0 {
		t.Fatal("long vs double compares numerically")
	}
	if sql.CompareCells(sql.CellBool{Value: true}, sql.CellLong{Value: 1}) != 0 {
		t.Fatal("bool true equals 1")
	}
	if sql.CompareCells(sql.CellJSON{JSON: "[1]"}, sql.CellString{Value: "z"}) <= 0 {
		t.Fatal("JSON orders after strings")
	}
	if sql.CompareCells(nil, sql.CellJSON{JSON: "{}"}) >= 0 {
		t.Fatal("nil is NULL")
	}
}
