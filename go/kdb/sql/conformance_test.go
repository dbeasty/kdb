package sql_test

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/storage/mem"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// Conformance suite - kdb-spec-layer16 §3.5: one test per clause the parser accepts, against
// an in-memory runtime and a fixed schemaless corpus. Every test asserts the clause either
// takes effect or errors; nothing is allowed to be silently ignored.

// corpus is the shared document set. Ids are fixed so search fakes can name them.
var corpus = []string{
	`{"title":"alpha","status":"open","priority":3,"tags":["x","y"],"projectIds":["p1","p2"],"collaborators":[{"userId":"u1"},{"userId":"u2"}],"steps":[{"text":"plan"},{"text":"deploy staging"}],"flag":true,"score":1.5}`,
	`{"title":"bravo","status":"done","priority":1,"tags":["y"],"projectIds":["p2"],"collaborators":[{"userId":"u3"}],"steps":[],"flag":false,"score":2}`,
	`{"title":"charlie","status":"open","priority":2,"tags":["x","y","z"],"projectIds":[],"flag":true}`,
	`{"title":"delta","status":"blocked","priority":null,"tags":[],"score":0.5}`,
	`{"title":"Echo","status":"done","priority":5,"tags":["z"],"projectIds":["p1"],"score":"high"}`,
}

func corpusID(n int) codec.UUID {
	id, err := codec.ParseUUID(fmt.Sprintf("00000000-0000-4000-8000-%012d", n))
	if err != nil {
		panic(err)
	}
	return id
}

type docFixture struct {
	t      *testing.T
	engine sql.Engine
	ctx    sql.QueryContext
	ns     string
	dag    *dag.InMemoryCommitDag
	store  *mem.InMemoryStorageAdapter
	tx     transaction.Engine
	sch    schema.KdbSchema
}

// newDocFixture writes docs (with ids corpusID(1..n)) under sch and returns an engine at head.
func newDocFixture(t *testing.T, ns string, sch schema.KdbSchema, provider sql.IndexProvider, docs ...string) *docFixture {
	t.Helper()
	d, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatal(err)
	}
	store := mem.NewInMemoryStorageAdapter()
	txEngine := transaction.NewEngine(transaction.ConflictPolicyStrict, nil)
	head, err := d.Head()
	if err != nil {
		t.Fatal(err)
	}
	for i, js := range docs {
		doc, err := document.FromJSONWithID(corpusID(i+1), js)
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
			t.Fatalf("fixture write %d: %T", i, res)
		}
		head = success.Commit.Hash
	}
	var eng sql.Engine
	if provider != nil {
		eng = sql.NewEngineWithIndexes(store, d, provider)
	} else {
		eng = sql.NewEngine(store, d)
	}
	return &docFixture{
		t: t, engine: eng, ns: ns, dag: d, store: store, tx: txEngine, sch: sch,
		ctx: sql.QueryContext{NamespaceID: ns, Schema: sch},
	}
}

func newCorpusFixture(t *testing.T, ns string) *docFixture {
	return newDocFixture(t, ns, schema.None(), nil, corpus...)
}

func cellString(c sql.Cell) string {
	switch v := c.(type) {
	case sql.CellNull:
		return "NULL"
	case sql.CellString:
		return v.Value
	case sql.CellLong:
		return strconv.FormatInt(v.Value, 10)
	case sql.CellDouble:
		return strconv.FormatFloat(v.Value, 'g', -1, 64)
	case sql.CellBool:
		return strconv.FormatBool(v.Value)
	case sql.CellJSON:
		return v.JSON
	default:
		return fmt.Sprintf("%T", c)
	}
}

// query runs a SELECT and returns each row as "|"-joined cell strings.
func (f *docFixture) query(query string, params ...sql.Parameter) ([]string, sql.QueryResult, error) {
	ctx := f.ctx
	ctx.Parameters = params
	res, err := f.engine.Execute(query, ctx)
	if err != nil {
		return nil, res, err
	}
	out := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		parts := make([]string, len(r.Values))
		for i, c := range r.Values {
			parts[i] = cellString(c)
		}
		out = append(out, strings.Join(parts, "|"))
	}
	return out, res, nil
}

func (f *docFixture) rows(query string, params ...sql.Parameter) []string {
	f.t.Helper()
	rows, _, err := f.query(query, params...)
	if err != nil {
		f.t.Fatalf("%s: %v", query, err)
	}
	return rows
}

func (f *docFixture) expect(query string, want ...string) {
	f.t.Helper()
	got := f.rows(query)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		f.t.Errorf("%s\n got %q\nwant %q", query, got, want)
	}
}

func (f *docFixture) expectPlanningError(query, contains string) {
	f.t.Helper()
	_, _, err := f.query(query)
	if err == nil {
		f.t.Fatalf("%s: expected an error", query)
	}
	var pe *sql.PlanningError
	if !errors.As(err, &pe) {
		f.t.Fatalf("%s: expected *sql.PlanningError, got %T: %v", query, err, err)
	}
	if contains != "" && !strings.Contains(err.Error(), contains) {
		f.t.Fatalf("%s: error %q does not mention %q", query, err.Error(), contains)
	}
}

// commitDML runs a DML statement and commits its ops, returning the DML result.
func (f *docFixture) commitDML(sqlText string, params ...sql.Parameter) (sql.DMLResult, error) {
	ctx := f.ctx
	ctx.Parameters = params
	dml, err := f.engine.ExecuteDML(sqlText, ctx)
	if err != nil {
		return sql.DMLResult{}, err
	}
	if len(dml.Operations) == 0 {
		return dml, nil
	}
	parent, err := f.dag.Head()
	if err != nil {
		return sql.DMLResult{}, err
	}
	txID, _ := codec.RandomUUID()
	author, _ := codec.RandomUUID()
	tx := document.Transaction{
		ID: txID, BaseVersion: parent, Operations: dml.Operations,
		Timestamp: codec.TimestampNow(), AuthorNodeID: author,
	}
	res, err := f.tx.Commit(tx, f.dag, f.store, f.sch, nil, "")
	if err != nil {
		return sql.DMLResult{}, err
	}
	if _, ok := res.(transaction.ResultSuccess); !ok {
		return sql.DMLResult{}, fmt.Errorf("commit failed: %T", res)
	}
	return dml, nil
}

// --- projection --------------------------------------------------------------------------

func TestConformanceProjectionSchemaless(t *testing.T) {
	f := newCorpusFixture(t, "app/conf-proj")
	f.expect(`SELECT kdb_id, title FROM tasks WHERE title = 'alpha'`, corpusID(1).String()+"|alpha")
	f.expect(`SELECT _doc FROM tasks WHERE title = 'delta'`, corpus[3])
	// Rule 2: an absent path is NULL, not an error, on a schemaless namespace.
	f.expect(`SELECT title, nosuch FROM tasks WHERE title = 'alpha'`, "alpha|NULL")
	// Nested path and terminal array project as JSON / first candidate.
	f.expect(`SELECT tags, collaborators.userId FROM tasks WHERE title = 'alpha'`, `["x","y"]|u1`)
	// Table name and alias prefixes are stripped.
	f.expect(`SELECT tasks.title FROM tasks WHERE tasks.title = 'bravo'`, "bravo")
	f.expect(`SELECT t.title AS name FROM tasks t WHERE t.title = 'bravo'`, "bravo")
	f.expect(`SELECT t.title AS name FROM tasks AS t WHERE t.title = 'bravo'`, "bravo")
	_, res, err := f.query(`SELECT t.title AS name, 1 AS one FROM tasks t WHERE t.title = 'bravo'`)
	if err != nil {
		t.Fatal(err)
	}
	if res.Columns[0].Name != "name" || res.Columns[1].Name != "one" {
		t.Fatalf("columns: %+v", res.Columns)
	}
	if res.Plan != "fullscan" {
		t.Fatalf("plan: %q", res.Plan)
	}
}

func TestConformanceSelectStarSchemaless(t *testing.T) {
	f := newCorpusFixture(t, "app/conf-star")
	rows, res, err := f.query(`SELECT * FROM tasks WHERE title = 'bravo'`)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Columns) != 2 || res.Columns[0].Name != "kdb_id" || res.Columns[1].Name != "_doc" {
		t.Fatalf("columns: %+v", res.Columns)
	}
	if len(rows) != 1 || rows[0] != corpusID(2).String()+"|"+corpus[1] {
		t.Fatalf("rows: %v", rows)
	}
}

// Rule 1: with a declared schema every column reference must resolve, wherever it appears.
func TestConformanceUnknownColumnIsPlanningErrorEverywhere(t *testing.T) {
	name := schema.MustField("name", schema.StringType{}, false, true, false)
	rank := schema.MustField("rank", schema.Int32Type{}, false, true, false)
	sch, err := schema.Build([]schema.Field{name, rank}, 1, codec.TimestampFromEpochMicros(1), "")
	if err != nil {
		t.Fatal(err)
	}
	f := newDocFixture(t, "app/conf-unknown", sch, nil, `{"name":"a","rank":1}`, `{"name":"b","rank":2}`)
	for _, q := range []string{
		`SELECT nosuch FROM t`,
		`SELECT name FROM t WHERE nosuch = 'x'`,
		`SELECT name FROM t WHERE nosuch <> 'x'`,
		`SELECT name FROM t ORDER BY nosuch`,
		`SELECT name, COUNT(*) FROM t GROUP BY nosuch`,
		`SELECT COUNT(nosuch) FROM t`,
		`SELECT name FROM t WHERE ARRAY_CONTAINS(nosuch, 'x')`,
		`SELECT name FROM t WHERE nosuch.deeper = 1`,
		`SELECT name FROM t WHERE t.nosuch = 1`,
	} {
		f.expectPlanningError(q, "unknown column: nosuch")
	}
	// Reserved names, table-prefixed names, and aliases in ORDER BY still resolve.
	f.expect(`SELECT t.name AS n, kdb_id, _doc FROM t AS t WHERE t.rank = 2 ORDER BY n`, "b|"+corpusID(2).String()+`|{"name":"b","rank":2}`)
	f.expectPlanningError(`SELECT name FROM t WHERE nosuchfn(name) = 1`, "unknown function")
}

// --- WHERE -------------------------------------------------------------------------------

func TestConformanceWhere(t *testing.T) {
	f := newCorpusFixture(t, "app/conf-where")
	f.expect(`SELECT title FROM tasks WHERE status = 'open' ORDER BY title`, "alpha", "charlie")
	f.expect(`SELECT title FROM tasks WHERE status = 'open' AND priority > 2`, "alpha")
	f.expect(`SELECT title FROM tasks WHERE status = 'blocked' OR priority >= 5 ORDER BY title`, "Echo", "delta")
	f.expect(`SELECT title FROM tasks WHERE (status = 'blocked' OR status = 'done') AND priority < 2`, "bravo")
	f.expectWithParams(`SELECT title FROM tasks WHERE priority = ?`, []sql.Parameter{sql.ParamInt{Value: 2}}, "charlie")
	f.expectWithParams(`SELECT title FROM tasks WHERE title = ? ORDER BY title`, []sql.Parameter{sql.ParamString{Value: "delta"}}, "delta")
	// Numeric comparison crosses integer/double.
	f.expect(`SELECT title FROM tasks WHERE score = 2`, "bravo")
	f.expect(`SELECT title FROM tasks WHERE score = 2.0`, "bravo")
	f.expect(`SELECT title FROM tasks WHERE priority = 3.0`, "alpha")
	// Booleans compare with TRUE/FALSE literals and parameters.
	f.expect(`SELECT title FROM tasks WHERE flag = TRUE ORDER BY title`, "alpha", "charlie")
	f.expectWithParams(`SELECT title FROM tasks WHERE flag = ? ORDER BY title`, []sql.Parameter{sql.ParamBool{Value: false}}, "bravo")
}

func (f *docFixture) expectWithParams(query string, params []sql.Parameter, want ...string) {
	f.t.Helper()
	got := f.rows(query, params...)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		f.t.Errorf("%s\n got %q\nwant %q", query, got, want)
	}
}

func TestConformanceWhereParameters(t *testing.T) {
	f := newCorpusFixture(t, "app/conf-params")
	f.expectWithParams(`SELECT title FROM tasks WHERE priority = ?`, []sql.Parameter{sql.ParamInt{Value: 2}}, "charlie")
	f.expectWithParams(`SELECT title FROM tasks WHERE flag = ? ORDER BY title`, []sql.Parameter{sql.ParamBool{Value: false}}, "bravo")
	f.expectWithParams(`SELECT title FROM tasks WHERE score = ?`, []sql.Parameter{sql.ParamDouble{Value: 0.5}}, "delta")
	f.expectWithParams(`SELECT title FROM tasks WHERE title = ? OR title = ? ORDER BY title`,
		[]sql.Parameter{sql.ParamString{Value: "alpha"}, sql.ParamString{Value: "bravo"}}, "alpha", "bravo")
	// A NULL parameter never matches (NULL = NULL is unknown).
	f.expectWithParams(`SELECT title FROM tasks WHERE priority = ?`, []sql.Parameter{sql.ParamNull{}})
	// A missing parameter is NULL, not a crash.
	f.expectWithParams(`SELECT title FROM tasks WHERE priority = ?`, nil)
}

// Mismatched types never panic: `=` is false, `<>` is true, ordering is false.
func TestConformanceMismatchedTypesAreIncomparable(t *testing.T) {
	f := newCorpusFixture(t, "app/conf-mismatch")
	f.expect(`SELECT title FROM tasks WHERE title = 5`)
	f.expect(`SELECT COUNT(*) FROM tasks WHERE title <> 5`, "5")
	f.expect(`SELECT title FROM tasks WHERE priority > 'x'`)
	// Same-type comparison still applies when the type is unexpected: Echo's score is the
	// string "high", and "high" < "zzz".
	f.expect(`SELECT title FROM tasks WHERE score < 'zzz'`, "Echo")
	f.expect(`SELECT title FROM tasks WHERE score = 'high'`, "Echo")
	f.expect(`SELECT title FROM tasks WHERE tags = 5`)
	f.expect(`SELECT title FROM tasks WHERE _doc = 'x'`)
	// NULL compared with anything, including NULL, is not a match.
	f.expect(`SELECT title FROM tasks WHERE priority = NULL`)
	f.expect(`SELECT title FROM tasks WHERE priority <> NULL`)
	f.expect(`SELECT title FROM tasks WHERE NULL = NULL`)
}

// --- ORDER BY ----------------------------------------------------------------------------

func TestConformanceOrderByBothDirectionsAndNullPlacement(t *testing.T) {
	f := newCorpusFixture(t, "app/conf-order")
	// NULL (delta's explicit null) sorts first ascending, last descending.
	f.expect(`SELECT title FROM tasks ORDER BY priority ASC`, "delta", "bravo", "charlie", "alpha", "Echo")
	f.expect(`SELECT title FROM tasks ORDER BY priority DESC`, "Echo", "alpha", "charlie", "bravo", "delta")
	f.expect(`SELECT title FROM tasks ORDER BY priority`, "delta", "bravo", "charlie", "alpha", "Echo")
	// Absent path (charlie has no score) is NULL too; mixed types order by kind, numbers first.
	f.expect(`SELECT title FROM tasks ORDER BY score ASC`, "charlie", "delta", "alpha", "bravo", "Echo")
	f.expect(`SELECT title FROM tasks ORDER BY score DESC`, "Echo", "bravo", "alpha", "delta", "charlie")
	// Multiple keys, mixed directions; strings compare byte-wise ("Echo" < "alpha").
	f.expect(`SELECT status, title FROM tasks ORDER BY status ASC, title DESC`,
		"blocked|delta", "done|bravo", "done|Echo", "open|charlie", "open|alpha")
	// Alias and expression keys.
	f.expect(`SELECT title AS t FROM tasks ORDER BY t DESC LIMIT 2`, "delta", "charlie")
	f.expect(`SELECT title FROM tasks ORDER BY ARRAY_LENGTH(tags) DESC, title ASC`, "charlie", "alpha", "Echo", "bravo", "delta")
	// Implicit traversal: first candidate orders.
	f.expect(`SELECT title FROM tasks WHERE collaborators.userId IS NOT NULL ORDER BY collaborators.userId DESC`, "bravo", "alpha")
}

// --- DISTINCT ----------------------------------------------------------------------------

func TestConformanceDistinct(t *testing.T) {
	f := newCorpusFixture(t, "app/conf-distinct")
	f.expect(`SELECT DISTINCT status FROM tasks ORDER BY status`, "blocked", "done", "open")
	// LIMIT bounds distinct rows, not pre-dedup rows (§3.3).
	f.expect(`SELECT DISTINCT status FROM tasks ORDER BY status LIMIT 2`, "blocked", "done")
	f.expect(`SELECT DISTINCT status FROM tasks ORDER BY status LIMIT 2 OFFSET 1`, "done", "open")
	f.expect(`SELECT DISTINCT status, flag FROM tasks ORDER BY status, flag`, "blocked|NULL", "done|NULL", "done|0", "open|1")
	if got := f.rows(`SELECT DISTINCT status FROM tasks`); len(got) != 3 {
		t.Fatalf("unordered DISTINCT: %v", got)
	}
	f.expect(`SELECT DISTINCT flag FROM tasks WHERE flag`, "1")
}

// --- LIMIT / OFFSET ----------------------------------------------------------------------

func TestConformanceLimitOffset(t *testing.T) {
	f := newCorpusFixture(t, "app/conf-limit")
	f.expect(`SELECT title FROM tasks ORDER BY title LIMIT 2`, "Echo", "alpha")
	f.expect(`SELECT title FROM tasks ORDER BY title LIMIT 2 OFFSET 2`, "bravo", "charlie")
	f.expect(`SELECT title FROM tasks ORDER BY title LIMIT 0`)
	f.expect(`SELECT title FROM tasks ORDER BY title OFFSET 4`, "delta")
	f.expect(`SELECT title FROM tasks ORDER BY title LIMIT 10 OFFSET 10`)
	if got := f.rows(`SELECT title FROM tasks WHERE status = 'done' LIMIT 1`); len(got) != 1 {
		t.Fatalf("LIMIT without ORDER BY after a residual filter: %v", got)
	}
	if got := f.rows(`SELECT title FROM tasks LIMIT 3 OFFSET 1`); len(got) != 3 {
		t.Fatalf("LIMIT/OFFSET without ORDER BY: %v", got)
	}
}

// --- GROUP BY and aggregates -------------------------------------------------------------

func TestConformanceGroupBy(t *testing.T) {
	f := newCorpusFixture(t, "app/conf-group")
	// Groups come out in ascending key order without ORDER BY; keys are projectable.
	f.expect(`SELECT status, COUNT(*) FROM tasks GROUP BY status`, "blocked|1", "done|2", "open|2")
	f.expect(`SELECT status, COUNT(*) AS n FROM tasks GROUP BY status ORDER BY n DESC, status`, "done|2", "open|2", "blocked|1")
	f.expect(`SELECT status, SUM(priority), AVG(priority), MIN(title), MAX(title) FROM tasks GROUP BY status`,
		"blocked|NULL|NULL|delta|delta", "done|6|3|Echo|bravo", "open|5|2.5|alpha|charlie")
	f.expect(`SELECT status, flag, COUNT(*) FROM tasks GROUP BY status, flag`, "blocked|NULL|1", "done|NULL|1", "done|0|1", "open|1|2")
	f.expect(`SELECT status, COUNT(*) FROM tasks GROUP BY status LIMIT 1 OFFSET 1`, "done|2")
	f.expect(`SELECT status FROM tasks GROUP BY status`, "blocked", "done", "open")
	f.expect(`SELECT t.status AS s, COUNT(*) FROM tasks t WHERE priority IS NOT NULL GROUP BY t.status ORDER BY s`, "done|2", "open|2")
	// Grouping by an array path groups by the whole array value; groups order by the total
	// comparator, which puts NULL first and then compares JSON text.
	f.expect(`SELECT projectIds, COUNT(*) FROM tasks GROUP BY projectIds`,
		"NULL|1", `["p1","p2"]|1`, `["p1"]|1`, `["p2"]|1`, `[]|1`)
	f.expectPlanningError(`SELECT title, COUNT(*) FROM tasks GROUP BY status`, "must appear in GROUP BY")
	f.expectPlanningError(`SELECT title, COUNT(*) FROM tasks`, "must appear in GROUP BY")
	f.expectPlanningError(`SELECT * FROM tasks GROUP BY status`, "")
	f.expectPlanningError(`SELECT title FROM tasks WHERE COUNT(*) > 1`, "not allowed")
}

func TestConformanceEachAggregate(t *testing.T) {
	f := newCorpusFixture(t, "app/conf-agg")
	f.expect(`SELECT COUNT(*) FROM tasks`, "5")
	f.expect(`SELECT COUNT(priority) FROM tasks`, "4")
	f.expect(`SELECT COUNT(score) FROM tasks`, "4")
	f.expect(`SELECT COUNT(collaborators.userId) FROM tasks`, "2")
	// SUM over integers only stays an integer; any double makes it a double; AVG is a double.
	f.expect(`SELECT SUM(priority) FROM tasks`, "11")
	f.expect(`SELECT SUM(score) FROM tasks`, "4")
	f.expect(`SELECT AVG(priority) FROM tasks`, "2.75")
	f.expect(`SELECT AVG(priority) FROM tasks WHERE status = 'done'`, "3")
	f.expect(`SELECT MIN(priority), MAX(priority) FROM tasks`, "1|5")
	f.expect(`SELECT MIN(title), MAX(title) FROM tasks`, "Echo|delta")
	// Zero rows: NULL for everything but COUNT.
	f.expect(`SELECT COUNT(*), COUNT(priority), SUM(priority), AVG(priority), MIN(priority), MAX(priority) FROM tasks WHERE title = 'none'`,
		"0|0|NULL|NULL|NULL|NULL")
	f.expect(`SELECT status, COUNT(*) FROM tasks WHERE title = 'none' GROUP BY status`)
	_, res, err := f.query(`SELECT SUM(priority) AS s, AVG(priority) AS a FROM tasks`)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.Rows[0].Values[0].(sql.CellLong); !ok {
		t.Fatalf("SUM of integers must be CellLong, got %T", res.Rows[0].Values[0])
	}
	if _, ok := res.Rows[0].Values[1].(sql.CellDouble); !ok {
		t.Fatalf("AVG must be CellDouble, got %T", res.Rows[0].Values[1])
	}
	_, res, err = f.query(`SELECT SUM(score) FROM tasks`)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.Rows[0].Values[0].(sql.CellDouble); !ok {
		t.Fatalf("SUM with a double input must be CellDouble, got %T", res.Rows[0].Values[0])
	}
	f.expectPlanningError(`SELECT SUM(priority, score) FROM tasks`, "exactly one argument")
}

// --- LIKE / IN / BETWEEN -----------------------------------------------------------------

func TestConformanceLike(t *testing.T) {
	f := newCorpusFixture(t, "app/conf-like")
	f.expect(`SELECT title FROM tasks WHERE title LIKE 'a%'`, "alpha")
	f.expect(`SELECT title FROM tasks WHERE title LIKE '%a' ORDER BY title`, "alpha", "delta")
	f.expect(`SELECT title FROM tasks WHERE title LIKE '_lpha'`, "alpha")
	f.expect(`SELECT title FROM tasks WHERE title LIKE '%l%' ORDER BY title`, "alpha", "charlie", "delta")
	// Case-sensitive LIKE, case-insensitive ILIKE.
	f.expect(`SELECT title FROM tasks WHERE title LIKE 'e%'`)
	f.expect(`SELECT title FROM tasks WHERE title ILIKE 'e%'`, "Echo")
	f.expect(`SELECT title FROM tasks WHERE title NOT LIKE '%a' AND title NOT LIKE 'c%' ORDER BY title`, "Echo", "bravo")
	f.expect(`SELECT title FROM tasks WHERE title NOT ILIKE '%A' ORDER BY title`, "Echo", "bravo", "charlie")
	// Regex metacharacters are literal.
	f.expect(`SELECT title FROM tasks WHERE title LIKE 'a.pha'`)
	f.expect(`SELECT title FROM tasks WHERE title LIKE '(alpha)'`)
	f.expect(`SELECT title FROM tasks WHERE title LIKE 'alpha|bravo'`)
	// Non-string operands never match; a parameter pattern works.
	f.expect(`SELECT title FROM tasks WHERE priority LIKE '%'`)
	f.expectWithParams(`SELECT title FROM tasks WHERE title LIKE ?`, []sql.Parameter{sql.ParamString{Value: "br%"}}, "bravo")
	// Implicit traversal through an array of objects.
	f.expect(`SELECT title FROM tasks WHERE steps.text LIKE '%deploy%'`, "alpha")
}

func TestConformanceIn(t *testing.T) {
	f := newCorpusFixture(t, "app/conf-in")
	f.expect(`SELECT title FROM tasks WHERE status IN ('open', 'blocked') ORDER BY title`, "alpha", "charlie", "delta")
	f.expect(`SELECT title FROM tasks WHERE status NOT IN ('open', 'blocked') ORDER BY title`, "Echo", "bravo")
	f.expect(`SELECT title FROM tasks WHERE priority IN (1, 2.0) ORDER BY title`, "bravo", "charlie")
	f.expectWithParams(`SELECT title FROM tasks WHERE title IN (?, ?) ORDER BY title`,
		[]sql.Parameter{sql.ParamString{Value: "alpha"}, sql.ParamString{Value: "Echo"}}, "Echo", "alpha")
	// Membership over an array path: any tag in the list.
	f.expect(`SELECT title FROM tasks WHERE tags IN ('z', 'q') ORDER BY title`, "Echo", "charlie")
	f.expect(`SELECT title FROM tasks WHERE priority IN (NULL)`)
}

func TestConformanceBetween(t *testing.T) {
	f := newCorpusFixture(t, "app/conf-between")
	f.expect(`SELECT title FROM tasks WHERE priority BETWEEN 2 AND 3 ORDER BY title`, "alpha", "charlie")
	// Predicates are two-valued per §2 (an unknown comparison is false), so NOT of a NULL
	// comparison is true and delta - whose priority is null - is included.
	f.expect(`SELECT title FROM tasks WHERE priority NOT BETWEEN 2 AND 3 ORDER BY title`, "Echo", "bravo", "delta")
	f.expect(`SELECT title FROM tasks WHERE score BETWEEN 0.5 AND 1.5 ORDER BY title`, "alpha", "delta")
	f.expect(`SELECT title FROM tasks WHERE title BETWEEN 'b' AND 'd' ORDER BY title`, "bravo", "charlie")
	f.expectWithParams(`SELECT title FROM tasks WHERE priority BETWEEN ? AND ? ORDER BY title`,
		[]sql.Parameter{sql.ParamInt{Value: 1}, sql.ParamInt{Value: 2}}, "bravo", "charlie")
}

// --- IS NULL / NOT / bare boolean --------------------------------------------------------

func TestConformanceIsNullNotAndBoolean(t *testing.T) {
	f := newCorpusFixture(t, "app/conf-null")
	f.expect(`SELECT title FROM tasks WHERE priority IS NULL`, "delta")
	f.expect(`SELECT title FROM tasks WHERE score IS NULL`, "charlie")
	f.expect(`SELECT title FROM tasks WHERE collaborators IS NULL ORDER BY title`, "Echo", "charlie", "delta")
	f.expect(`SELECT title FROM tasks WHERE priority IS NOT NULL ORDER BY title`, "Echo", "alpha", "bravo", "charlie")
	f.expect(`SELECT title FROM tasks WHERE NOT status = 'open' ORDER BY title`, "Echo", "bravo", "delta")
	f.expect(`SELECT title FROM tasks WHERE NOT (status = 'open' OR status = 'done')`, "delta")
	f.expect(`SELECT title FROM tasks WHERE NOT NOT status = 'blocked'`, "delta")
	f.expect(`SELECT title FROM tasks WHERE flag ORDER BY title`, "alpha", "charlie")
	f.expect(`SELECT title FROM tasks WHERE NOT flag ORDER BY title`, "Echo", "bravo", "delta")
	f.expect(`SELECT title FROM tasks WHERE flag AND priority = 2`, "charlie")
	// A non-boolean column as a predicate is simply false.
	f.expect(`SELECT title FROM tasks WHERE title`)
}

// --- MATCH / SIMILARITY / FUSE without indexes -------------------------------------------

func TestConformanceSearchFunctionsRequireIndexes(t *testing.T) {
	f := newCorpusFixture(t, "app/conf-search")
	f.expectPlanningError(`SELECT title FROM tasks WHERE MATCH(tasks_text, 'deploy')`, "no FULLTEXT index for tasks_text")
	f.expectPlanningError(`SELECT MATCH(tasks_text, 'deploy') AS score FROM tasks ORDER BY score DESC`, "no FULLTEXT index for tasks_text")
	f.expectPlanningError(`SELECT SIMILARITY(embedding, [0.1, 0.2]) AS score FROM tasks ORDER BY score DESC LIMIT 3`, "no VECTOR index for embedding")
	f.expectPlanningError(`SELECT FUSE(MATCH(tasks_text, 'x'), SIMILARITY(embedding, [1])) AS s FROM tasks ORDER BY s DESC`, "no ")
}
