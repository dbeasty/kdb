package sql_test

import (
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/sql"
)

// UPDATE ... ORDER BY ... LIMIT ... RETURNING: the claim statement (the server runs it under its
// write gate; these tests cover what it selects, writes and returns).

func returnedRows(res sql.DMLResult) []string {
	out := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		parts := make([]string, len(r.Values))
		for i, c := range r.Values {
			parts[i] = cellString(c)
		}
		out = append(out, strings.Join(parts, "|"))
	}
	return out
}

func TestUpdateOrderByLimitReturning(t *testing.T) {
	f := newCorpusFixture(t, "app/dml-claim")
	// The open task with the lowest priority is alpha (3)? No: charlie (2). ORDER BY picks it.
	res, err := f.commitDML(`UPDATE tasks SET status = 'claimed', priority = priority + 10
		WHERE status = 'open' ORDER BY priority LIMIT 1 RETURNING kdb_id, title, status, priority`)
	if err != nil {
		t.Fatal(err)
	}
	if res.RowsAffected != 1 || len(res.Operations) != 1 {
		t.Fatalf("%+v", res)
	}
	got := returnedRows(res)
	want := corpusID(3).String() + "|charlie|claimed|12"
	if len(got) != 1 || got[0] != want {
		t.Fatalf("returned %q, want %q", got, want)
	}
	if names := []string{res.Columns[0].Name, res.Columns[1].Name, res.Columns[2].Name, res.Columns[3].Name}; strings.Join(names, ",") != "kdb_id,title,status,priority" {
		t.Fatalf("columns %v", names)
	}
	f.expect(`SELECT title, priority FROM tasks WHERE status = 'claimed'`, "charlie|12")
	// The next claim takes the next one, DESC respected.
	res, err = f.commitDML(`UPDATE tasks SET status = 'claimed' WHERE status = 'open' OR status = 'done' ORDER BY priority DESC LIMIT 2 RETURNING title`)
	if err != nil {
		t.Fatal(err)
	}
	if got := returnedRows(res); strings.Join(got, ",") != "Echo,alpha" {
		t.Fatalf("returned %q", got)
	}
	// No match: zero rows, no ops, and a RETURNING result that is empty rather than absent.
	res, err = f.commitDML(`UPDATE tasks SET status = 'x' WHERE status = 'nope' ORDER BY priority LIMIT 1 RETURNING kdb_id`)
	if err != nil {
		t.Fatal(err)
	}
	if res.RowsAffected != 0 || len(res.Operations) != 0 || res.Rows == nil || len(res.Rows) != 0 || len(res.Columns) != 1 {
		t.Fatalf("%+v", res)
	}
}

func TestUpdateReturningDocIsPostUpdate(t *testing.T) {
	f := newCorpusFixture(t, "app/dml-claim-doc")
	res, err := f.commitDML(`UPDATE tasks SET status = 'claimed' WHERE title = 'delta' RETURNING _doc`)
	if err != nil {
		t.Fatal(err)
	}
	got := returnedRows(res)
	if len(got) != 1 || !strings.Contains(got[0], `"status":"claimed"`) || !strings.Contains(got[0], `"title":"delta"`) {
		t.Fatalf("returned %q", got)
	}
	// Without LIMIT every match is updated and returned.
	res, err = f.commitDML(`UPDATE tasks SET seen = 1 WHERE tags = 'y' ORDER BY title RETURNING title, seen`)
	if err != nil {
		t.Fatal(err)
	}
	if got := returnedRows(res); strings.Join(got, ",") != "alpha|1,bravo|1,charlie|1" {
		t.Fatalf("returned %q", got)
	}
}

func TestUpdateArithmetic(t *testing.T) {
	f := newCorpusFixture(t, "app/dml-arith")
	if _, err := f.commitDML(`UPDATE tasks SET priority = priority + 1, half = priority / 2, ratio = score * 2, neg = 0 - priority WHERE title = 'alpha'`); err != nil {
		t.Fatal(err)
	}
	f.expect(`SELECT priority, half, ratio, neg FROM tasks WHERE title = 'alpha'`, "4|1|3|-3")
	// Precedence and grouping; NULL and non-numbers make NULL; division by zero is NULL.
	f.expect(`SELECT 1 + 2 * 3, (1 + 2) * 3, 7 - 2 - 1, 7 / 0, 1.5 + 1`, "7|9|4|NULL|2.5")
	if _, err := f.commitDML(`UPDATE tasks SET priority = priority + 1, bad = title + 1 WHERE title = 'delta'`); err != nil {
		t.Fatal(err)
	}
	f.expect(`SELECT priority, bad FROM tasks WHERE title = 'delta'`, "NULL|NULL")
	// Arithmetic in a predicate.
	f.expect(`SELECT title FROM tasks WHERE priority + 1 > 4 ORDER BY title`, "Echo", "alpha") // alpha is 4 now
	// A plain UPDATE (no claim clauses) is unchanged: every match, no rows returned.
	res, err := f.commitDML(`UPDATE tasks SET n = 1`)
	if err != nil || res.RowsAffected != 5 || len(res.Rows) != 0 || res.Columns != nil {
		t.Fatalf("%+v %v", res, err)
	}
}

func TestUpdateClaimClauseErrors(t *testing.T) {
	f := newCorpusFixture(t, "app/dml-claim-err")
	for _, q := range []string{
		`UPDATE tasks SET a = 1 RETURNING COUNT(*)`,
		`UPDATE tasks SET a = 1 ORDER BY COUNT(*) LIMIT 1`,
		`UPDATE tasks SET a = 1 LIMIT -1`,
		`UPDATE tasks SET a = 1 RETURNING`,
	} {
		if _, err := f.engine.ExecuteDML(q, f.ctx); err == nil {
			t.Errorf("%s: expected an error", q)
		}
	}
	stmt, err := sql.DefaultParser{}.Parse(`UPDATE tasks SET a = 1 WHERE b = 2`)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := sql.ImmediateUpdate(stmt); ok {
		t.Fatal("a plain UPDATE is not immediate")
	}
	stmt, err = sql.DefaultParser{}.Parse(`UPDATE tasks SET a = 1 WHERE b = 2 LIMIT 1`)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := sql.ImmediateUpdate(stmt); !ok {
		t.Fatal("UPDATE ... LIMIT is immediate")
	}
}

// The two halves the server runs around its write gate: candidates at one head, the update among
// them (and whatever changed since) at a later one.
func TestUpdateCandidatesAndAmong(t *testing.T) {
	f := newCorpusFixture(t, "app/dml-claim-among")
	stmt, err := sql.DefaultParser{}.Parse(`UPDATE tasks SET status = 'claimed' WHERE status = 'open' OR status = 'done' ORDER BY priority LIMIT 1 RETURNING title`)
	if err != nil {
		t.Fatal(err)
	}
	upd, _ := sql.ImmediateUpdate(stmt)
	iu := f.engine.(sql.ImmediateUpdater)
	ids, complete, err := iu.UpdateCandidates(upd, f.ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	// bravo (1), charlie (2)
	if complete || len(ids) != 2 || ids[0] != corpusID(2) || ids[1] != corpusID(3) {
		t.Fatalf("ids %v complete %v", ids, complete)
	}
	ids, complete, err = iu.UpdateCandidates(upd, f.ctx, 10)
	if err != nil || !complete || len(ids) != 4 {
		t.Fatalf("ids %v complete %v err %v", ids, complete, err)
	}
	// Among a set that no longer matches the best: bravo was closed meanwhile.
	if _, err := f.commitDML(`UPDATE tasks SET status = 'closed' WHERE title = 'bravo'`); err != nil {
		t.Fatal(err)
	}
	res, err := iu.ExecuteUpdateAmong(upd, f.ctx, []codec.UUID{corpusID(2), corpusID(3)})
	if err != nil {
		t.Fatal(err)
	}
	if got := returnedRows(res); strings.Join(got, ",") != "charlie" {
		t.Fatalf("returned %q", got)
	}
	if w, ok := res.Operations[0].(document.WriteOp); !ok || w.DocID != corpusID(3) {
		t.Fatalf("ops %+v", res.Operations)
	}
	// An empty among updates nothing (nil means every document).
	res, err = iu.ExecuteUpdateAmong(upd, f.ctx, []codec.UUID{})
	if err != nil || len(res.Operations) != 0 || len(res.Rows) != 0 {
		t.Fatalf("%+v %v", res, err)
	}
}
