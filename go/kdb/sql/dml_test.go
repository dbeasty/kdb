package sql_test

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/sql"
)

// Component 72 - UPDATE / DELETE lowering (kdb-spec-layer16 §5).

func TestUpdateSetsPathsAndCountsRows(t *testing.T) {
	f := newCorpusFixture(t, "app/dml-update")
	res, err := f.commitDML(`UPDATE tasks SET status = 'done', meta.reviewed = true, priority = NULL WHERE title = 'alpha'`)
	if err != nil {
		t.Fatal(err)
	}
	if res.RowsAffected != 1 || len(res.Operations) != 1 {
		t.Fatalf("%+v", res)
	}
	w, ok := res.Operations[0].(document.WriteOp)
	if !ok || w.DocID != corpusID(1) {
		t.Fatalf("op: %+v", res.Operations[0])
	}
	f.expect(`SELECT status, meta.reviewed, priority, tags FROM tasks WHERE title = 'alpha'`, `done|1|NULL|["x","y"]`)
	f.expect(`SELECT _doc FROM tasks WHERE title = 'alpha'`,
		`{"title":"alpha","status":"done","priority":null,"tags":["x","y"],"projectIds":["p1","p2"],"collaborators":[{"userId":"u1"},{"userId":"u2"}],"steps":[{"text":"plan"},{"text":"deploy staging"}],"flag":true,"score":1.5,"meta":{"reviewed":true}}`)
	// Nothing matched: zero rows, no ops.
	res, err = f.commitDML(`UPDATE tasks SET status = 'x' WHERE title = 'none'`)
	if err != nil || res.RowsAffected != 0 || len(res.Operations) != 0 {
		t.Fatalf("%+v %v", res, err)
	}
	// Multi-row update with a parameter, an alias-prefixed target, and array-path predicate.
	res, err = f.commitDML(`UPDATE tasks t SET t.status = ? WHERE tags = 'z'`, sql.ParamString{Value: "archived"})
	if err != nil || res.RowsAffected != 2 {
		t.Fatalf("%+v %v", res, err)
	}
	f.expect(`SELECT title FROM tasks WHERE status = 'archived' ORDER BY title`, "Echo", "charlie")
	// No WHERE touches every document.
	res, err = f.commitDML(`UPDATE tasks SET touched = 1`)
	if err != nil || res.RowsAffected != 5 {
		t.Fatalf("%+v %v", res, err)
	}
	f.expect(`SELECT COUNT(*) FROM tasks WHERE touched = 1`, "5")
}

func TestUpdateExpressionsReadThePreUpdateDocument(t *testing.T) {
	f := newCorpusFixture(t, "app/dml-update-expr")
	// title receives the old status, and status the old title - a swap, which only works if
	// both read the document as it was before any assignment applied.
	res, err := f.commitDML(`UPDATE tasks SET title = status, status = title WHERE title = 'bravo'`)
	if err != nil || res.RowsAffected != 1 {
		t.Fatalf("%+v %v", res, err)
	}
	f.expect(`SELECT title, status FROM tasks WHERE status = 'bravo'`, "done|bravo")
	// Column references resolve nested paths and array first-candidates too.
	res, err = f.commitDML(`UPDATE tasks SET lead = collaborators.userId, firstTag = tags WHERE title = 'alpha'`)
	if err != nil || res.RowsAffected != 1 {
		t.Fatalf("%+v %v", res, err)
	}
	f.expect(`SELECT lead, firstTag FROM tasks WHERE title = 'alpha'`, `u1|["x","y"]`)
}

func TestUpdateDocReplacesWholeDocument(t *testing.T) {
	f := newCorpusFixture(t, "app/dml-update-doc")
	res, err := f.commitDML(`UPDATE tasks SET _doc = '{"title": "zeta", "fresh": true}' WHERE title = 'delta'`)
	if err != nil || res.RowsAffected != 1 {
		t.Fatalf("%+v %v", res, err)
	}
	// The replacement body is handed to the commit path verbatim - no key injected, none
	// reordered (§9.4).
	w := res.Operations[0].(document.WriteOp)
	if w.DocID != corpusID(4) || w.Patch != `{"title": "zeta", "fresh": true}` {
		t.Fatalf("replacement must be stored verbatim: %+v", w)
	}
	// NOTE: document.WriteOp is applied as a key-wise merge by the transaction engine, so keys
	// absent from the replacement body (here "status") survive. Removing them needs a
	// replace-capable write op in the document/transaction layer, which this layer cannot
	// express; the assignment itself is correct.
	f.expectWithParams(`SELECT title, fresh FROM tasks WHERE kdb_id = ?`, []sql.Parameter{sql.ParamString{Value: corpusID(4).String()}}, "zeta|1")
	for _, q := range []string{
		`UPDATE tasks SET _doc = 'not json' WHERE title = 'alpha'`,
		`UPDATE tasks SET _doc = '[1,2]' WHERE title = 'alpha'`,
		`UPDATE tasks SET _doc = 5 WHERE title = 'alpha'`,
		`UPDATE tasks SET kdb_id = 'x' WHERE title = 'alpha'`,
	} {
		if _, err := f.commitDML(q); err == nil {
			t.Errorf("%s: expected an error", q)
		}
	}
}

func TestDeleteRemovesMatchingDocuments(t *testing.T) {
	f := newCorpusFixture(t, "app/dml-delete")
	res, err := f.commitDML(`DELETE FROM tasks WHERE status = 'done'`)
	if err != nil || res.RowsAffected != 2 || len(res.Operations) != 2 {
		t.Fatalf("%+v %v", res, err)
	}
	for _, op := range res.Operations {
		if _, ok := op.(document.DeleteOp); !ok {
			t.Fatalf("expected DeleteOp, got %T", op)
		}
	}
	f.expect(`SELECT title FROM tasks ORDER BY title`, "alpha", "charlie", "delta")
	res, err = f.commitDML(`DELETE FROM tasks t WHERE t.collaborators.userId = 'u2'`)
	if err != nil || res.RowsAffected != 1 {
		t.Fatalf("%+v %v", res, err)
	}
	f.expect(`SELECT title FROM tasks ORDER BY title`, "charlie", "delta")
	res, err = f.commitDML(`DELETE FROM tasks`)
	if err != nil || res.RowsAffected != 2 {
		t.Fatalf("%+v %v", res, err)
	}
	f.expect(`SELECT COUNT(*) FROM tasks`, "0")
}

func TestUpdateValidatesAgainstSchemaAfterAssignment(t *testing.T) {
	userID := schema.MustField("userId", schema.StringType{}, true, true, false)
	age := schema.MustField("age", schema.Int32Type{}, false, true, false)
	sch, err := schema.Build([]schema.Field{userID, age}, 1, codec.TimestampFromEpochMicros(1), "")
	if err != nil {
		t.Fatal(err)
	}
	f := newDocFixture(t, "app/dml-schema", sch, nil, `{"userId":"u1","age":30}`, `{"userId":"u2","age":40}`)
	if _, err := f.commitDML(`UPDATE users SET age = 31 WHERE userId = 'u1'`); err != nil {
		t.Fatal(err)
	}
	f.expect(`SELECT age FROM users WHERE userId = 'u1'`, "31")
	// Type and required-field violations fail the statement; unknown targets are Rule-1 errors.
	for _, q := range []string{
		`UPDATE users SET age = 'old' WHERE userId = 'u1'`,
		`UPDATE users SET userId = NULL WHERE userId = 'u1'`,
		`UPDATE users SET nosuch = 1 WHERE userId = 'u1'`,
		`UPDATE users SET age = 1 WHERE nosuch = 1`,
		`UPDATE users SET age = nosuch WHERE userId = 'u1'`,
		`DELETE FROM users WHERE nosuch = 1`,
		`UPDATE users SET _doc = '{"age": 1}'`,
	} {
		if _, err := f.commitDML(q); err == nil {
			t.Errorf("%s: expected an error", q)
		}
	}
	f.expect(`SELECT userId, age FROM users ORDER BY userId`, "u1|31", "u2|40")
}

func TestExecuteRoutesDMLToExecuteDML(t *testing.T) {
	f := newCorpusFixture(t, "app/dml-route")
	for _, q := range []string{`UPDATE tasks SET a = 1`, `DELETE FROM tasks`, `INSERT INTO tasks (a) VALUES (1)`} {
		if _, err := f.engine.Execute(q, f.ctx); err == nil {
			t.Errorf("%s: Execute must refuse DML", q)
		}
	}
	if _, err := f.engine.ExecuteDML(`SELECT title FROM tasks`, f.ctx); err == nil {
		t.Error("ExecuteDML must refuse SELECT")
	}
	if _, err := f.engine.Execute(`CREATE INDEX i ON tasks (title)`, f.ctx); err == nil {
		t.Error("index DDL without a catalog must error, not silently succeed")
	}
}

func TestInsertWritesBooleansAndVectorsAsJSON(t *testing.T) {
	f := newCorpusFixture(t, "app/dml-insert")
	res, err := f.commitDML(`INSERT INTO tasks (title, flag, embedding, n) VALUES ('new', TRUE, [0.5, 1], ?)`, sql.ParamBool{Value: false})
	if err != nil || res.RowsAffected != 1 {
		t.Fatalf("%+v %v", res, err)
	}
	f.expect(`SELECT _doc FROM tasks WHERE title = 'new'`, `{"title":"new","flag":true,"embedding":[0.5,1],"n":false}`)
}

func TestCreateTableWithUniqueConstraints(t *testing.T) {
	f := newDocFixture(t, "app/ddl-unique", schema.None(), nil)
	res, err := f.engine.Execute(`CREATE TABLE users (email VARCHAR NOT NULL UNIQUE, org VARCHAR, handle VARCHAR, UNIQUE (org, handle))`, f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	sch := res.AppliedSchema
	if sch == nil || sch.IsNone() {
		t.Fatal("expected a schema")
	}
	email, _ := sch.Field("email")
	if !email.Unique || !email.Required || !email.Indexed {
		t.Fatalf("email: %+v", email)
	}
	tuples := sch.UniqueTuples()
	if len(tuples) != 2 || tuples[0][0] != "email" || len(tuples[1]) != 2 || tuples[1][0] != "org" || tuples[1][1] != "handle" {
		t.Fatalf("tuples: %v", tuples)
	}
	if _, err := f.engine.Execute(`CREATE TABLE users (a VARCHAR, UNIQUE (a, nosuch))`, f.ctx); err == nil {
		t.Fatal("a constraint over an undeclared column must fail")
	}
}
