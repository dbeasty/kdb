package server

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/index/stores"
	"github.com/limidus/kdb/go/kdb/sql"
)

// U7 acceptance: a 200-row page of a user with 8k tasks, ordered by a BTREE-indexed due date,
// reads ~200 documents - not the user's 8,000 - and returns what reading and sorting all of them
// returns (checked by dropping the BTREE and asking again).
func TestBTreeOrderedPageOfAUsersTasksReadsAboutThePage(t *testing.T) {
	rt := newTestRuntime(t)
	defer rt.Release()
	p, err := rt.OpenIndexes(stores.Options{})
	if err != nil {
		t.Fatal(err)
	}
	createHashIndex(t, p, rt, "by_c", "_c", "HASH")
	createHashIndex(t, p, rt, "by_user", "userId", "HASH")
	createHashIndex(t, p, rt, "by_due", "dueDate", "BTREE")

	r := rand.New(rand.NewSource(11))
	type task struct {
		due string
		n   int
	}
	var mine []task
	var ops []document.Op
	flush := func() {
		head, err := rt.Runtime.DAG.Head()
		if err != nil {
			t.Fatal(err)
		}
		txID, _ := codec.RandomUUID()
		tx := document.Transaction{ID: txID, BaseVersion: head, Operations: ops, Timestamp: codec.TimestampNow()}
		if _, err := rt.Commit(rt.Runtime.DefaultNamespace, tx, "", auth.Principal{ID: "tester"}); err != nil {
			t.Fatal(err)
		}
		ops = ops[:0]
	}
	for i := 0; i < 13000; i++ {
		// 8,000 tasks of user u1, then other users' tasks and notes.
		user, coll := "u1", "tasks"
		if i >= 8000 {
			user = fmt.Sprintf("u%d", 2*(i%2))
			if i%5 == 0 {
				coll = "notes"
			}
		}
		// Two years of due dates: ~11 tasks a day, so a page boundary usually falls in a tie.
		due := fmt.Sprintf("2026-%02d-%02dT09:00:00Z", 1+r.Intn(12), 1+r.Intn(28))
		if r.Intn(2) == 0 {
			due = "2027" + due[4:]
		}
		if user == "u1" && coll == "tasks" {
			mine = append(mine, task{due: due, n: i})
		}
		id, _ := codec.RandomUUID()
		ops = append(ops, document.WriteOp{DocID: id, Patch: fmt.Sprintf(`{"_c":%q,"userId":%q,"dueDate":%q,"n":%d,"title":"task %d"}`, coll, user, due, i, i)})
		if len(ops) == 1000 {
			flush()
		}
	}
	flush()
	if len(mine) != 8000 {
		t.Fatalf("shape: user u1 has %d tasks", len(mine))
	}
	sort.Slice(mine, func(i, j int) bool {
		if mine[i].due != mine[j].due {
			return mine[i].due < mine[j].due
		}
		return mine[i].n < mine[j].n
	})

	query := func(q string) ([][]sql.Cell, *sql.ExecStats, string) {
		t.Helper()
		stats := &sql.ExecStats{}
		ctx := sql.QueryContext{NamespaceID: rt.Runtime.DefaultNamespace, Schema: rt.Runtime.Schema, Stats: stats,
			Parameters: []sql.Parameter{sql.ParamString{Value: "tasks"}, sql.ParamString{Value: "u1"}}}
		res, err := rt.SQLEngine().Execute(q, ctx)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		out := make([][]sql.Cell, len(res.Rows))
		for i, row := range res.Rows {
			out[i] = row.Values
		}
		return out, stats, res.Plan
	}

	// Fully ordered (n breaks the due-date ties): the rows are known without the executor.
	q := `SELECT dueDate, n FROM docs WHERE _c = ? AND userId = ? ORDER BY dueDate, n LIMIT 200 OFFSET 200`
	rows, stats, plan := query(q)
	if len(rows) != 200 {
		t.Fatalf("%s: %d rows", q, len(rows))
	}
	for i, row := range rows {
		want := mine[200+i]
		if row[0].(sql.CellString).Value != want.due || row[1].(sql.CellLong).Value != int64(want.n) {
			t.Fatalf("%s: row %d is %v, want %v", q, i, row, want)
		}
	}
	t.Logf("%s: plan %s, %d documents read", q, plan, stats.DocsRead)
	if stats.DocsRead > 400+64 {
		t.Fatalf("%s: read %d documents for rows 200-399 of 8000", q, stats.DocsRead)
	}

	// The acceptance shape: the first page, ties at the boundary broken as the full sort breaks them.
	shapes := []string{
		`SELECT kdb_id, dueDate FROM docs WHERE _c = ? AND userId = ? ORDER BY dueDate LIMIT 200`,
		`SELECT kdb_id, dueDate, title FROM docs WHERE _c = ? AND userId = ? ORDER BY dueDate ASC, title DESC LIMIT 200`,
	}
	got := map[string][][]sql.Cell{}
	for _, q := range shapes {
		rows, stats, plan := query(q)
		t.Logf("%s: plan %s, %d documents read", q, plan, stats.DocsRead)
		if len(rows) != 200 {
			t.Fatalf("%s: %d rows", q, len(rows))
		}
		if stats.DocsRead > 200+64 {
			t.Fatalf("%s: read %d documents for a 200-row page of 8000 tasks", q, stats.DocsRead)
		}
		got[q] = rows
	}
	// Without the BTREE the same queries read every task and sort them: the oracle.
	if err := p.DropIndex(sql.StmtDropIndex{Name: "by_due"}, sql.QueryContext{NamespaceID: rt.Runtime.DefaultNamespace}); err != nil {
		t.Fatal(err)
	}
	for _, q := range shapes {
		rows, stats, plan := query(q)
		t.Logf("without the BTREE: %s: plan %s, %d documents read", q, plan, stats.DocsRead)
		if stats.DocsRead != 8000 {
			t.Fatalf("%s without the BTREE read %d documents, want all 8000", q, stats.DocsRead)
		}
		if fmt.Sprint(rows) != fmt.Sprint(got[q]) {
			t.Fatalf("%s: the BTREE page differs from the full sort", q)
		}
	}
}
