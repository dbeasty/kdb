package sql_test

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/sql"
)

// INSERT ... ON CONFLICT parses into the statement the server decides under its write gate.
func TestParseInsertOnConflict(t *testing.T) {
	p := sql.DefaultParser{}
	stmt, err := p.Parse(`INSERT INTO docs (kdb_id, jobId, n) VALUES (?, ?, 0)
		ON CONFLICT (jobId) DO UPDATE SET n = n + 1, status = 'pending' REMOVE lastError, meta.x WHERE n < 5
		RETURNING kdb_id, _doc`)
	if err != nil {
		t.Fatal(err)
	}
	ins, ok := sql.ImmediateInsert(stmt)
	if !ok || ins.OnConflict == nil || ins.OnConflict.Update == nil {
		t.Fatalf("not an immediate upsert: %#v", stmt)
	}
	upd := ins.OnConflict.Update
	if len(ins.OnConflict.Columns) != 1 || ins.OnConflict.Columns[0] != "jobId" ||
		len(upd.Assignments) != 2 || len(upd.Removes) != 2 || upd.Where == nil || len(ins.Returning) != 2 {
		t.Fatalf("parsed %#v / %#v", ins, upd)
	}
	if c := sql.ConflictUpdate(ins); c.Table.Name != "docs" || len(c.Returning) != 2 || c.Limit == nil || *c.Limit != 1 {
		t.Fatalf("conflict update %#v", c)
	}

	stmt, err = p.Parse(`INSERT INTO docs (a, b) VALUES (1, 2) ON CONFLICT (b, a) DO NOTHING`)
	if err != nil {
		t.Fatal(err)
	}
	if ins, ok = sql.ImmediateInsert(stmt); !ok || ins.OnConflict.Update != nil || len(ins.OnConflict.Columns) != 2 {
		t.Fatalf("DO NOTHING parsed as %#v", ins)
	}
	// REMOVE alone, no SET.
	if _, err := p.Parse(`INSERT INTO docs (a) VALUES (1) ON CONFLICT (a) DO UPDATE REMOVE b`); err != nil {
		t.Fatal(err)
	}
	// RETURNING alone makes an INSERT immediate too; a plain INSERT is not.
	stmt, _ = p.Parse(`INSERT INTO docs (a) VALUES (1) RETURNING kdb_id`)
	if _, ok := sql.ImmediateInsert(stmt); !ok {
		t.Fatal("INSERT ... RETURNING is not immediate")
	}
	stmt, _ = p.Parse(`INSERT INTO docs (a) VALUES (1)`)
	if _, ok := sql.ImmediateInsert(stmt); ok {
		t.Fatal("a plain INSERT is immediate")
	}
	for _, bad := range []string{
		`INSERT INTO docs (a) VALUES (1) ON`,
		`INSERT INTO docs (a) VALUES (1) ON CONFLICT`,
		`INSERT INTO docs (a) VALUES (1) ON CONFLICT ()`,
		`INSERT INTO docs (a) VALUES (1) ON CONFLICT (a)`,
		`INSERT INTO docs (a) VALUES (1) ON CONFLICT (a) DO`,
		`INSERT INTO docs (a) VALUES (1) ON CONFLICT (a) DO SOMETHING`,
		`INSERT INTO docs (a) VALUES (1) ON CONFLICT (a) DO UPDATE`,
		`INSERT INTO docs (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET`,
		`INSERT INTO docs (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET b = 1 WHERE`,
		`INSERT INTO docs (a) VALUES (1) ON CONFLICT (a) DO NOTHING RETURNING`,
		`INSERT INTO docs (a) VALUES (1) RETURNING`,
	} {
		if _, err := p.Parse(bad); err == nil {
			t.Errorf("%s: parsed", bad)
		}
	}
}
