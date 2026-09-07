package sql_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/sql"
)

// No SQL text may panic the parser (kdb-spec-layer16 §3.4). Each of these used to, or plausibly
// could; every one must come back as an error (or a statement), never a crash.
func TestParserNeverPanicsOnMalformedInput(t *testing.T) {
	malformed := []string{
		"", " ", ";", "SELECT", "SELECT FROM t", "SELECT * FROM", "SELECT * FROM 1", "SELECT , FROM t",
		"SELECT a, FROM t", "SELECT a FROM t WHERE", "SELECT a FROM t WHERE a =", "SELECT a FROM t WHERE = 1",
		"SELECT a FROM t WHERE a = 'unterminated", "SELECT a FROM t WHERE a IN", "SELECT a FROM t WHERE a IN (",
		"SELECT a FROM t WHERE a IN ()", "SELECT a FROM t WHERE a BETWEEN 1", "SELECT a FROM t WHERE a BETWEEN 1 AND",
		"SELECT a FROM t WHERE a LIKE", "SELECT a FROM t WHERE a IS", "SELECT a FROM t WHERE a IS NOT",
		"SELECT a FROM t WHERE NOT", "SELECT a FROM t WHERE a NOT = 1", "SELECT a FROM t ORDER", "SELECT a FROM t ORDER BY",
		"SELECT a FROM t GROUP", "SELECT a FROM t GROUP BY", "SELECT a FROM t LIMIT", "SELECT a FROM t LIMIT x",
		"SELECT a FROM t LIMIT -1", "SELECT a FROM t OFFSET", "SELECT a FROM t trailing garbage", "SELECT a FROM t;;",
		"SELECT a AS FROM t", "SELECT COUNT( FROM t", "SELECT COUNT(*", "SELECT COUNT(*) FROM", "SELECT f(",
		"SELECT f(a,) FROM t", "SELECT a FROM t WHERE (a = 1", "SELECT a FROM t WHERE a = 1)", "SELECT a FROM t WHERE a = ?)",
		"SELECT a FROM t WHERE a = 1.2.3", "SELECT a FROM t WHERE a = -", "SELECT a FROM t WHERE a = [", "SELECT a FROM t WHERE a = [1,",
		"SELECT a FROM t WHERE a = [1,]", "SELECT a FROM t WHERE a = ['x']", "SELECT a.", "SELECT .a FROM t", "SELECT a..b FROM t",
		"INSERT", "INSERT INTO", "INSERT INTO t", "INSERT INTO t (", "INSERT INTO t () VALUES ()", "INSERT INTO t (a) VALUES",
		"INSERT INTO t (a) VALUES (", "INSERT INTO t (a) VALUES (1", "INSERT INTO t (a) VALUES (1),", "INSERT INTO t (a) VALUE (1)",
		"UPDATE", "UPDATE t", "UPDATE t SET", "UPDATE t SET a", "UPDATE t SET a =", "UPDATE t SET = 1", "UPDATE t SET a = 1,",
		"UPDATE t SET a = 1 WHERE", "DELETE", "DELETE t", "DELETE FROM", "DELETE FROM t WHERE", "DELETE FROM t WHERE a",
		"CREATE", "CREATE TABLE", "CREATE TABLE t", "CREATE TABLE t (", "CREATE TABLE t ()", "CREATE TABLE t (a)",
		"CREATE TABLE t (a FOO)", "CREATE TABLE t (a VARCHAR NOT)", "CREATE TABLE t (a VARCHAR, UNIQUE)", "CREATE TABLE t (a VARCHAR, UNIQUE ())",
		"CREATE TABLE t (a VARCHAR(", "CREATE INDEX", "CREATE INDEX i", "CREATE INDEX i ON", "CREATE INDEX i ON t", "CREATE INDEX i ON t (",
		"CREATE INDEX i ON t ()", "CREATE INDEX i ON t (a WEIGHT)", "CREATE INDEX i ON t (a) USING", "CREATE INDEX i ON t (a) USING FOO",
		"CREATE INDEX i ON t (a) WITH", "CREATE INDEX i ON t (a) WITH (", "CREATE INDEX i ON t (a) WITH (k)", "CREATE INDEX i ON t (a) WITH (k =)",
		"CREATE UNIQUE", "CREATE UNIQUE TABLE t (a INT)", "DROP", "DROP INDEX", "DROP INDEX i", "DROP INDEX i ON", "DROP TABLE t",
		"MATCH(a, b)", "SELECT MATCH() FROM t", "SELECT MATCH(1, 'q') FROM t", "SELECT SIMILARITY(a) FROM t", "SELECT SIMILARITY(a, 'x') FROM t",
		"SELECT FUSE(a) FROM t", "SELECT FUSE(a, b) FROM t", "SELECT FUSE(MATCH(a,'q'), SIMILARITY(b, [1]), 'nope') FROM t",
		"SELECT FUSE(MATCH(a,'q'), SIMILARITY(b, [1]), 1) FROM t", "\x00", "SELECT \xff FROM t", "SELECT a FROM t WHERE a = '\xff'",
		"SELECT ((((((((((a)))))))))) FROM t", strings.Repeat("(", 500) + "a" + strings.Repeat(")", 500),
		"SELECT * FROM t WHERE a = 99999999999999999999999", "SELECT * FROM t LIMIT 99999999999999999999",
	}
	p := sql.DefaultParser{}
	for _, q := range malformed {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("parser panicked on %q: %v", q, r)
				}
			}()
			_, _ = p.Parse(q)
		}()
	}
	// The clearly-broken ones must be errors, and specifically parse errors.
	for _, q := range malformed[:40] {
		_, err := p.Parse(q)
		if err == nil {
			t.Errorf("expected an error for %q", q)
			continue
		}
		var pe *sql.ParseError
		if !errors.As(err, &pe) {
			t.Errorf("%q: expected *sql.ParseError, got %T", q, err)
		}
	}
}

// Malformed statements that parse must still not panic when executed against a live runtime.
func TestExecuteNeverPanicsOnOddButParseableInput(t *testing.T) {
	f := newCorpusFixture(t, "app/parse-exec")
	for _, q := range []string{
		"SELECT title FROM tasks WHERE title = 5",
		"SELECT title FROM tasks WHERE 5 = title",
		"SELECT title FROM tasks WHERE 1 = 1",
		"SELECT title FROM tasks WHERE 'a' < 1",
		"SELECT title FROM tasks WHERE _doc > 1 ORDER BY _doc",
		"SELECT title FROM tasks WHERE tags.x.y.z = 1",
		"SELECT title FROM tasks WHERE title LIKE 5",
		"SELECT title FROM tasks WHERE ? LIKE ?",
		"SELECT title FROM tasks WHERE title IN (?, ?, ?)",
		"SELECT title FROM tasks WHERE title BETWEEN ? AND ?",
		"SELECT title FROM tasks ORDER BY ?",
		"SELECT title FROM tasks ORDER BY 1",
		"SELECT ?, 1, 'x', NULL, TRUE, [1,2] FROM tasks",
		"SELECT title FROM tasks GROUP BY title, title, title",
		"SELECT COUNT(*) FROM tasks GROUP BY nosuch.path",
		"SELECT ARRAY_LENGTH(_doc) FROM tasks",
		"SELECT ARRAY_CONTAINS(_doc, 1) FROM tasks",
		"SELECT ARRAY_CONTAINS(kdb_id, 'x') FROM tasks",
		"SELECT title FROM tasks WHERE kdb_id = 'not-a-uuid'",
		"SELECT COUNT(*) FROM tasks WHERE COUNT(*) = 1",
		"SELECT SUM(title), MIN(tags), MAX(_doc), AVG(kdb_id) FROM tasks",
	} {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("execute panicked on %q: %v", q, r)
				}
			}()
			_, _, _ = f.query(q)
		}()
	}
}

func TestParserAcceptsLayer16Surface(t *testing.T) {
	p := sql.DefaultParser{}
	for _, q := range []string{
		"SELECT DISTINCT a, b.c AS bc, COUNT(*) n FROM t alias WHERE (a = 1 OR b <> 2) AND c IS NOT NULL GROUP BY a, b.c ORDER BY n DESC, a LIMIT 5 OFFSET 2;",
		"SELECT * FROM t WHERE a NOT LIKE 'x%' AND b ILIKE '_y' AND c NOT IN (1, 2, ?) AND d NOT BETWEEN 1 AND 2 AND NOT e AND ARRAY_CONTAINS(tags, 'a', 'b')",
		"SELECT kdb_id, _doc, MATCH(tasks_text, 'deploy staging') AS score FROM tasks WHERE MATCH(tasks_text, 'deploy staging') ORDER BY score DESC LIMIT 20",
		"SELECT kdb_id, SIMILARITY(embedding, ?) AS score FROM tasks ORDER BY score DESC LIMIT 10",
		"SELECT kdb_id, SIMILARITY(embedding, [0.1, -0.2, 3, 1e-3]) AS score FROM tasks ORDER BY score DESC LIMIT 10",
		"SELECT kdb_id, FUSE(MATCH(tasks_text, ?), SIMILARITY(embedding, ?), 'rrf') AS score FROM tasks ORDER BY score DESC LIMIT 20",
		"SELECT FUSE(MATCH(tasks_text, ?), SIMILARITY(embedding, ?)) AS score FROM tasks ORDER BY score DESC",
		"UPDATE tasks SET status = 'done', meta.reviewed = true WHERE title = 'alpha'",
		"UPDATE tasks t SET t.status = ?, _doc = '{\"a\":1}' WHERE t.kdb_id = ?",
		"DELETE FROM tasks WHERE status = 'done'",
		"DELETE FROM tasks",
		"CREATE TABLE t (a VARCHAR NOT NULL UNIQUE, b INT UNIQUE NOT NULL, c DOUBLE, UNIQUE (a, b), UNIQUE (b, c))",
		"CREATE INDEX tasks_text ON tasks (title WEIGHT 3, description, tags WEIGHT 2, steps.text) USING FULLTEXT",
		"CREATE INDEX tasks_vec ON tasks (embedding) USING VECTOR WITH (dimensions = 768, metric = 'cosine', m = 16, ef_construction = 200, ef_search = 64)",
		"CREATE UNIQUE INDEX by_email ON users (email) USING HASH",
		"CREATE INDEX by_age ON users (age) USING BTREE",
		"CREATE INDEX by_age ON users (age)",
		"DROP INDEX by_age ON users",
		"select title from tasks where status in ('a') order by title asc",
	} {
		if _, err := p.Parse(q); err != nil {
			t.Errorf("parse %q: %v", q, err)
		}
	}
}

func TestParserStatementShapes(t *testing.T) {
	p := sql.DefaultParser{}
	stmt, err := p.Parse("CREATE UNIQUE INDEX tasks_text ON tasks (title WEIGHT 3, steps.text) USING FULLTEXT WITH (k = 'v', n = 2, w = word)")
	if err != nil {
		t.Fatal(err)
	}
	ci, ok := stmt.(sql.StmtCreateIndex)
	if !ok {
		t.Fatalf("%T", stmt)
	}
	if ci.Name != "tasks_text" || ci.Table != "tasks" || !ci.Unique || ci.Using != "FULLTEXT" {
		t.Fatalf("%+v", ci)
	}
	if len(ci.Fields) != 2 || ci.Fields[0] != (sql.IndexField{Path: "title", Weight: 3}) || ci.Fields[1] != (sql.IndexField{Path: "steps.text", Weight: 1}) {
		t.Fatalf("fields: %+v", ci.Fields)
	}
	if ci.With["k"] != "v" || ci.With["n"] != "2" || ci.With["w"] != "word" {
		t.Fatalf("with: %+v", ci.With)
	}
	stmt, err = p.Parse("DROP INDEX i ON t")
	if err != nil {
		t.Fatal(err)
	}
	if di, ok := stmt.(sql.StmtDropIndex); !ok || di.Name != "i" || di.Table != "t" {
		t.Fatalf("%+v", stmt)
	}
	stmt, err = p.Parse("CREATE TABLE t (a VARCHAR NOT NULL UNIQUE, b INT, UNIQUE (a, b))")
	if err != nil {
		t.Fatal(err)
	}
	ct := stmt.(sql.StmtCreateTable).DDL
	if !ct.Columns[0].Unique || !ct.Columns[0].Required || ct.Columns[1].Unique {
		t.Fatalf("columns: %+v", ct.Columns)
	}
	if len(ct.UniqueConstraints) != 1 || strings.Join(ct.UniqueConstraints[0], ",") != "a,b" {
		t.Fatalf("constraints: %+v", ct.UniqueConstraints)
	}
	stmt, err = p.Parse("UPDATE tasks t SET t.status = ?, meta.reviewed = TRUE WHERE t.title = 'x'")
	if err != nil {
		t.Fatal(err)
	}
	up := stmt.(sql.StmtUpdate).Update
	if up.Table.Alias != "t" || len(up.Assignments) != 2 || up.Assignments[0].Path != "t.status" || up.Assignments[1].Path != "meta.reviewed" || up.Where == nil {
		t.Fatalf("%+v", up)
	}
	if lit, ok := up.Assignments[1].Value.(sql.ExprLiteral); !ok || lit.Cell != (sql.CellBool{Value: true}) {
		t.Fatalf("TRUE literal: %+v", up.Assignments[1].Value)
	}
	stmt, err = p.Parse("SELECT FUSE(MATCH(tasks_text, ?), SIMILARITY(embedding, [0.5, 1]), 'weighted') AS s FROM t")
	if err != nil {
		t.Fatal(err)
	}
	proj := stmt.(sql.StmtSelect).Query.Projections[0].(sql.ProjExpression)
	fuse, ok := proj.Expr.(sql.ExprFuse)
	if !ok || fuse.Mode != "weighted" || proj.Alias != "s" {
		t.Fatalf("%+v", proj)
	}
	m := fuse.Arms[0].(sql.ExprMatch)
	if m.IndexOrField != "tasks_text" || m.Query != (sql.ExprParameter{Index: 0}) {
		t.Fatalf("%+v", m)
	}
	sim := fuse.Arms[1].(sql.ExprSimilarity)
	vec, err := sql.DecodeVector(sim.Vector.(sql.ExprLiteral).Cell)
	if err != nil || sim.Field != "embedding" || len(vec) != 2 || vec[0] != 0.5 || vec[1] != 1 {
		t.Fatalf("%+v %v %v", sim, vec, err)
	}
}

// Admission cost fingerprints must stay stable for existing shapes and cover the new nodes.
func TestShapeFingerprintCoversLayer16Nodes(t *testing.T) {
	p := sql.DefaultParser{}
	for _, tc := range []struct{ q, want string }{
		{"SELECT name FROM t WHERE age > 30", "select [name] from t where (age > ?)"},
		{"SELECT name FROM t WHERE age > 50 ORDER BY name DESC LIMIT 5", "select [name] from t where (age > ?) order[name desc] limited"},
		{"SELECT DISTINCT a FROM t WHERE a IN (1,2,3) AND b BETWEEN 1 AND 2", "select distinct [a] from t where ((a in[3]) and (b between ? and ?))"},
		{"SELECT a, COUNT(*) FROM t WHERE a ILIKE 'x' GROUP BY a", "select [a,count(*)] from t where (a ilike ?) group[a]"},
		{"SELECT FUSE(MATCH(idx, 'q'), SIMILARITY(v, [1])) AS s FROM t WHERE MATCH(idx, ?) ORDER BY s DESC", "select [fuse[rrf](match(idx,?),similarity(v,?))] from t where match(idx,?) order[s desc]"},
	} {
		stmt, err := p.Parse(tc.q)
		if err != nil {
			t.Fatal(err)
		}
		shape := sql.ShapeOfSelect(stmt.(sql.StmtSelect).Query)
		if shape.Fingerprint != tc.want {
			t.Errorf("%s\n got %q\nwant %q", tc.q, shape.Fingerprint, tc.want)
		}
	}
	stmt, _ := p.Parse("SELECT a, COUNT(*) FROM t GROUP BY a")
	if !sql.ShapeOfSelect(stmt.(sql.StmtSelect).Query).HasAggregate {
		t.Fatal("HasAggregate")
	}
}
