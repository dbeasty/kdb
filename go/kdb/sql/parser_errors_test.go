package sql

import (
	"errors"
	"strings"
	"testing"
)

// TestParseRejectsMalformedInputWithoutPanicking is the regression suite for the parser's one
// panicking path.
//
// `readIdentifier` used to panic instead of returning an error, and because nothing on the
// server's frame-handling path recovered, `SELECT 1` - a projection that is a literal rather
// than an identifier, and a standard connectivity probe - killed the whole server process,
// taking every other connection and namespace with it.
//
// Every case below reached that panic, or one of the two numeric conversions that silently
// discarded their error. The assertion is deliberately broad: a *ParseError, no panic, and a
// message that names what was actually found rather than only what was wanted.
func TestParseRejectsMalformedInputWithoutPanicking(t *testing.T) {
	cases := []struct {
		name    string
		sql     string
		wantMsg string
	}{
		// `SELECT 1` itself is now a supported statement - see
		// TestParseAcceptsTablelessSelect. What remains invalid is everything genuinely
		// malformed.
		{"select nothing", "SELECT", "found end of statement"},
		{"select star from nothing", "SELECT * FROM", "found end of statement"},
		{"select from a literal", "SELECT a FROM 1", `found "1"`},
		{"trailing AS at end", "SELECT a AS", "found end of statement"},

		// A clause keyword is not an identifier. Before reserved words existed, `FROM` was read
		// as a perfectly good column name; that was merely confusing while FROM was mandatory
		// (the failure surfaced one token later as "expected FROM"), but became genuinely
		// wrong once FROM turned optional, since there was then no later keyword check to fail.
		{"select from nothing", "SELECT FROM", `reserved word "FROM"`},
		{"select comma dangling", "SELECT a, FROM players", `reserved word "FROM"`},
		{"trailing AS with no alias", "SELECT a AS FROM players", `reserved word "FROM"`},
		{"reserved word as a column name", "CREATE TABLE t (from VARCHAR)", `reserved word "from"`},
		{"reserved word as a table name", "SELECT a FROM select", `reserved word "select"`},

		{"insert into nothing", "INSERT INTO", "found end of statement"},
		{"insert into a literal", "INSERT INTO 5 (a) VALUES (1)", `found "5"`},
		{"insert column list dangling", "INSERT INTO t (a, ) VALUES (1)", `found ")"`},
		{"insert no column list", "INSERT INTO t VALUES (1)", "expected '('"},

		{"create table nothing", "CREATE TABLE", "found end of statement"},
		{"create table literal name", "CREATE TABLE 7 (a VARCHAR)", `found "7"`},
		{"create table no column name", "CREATE TABLE t (VARCHAR)", "expected"},
		{"create table dangling column", "CREATE TABLE t (a VARCHAR, )", `found ")"`},
		{"create table missing type", "CREATE TABLE t (a)", `found ")"`},

		{"empty statement", "", "expected SELECT, INSERT, or CREATE TABLE"},
		{"whitespace only", "   \t\n ", "expected SELECT, INSERT, or CREATE TABLE"},
		{"not a statement", "DROP TABLE players", "expected SELECT, INSERT, or CREATE TABLE"},
		{"create without table", "CREATE INDEX x", "expected TABLE"},

		// The two silently-discarded strconv errors: readNumber accepts these, strconv does
		// not, and the value used to become 0 rather than an error.
		{"malformed float", "SELECT a FROM t WHERE b = 1.2.3", "invalid numeric literal"},
		{"integer past int64", "SELECT a FROM t WHERE b = 99999999999999999999", "invalid integer literal"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// A panic here fails the test rather than taking the test binary down with it,
			// which is the whole point of the change under test.
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Parse(%q) panicked: %v", tc.sql, r)
				}
			}()

			stmt, err := DefaultParser{}.Parse(tc.sql)
			if err == nil {
				t.Fatalf("Parse(%q) unexpectedly succeeded: %#v", tc.sql, stmt)
			}
			if stmt != nil {
				t.Fatalf("Parse(%q) returned both a statement and an error", tc.sql)
			}

			var parseErr *ParseError
			if !errors.As(err, &parseErr) {
				t.Fatalf("Parse(%q) returned %T, want *ParseError: %v", tc.sql, err, err)
			}
			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Fatalf("Parse(%q) error = %q, want it to contain %q", tc.sql, err.Error(), tc.wantMsg)
			}
		})
	}
}

// TestParseErrorCarriesPosition checks the part of ParseError a caller can act on: where in the
// statement the problem is. Without it "expected identifier" is untraceable in a long query.
func TestParseErrorCarriesPosition(t *testing.T) {
	const query = "SELECT a, FROM players"
	_, err := DefaultParser{}.Parse(query)
	if err == nil {
		t.Fatal("expected a parse error")
	}
	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("got %T, want *ParseError", err)
	}
	if parseErr.SQL != query {
		t.Fatalf("ParseError.SQL = %q, want the original statement", parseErr.SQL)
	}
	// Points at the offending FROM, not past it - readIdentifier rewinds before reporting a
	// reserved word precisely so the position stays useful.
	if parseErr.Pos != strings.Index(query, "FROM") {
		t.Fatalf("ParseError.Pos = %d, want %d (the offending token)", parseErr.Pos, strings.Index(query, "FROM"))
	}
}

// TestParseAcceptsTablelessSelect covers the FROM-less form: `SELECT 1` is what SQL tools and
// connection pools send to check a connection is alive, and it is the statement that used to
// panic this parser and kill the server.
func TestParseAcceptsTablelessSelect(t *testing.T) {
	cases := []struct {
		sql       string
		wantAlias string
	}{
		{"SELECT 1", ""},
		{"SELECT 1 AS one", "one"},
		{"SELECT 'ok'", ""},
		{"SELECT ?", ""},
		{"SELECT 1.5", ""},
		{"SELECT NULL", ""},
	}

	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := DefaultParser{}.Parse(tc.sql)
			if err != nil {
				t.Fatalf("Parse(%q) failed: %v", tc.sql, err)
			}
			sel, ok := stmt.(StmtSelect)
			if !ok {
				t.Fatalf("got %T, want StmtSelect", stmt)
			}
			if sel.Query.HasFrom() {
				t.Fatalf("Parse(%q) invented a FROM clause: %+v", tc.sql, sel.Query.From)
			}
			if len(sel.Query.Projections) != 1 {
				t.Fatalf("want one projection, got %d", len(sel.Query.Projections))
			}
			proj, ok := sel.Query.Projections[0].(ProjExpression)
			if !ok {
				t.Fatalf("got %T, want ProjExpression", sel.Query.Projections[0])
			}
			if proj.Alias != tc.wantAlias {
				t.Fatalf("alias = %q, want %q", proj.Alias, tc.wantAlias)
			}
		})
	}
}

// TestParseLiteralProjectionWithTable checks the other half of making projections general
// expressions: a literal alongside a real table is now a valid projection too.
func TestParseLiteralProjectionWithTable(t *testing.T) {
	stmt, err := DefaultParser{}.Parse("SELECT 1 AS n, name FROM players")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel := stmt.(StmtSelect)
	if !sel.Query.HasFrom() || sel.Query.From.Name != "players" {
		t.Fatalf("From = %+v, want players", sel.Query.From)
	}
	if len(sel.Query.Projections) != 2 {
		t.Fatalf("want two projections, got %d", len(sel.Query.Projections))
	}
	if _, ok := sel.Query.Projections[0].(ProjExpression); !ok {
		t.Fatalf("first projection is %T, want ProjExpression", sel.Query.Projections[0])
	}
	// A bare column reference must still be a ProjColumn, so the planner can check it against
	// the schema and columnsFor can report the field's real SQL type.
	col, ok := sel.Query.Projections[1].(ProjColumn)
	if !ok {
		t.Fatalf("second projection is %T, want ProjColumn", sel.Query.Projections[1])
	}
	if col.Name != "name" {
		t.Fatalf("column = %q, want name", col.Name)
	}
}

// TestParseStillAcceptsValidStatements guards against the error threading having broken a
// success path - every one of these flows through a readIdentifier call site that changed.
func TestParseStillAcceptsValidStatements(t *testing.T) {
	cases := []string{
		"SELECT * FROM players",
		"SELECT name, level FROM players",
		"SELECT name AS who FROM players",
		"SELECT DISTINCT name FROM players",
		"SELECT COUNT(*) FROM players",
		"SELECT COUNT(*) AS total FROM players",
		"SELECT name FROM players WHERE level = '7'",
		"SELECT name FROM players WHERE level = ? ORDER BY name DESC LIMIT 10",
		"SELECT name FROM players WHERE score = 1.5",
		"SELECT name FROM players WHERE score = 9223372036854775807",
		"INSERT INTO players (name, level) VALUES ('Alice', '7')",
		"INSERT INTO players (name) VALUES ('Bob'), ('Carol')",
		"CREATE TABLE players (name VARCHAR NOT NULL, level VARCHAR)",
		"CREATE TABLE players (name VARCHAR(64))",
	}

	for _, sql := range cases {
		t.Run(sql, func(t *testing.T) {
			stmt, err := DefaultParser{}.Parse(sql)
			if err != nil {
				t.Fatalf("Parse(%q) failed: %v", sql, err)
			}
			if stmt == nil {
				t.Fatalf("Parse(%q) returned no statement and no error", sql)
			}
		})
	}
}

// TestParseNumericLiteralsRoundTrip pins the values the two fixed conversions produce, since a
// discarded error previously turned an unparseable literal into 0 - a query that ran and
// returned the wrong rows rather than one that failed.
func TestParseNumericLiteralsRoundTrip(t *testing.T) {
	stmt, err := DefaultParser{}.Parse("SELECT a FROM t WHERE b = 42")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel, ok := stmt.(StmtSelect)
	if !ok {
		t.Fatalf("got %T, want StmtSelect", stmt)
	}
	cmp, ok := sel.Query.Where.(ExprBinary)
	if !ok {
		t.Fatalf("got %T for WHERE, want ExprBinary", sel.Query.Where)
	}
	lit, ok := cmp.Right.(ExprLiteral)
	if !ok {
		t.Fatalf("got %T for the right operand, want ExprLiteral", cmp.Right)
	}
	long, ok := lit.Cell.(CellLong)
	if !ok {
		t.Fatalf("got %T for the literal cell, want CellLong", lit.Cell)
	}
	if long.Value != 42 {
		t.Fatalf("literal = %d, want 42", long.Value)
	}
}
