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
		// The original reproducer, and the reason this suite exists.
		{"select literal", "SELECT 1", `found "1"`},
		{"select literal with from", "SELECT 1 FROM players", `found "1"`},
		{"select string literal", "SELECT 'x' FROM players", `found "'x'"`},
		{"select nothing", "SELECT", "found end of statement"},
		{"select star from nothing", "SELECT * FROM", "found end of statement"},
		{"select from a literal", "SELECT a FROM 1", `found "1"`},
		{"trailing AS at end", "SELECT a AS", "found end of statement"},

		// These three land on "expected FROM" rather than an identifier error, because the
		// lexer has no notion of reserved words: `FROM` is a perfectly good identifier, so it
		// is consumed as the projection name (or the alias), and the failure surfaces one token
		// later when the real FROM clause turns out to be missing.
		//
		// That is a pre-existing limitation of the parser, not something this change
		// introduced, and it is a merely confusing message rather than a crash or a wrong
		// answer. Pinned here so it is visible and so a future fix for reserved words has a
		// test that will notice.
		{"select from nothing", "SELECT FROM", "expected FROM"},
		{"select comma dangling", "SELECT a, FROM players", "expected FROM"},
		{"trailing AS with no alias", "SELECT a AS FROM players", "expected FROM"},

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
	const query = "SELECT a, 1 FROM players"
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
	// The offending token is the "1" at index 10.
	if parseErr.Pos != strings.Index(query, "1") {
		t.Fatalf("ParseError.Pos = %d, want %d (the offending token)", parseErr.Pos, strings.Index(query, "1"))
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
