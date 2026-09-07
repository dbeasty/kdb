package server

import (
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/wire"
)

// TestSqlExecRoutesUpdateAndDeleteThroughDML is the routing guarantee (Component 71's wire side):
// handleSqlExec used to send only INSERT down the DML path, so an UPDATE or DELETE fell through
// to the read path and was rejected as "DML must be executed via ExecuteDML". Every statement
// that is neither a SELECT nor DDL is now buffered as document ops on the session.
func TestSqlExecRoutesUpdateAndDeleteThroughDML(t *testing.T) {
	rt := newTestRuntime(t)
	addr := listenFor(t, rt)
	c := dialRawWireClient(t, addr)
	c.handshake(t, wire.ClientSQL, "app/data")
	sess := c.sessionBegin(t, "app/data", "READ_COMMITTED")

	exec := func(sqlText string) wire.SqlResultMessage {
		t.Helper()
		r := c.sqlExec(t, "app/data", sess.SessionID, sqlText)
		if r.Error != nil {
			t.Fatalf("%s: %s", sqlText, *r.Error)
		}
		return r
	}
	commit := func() {
		t.Helper()
		reply := c.txCommit(t, "app/data", sess.SessionID)
		r, ok := reply.(wire.SqlResultMessage)
		if !ok || r.Error != nil {
			t.Fatalf("commit: %+v", reply)
		}
	}
	count := func(where string) string {
		t.Helper()
		r := exec("SELECT COUNT(*) AS n FROM t " + where)
		return r.Rows[0][0]
	}

	exec(`CREATE TABLE t (id VARCHAR NOT NULL, v VARCHAR NOT NULL)`)
	for i := 0; i < 3; i++ {
		exec(fmt.Sprintf(`INSERT INTO t (id, v) VALUES ('doc-%d', 'before')`, i))
	}
	commit()
	if got := count(""); got != "3" {
		t.Fatalf("rows after insert = %s, want 3", got)
	}

	// UPDATE is DML: buffered, not executed as a read, and reported as a write.
	updated := exec(`UPDATE t SET v = 'after' WHERE id = 'doc-1'`)
	if updated.ReadOnly {
		t.Fatal("UPDATE was reported as a read-only result")
	}
	if updated.RowsAffected != 1 {
		t.Fatalf("UPDATE rowsAffected = %d, want 1", updated.RowsAffected)
	}
	commit()
	if got := count(`WHERE v = 'after'`); got != "1" {
		t.Fatalf("rows with v='after' = %s, want 1", got)
	}

	deleted := exec(`DELETE FROM t WHERE id = 'doc-2'`)
	if deleted.ReadOnly {
		t.Fatal("DELETE was reported as a read-only result")
	}
	if deleted.RowsAffected != 1 {
		t.Fatalf("DELETE rowsAffected = %d, want 1", deleted.RowsAffected)
	}
	commit()
	if got := count(""); got != "2" {
		t.Fatalf("rows after delete = %s, want 2", got)
	}
}

// TestClassifyStatementSortsEveryStatementKind pins the routing table itself, including the
// deliberate default: an unrecognized statement kind is treated as DML, so a new mutating
// statement in package sql works over the wire without this switch having to learn its name.
func TestClassifyStatementSortsEveryStatementKind(t *testing.T) {
	parser := sql.DefaultParser{}
	for _, tc := range []struct {
		sqlText         string
		isSelect, isDDL bool
	}{
		{`SELECT * FROM t`, true, false},
		{`CREATE TABLE t (a VARCHAR NOT NULL)`, false, true},
		{`INSERT INTO t (a) VALUES ('x')`, false, false},
		{`UPDATE t SET a = 'y' WHERE a = 'x'`, false, false},
		{`DELETE FROM t WHERE a = 'x'`, false, false},
	} {
		stmt, err := parser.Parse(tc.sqlText)
		if err != nil {
			t.Fatalf("%s: %v", tc.sqlText, err)
		}
		isSelect, isDDL := classifyStatement(stmt)
		if isSelect != tc.isSelect || isDDL != tc.isDDL {
			t.Errorf("%s: select=%v ddl=%v, want select=%v ddl=%v", tc.sqlText, isSelect, isDDL, tc.isSelect, tc.isDDL)
		}
	}
}

// TestUpsertStoresANewBodyByteExact is the wire half of §9.4's round-trip promise: creating a
// document through UPSERT stores exactly the bytes supplied - no injected "id", no reordered
// keys. (Upserting over an existing document merges at the root level, which is a separate,
// deliberate behaviour - see KdbServerRuntime.Upsert.)
func TestUpsertStoresANewBodyByteExact(t *testing.T) {
	rt := newTestRuntime(t)
	addr := listenFor(t, rt)
	c := dialRawWireClient(t, addr)
	c.handshake(t, wire.ClientSQL, "app/data")

	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	const body = `{"zeta":1,"name":"Ada","alpha":{"b":2,"a":1}}`
	if r := c.upsert(t, "app/data", "", docID.String(), body); r.Error != nil {
		t.Fatalf("upsert: %s", *r.Error)
	}
	stored, _, found, err := rt.GetDocument("app/data", docID)
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if stored != body {
		t.Fatalf("stored body was rewritten:\n got %s\nwant %s", stored, body)
	}
}

// TestDecodeParametersJSONDecodesVectors covers the wire's vector parameter (§9.1): a JSON array
// of numbers becomes sql.ParamVector, so SIMILARITY works over SqlExec, and an array holding a
// non-number is a named error rather than a silently zeroed vector.
func TestDecodeParametersJSONDecodesVectors(t *testing.T) {
	raw := `[[0.5, -0.25, 1], "text", 3, true, null]`
	params, err := decodeParametersJSON(&raw)
	if err != nil {
		t.Fatal(err)
	}
	vec, ok := params[0].(sql.ParamVector)
	if !ok {
		t.Fatalf("parameter 0 is %T, want sql.ParamVector", params[0])
	}
	if len(vec.Value) != 3 || vec.Value[0] != 0.5 || vec.Value[1] != -0.25 || vec.Value[2] != 1 {
		t.Fatalf("vector = %v", vec.Value)
	}
	if _, ok := params[1].(sql.ParamString); !ok {
		t.Fatalf("parameter 1 is %T, want sql.ParamString", params[1])
	}
	bad := `[["a"]]`
	if _, err := decodeParametersJSON(&bad); err == nil {
		t.Fatal("expected an error for a vector parameter holding a non-number")
	}
}

// TestCellToStringCoversEveryCellKind pins the wire encoding of each result cell, including the
// boolean kind added with the predicate work - without a case of its own it would have gone out
// as Go's "{true}" struct rendering.
func TestCellToStringCoversEveryCellKind(t *testing.T) {
	for _, tc := range []struct {
		cell sql.Cell
		want string
	}{
		{sql.CellNull{}, ""},
		{sql.CellString{Value: "x"}, "x"},
		{sql.CellLong{Value: -7}, "-7"},
		{sql.CellDouble{Value: 1.5}, "1.5"},
		{sql.CellBool{Value: true}, "1"},
		{sql.CellBool{Value: false}, "0"},
		{sql.CellJSON{JSON: `{"a":1}`}, `{"a":1}`},
	} {
		if got := cellToString(tc.cell); got != tc.want {
			t.Errorf("cellToString(%T %+v) = %q, want %q", tc.cell, tc.cell, got, tc.want)
		}
	}
}
