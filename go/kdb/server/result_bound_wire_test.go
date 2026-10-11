package server

import (
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/wire"
)

// The result bound over a real listener: what a client actually sees (U2 and U6 of
// docs/kdb-scan-cap-plan.md). A namespace larger than the bound must still answer selective
// reads, counts and DML correctly; only a result too large to return is refused, with a code a
// client can act on.

type boundFixture struct {
	t    *testing.T
	rt   *KdbServerRuntime
	c    *rawWireClient
	sess wire.SessionBeginAckMessage
	ns   string
}

func newBoundFixture(t *testing.T, docs, bound int) *boundFixture {
	t.Helper()
	rt := newTestRuntime(t)
	rt.SetMaxResultRows(bound)
	addr := listenFor(t, rt)
	c := dialRawWireClient(t, addr)
	c.handshake(t, wire.ClientSQL, "app/data")
	f := &boundFixture{t: t, rt: rt, c: c, ns: "app/data"}
	f.sess = c.sessionBegin(t, f.ns, "READ_COMMITTED")
	f.exec(`CREATE TABLE t (id VARCHAR NOT NULL, v VARCHAR NOT NULL)`)
	for i := 0; i < docs; i++ {
		f.exec(fmt.Sprintf(`INSERT INTO t (id, v) VALUES ('doc-%03d', 'before')`, i))
	}
	f.commit()
	return f
}

func (f *boundFixture) run(sqlText string) wire.SqlResultMessage {
	f.t.Helper()
	return f.c.sqlExec(f.t, f.ns, f.sess.SessionID, sqlText)
}

func (f *boundFixture) exec(sqlText string) wire.SqlResultMessage {
	f.t.Helper()
	r := f.run(sqlText)
	if r.Error != nil {
		f.t.Fatalf("%s: %s", sqlText, *r.Error)
	}
	return r
}

func (f *boundFixture) commit() {
	f.t.Helper()
	reply := f.c.txCommit(f.t, f.ns, f.sess.SessionID)
	if r, ok := reply.(wire.SqlResultMessage); !ok || r.Error != nil {
		f.t.Fatalf("commit: %+v", reply)
	}
}

func (f *boundFixture) count(where string) string {
	f.t.Helper()
	return f.exec("SELECT COUNT(*) AS n FROM t " + where).Rows[0][0]
}

func TestOverLargeResultIsResourceExhaustedOverTheWire(t *testing.T) {
	f := newBoundFixture(t, 20, 5)

	r := f.run(`SELECT id FROM t`)
	if r.Error == nil {
		t.Fatalf("20 rows against a bound of 5 returned %d rows instead of an error", len(r.Rows))
	}
	if r.ErrorCode == nil || *r.ErrorCode != wire.ErrorCodeResourceExhausted {
		t.Fatalf("want RESOURCE_EXHAUSTED, got code %v (%s)", r.ErrorCode, *r.Error)
	}
	if r.RetryAfterMs != nil {
		t.Error("an over-large result is not retryable; no retry hint should be sent")
	}

	// The same namespace still answers everything that fits.
	if got := f.count(""); got != "20" {
		t.Fatalf("COUNT(*) = %s, want 20 (the whole namespace, not a prefix of it)", got)
	}
	if got := f.exec(`SELECT id FROM t WHERE id = 'doc-019'`).Rows; len(got) != 1 {
		t.Fatalf("a document past the bound must be found by a selective query, got %v", got)
	}
	if got := f.exec(`SELECT id FROM t LIMIT 3`).Rows; len(got) != 3 {
		t.Fatalf("LIMIT within the bound returned %d rows", len(got))
	}
}

func TestMaxResultRowsIsConfigurable(t *testing.T) {
	f := newBoundFixture(t, 20, 5)
	if r := f.run(`SELECT id FROM t`); r.Error == nil {
		t.Fatal("expected the bound of 5 to refuse a 20-row result")
	}

	f.rt.SetMaxResultRows(20)
	if r := f.exec(`SELECT id FROM t`); len(r.Rows) != 20 {
		t.Fatalf("with the bound at 20, got %d rows", len(r.Rows))
	}

	f.rt.SetMaxResultRows(0) // restores the default
	if got := f.rt.MaxResultRows(); got != DefaultMaxResultRows {
		t.Fatalf("SetMaxResultRows(0) should restore the default, got %d", got)
	}
	if r := f.exec(`SELECT id FROM t`); len(r.Rows) != 20 {
		t.Fatalf("with the default bound, got %d rows", len(r.Rows))
	}
}

// UPDATE and DELETE resolve their targets by scanning, so they were exposed to the same
// truncation: a statement could report success having acted on nothing.
func TestDMLActsPastTheResultBound(t *testing.T) {
	f := newBoundFixture(t, 20, 5)

	updated := f.exec(`UPDATE t SET v = 'after' WHERE id = 'doc-019'`)
	if updated.RowsAffected != 1 {
		t.Fatalf("UPDATE of a document past the bound affected %d rows, want 1", updated.RowsAffected)
	}
	f.commit()
	if got := f.count(`WHERE v = 'after'`); got != "1" {
		t.Fatalf("rows updated = %s, want 1", got)
	}

	// A DML predicate may match more rows than a reply could carry: nothing is returned.
	many := f.exec(`UPDATE t SET v = 'bulk' WHERE id >= 'doc-000'`)
	if many.RowsAffected != 20 {
		t.Fatalf("a 20-row UPDATE affected %d rows, want 20", many.RowsAffected)
	}
	f.commit()
	if got := f.count(`WHERE v = 'bulk'`); got != "20" {
		t.Fatalf("rows updated in bulk = %s, want 20", got)
	}

	deleted := f.exec(`DELETE FROM t WHERE id = 'doc-019'`)
	if deleted.RowsAffected != 1 {
		t.Fatalf("DELETE of a document past the bound affected %d rows, want 1", deleted.RowsAffected)
	}
	f.commit()
	if got := f.count(""); got != "19" {
		t.Fatalf("rows after deleting one = %s, want 19", got)
	}

	all := f.exec(`DELETE FROM t WHERE id >= 'doc-000'`)
	if all.RowsAffected != 19 {
		t.Fatalf("a 19-row DELETE affected %d rows, want 19", all.RowsAffected)
	}
	f.commit()
	if got := f.count(""); got != "0" {
		t.Fatalf("rows after deleting all = %s, want 0", got)
	}
}

// DELETE by the reserved document id is what an application without a delete operation sends.
func TestDeleteByDocumentIDPastTheBound(t *testing.T) {
	f := newBoundFixture(t, 20, 5)
	ids := f.rt.rowsOfIDs(t, f)
	last := ids[len(ids)-1]

	if got := f.exec(fmt.Sprintf(`DELETE FROM t WHERE kdb_id = '%s'`, last)).RowsAffected; got != 1 {
		t.Fatalf("DELETE by kdb_id affected %d rows, want 1", got)
	}
	f.commit()
	if got := f.count(fmt.Sprintf(`WHERE kdb_id = '%s'`, last)); got != "0" {
		t.Fatalf("the document is still there after a reported delete (count %s)", got)
	}
}

// ids of every document, read without exceeding the bound.
func (rt *KdbServerRuntime) rowsOfIDs(t *testing.T, f *boundFixture) []string {
	t.Helper()
	var ids []string
	for off := 0; ; off += 5 {
		r := f.exec(fmt.Sprintf(`SELECT kdb_id FROM t ORDER BY kdb_id LIMIT 5 OFFSET %d`, off))
		for _, row := range r.Rows {
			ids = append(ids, row[0])
		}
		if len(r.Rows) < 5 {
			return ids
		}
	}
}
