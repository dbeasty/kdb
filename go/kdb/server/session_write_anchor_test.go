package server

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/wire"
)

// commitOK fails the test unless the commit succeeded, naming what came back instead. A refused
// commit arrives as a ConflictReportMessage rather than an error, so a test that only checked
// for a SqlResultMessage would pass on a conflict it never looked at.
func commitOK(t *testing.T, reply wire.Message, context string) {
	t.Helper()
	switch m := reply.(type) {
	case wire.SqlResultMessage:
		if m.Error != nil {
			t.Fatalf("%s: commit failed: %s", context, *m.Error)
		}
	case wire.ConflictReportMessage:
		t.Fatalf("%s: commit reported a conflict", context)
	default:
		t.Fatalf("%s: unexpected commit reply %T", context, reply)
	}
}

func commitConflicted(t *testing.T, reply wire.Message, context string) {
	t.Helper()
	if _, ok := reply.(wire.ConflictReportMessage); !ok {
		t.Fatalf("%s: expected a conflict, got %T", context, reply)
	}
}

// TestUpsertThenDmlOnSameSessionCommits is the reported bug. An Upsert commits outside any
// session, advancing the DAG head; the session's write base was frozen at session begin, so the
// very next DML statement on that same session committed against a version older than the one it
// had just read, and the engine reported the client's own write back to it as a conflict.
//
// This is not an edge case for an application server: a pooled connection interleaving document
// writes with SQL is the normal path, not an unusual one.
func TestUpsertThenDmlOnSameSessionCommits(t *testing.T) {
	rt := newTestRuntimeWithSchema(t, schema.None())
	addr := listenFor(t, rt)

	c := dialRawWireClient(t, addr)
	c.handshake(t, wire.ClientSQL, "app/data")
	sess := c.sessionBegin(t, "app/data", "READ_COMMITTED")

	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	up := c.upsert(t, "app/data", sess.SessionID, docID.String(), `{"title":"fresh doc","status":"todo"}`)
	if up.Error != nil {
		t.Fatalf("upsert failed: %s", *up.Error)
	}

	res := c.sqlExec(t, "app/data", sess.SessionID, `UPDATE data SET status = 'blocked' WHERE title = 'fresh doc'`)
	if res.Error != nil {
		t.Fatalf("UPDATE failed: %s", *res.Error)
	}
	if res.RowsAffected != 1 {
		t.Fatalf("expected 1 row affected, got %d", res.RowsAffected)
	}
	commitOK(t, c.txCommit(t, "app/data", sess.SessionID), "upsert then UPDATE")

	after := c.sqlExec(t, "app/data", sess.SessionID, `SELECT status FROM data WHERE title = 'fresh doc'`)
	if len(after.Rows) != 1 || after.Rows[0][0] != "blocked" {
		t.Fatalf("expected the UPDATE to be visible, got rows %v", after.Rows)
	}
}

// TestAnotherConnectionsCommitBeforeFirstDmlDoesNotConflict is the general form of the same
// defect, and it does not need Upsert to reproduce: any commit landing between session begin and
// this session's first buffered write left the session anchored behind the head its own reads
// were resolving at.
func TestAnotherConnectionsCommitBeforeFirstDmlDoesNotConflict(t *testing.T) {
	rt := newTestRuntimeWithSchema(t, schema.None())
	addr := listenFor(t, rt)

	reader := dialRawWireClient(t, addr)
	reader.handshake(t, wire.ClientSQL, "app/data")
	readerSess := reader.sessionBegin(t, "app/data", "READ_COMMITTED")

	// A second connection writes a document after the first session was opened.
	other := dialRawWireClient(t, addr)
	other.handshake(t, wire.ClientSQL, "app/data")
	otherSess := other.sessionBegin(t, "app/data", "READ_COMMITTED")
	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	if up := other.upsert(t, "app/data", otherSess.SessionID, docID.String(), `{"title":"shared","status":"theirs"}`); up.Error != nil {
		t.Fatalf("second connection's upsert failed: %s", *up.Error)
	}

	// The first session now updates that same document. Under READ_COMMITTED its statement reads
	// at the live head, so it sees their value and writes on top of it - which is the ordinary,
	// correct outcome, not a conflict. It only looked like one because the write anchored behind
	// the read.
	res := reader.sqlExec(t, "app/data", readerSess.SessionID, `UPDATE data SET status = 'mine' WHERE title = 'shared'`)
	if res.Error != nil {
		t.Fatalf("UPDATE failed: %s", *res.Error)
	}
	if res.RowsAffected != 1 {
		t.Fatalf("expected the update to see the other connection's document, got %d rows", res.RowsAffected)
	}
	commitOK(t, reader.txCommit(t, "app/data", readerSess.SessionID), "update after another connection committed")
}

// TestConcurrentWriteMidTransactionStillConflicts guards the fix from going too far. Re-anchoring
// happens only when a transaction opens; once it is open, a writer arriving underneath it must
// still be caught. Without this, "fixing" the false conflict would have quietly removed the
// optimistic concurrency the base version exists to provide.
func TestConcurrentWriteMidTransactionStillConflicts(t *testing.T) {
	rt := newTestRuntimeWithSchema(t, schema.None())
	addr := listenFor(t, rt)

	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}

	seed := dialRawWireClient(t, addr)
	seed.handshake(t, wire.ClientSQL, "app/data")
	seedSess := seed.sessionBegin(t, "app/data", "READ_COMMITTED")
	if up := seed.upsert(t, "app/data", seedSess.SessionID, docID.String(), `{"title":"contested","status":"todo"}`); up.Error != nil {
		t.Fatalf("seed upsert failed: %s", *up.Error)
	}

	// A opens its transaction by buffering a write to the contested document.
	a := dialRawWireClient(t, addr)
	a.handshake(t, wire.ClientSQL, "app/data")
	aSess := a.sessionBegin(t, "app/data", "READ_COMMITTED")
	if res := a.sqlExec(t, "app/data", aSess.SessionID, `UPDATE data SET status = 'a-won' WHERE title = 'contested'`); res.Error != nil {
		t.Fatalf("A's UPDATE failed: %s", *res.Error)
	}

	// B writes and commits the same document while A's transaction is open.
	b := dialRawWireClient(t, addr)
	b.handshake(t, wire.ClientSQL, "app/data")
	bSess := b.sessionBegin(t, "app/data", "READ_COMMITTED")
	if up := b.upsert(t, "app/data", bSess.SessionID, docID.String(), `{"title":"contested","status":"b-won"}`); up.Error != nil {
		t.Fatalf("B's upsert failed: %s", *up.Error)
	}

	commitConflicted(t, a.txCommit(t, "app/data", aSess.SessionID), "A committing over B's write")
}

// TestSnapshotSessionStillConflictsOnStaleBase pins the other half of the boundary: a SNAPSHOT
// session reads at the pin taken when its transaction started, so its writes must stay anchored
// there too. Re-anchoring it to the live head would let it commit over a change it cannot see,
// which is exactly what snapshot isolation exists to prevent.
func TestSnapshotSessionStillConflictsOnStaleBase(t *testing.T) {
	rt := newTestRuntimeWithSchema(t, schema.None())
	addr := listenFor(t, rt)

	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}

	seed := dialRawWireClient(t, addr)
	seed.handshake(t, wire.ClientSQL, "app/data")
	seedSess := seed.sessionBegin(t, "app/data", "READ_COMMITTED")
	if up := seed.upsert(t, "app/data", seedSess.SessionID, docID.String(), `{"title":"snap","status":"todo"}`); up.Error != nil {
		t.Fatalf("seed upsert failed: %s", *up.Error)
	}

	// The snapshot session opens here, pinned to the state above.
	snap := dialRawWireClient(t, addr)
	snap.handshake(t, wire.ClientSQL, "app/data")
	snapSess := snap.sessionBegin(t, "app/data", "SNAPSHOT")

	// Someone else moves the document on after the pin was taken.
	if up := seed.upsert(t, "app/data", seedSess.SessionID, docID.String(), `{"title":"snap","status":"moved-on"}`); up.Error != nil {
		t.Fatalf("competing upsert failed: %s", *up.Error)
	}

	if res := snap.sqlExec(t, "app/data", snapSess.SessionID, `UPDATE data SET status = 'from-snapshot' WHERE title = 'snap'`); res.Error != nil {
		t.Fatalf("snapshot UPDATE failed: %s", *res.Error)
	}
	commitConflicted(t, snap.txCommit(t, "app/data", snapSess.SessionID), "snapshot session committing over a change it cannot see")
}

