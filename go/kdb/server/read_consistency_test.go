package server

import (
	"fmt"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/wire"
)

// TestSnapshotSessionStaysPinned is the regression test for the finding recorded in
// docs/kdb-finish-up-plan.md as 1-G8: SessionManager.Begin computed sess.ReadPin for SNAPSHOT
// consistency, but execRead always read at the live DAG head regardless - a SNAPSHOT session's
// SessionBeginAck claimed SNAPSHOT while every read it made silently behaved as READ_COMMITTED,
// seeing writes made after the session began. Proves, over a real TCP socket: a write committed
// after a SNAPSHOT session begins is invisible to that session, but visible to a fresh
// READ_COMMITTED session started afterward.
func TestSnapshotSessionStaysPinned(t *testing.T) {
	rt := newTestRuntime(t)
	ln, err := ListenSqlWire("tcp://127.0.0.1:0?bind=true", rt)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	addr := fmt.Sprintf("tcp://%s", ln.Addr().String())

	setup := dialRawWireClient(t, addr)
	setup.handshake(t, wire.ClientSQL, "app/data")
	setupSess := setup.sessionBegin(t, "app/data", "READ_COMMITTED")
	if r := setup.sqlExec(t, "app/data", setupSess.SessionID, `INSERT INTO t (_doc) VALUES ('{"marker":"before-snapshot"}')`); r.Error != nil {
		t.Fatalf("insert before-snapshot: %s", *r.Error)
	}
	mustCommit(t, setup, setupSess.SessionID)

	snap := dialRawWireClient(t, addr)
	snap.handshake(t, wire.ClientSQL, "app/data")
	snapSess := snap.sessionBegin(t, "app/data", "SNAPSHOT")

	writer := dialRawWireClient(t, addr)
	writer.handshake(t, wire.ClientSQL, "app/data")
	writerSess := writer.sessionBegin(t, "app/data", "READ_COMMITTED")
	if r := writer.sqlExec(t, "app/data", writerSess.SessionID, `INSERT INTO t (_doc) VALUES ('{"marker":"after-snapshot"}')`); r.Error != nil {
		t.Fatalf("insert after-snapshot: %s", *r.Error)
	}
	mustCommit(t, writer, writerSess.SessionID)

	snapResult := snap.sqlExec(t, "app/data", snapSess.SessionID, `SELECT _doc FROM t`)
	if snapResult.Error != nil {
		t.Fatalf("snapshot select: %s", *snapResult.Error)
	}
	snapDocs := flattenRows(snapResult)
	if !containsSubstring(snapDocs, "before-snapshot") {
		t.Fatalf("expected the snapshot session to see its pre-existing write, got %v", snapDocs)
	}
	if containsSubstring(snapDocs, "after-snapshot") {
		t.Fatalf("expected the snapshot session to NOT see a write committed after it began, got %v", snapDocs)
	}

	fresh := dialRawWireClient(t, addr)
	fresh.handshake(t, wire.ClientSQL, "app/data")
	freshSess := fresh.sessionBegin(t, "app/data", "READ_COMMITTED")
	freshResult := fresh.sqlExec(t, "app/data", freshSess.SessionID, `SELECT _doc FROM t`)
	if freshResult.Error != nil {
		t.Fatalf("fresh select: %s", *freshResult.Error)
	}
	freshDocs := flattenRows(freshResult)
	if !containsSubstring(freshDocs, "before-snapshot") || !containsSubstring(freshDocs, "after-snapshot") {
		t.Fatalf("expected a fresh READ_COMMITTED session to see both writes, got %v", freshDocs)
	}
}

func mustCommit(t *testing.T, c *rawWireClient, sessionID string) {
	t.Helper()
	reply := c.txCommit(t, "app/data", sessionID)
	result, ok := reply.(wire.SqlResultMessage)
	if !ok {
		t.Fatalf("expected SqlResultMessage from commit, got %T", reply)
	}
	if result.Error != nil {
		t.Fatalf("commit: %s", *result.Error)
	}
}

func flattenRows(r wire.SqlResultMessage) []string {
	var out []string
	for _, row := range r.Rows {
		out = append(out, row...)
	}
	return out
}

func containsSubstring(rows []string, needle string) bool {
	for _, row := range rows {
		if strings.Contains(row, needle) {
			return true
		}
	}
	return false
}

// TestSnapshotSessionSeesOwnCommittedWrites is the regression test for the other half of the
// SNAPSHOT bug. Fixing 1-G8 made execRead honour sess.ReadPin, but the pin was taken once at
// SessionManager.Begin and never moved again - so a SNAPSHOT session that committed a write
// went on reading at the version it opened with and could not see its own write. Measured
// before the fix: the SELECT below returned zero rows after a successful INSERT + commit.
//
// Snapshot isolation pins a *transaction*, not a session: the pin is re-taken at every
// transaction boundary, so a session observes everything it has itself committed.
func TestSnapshotSessionSeesOwnCommittedWrites(t *testing.T) {
	rt := newTestRuntime(t)
	ln, err := ListenSqlWire("tcp://127.0.0.1:0?bind=true", rt)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	addr := fmt.Sprintf("tcp://%s", ln.Addr().String())

	c := dialRawWireClient(t, addr)
	c.handshake(t, wire.ClientSQL, "app/data")
	sess := c.sessionBegin(t, "app/data", "SNAPSHOT")

	if r := c.sqlExec(t, "app/data", sess.SessionID, `INSERT INTO t (_doc) VALUES ('{"marker":"my-own-write"}')`); r.Error != nil {
		t.Fatalf("insert: %s", *r.Error)
	}
	mustCommit(t, c, sess.SessionID)

	res := c.sqlExec(t, "app/data", sess.SessionID, `SELECT _doc FROM t`)
	if res.Error != nil {
		t.Fatalf("select: %s", *res.Error)
	}
	if docs := flattenRows(res); !containsSubstring(docs, "my-own-write") {
		t.Fatalf("a SNAPSHOT session must see the write it committed itself, got %v", docs)
	}
}

// TestSnapshotSessionRepeatableReadAcrossStatements is the multi-statement guarantee: two
// SELECTs in the same transaction must return the same rows even though another session
// committed in between. Without a pin held for the whole transaction the second SELECT would
// pick up the interleaved write, which is a non-repeatable read.
func TestSnapshotSessionRepeatableReadAcrossStatements(t *testing.T) {
	rt := newTestRuntime(t)
	ln, err := ListenSqlWire("tcp://127.0.0.1:0?bind=true", rt)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	addr := fmt.Sprintf("tcp://%s", ln.Addr().String())

	seed := dialRawWireClient(t, addr)
	seed.handshake(t, wire.ClientSQL, "app/data")
	seedSess := seed.sessionBegin(t, "app/data", "READ_COMMITTED")
	if r := seed.sqlExec(t, "app/data", seedSess.SessionID, `INSERT INTO t (_doc) VALUES ('{"marker":"seeded"}')`); r.Error != nil {
		t.Fatalf("seed insert: %s", *r.Error)
	}
	mustCommit(t, seed, seedSess.SessionID)

	reader := dialRawWireClient(t, addr)
	reader.handshake(t, wire.ClientSQL, "app/data")
	readerSess := reader.sessionBegin(t, "app/data", "SNAPSHOT")

	first := reader.sqlExec(t, "app/data", readerSess.SessionID, `SELECT _doc FROM t`)
	if first.Error != nil {
		t.Fatalf("first select: %s", *first.Error)
	}
	firstRows := flattenRows(first)

	// A different session commits between the reader's two statements.
	writer := dialRawWireClient(t, addr)
	writer.handshake(t, wire.ClientSQL, "app/data")
	writerSess := writer.sessionBegin(t, "app/data", "READ_COMMITTED")
	if r := writer.sqlExec(t, "app/data", writerSess.SessionID, `INSERT INTO t (_doc) VALUES ('{"marker":"interleaved"}')`); r.Error != nil {
		t.Fatalf("interleaved insert: %s", *r.Error)
	}
	mustCommit(t, writer, writerSess.SessionID)

	second := reader.sqlExec(t, "app/data", readerSess.SessionID, `SELECT _doc FROM t`)
	if second.Error != nil {
		t.Fatalf("second select: %s", *second.Error)
	}
	secondRows := flattenRows(second)

	if containsSubstring(secondRows, "interleaved") {
		t.Fatalf("non-repeatable read: the second SELECT in the same transaction saw a write "+
			"committed after it began, got %v", secondRows)
	}
	if len(firstRows) != len(secondRows) {
		t.Fatalf("two SELECTs in one transaction disagreed: first %v, second %v", firstRows, secondRows)
	}
	if !containsSubstring(secondRows, "seeded") {
		t.Fatalf("expected the pre-existing row to remain visible, got %v", secondRows)
	}
}
