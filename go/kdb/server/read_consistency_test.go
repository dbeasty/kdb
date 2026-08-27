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
