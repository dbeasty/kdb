package server

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

func newTestRuntime(t *testing.T) *KdbServerRuntime {
	t.Helper()
	rt, err := embed.OpenMemoryRuntime("demo", "app/data", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	return NewKdbServerRuntime(rt)
}

// dialRawWireClient is a bare wire client - no session/DAG state of its own - used to exercise
// ListenSqlWire's accept loop the way a raw client (spec §7 test 1) would, without depending on
// the peersync or SQL client packages.
type rawWireClient struct {
	conn        stream.ConnectionHandle
	codec       wire.Codec
	correlation int
}

func dialRawWireClient(t *testing.T, addr string) *rawWireClient {
	t.Helper()
	transport := tcp.NewTransport(core.DefaultConnectOptions())
	conn, err := transport.Connect(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	return &rawWireClient{conn: conn, codec: wire.NewCodec(wire.EncodingJSON), correlation: 1}
}

func (c *rawWireClient) request(t *testing.T, msg wire.Message) wire.Message {
	t.Helper()
	frame, err := c.codec.Encode(msg)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.conn.Send(frame); err != nil {
		t.Fatal(err)
	}
	cid := msg.Header().CorrelationID
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if reply := c.conn.TryPoll(); reply != nil {
			decoded, err := c.codec.Decode(reply)
			if err != nil {
				t.Fatal(err)
			}
			if decoded.Header().CorrelationID == cid {
				return decoded
			}
			continue
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no response for correlation %d", cid)
	return nil
}

func (c *rawWireClient) nextCorrelation() int {
	id := c.correlation
	c.correlation++
	return id
}

func (c *rawWireClient) sessionBegin(t *testing.T, namespace, readConsistency string) wire.SessionBeginAckMessage {
	t.Helper()
	msg := wire.SessionBeginMessage{
		H:               wire.Header{MessageType: wire.MsgSessionBegin, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.nextCorrelation()},
		Namespace:       namespace,
		ReadConsistency: readConsistency,
	}
	reply := c.request(t, msg)
	ack, ok := reply.(wire.SessionBeginAckMessage)
	if !ok {
		t.Fatalf("expected SessionBeginAckMessage, got %T", reply)
	}
	return ack
}

func (c *rawWireClient) sqlExec(t *testing.T, namespace, sessionID, sqlText string) wire.SqlResultMessage {
	t.Helper()
	msg := wire.SqlExecMessage{
		H:         wire.Header{MessageType: wire.MsgSqlExec, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.nextCorrelation()},
		Namespace: namespace,
		SessionID: sessionID,
		SQL:       sqlText,
	}
	reply := c.request(t, msg)
	result, ok := reply.(wire.SqlResultMessage)
	if !ok {
		t.Fatalf("expected SqlResultMessage, got %T", reply)
	}
	return result
}

func (c *rawWireClient) txCommit(t *testing.T, namespace, sessionID string) wire.Message {
	t.Helper()
	msg := wire.TxCommitMessage{
		H:         wire.Header{MessageType: wire.MsgTxCommit, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.nextCorrelation()},
		Namespace: namespace,
		SessionID: sessionID,
	}
	return c.request(t, msg)
}

func (c *rawWireClient) txRollback(t *testing.T, namespace, sessionID string) wire.SqlResultMessage {
	t.Helper()
	msg := wire.TxRollbackMessage{
		H:         wire.Header{MessageType: wire.MsgTxRollback, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.nextCorrelation()},
		Namespace: namespace,
		SessionID: sessionID,
	}
	reply := c.request(t, msg)
	result, ok := reply.(wire.SqlResultMessage)
	if !ok {
		t.Fatalf("expected SqlResultMessage, got %T", reply)
	}
	return result
}

func (c *rawWireClient) handshake(t *testing.T, mode wire.ClientMode, namespace string) wire.HandshakeAckMessage {
	t.Helper()
	msg := wire.HandshakeMessage{
		H: wire.Header{
			MessageType:     wire.MsgHandshake,
			ProtocolVersion: wire.KdbWireProtocolVersion,
			CorrelationID:   c.nextCorrelation(),
		},
		Request: wire.HandshakePayload{
			NodeID:     "raw-test-client",
			Namespaces: []string{namespace},
			ClientMode: mode,
		},
	}
	reply := c.request(t, msg)
	ack, ok := reply.(wire.HandshakeAckMessage)
	if !ok {
		t.Fatalf("expected HandshakeAckMessage, got %T", reply)
	}
	return ack
}

// handshakeWithCredentials is like handshake, but supplies User/Password so a RegistryAuthStore-
// backed AuthEngine can authenticate this connection (component 38 sub-phase C) - raw TCP has no
// other side channel to carry them.
func (c *rawWireClient) handshakeWithCredentials(t *testing.T, user, password string) wire.HandshakeAckMessage {
	t.Helper()
	msg := wire.HandshakeMessage{
		H: wire.Header{
			MessageType:     wire.MsgHandshake,
			ProtocolVersion: wire.KdbWireProtocolVersion,
			CorrelationID:   c.nextCorrelation(),
		},
		Request: wire.HandshakePayload{
			NodeID:     "raw-test-client",
			Namespaces: []string{"app/data"},
			ClientMode: wire.ClientSQL,
			User:       &user,
			Password:   &password,
		},
	}
	reply := c.request(t, msg)
	ack, ok := reply.(wire.HandshakeAckMessage)
	if !ok {
		t.Fatalf("expected HandshakeAckMessage, got %T", reply)
	}
	return ack
}

// TestListenSqlWireHandshake is the Phase 1 sub-phase A exit criteria test (component 38 spec
// §7 test 1): a raw wire client completes a handshake against the listener and gets a valid
// response frame, over a real TCP socket end to end (not an in-memory codec round trip).
func TestListenSqlWireHandshake(t *testing.T) {
	rt := newTestRuntime(t)
	ln, err := ListenSqlWire("tcp://127.0.0.1:0?bind=true", rt)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	addr := fmt.Sprintf("tcp://%s", ln.Addr().String())
	client := dialRawWireClient(t, addr)
	ack := client.handshake(t, wire.ClientSQL, "app/data")

	if !ack.Response.Accepted {
		reason := ""
		if ack.Response.RejectionReason != nil {
			reason = *ack.Response.RejectionReason
		}
		t.Fatalf("expected handshake accepted, got rejected: %s", reason)
	}
	if ack.Response.ProtocolVersion != wire.KdbWireProtocolVersion {
		t.Fatalf("protocolVersion: %d", ack.Response.ProtocolVersion)
	}
	head, ok := ack.Response.RemoteHeads["app/data"]
	if !ok || head == "" {
		t.Fatalf("expected remote head for app/data, got %+v", ack.Response.RemoteHeads)
	}
}

func TestListenSqlWireRejectsNonSqlClientMode(t *testing.T) {
	rt := newTestRuntime(t)
	ln, err := ListenSqlWire("tcp://127.0.0.1:0?bind=true", rt)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	addr := fmt.Sprintf("tcp://%s", ln.Addr().String())
	client := dialRawWireClient(t, addr)
	ack := client.handshake(t, wire.ClientFullPeer, "app/data")

	if ack.Response.Accepted {
		t.Fatal("expected handshake to be rejected for non-SQL_CLIENT mode")
	}
	if ack.Response.RejectionReason == nil || *ack.Response.RejectionReason != "SQL_CLIENT mode required" {
		t.Fatalf("rejectionReason: %+v", ack.Response.RejectionReason)
	}
}

func TestListenSqlWireRejectsHandshakeOnAuthDenial(t *testing.T) {
	rt := newTestRuntime(t)
	rt.AuthEngine = denyAllEngine{}
	ln, err := ListenSqlWire("tcp://127.0.0.1:0?bind=true", rt)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	addr := fmt.Sprintf("tcp://%s", ln.Addr().String())
	client := dialRawWireClient(t, addr)
	ack := client.handshake(t, wire.ClientSQL, "app/data")

	if ack.Response.Accepted {
		t.Fatal("expected handshake to be rejected when auth denies")
	}
}

// TestListenSqlWireSqlExecUnknownSession pins a boundary case: a SqlExec against a session id
// nobody began must surface a clean, named error rather than hanging or being silently dropped,
// per component 38 spec §6.
func TestListenSqlWireSqlExecUnknownSession(t *testing.T) {
	rt := newTestRuntime(t)
	ln, err := ListenSqlWire("tcp://127.0.0.1:0?bind=true", rt)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	addr := fmt.Sprintf("tcp://%s", ln.Addr().String())
	client := dialRawWireClient(t, addr)
	client.handshake(t, wire.ClientSQL, "app/data")

	result := client.sqlExec(t, "app/data", "no-such-session", "SELECT 1")
	if result.Error == nil || *result.Error == "" {
		t.Fatal("expected a non-empty unknown-session error")
	}
}

// TestListenSqlWireCreateInsertSelectCommit is the Phase 2 exit criteria test (component 38
// spec §9 port order: "prove the SQL/document path end-to-end"): over a real TCP socket, a
// client creates a table, inserts a row, commits, and reads it back in a fresh session - proving
// SqlExec/TxCommit reach the real TransactionEngine/DAG rather than a stub.
func TestListenSqlWireCreateInsertSelectCommit(t *testing.T) {
	rt := newTestRuntime(t)
	ln, err := ListenSqlWire("tcp://127.0.0.1:0?bind=true", rt)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	addr := fmt.Sprintf("tcp://%s", ln.Addr().String())

	client := dialRawWireClient(t, addr)
	client.handshake(t, wire.ClientSQL, "app/data")
	sess := client.sessionBegin(t, "app/data", "READ_COMMITTED")
	if sess.SessionID == "" {
		t.Fatal("expected a session id")
	}

	created := client.sqlExec(t, "app/data", sess.SessionID, `CREATE TABLE users (userId VARCHAR NOT NULL, status VARCHAR NOT NULL)`)
	if created.Error != nil {
		t.Fatalf("create table: %s", *created.Error)
	}

	inserted := client.sqlExec(t, "app/data", sess.SessionID, `INSERT INTO users (userId, status) VALUES ('u1', 'active')`)
	if inserted.Error != nil {
		t.Fatalf("insert: %s", *inserted.Error)
	}
	if inserted.RowsAffected != 1 || len(inserted.GeneratedIDs) != 1 {
		t.Fatalf("insert result: %+v", inserted)
	}

	// Before commit, a fresh session's read must not see the buffered-but-uncommitted row.
	other := dialRawWireClient(t, addr)
	other.handshake(t, wire.ClientSQL, "app/data")
	otherSess := other.sessionBegin(t, "app/data", "READ_COMMITTED")
	preCommit := other.sqlExec(t, "app/data", otherSess.SessionID, `SELECT COUNT(*) AS n FROM users`)
	if preCommit.Error != nil {
		t.Fatalf("pre-commit select: %s", *preCommit.Error)
	}
	if len(preCommit.Rows) != 1 || preCommit.Rows[0][0] != "0" {
		t.Fatalf("expected 0 rows visible before commit, got %+v", preCommit.Rows)
	}

	commitReply := client.txCommit(t, "app/data", sess.SessionID)
	commitResult, ok := commitReply.(wire.SqlResultMessage)
	if !ok {
		t.Fatalf("expected SqlResultMessage from commit, got %T", commitReply)
	}
	if commitResult.Error != nil {
		t.Fatalf("commit: %s", *commitResult.Error)
	}
	if commitResult.RowsAffected != 1 || commitResult.ResolvedCommitHex == "" {
		t.Fatalf("commit result: %+v", commitResult)
	}

	postCommit := other.sqlExec(t, "app/data", otherSess.SessionID, `SELECT COUNT(*) AS n FROM users`)
	if postCommit.Error != nil {
		t.Fatalf("post-commit select: %s", *postCommit.Error)
	}
	if len(postCommit.Rows) != 1 || postCommit.Rows[0][0] != "1" {
		t.Fatalf("expected 1 row visible after commit, got %+v", postCommit.Rows)
	}
}

// TestListenSqlWireSqlExecUnknownColumnDoesNotCrashServer is the reported repro: a SELECT on an
// unknown column used to panic inside DefaultPlanner.PlanSelect with no recover between it and
// the connection's read loop, killing the whole process - every other connection, not just this
// one query, went down with it. The client should instead get a normal error reply, and the
// server (and this same connection) must still work afterward.
func TestListenSqlWireSqlExecUnknownColumnDoesNotCrashServer(t *testing.T) {
	rt := newTestRuntime(t)
	ln, err := ListenSqlWire("tcp://127.0.0.1:0?bind=true", rt)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	addr := fmt.Sprintf("tcp://%s", ln.Addr().String())

	client := dialRawWireClient(t, addr)
	client.handshake(t, wire.ClientSQL, "app/data")
	sess := client.sessionBegin(t, "app/data", "READ_COMMITTED")

	created := client.sqlExec(t, "app/data", sess.SessionID, `CREATE TABLE t (id VARCHAR NOT NULL)`)
	if created.Error != nil {
		t.Fatalf("create table: %s", *created.Error)
	}

	bad := client.sqlExec(t, "app/data", sess.SessionID, `SELECT nosuchcolumn FROM t`)
	if bad.Error == nil || *bad.Error == "" {
		t.Fatal("expected an error for an unknown column, got none")
	}

	// The server (and this connection) must still be alive and usable.
	ok := client.sqlExec(t, "app/data", sess.SessionID, `SELECT COUNT(*) AS n FROM t`)
	if ok.Error != nil {
		t.Fatalf("select after unknown-column error: %s", *ok.Error)
	}
}

// TestListenSqlWireTxRollbackDiscardsPending proves TxRollback actually discards buffered
// writes rather than leaving them to leak into the next commit.
func TestListenSqlWireTxRollbackDiscardsPending(t *testing.T) {
	rt := newTestRuntime(t)
	ln, err := ListenSqlWire("tcp://127.0.0.1:0?bind=true", rt)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	addr := fmt.Sprintf("tcp://%s", ln.Addr().String())

	client := dialRawWireClient(t, addr)
	client.handshake(t, wire.ClientSQL, "app/data")
	sess := client.sessionBegin(t, "app/data", "READ_COMMITTED")

	if r := client.sqlExec(t, "app/data", sess.SessionID, `CREATE TABLE t (id VARCHAR NOT NULL)`); r.Error != nil {
		t.Fatalf("create: %s", *r.Error)
	}
	if r := client.sqlExec(t, "app/data", sess.SessionID, `INSERT INTO t (id) VALUES ('a')`); r.Error != nil {
		t.Fatalf("insert: %s", *r.Error)
	}
	rollback := client.txRollback(t, "app/data", sess.SessionID)
	if rollback.Error != nil {
		t.Fatalf("rollback: %s", *rollback.Error)
	}

	// Nothing was ever committed (the CREATE ran but the row was rolled back), so a commit
	// now must fail with "no pending transaction" rather than silently persisting the
	// rolled-back row.
	commitReply := client.txCommit(t, "app/data", sess.SessionID)
	commitResult, ok := commitReply.(wire.SqlResultMessage)
	if !ok {
		t.Fatalf("expected SqlResultMessage, got %T", commitReply)
	}
	if commitResult.Error == nil {
		t.Fatal("expected commit after rollback with nothing re-buffered to fail")
	}
}

// TestListenSqlWireConcurrentCommitsChainCleanly exercises the concurrency half of component 38
// spec §7 test 3 that's reachable over the wire today: many connections commit concurrently
// (disjoint documents - INSERT always mints a fresh document id, so the wire protocol can't yet
// target one existing document for a second write; there's no UPDATE/explicit-docID write path
// parsed yet, see TestKdbServerRuntimeCommitConcurrentSameDocumentConflict in
// server_runtime_test.go for the same-document conflict-detection half of test 3, exercised
// directly against KdbServerRuntime.Commit instead). What this test proves is what
// KdbServerRuntime.commitMu exists to guarantee (see its doc comment in server_runtime.go):
// without it, InMemoryCommitDag.AppendCommit's unconditional branch-head advance would let two
// concurrent commits both anchor on the same stale head and silently fork main, orphaning
// whichever one lands first - so every successful commit here must still be reachable from
// "main" afterward, not just individually "successful".
func TestListenSqlWireConcurrentCommitsChainCleanly(t *testing.T) {
	rt := newTestRuntime(t)
	ln, err := ListenSqlWire("tcp://127.0.0.1:0?bind=true", rt)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	addr := fmt.Sprintf("tcp://%s", ln.Addr().String())

	// Set up the table and one row via a dedicated setup connection/session, committed
	// before either racing client connects, so both racers start from the same base.
	setup := dialRawWireClient(t, addr)
	setup.handshake(t, wire.ClientSQL, "app/data")
	setupSess := setup.sessionBegin(t, "app/data", "READ_COMMITTED")
	if r := setup.sqlExec(t, "app/data", setupSess.SessionID, `CREATE TABLE t (id VARCHAR NOT NULL, v VARCHAR NOT NULL)`); r.Error != nil {
		t.Fatalf("create: %s", *r.Error)
	}
	if r := setup.sqlExec(t, "app/data", setupSess.SessionID, `INSERT INTO t (id, v) VALUES ('doc-1', 'base')`); r.Error != nil {
		t.Fatalf("insert: %s", *r.Error)
	}
	setupCommit := setup.txCommit(t, "app/data", setupSess.SessionID)
	if r, ok := setupCommit.(wire.SqlResultMessage); !ok || r.Error != nil {
		t.Fatalf("setup commit: %+v", setupCommit)
	}

	const racers = 8
	results := make([]wire.Message, racers)
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := dialRawWireClient(t, addr)
			c.handshake(t, wire.ClientSQL, "app/data")
			s := c.sessionBegin(t, "app/data", "READ_COMMITTED")
			// Each racer's INSERT mints its own new document id (disjoint from every
			// other racer's), so ConflictPolicyStrict never has grounds to reject any
			// of these individually - the property under test is that all N concurrent
			// commits still end up chained into "main" with none lost, which the
			// row-count assertion below checks.
			if r := c.sqlExec(t, "app/data", s.SessionID, fmt.Sprintf(`INSERT INTO t (id, v) VALUES ('doc-1', 'racer-%d')`, i)); r.Error != nil {
				results[i] = wire.SqlResultMessage{Error: r.Error}
				return
			}
			results[i] = c.txCommit(t, "app/data", s.SessionID)
		}(i)
	}
	wg.Wait()

	successes := 0
	for i, r := range results {
		m, ok := r.(wire.SqlResultMessage)
		if !ok {
			t.Fatalf("racer %d: unexpected result %T (%+v) - disjoint-document commits should never conflict", i, r, r)
		}
		if m.Error == nil {
			successes++
		} else {
			t.Errorf("racer %d: unexpected commit error for a disjoint document: %s", i, *m.Error)
		}
	}
	if successes != racers {
		t.Fatalf("successes = %d, want %d", successes, racers)
	}

	// The load-bearing assertion: every successful commit must be reachable from "main"
	// (chained, not forked/orphaned). Count rows in table t after all racers finish - it
	// must equal exactly the number of successful commits plus the 1 setup row, proving no
	// commit silently vanished from the branch history commitMu protects.
	final := dialRawWireClient(t, addr)
	final.handshake(t, wire.ClientSQL, "app/data")
	finalSess := final.sessionBegin(t, "app/data", "READ_COMMITTED")
	count := final.sqlExec(t, "app/data", finalSess.SessionID, `SELECT COUNT(*) AS n FROM t`)
	if count.Error != nil {
		t.Fatalf("final count: %s", *count.Error)
	}
	got, err := strconv.Atoi(count.Rows[0][0])
	if err != nil {
		t.Fatal(err)
	}
	want := successes + 1
	if got != want {
		t.Fatalf("row count after concurrent commits = %d, want %d (successes=%d) - a commit was lost/orphaned from main", got, want, successes)
	}
}

func TestListenSqlWireCloseStopsAccepting(t *testing.T) {
	rt := newTestRuntime(t)
	ln, err := ListenSqlWire("tcp://127.0.0.1:0?bind=true", rt)
	if err != nil {
		t.Fatal(err)
	}
	addr := fmt.Sprintf("tcp://%s", ln.Addr().String())
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	transport := tcp.NewTransport(core.DefaultConnectOptions())
	if _, err := transport.Connect(addr); err == nil {
		t.Fatal("expected connect to fail after Close")
	}
}

// TestListenSqlWireTransactionReplayAppliesAndIsQueryable exercises handleTransactionReplay via
// a real SQL_CLIENT wire connection - the entry point this gets reached through independent of
// go/kdb/stream's write-back coordinator (see kdb/stream/write_back_test.go for that path). No
// session is used, matching handleTransactionReplay's own contract: it's one self-contained
// transaction, not built up against a session's pending statement builder.
func TestListenSqlWireTransactionReplayAppliesAndIsQueryable(t *testing.T) {
	rt := newTestRuntime(t)
	ln, err := ListenSqlWire("tcp://127.0.0.1:0?bind=true", rt)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	addr := fmt.Sprintf("tcp://%s", ln.Addr().String())

	client := dialRawWireClient(t, addr)
	client.handshake(t, wire.ClientSQL, "app/data")

	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	txID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	tx := document.Transaction{
		ID:         txID,
		Operations: []document.Op{document.WriteOp{DocID: docID, Patch: `{"v":"replayed"}`}},
		Timestamp:  codec.TimestampNow(),
	}
	txBytes, err := wire.EncodeTransaction(tx)
	if err != nil {
		t.Fatal(err)
	}
	replayMsg := wire.TransactionReplayMessage{
		H:                wire.Header{MessageType: wire.MsgTransactionReplay, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: client.nextCorrelation()},
		Namespace:        "app/data",
		TransactionBytes: txBytes,
	}
	reply := client.request(t, replayMsg)
	result, ok := reply.(wire.SqlResultMessage)
	if !ok {
		t.Fatalf("expected SqlResultMessage, got %T", reply)
	}
	if result.Error != nil {
		t.Fatalf("replay: %s", *result.Error)
	}
	if result.ResolvedCommitHex == "" {
		t.Fatal("expected a resolved commit hash")
	}

	// It's a real commit on main, immediately visible - not a buffered-but-uncommitted change
	// the way a SqlExec INSERT is before TxCommit.
	jsonBody, commitHex, found, err := rt.GetDocument("app/data", docID)
	if err != nil {
		t.Fatalf("GetDocument: %v", err)
	}
	if !found {
		t.Fatal("expected the replayed document to be visible via GetDocument")
	}
	if jsonBody != `{"v":"replayed"}` {
		t.Fatalf("expected the replayed JSON, got %q", jsonBody)
	}
	if commitHex != result.ResolvedCommitHex {
		t.Fatalf("expected GetDocument's commit %s to match the replay response %s", commitHex, result.ResolvedCommitHex)
	}
}

// TestListenSqlWireTransactionReplayRequiresAuthentication mirrors the other write paths'
// (handleUpsert, handleTxCommit) auth gating - a TransactionReplay before a successful
// handshake must be rejected, not silently applied.
func TestListenSqlWireTransactionReplayRequiresAuthentication(t *testing.T) {
	rt := newTestRuntime(t)
	ln, err := ListenSqlWire("tcp://127.0.0.1:0?bind=true", rt)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	addr := fmt.Sprintf("tcp://%s", ln.Addr().String())

	client := dialRawWireClient(t, addr)
	txID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	txBytes, err := wire.EncodeTransaction(document.Transaction{ID: txID, Timestamp: codec.TimestampNow()})
	if err != nil {
		t.Fatal(err)
	}
	replayMsg := wire.TransactionReplayMessage{
		H:                wire.Header{MessageType: wire.MsgTransactionReplay, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: client.nextCorrelation()},
		Namespace:        "app/data",
		TransactionBytes: txBytes,
	}
	reply := client.request(t, replayMsg)
	result, ok := reply.(wire.SqlResultMessage)
	if !ok {
		t.Fatalf("expected SqlResultMessage, got %T", reply)
	}
	if result.Error == nil {
		t.Fatal("expected an error before handshake")
	}
}

type denyAllEngine struct{}

func (denyAllEngine) Authenticator() auth.Authenticator { return denyAllEngine{} }
func (denyAllEngine) Authorizer() auth.Authorizer       { return denyAllEngine{} }

func (denyAllEngine) Authenticate(_ context.Context, _ auth.Credentials) (auth.Principal, error) {
	return auth.Principal{}, fmt.Errorf("denied")
}

func (denyAllEngine) Authorize(_ context.Context, _ auth.Principal, _ auth.Action) error {
	return fmt.Errorf("denied")
}
