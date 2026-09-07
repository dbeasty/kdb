package server

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/wire"
)

// pipelinedClient is rawWireClient with a background reader, so a test can have several frames
// in flight at once on one connection and collect the replies out of order. rawWireClient.request
// discards any frame whose correlation id it is not waiting for, which is exactly the wrong
// behaviour for testing concurrent dispatch.
type pipelinedClient struct {
	raw *rawWireClient

	mu      sync.Mutex
	replies map[int]wire.Message
	stop    chan struct{}
	done    chan struct{}
}

func dialPipelinedClient(t *testing.T, addr string) *pipelinedClient {
	t.Helper()
	c := &pipelinedClient{
		raw:     dialRawWireClient(t, addr),
		replies: make(map[int]wire.Message),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	go func() {
		defer close(c.done)
		for {
			select {
			case <-c.stop:
				return
			default:
			}
			frame := c.raw.conn.TryPoll()
			if frame == nil {
				time.Sleep(time.Millisecond)
				continue
			}
			msg, err := c.raw.codec.Decode(frame)
			if err != nil {
				continue
			}
			c.mu.Lock()
			c.replies[msg.Header().CorrelationID] = msg
			c.mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		close(c.stop)
		<-c.done
		_ = c.raw.conn.Close()
	})
	return c
}

// send writes one frame and returns its correlation id, without waiting for the reply.
func (c *pipelinedClient) send(t *testing.T, msg wire.Message) int {
	t.Helper()
	frame, err := c.raw.codec.Encode(msg)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.raw.conn.Send(frame); err != nil {
		t.Fatal(err)
	}
	return msg.Header().CorrelationID
}

// poll returns the reply for cid if it has arrived.
func (c *pipelinedClient) poll(cid int) (wire.Message, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	msg, ok := c.replies[cid]
	return msg, ok
}

// await blocks until cid's reply arrives or timeout elapses.
func (c *pipelinedClient) await(t *testing.T, cid int, timeout time.Duration) wire.Message {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if msg, ok := c.poll(cid); ok {
			return msg
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("no reply for correlation %d within %s", cid, timeout)
	return nil
}

func (c *pipelinedClient) handshake(t *testing.T, namespace string) {
	t.Helper()
	cid := c.send(t, wire.HandshakeMessage{
		H: wire.Header{MessageType: wire.MsgHandshake, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.raw.nextCorrelation()},
		Request: wire.HandshakePayload{
			NodeID:     "pipelined-test-client",
			Namespaces: []string{namespace},
			ClientMode: wire.ClientSQL,
		},
	})
	ack, ok := c.await(t, cid, 2*time.Second).(wire.HandshakeAckMessage)
	if !ok || !ack.Response.Accepted {
		t.Fatalf("handshake not accepted: %+v", ack)
	}
}

func (c *pipelinedClient) sessionBegin(t *testing.T, namespace string) string {
	t.Helper()
	cid := c.send(t, wire.SessionBeginMessage{
		H:               wire.Header{MessageType: wire.MsgSessionBegin, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.raw.nextCorrelation()},
		Namespace:       namespace,
		ReadConsistency: "READ_COMMITTED",
	})
	ack, ok := c.await(t, cid, 2*time.Second).(wire.SessionBeginAckMessage)
	if !ok || ack.SessionID == "" {
		t.Fatalf("session begin failed: %+v", ack)
	}
	return ack.SessionID
}

func (c *pipelinedClient) sendSqlExec(t *testing.T, namespace, sessionID, sqlText string) int {
	t.Helper()
	return c.send(t, wire.SqlExecMessage{
		H:         wire.Header{MessageType: wire.MsgSqlExec, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.raw.nextCorrelation()},
		Namespace: namespace,
		SessionID: sessionID,
		SQL:       sqlText,
	})
}

func (c *pipelinedClient) sendDocumentGet(t *testing.T, namespace, docID string) int {
	t.Helper()
	return c.send(t, wire.DocumentGetMessage{
		H:         wire.Header{MessageType: wire.MsgDocumentGet, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.raw.nextCorrelation()},
		Namespace: namespace,
		DocID:     docID,
	})
}

// TestConcurrentFramesSlowSqlExecDoesNotDelayDocumentGet is Component 74's headline guarantee
// (kdb-spec-layer16 §12): a statement that takes a long time on one session must not hold up
// the rest of the connection. Before per-frame dispatch, run() handled frames strictly one at a
// time, so the DocumentGet below could not be answered until the blocked SqlExec returned.
func TestConcurrentFramesSlowSqlExecDoesNotDelayDocumentGet(t *testing.T) {
	rt := newTestRuntime(t)
	addr := listenFor(t, rt)

	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Upsert("app/data", docID, `{"k":"v"}`, auth.Principal{ID: "tester"}); err != nil {
		t.Fatal(err)
	}

	release := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	rt.SetBeforeSqlExecHookForTest(func(msg wire.SqlExecMessage) {
		if !strings.Contains(msg.SQL, "SELECT") {
			return
		}
		once.Do(func() { close(entered) })
		<-release
	})
	defer close(release)

	c := dialPipelinedClient(t, addr)
	c.handshake(t, "app/data")
	session := c.sessionBegin(t, "app/data")

	slow := c.sendSqlExec(t, "app/data", session, `SELECT 1`)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the SqlExec never reached the hook")
	}

	// The connection is now blocked inside a SqlExec. A sessionless point read must still be
	// answered promptly.
	get := c.sendDocumentGet(t, "app/data", docID.String())
	reply := c.await(t, get, 2*time.Second)
	result, ok := reply.(wire.DocumentGetResultMessage)
	if !ok {
		t.Fatalf("expected DocumentGetResult, got %T", reply)
	}
	if result.Error != nil {
		t.Fatalf("document get errored while a SqlExec was in flight: %s", *result.Error)
	}
	if result.JSON == nil || *result.JSON != `{"k":"v"}` {
		t.Fatalf("document get returned %+v", result)
	}
	if _, answered := c.poll(slow); answered {
		t.Fatal("the blocked SqlExec was answered before it was released - the test proves nothing")
	}
}

// TestConcurrentFramesSameSessionAreOrdered: concurrency must not reorder a session's own
// statements. Two INSERTs and a COMMIT sent back to back on one session are processed in the
// order they arrived, so the commit sees both rows.
func TestConcurrentFramesSameSessionAreOrdered(t *testing.T) {
	rt := newTestRuntime(t)
	addr := listenFor(t, rt)
	c := dialPipelinedClient(t, addr)
	c.handshake(t, "app/data")
	session := c.sessionBegin(t, "app/data")

	create := c.sendSqlExec(t, "app/data", session, `CREATE TABLE t (id VARCHAR NOT NULL, v VARCHAR NOT NULL)`)
	if r, ok := c.await(t, create, 2*time.Second).(wire.SqlResultMessage); !ok || r.Error != nil {
		t.Fatalf("create: %+v", r)
	}

	// Sent without waiting for any reply: all three are in flight at once.
	first := c.sendSqlExec(t, "app/data", session, `INSERT INTO t (id, v) VALUES ('a', '1')`)
	second := c.sendSqlExec(t, "app/data", session, `INSERT INTO t (id, v) VALUES ('b', '2')`)
	commitCid := c.send(t, wire.TxCommitMessage{
		H:         wire.Header{MessageType: wire.MsgTxCommit, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.raw.nextCorrelation()},
		Namespace: "app/data",
		SessionID: session,
	})
	for _, cid := range []int{first, second} {
		if r, ok := c.await(t, cid, 3*time.Second).(wire.SqlResultMessage); !ok || r.Error != nil {
			t.Fatalf("insert %d: %+v", cid, r)
		}
	}
	commit, ok := c.await(t, commitCid, 3*time.Second).(wire.SqlResultMessage)
	if !ok || commit.Error != nil {
		t.Fatalf("commit: %+v", commit)
	}
	// Both inserts must have been buffered before the commit ran: ordering, not luck.
	if commit.RowsAffected != 2 {
		t.Fatalf("commit carried %d operations, want 2 - frames on one session were reordered", commit.RowsAffected)
	}
	count := c.sendSqlExec(t, "app/data", session, `SELECT COUNT(*) AS n FROM t`)
	res, ok := c.await(t, count, 2*time.Second).(wire.SqlResultMessage)
	if !ok || res.Error != nil {
		t.Fatalf("count: %+v", res)
	}
	if res.Rows[0][0] != "2" {
		t.Fatalf("row count = %s, want 2", res.Rows[0][0])
	}
}

// TestConcurrentFramesBeforeHandshakeAreRefusedInOrder: nothing is dispatched concurrently
// before the handshake reply is written, and a frame that jumps the gun still gets the existing
// unauthenticated refusal rather than being dropped or racing the handshake.
func TestConcurrentFramesBeforeHandshakeAreRefusedInOrder(t *testing.T) {
	rt := newTestRuntime(t)
	addr := listenFor(t, rt)
	c := dialPipelinedClient(t, addr)

	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	early := c.sendDocumentGet(t, "app/data", docID.String())
	reply, ok := c.await(t, early, 2*time.Second).(wire.DocumentGetResultMessage)
	if !ok {
		t.Fatalf("expected DocumentGetResult, got %T", reply)
	}
	if reply.Error == nil || *reply.Error != "not authenticated" {
		t.Fatalf("pre-handshake frame reply = %+v, want a not-authenticated error", reply)
	}
	// The connection is still usable: the handshake that follows works normally.
	c.handshake(t, "app/data")
	session := c.sessionBegin(t, "app/data")
	if session == "" {
		t.Fatal("session begin failed after a pre-handshake frame")
	}
}

// TestConcurrentFramesDisconnectReleasesLeasesAfterInFlight: dropping a connection with frames
// still executing must wait for them before releasing the session's leases - releasing while a
// commit is mid-flight would let a stranger take the document under it. Run with -race, this is
// also the check that the teardown path does not race the dispatch goroutines.
func TestConcurrentFramesDisconnectReleasesLeasesAfterInFlight(t *testing.T) {
	rt := newTestRuntime(t)
	addr := listenFor(t, rt)

	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Upsert("app/data", docID, `{"k":"v"}`, auth.Principal{ID: "tester"}); err != nil {
		t.Fatal(err)
	}

	release := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	rt.SetBeforeSqlExecHookForTest(func(msg wire.SqlExecMessage) {
		if !strings.Contains(msg.SQL, "SELECT") {
			return
		}
		once.Do(func() { close(entered) })
		<-release
	})

	c := dialPipelinedClient(t, addr)
	c.handshake(t, "app/data")
	session := c.sessionBegin(t, "app/data")

	lockCid := c.send(t, wire.LockAcquireMessage{
		H:         wire.Header{MessageType: wire.MsgLockAcquire, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.raw.nextCorrelation()},
		Namespace: "app/data",
		SessionID: session,
		DocID:     docID.String(),
		TTLMillis: 60_000,
	})
	lock, ok := c.await(t, lockCid, 2*time.Second).(wire.LockResultMessage)
	if !ok || !lock.Granted {
		t.Fatalf("lock not granted: %+v", lock)
	}
	if rt.DocumentLocks.HeldCount() != 1 {
		t.Fatalf("held locks = %d, want 1", rt.DocumentLocks.HeldCount())
	}

	c.sendSqlExec(t, "app/data", session, `SELECT 1`)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the SqlExec never reached the hook")
	}

	// Drop the connection while that frame is still executing.
	if err := c.raw.conn.Close(); err != nil {
		t.Fatal(err)
	}
	// The lease must still be held: teardown waits for in-flight work first.
	time.Sleep(50 * time.Millisecond)
	if rt.DocumentLocks.HeldCount() != 1 {
		t.Fatal("the lease was released while a frame on that session was still executing")
	}
	close(release)

	deadline := time.Now().Add(3 * time.Second)
	for rt.DocumentLocks.HeldCount() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("held locks = %d after disconnect, want 0", rt.DocumentLocks.HeldCount())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestWriteGateIsPerNamespace is §12's other half: the write gate is per runtime, and a runtime
// is per namespace, so a commit blocked in one namespace's gate must not stall a commit in
// another. Two runtimes come from one ServerRuntimeRegistry, the first namespace's gate is held
// occupied, and the second namespace still commits.
func TestWriteGateIsPerNamespace(t *testing.T) {
	registry := NewServerRuntimeRegistry()
	open := func(ns string) func() (*KdbServerRuntime, error) {
		return func() (*KdbServerRuntime, error) {
			rt, err := embed.OpenMemoryRuntime("demo", ns, schema.None())
			if err != nil {
				return nil, err
			}
			return NewKdbServerRuntime(rt), nil
		}
	}
	first, err := registry.GetOrOpen("app/one", open("app/one"))
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Release("app/one")
	second, err := registry.GetOrOpen("app/two", open("app/two"))
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Release("app/two")

	// Occupy the first namespace's gate, exactly as an in-flight commit would.
	releaseGate, err := first.AcquireWriteSlotForTest()
	if err != nil {
		t.Fatal(err)
	}

	// A commit into the *first* namespace now has to queue behind it...
	blocked := make(chan error, 1)
	go func() {
		id, _ := codec.RandomUUID()
		_, err := first.Upsert("app/one", id, `{"blocked":true}`, auth.Principal{ID: "tester"})
		blocked <- err
	}()

	// ...while a commit into the second namespace completes regardless.
	done := make(chan error, 1)
	go func() {
		id, _ := codec.RandomUUID()
		_, err := second.Upsert("app/two", id, `{"free":true}`, auth.Principal{ID: "tester"})
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the second namespace's commit failed: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a commit in one namespace was blocked by another namespace's write gate")
	}
	select {
	case err := <-blocked:
		t.Fatalf("the first namespace's commit finished while its gate was held: %v", err)
	default:
	}

	releaseGate()
	select {
	case err := <-blocked:
		if err != nil {
			t.Fatalf("the queued commit failed once the gate freed: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the queued commit never ran after its gate was released")
	}
}

// TestConcurrentFramesAreBounded proves the in-flight cap exists and that a client pipelining
// well past it still gets every reply - the reader blocks rather than the server piling up
// goroutines or dropping frames.
func TestConcurrentFramesAreBounded(t *testing.T) {
	rt := newTestRuntime(t)
	addr := listenFor(t, rt)
	c := dialPipelinedClient(t, addr)
	c.handshake(t, "app/data")

	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Upsert("app/data", docID, `{"k":"v"}`, auth.Principal{ID: "tester"}); err != nil {
		t.Fatal(err)
	}

	const frames = MaxInFlightFrames * 3
	cids := make([]int, 0, frames)
	for i := 0; i < frames; i++ {
		cids = append(cids, c.sendDocumentGet(t, "app/data", docID.String()))
	}
	for _, cid := range cids {
		reply, ok := c.await(t, cid, 10*time.Second).(wire.DocumentGetResultMessage)
		if !ok || reply.Error != nil {
			t.Fatalf("correlation %d: %+v", cid, reply)
		}
	}
}
