package server

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

// TestListenStreamPublishesRealWriteToReadOnlySubscriber is the front-door regression test for
// Mode 1: a real stream.Subscriber, over a real TCP socket, receives a DeltaCommit for a write
// that happened through the ordinary server API (Upsert) - proving both ListenStream's handshake/
// fan-out wiring and KdbServerRuntime.CommitListener's notification bridge (without which
// nothing would ever call StreamHub.Publish at all).
func TestListenStreamPublishesRealWriteToReadOnlySubscriber(t *testing.T) {
	const ns = "app/data" // matches newTestRuntime's fixed namespace
	rt := newTestRuntime(t)

	hub, listener, err := ListenStream("tcp://127.0.0.1:0?bind=true", rt, ns)
	if err != nil {
		t.Fatalf("ListenStream: %v", err)
	}
	defer listener.Close()
	rt.CommitListener = func(namespaceID string, commit document.Commit) {
		parentHash := codec.Hash{}
		if len(commit.ParentHashes) > 0 {
			parentHash = commit.ParentHashes[0]
		}
		hub.Publish(stream.PublishedCommit{
			CommitHash:      commit.Hash,
			ParentHash:      parentHash,
			Operations:      commit.Operations,
			TimestampMicros: commit.Timestamp.EpochMicros(),
		})
	}

	head, err := rt.Runtime.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	transport := tcp.NewTransport(core.DefaultConnectOptions())
	subscriber := stream.NewSubscriber(wire.NewCodec(wire.EncodingJSON), transport, nil)
	conn, err := subscriber.Connect(stream.SubscriberConfig{
		NamespaceID:    ns,
		NodeID:         "test-subscriber",
		Mode:           stream.ClientReadOnly,
		CoordinatorURI: "tcp://" + listener.Addr().String(),
		ResumeFrom:     &head,
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer subscriber.Disconnect()

	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	commit, err := rt.Upsert(ns, docID, `{"v":"streamed"}`, auth.Principal{})
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}

	deadline := time.After(2 * time.Second)
	for {
		select {
		case ev := <-subscriber.Events():
			if ev.Kind == stream.EventDeltaReceived && ev.CommitHash == commit.Hash {
				if pos := conn.Position(); pos == nil || *pos != commit.Hash {
					t.Fatalf("expected subscriber position to advance to %s, got %v", commit.Hash.Hex(), pos)
				}
				return
			}
			if ev.Kind == stream.EventError {
				t.Fatalf("subscriber error: %v", ev.Cause)
			}
		case <-deadline:
			t.Fatal("timed out waiting for the published delta commit")
		}
	}
}

// TestListenStreamWriteBackSubmitsAndPersists is the front-door regression test for Mode 2: a
// real write-back stream.Subscriber, over a real TCP socket, submits a transaction to
// ListenStream's StreamHub and gets it applied via KdbServerRuntime.Replay - proving the whole
// chain (handshake mode gating, TransactionReplay handling, the resulting document actually
// being queryable) works over an actual network connection, not just in the InMemoryTransport
// unit tests in kdb/stream/write_back_test.go.
func TestListenStreamWriteBackSubmitsAndPersists(t *testing.T) {
	const ns = "app/data"
	rt := newTestRuntime(t)

	_, listener, err := ListenStream("tcp://127.0.0.1:0?bind=true", rt, ns)
	if err != nil {
		t.Fatalf("ListenStream: %v", err)
	}
	defer listener.Close()

	transport := tcp.NewTransport(core.DefaultConnectOptions())
	subscriber := stream.NewSubscriber(wire.NewCodec(wire.EncodingJSON), transport, nil)
	conn, err := subscriber.Connect(stream.SubscriberConfig{
		NamespaceID:    ns,
		NodeID:         "test-write-back",
		Mode:           stream.ClientWriteBack,
		CoordinatorURI: "tcp://" + listener.Addr().String(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer subscriber.Disconnect()
	if conn.SubmitTransaction == nil {
		t.Fatal("expected Connection.SubmitTransaction to be set for a write-back connection")
	}

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
		Operations: []document.Op{document.WriteOp{DocID: docID, Patch: `{"v":"write-back-over-tcp"}`}},
		Timestamp:  codec.TimestampNow(),
	}

	result := conn.SubmitTransaction(tx)
	if result.Rejected != nil {
		t.Fatalf("expected the transaction to apply, got Rejected: %s", *result.Rejected)
	}
	if result.Applied == nil {
		t.Fatal("expected Applied to be set")
	}

	jsonBody, _, found, err := rt.GetDocument(ns, docID)
	if err != nil {
		t.Fatalf("GetDocument: %v", err)
	}
	if !found {
		t.Fatal("expected the write-back document to be visible via GetDocument")
	}
	if jsonBody != `{"v":"write-back-over-tcp"}` {
		t.Fatalf("expected the submitted document, got %q", jsonBody)
	}
}

// TestListenStreamRejectsSqlClientHandshake proves StreamHub's mode gating, the stream-side
// mirror of ListenSqlWire's own "SQL_CLIENT mode required" gate.
func TestListenStreamRejectsSqlClientHandshake(t *testing.T) {
	const ns = "app/data"
	rt := newTestRuntime(t)

	_, listener, err := ListenStream("tcp://127.0.0.1:0?bind=true", rt, ns)
	if err != nil {
		t.Fatalf("ListenStream: %v", err)
	}
	defer listener.Close()

	transport := tcp.NewTransport(core.DefaultConnectOptions())
	conn, err := transport.Connect("tcp://" + listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	codecWire := wire.NewCodec(wire.EncodingJSON)
	hs := wire.HandshakeMessage{
		H: wire.Header{MessageType: wire.MsgHandshake, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 1},
		Request: wire.HandshakePayload{
			NodeID:     "sql-client",
			Namespaces: []string{ns},
			ClientMode: wire.ClientSQL,
		},
	}
	frame, err := codecWire.Encode(hs)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Send(frame); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if reply := conn.TryPoll(); reply != nil {
			decoded, err := codecWire.Decode(reply)
			if err != nil {
				t.Fatal(err)
			}
			ack, ok := decoded.(wire.HandshakeAckMessage)
			if !ok {
				t.Fatalf("expected HandshakeAckMessage, got %T", decoded)
			}
			if ack.Response.Accepted {
				t.Fatal("expected a SQL_CLIENT handshake to be rejected by StreamHub")
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("no handshake response received")
}

// stuckConn is a ConnectionHandle whose Send blocks until it is released - what a real TCP
// subscriber looks like once its receive window has filled and the socket write can no longer
// make progress.
type stuckConn struct {
	release chan struct{}
	sent    atomic.Int64
	mu      sync.Mutex
	lastHex codec.Hash
}

func newStuckConn() *stuckConn { return &stuckConn{release: make(chan struct{})} }

func (c *stuckConn) Send(frame []byte) error {
	<-c.release
	c.sent.Add(1)
	if msg, err := wire.NewCodec(wire.EncodingJSON).Decode(frame); err == nil {
		if d, ok := msg.(wire.DeltaCommitMessage); ok {
			c.mu.Lock()
			c.lastHex = d.Payload.CommitHash
			c.mu.Unlock()
		}
	}
	return nil
}

// last is the commit of the latest delta sent to c.
func (c *stuckConn) last() codec.Hash {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastHex
}
func (c *stuckConn) Incoming() <-chan []byte { return nil }
func (c *stuckConn) Close() error            { return nil }
func (c *stuckConn) TryPoll() []byte         { return nil }

// TestPublishDoesNotBlockOnAStuckSubscriber is the regression test for the fan-out hazard.
// Publish used to call conn.Send inline for every subscriber, and socketConnection.Send does a
// blocking socket write - so one subscriber that had stopped reading stalled the whole fan-out
// and, behind it, the goroutine that had just committed (Publish is called from
// KdbServerRuntime.CommitListener on the commit path). Publish now only wakes each subscriber's
// own sender goroutine, so it returns regardless - and once the subscriber drains, it is caught
// up to the head rather than having lost what it could not take.
func TestPublishDoesNotBlockOnAStuckSubscriber(t *testing.T) {
	const ns = "app/data"
	rt := newTestRuntime(t)
	hub := NewStreamHub(wire.NewCodec(wire.EncodingJSON), ns, rt)
	rt.CommitListener = func(string, document.Commit) { hub.Publish(stream.PublishedCommit{}) }

	stuck := newStuckConn()
	handshakeStreamSubscriber(t, hub, stuck, ns)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 300; i++ {
			if _, err := rt.Upsert(ns, mustRandomUUID(t), `{"i":1}`, auth.Principal{}); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("a write blocked on a subscriber that stopped reading")
	}
	close(stuck.release)
	head, _ := rt.dag.Head()
	deadline := time.Now().Add(5 * time.Second)
	for stuck.last() != head {
		if time.Now().After(deadline) {
			t.Fatalf("the drained subscriber never caught up to the head (sent %d frames)", stuck.sent.Load())
		}
		time.Sleep(time.Millisecond)
	}
	// 300 commits reached it in far fewer frames: everything it missed while stuck came as one.
	if n := stuck.sent.Load(); n >= 300 {
		t.Fatalf("expected the backlog coalesced, got %d frames for 300 commits", n)
	}
}

// TestPublishReachesAHealthySubscriberBesideAStuckOne: the fan-out is per-subscriber, so a
// subscriber that stopped reading must not cost the others their frames either.
func TestPublishReachesAHealthySubscriberBesideAStuckOne(t *testing.T) {
	const ns = "app/data"
	rt := newTestRuntime(t)
	hub := NewStreamHub(wire.NewCodec(wire.EncodingJSON), ns, rt)
	rt.CommitListener = func(string, document.Commit) { hub.Publish(stream.PublishedCommit{}) }

	stuck := newStuckConn()
	defer close(stuck.release)
	handshakeStreamSubscriber(t, hub, stuck, ns)

	healthy := newStuckConn()
	close(healthy.release) // never blocks
	handshakeStreamSubscriber(t, hub, healthy, ns)

	if _, err := rt.Upsert(ns, mustRandomUUID(t), `{"i":1}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for healthy.sent.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("healthy subscriber never received the frame behind a stuck one")
		}
		time.Sleep(time.Millisecond)
	}
}

// handshakeStreamSubscriber registers conn as a Mode 1 subscriber through the hub's real
// handshake path, so the test exercises the same registration Publish fans out to.
func handshakeStreamSubscriber(t *testing.T, hub *StreamHub, conn stream.ConnectionHandle, ns string) {
	t.Helper()
	frame, start := hub.handleHandshake(conn, wire.HandshakeMessage{
		H: wire.Header{MessageType: wire.MsgHandshake, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 1},
		Request: wire.HandshakePayload{
			NodeID:     "sub",
			Namespaces: []string{ns},
			ClientMode: wire.ClientStreamReadOnly,
		},
	})
	if frame == nil || start == nil {
		t.Fatal("handshake was not accepted")
	}
	start()
}

// streamRBACRuntime is a runtime under RBAC with one user allowed to read and write app/data
// ("rw") and one with no grant at all ("nobody").
func streamRBACRuntime(t *testing.T) *KdbServerRuntime {
	t.Helper()
	rt := newTestRuntime(t)
	engine, store := newTestRegistryAuthEngine(t)
	rt.AuthEngine = engine
	if err := store.CreateRole("rw", []string{"read:app/data", "write:app/data"}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateUser("rw", "pw", []string{"rw"}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateUser("nobody", "pw", nil); err != nil {
		t.Fatal(err)
	}
	return rt
}

func connectStream(t *testing.T, addr, user string, mode stream.ClientMode) (*stream.Connection, error) {
	t.Helper()
	subscriber := stream.NewSubscriber(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), nil)
	t.Cleanup(func() { subscriber.Disconnect() })
	cfg := stream.SubscriberConfig{NamespaceID: "app/data", NodeID: "sub-" + user, Mode: mode, CoordinatorURI: "tcp://" + addr}
	if user != "" {
		pw := "pw"
		cfg.User, cfg.Password = &user, &pw
	}
	return subscriber.Connect(cfg)
}

// TestStreamRejectsUnauthenticatedUnderRBAC is D11: the stream handshake never authenticated,
// so under RBAC anyone could subscribe and receive every commit's full operations.
func TestStreamRejectsUnauthenticatedUnderRBAC(t *testing.T) {
	rt := streamRBACRuntime(t)
	_, listener, err := ListenStream("tcp://127.0.0.1:0?bind=true", rt, "app/data")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	addr := listener.Addr().String()
	if _, err := connectStream(t, addr, "", stream.ClientReadOnly); err == nil {
		t.Fatal("an anonymous subscriber was accepted under RBAC")
	}
	if _, err := connectStream(t, addr, "nobody", stream.ClientReadOnly); err == nil {
		t.Fatal("a subscriber with no read grant was accepted")
	}
	if _, err := connectStream(t, addr, "rw", stream.ClientReadOnly); err != nil {
		t.Fatalf("a granted subscriber was refused: %v", err)
	}
}

// TestStreamWriteBackUsesPrincipal: write-back runs as the handshake's principal, so a granted
// subscriber's replay commits.
func TestStreamWriteBackUsesPrincipal(t *testing.T) {
	rt := streamRBACRuntime(t)
	_, listener, err := ListenStream("tcp://127.0.0.1:0?bind=true", rt, "app/data")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	conn, err := connectStream(t, listener.Addr().String(), "rw", stream.ClientWriteBack)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	docID, _ := codec.RandomUUID()
	txID, _ := codec.RandomUUID()
	result := conn.SubmitTransaction(document.Transaction{
		ID: txID, Timestamp: codec.TimestampNow(),
		Operations: []document.Op{document.WriteOp{DocID: docID, Patch: `{"v":"as rw"}`}},
	})
	if result.Rejected != nil {
		t.Fatalf("write-back as a granted principal was rejected: %s", *result.Rejected)
	}
}
