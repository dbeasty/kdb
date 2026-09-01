package server

import (
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
}

func newStuckConn() *stuckConn { return &stuckConn{release: make(chan struct{})} }

func (c *stuckConn) Send(frame []byte) error {
	<-c.release
	c.sent.Add(1)
	return nil
}
func (c *stuckConn) Incoming() <-chan []byte { return nil }
func (c *stuckConn) Close() error            { return nil }
func (c *stuckConn) TryPoll() []byte         { return nil }

// TestPublishDoesNotBlockOnAStuckSubscriber is the regression test for the fan-out hazard.
// Publish used to call conn.Send inline for every subscriber, and socketConnection.Send does a
// blocking socket write - so one subscriber that had stopped reading stalled the whole fan-out
// and, behind it, the goroutine that had just committed (Publish is called from
// KdbServerRuntime.CommitListener on the commit path). Every subscriber now has its own queue
// and its own sender goroutine, so Publish returns regardless.
func TestPublishDoesNotBlockOnAStuckSubscriber(t *testing.T) {
	const ns = "app/data"
	rt := newTestRuntime(t)
	hub := NewStreamHub(wire.NewCodec(wire.EncodingJSON), ns, rt)

	stuck := newStuckConn()
	defer close(stuck.release)
	handshakeStreamSubscriber(t, hub, stuck, ns)

	// One more frame than the queue can hold: the first fills it (the sender goroutine is parked
	// inside the blocked Send), the rest are dropped. None of it may block Publish.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < subscriberQueueDepth+8; i++ {
			hub.Publish(stream.PublishedCommit{TimestampMicros: int64(i)})
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Publish blocked on a subscriber that stopped reading")
	}

	if dropped := hub.DroppedFrames(); dropped == 0 {
		t.Fatal("expected the overflowing frames to be counted as dropped, got 0")
	}
}

// TestPublishReachesAHealthySubscriberBesideAStuckOne: the fan-out is per-subscriber, so a
// subscriber that stopped reading must not cost the others their frames either.
func TestPublishReachesAHealthySubscriberBesideAStuckOne(t *testing.T) {
	const ns = "app/data"
	rt := newTestRuntime(t)
	hub := NewStreamHub(wire.NewCodec(wire.EncodingJSON), ns, rt)

	stuck := newStuckConn()
	defer close(stuck.release)
	handshakeStreamSubscriber(t, hub, stuck, ns)

	healthy := newStuckConn()
	close(healthy.release) // never blocks
	handshakeStreamSubscriber(t, hub, healthy, ns)

	hub.Publish(stream.PublishedCommit{TimestampMicros: 1})

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
	frame := hub.handleHandshake(conn, wire.HandshakeMessage{
		H: wire.Header{MessageType: wire.MsgHandshake, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 1},
		Request: wire.HandshakePayload{
			NodeID:     "sub",
			Namespaces: []string{ns},
			ClientMode: wire.ClientStreamReadOnly,
		},
	})
	if frame == nil {
		t.Fatal("handshake produced no ack frame")
	}
}
