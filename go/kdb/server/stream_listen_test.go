package server

import (
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
