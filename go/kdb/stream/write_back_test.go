package stream_test

import (
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/wire"
)

func newWriteBackTestRuntime(t *testing.T, ns string) *server.KdbServerRuntime {
	t.Helper()
	rt, err := embed.OpenMemoryRuntime("demo", ns, schema.None())
	if err != nil {
		t.Fatalf("open memory runtime: %v", err)
	}
	return server.NewKdbServerRuntime(rt)
}

// TestSubmitTransactionAppliesOverWriteBackStream is the end-to-end regression test for the gap
// this closes: go/kdb/stream's Mode 2 (write-back) client previously did nothing at all -
// Connect's ClientWriteBack branch was an empty comment, and the old package-level
// SubmitTransaction always returned Rejected("async replay not awaited in v1") without even
// sending a frame. This proves a real TransactionReplay round trip: a write-back subscriber
// submits a transaction, the coordinator's TransactionReplayer routes it into a real
// KdbServerRuntime.Replay (the same server-side path wire_listen.go's SQL_CLIENT handler uses),
// and the resulting commit is both reported back to the caller and actually queryable.
func TestSubmitTransactionAppliesOverWriteBackStream(t *testing.T) {
	ns := "app/write-back"
	w := wire.NewCodec(wire.EncodingKdbBinary)
	transport := stream.NewInMemoryTransport()
	rt := newWriteBackTestRuntime(t, ns)

	coordinator := stream.NewCoordinator(w, transport)
	coordinator.SetTransactionReplayer(func(msg wire.TransactionReplayMessage) wire.Message {
		tx, err := wire.DecodeTransaction(msg.TransactionBytes)
		if err != nil {
			errMsg := err.Error()
			return wire.SqlResultMessage{
				H:         wire.Header{MessageType: wire.MsgSqlResult, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: msg.H.CorrelationID},
				Namespace: msg.Namespace,
				Error:     &errMsg,
			}
		}
		head, err := rt.Runtime.DAG.Head()
		if err != nil {
			errMsg := err.Error()
			return wire.SqlResultMessage{
				H:         wire.Header{MessageType: wire.MsgSqlResult, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: msg.H.CorrelationID},
				Namespace: msg.Namespace,
				Error:     &errMsg,
			}
		}
		commit, err := rt.Replay(msg.Namespace, tx, head, auth.Principal{})
		if err != nil {
			errMsg := err.Error()
			return wire.SqlResultMessage{
				H:         wire.Header{MessageType: wire.MsgSqlResult, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: msg.H.CorrelationID},
				Namespace: msg.Namespace,
				Error:     &errMsg,
			}
		}
		return wire.SqlResultMessage{
			H:                 wire.Header{MessageType: wire.MsgSqlResult, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: msg.H.CorrelationID},
			Namespace:         msg.Namespace,
			RowsAffected:      len(tx.Operations),
			ResolvedCommitHex: commit.Hash.Hex(),
		}
	})
	if err := coordinator.Start(stream.SessionConfig{
		NamespaceID:  ns,
		NodeID:       "coord",
		HeadProvider: rt.Runtime.DAG.Head,
	}); err != nil {
		t.Fatal(err)
	}
	defer coordinator.Stop()

	subscriber := stream.NewSubscriber(w, transport, nil)
	head, err := rt.Runtime.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	conn, err := subscriber.Connect(stream.SubscriberConfig{
		NamespaceID:    ns,
		NodeID:         "sub",
		Mode:           stream.ClientWriteBack,
		CoordinatorURI: "memory://" + ns,
		ResumeFrom:     &head,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer subscriber.Disconnect()

	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	txID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	tx := document.Transaction{
		ID:          txID,
		BaseVersion: head,
		Operations:  []document.Op{document.WriteOp{DocID: docID, Patch: `{"v":"write-back"}`}},
		Timestamp:   codec.TimestampNow(),
	}

	if conn.SubmitTransaction == nil {
		t.Fatal("expected Connection.SubmitTransaction to be set for a write-back connection")
	}
	result := conn.SubmitTransaction(tx)
	if result.Rejected != nil {
		t.Fatalf("expected the transaction to apply, got Rejected: %s", *result.Rejected)
	}
	if result.Conflict != nil {
		t.Fatalf("expected no conflict, got %+v", result.Conflict)
	}
	if result.Applied == nil {
		t.Fatal("expected Applied to be set")
	}

	json, _, found, err := rt.GetDocument(ns, docID)
	if err != nil {
		t.Fatalf("GetDocument: %v", err)
	}
	if !found {
		t.Fatal("expected the replayed write to be visible via GetDocument")
	}
	if json != `{"v":"write-back"}` {
		t.Fatalf("expected the submitted document, got %q", json)
	}
}

// TestSubmitTransactionRejectedWithoutReplayerWired proves the explicit-rejection default
// (mirrors Kotlin's own default: TransactionReplay is refused, not silently dropped, when no
// TransactionReplayer has been wired) - SubmitTransaction must not hang until timeout for a
// coordinator that was never going to answer.
func TestSubmitTransactionRejectedWithoutReplayerWired(t *testing.T) {
	ns := "app/write-back-unwired"
	w := wire.NewCodec(wire.EncodingKdbBinary)
	transport := stream.NewInMemoryTransport()

	coordinator := stream.NewCoordinator(w, transport)
	if err := coordinator.Start(stream.SessionConfig{
		NamespaceID:  ns,
		NodeID:       "coord",
		HeadProvider: func() (codec.Hash, error) { return codec.Hash{}, nil },
	}); err != nil {
		t.Fatal(err)
	}
	defer coordinator.Stop()

	subscriber := stream.NewSubscriber(w, transport, nil)
	conn, err := subscriber.Connect(stream.SubscriberConfig{
		NamespaceID:    ns,
		NodeID:         "sub",
		Mode:           stream.ClientWriteBack,
		CoordinatorURI: "memory://" + ns,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer subscriber.Disconnect()

	txID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	tx := document.Transaction{ID: txID, Timestamp: codec.TimestampNow()}

	done := make(chan stream.ReplayResult, 1)
	go func() { done <- conn.SubmitTransaction(tx) }()

	select {
	case result := <-done:
		if result.Rejected == nil {
			t.Fatalf("expected Rejected, got %+v", result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SubmitTransaction did not return promptly - it should not need to wait out the full replay timeout for an explicit rejection")
	}
}
