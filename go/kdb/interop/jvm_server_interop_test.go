//go:build interop

// Package interop's jvm_server_interop_test.go: component 38 spec §7 test 2 / component 40 spec
// §7 test 1 - a real Go client against a real running JVM kdb-server process, not just wire-shape
// assertions against fixtures (see wire_interop_test.go) or an in-process Go server (see
// go/kdb/client's own tests). Gated behind the "interop" build tag and a running server because
// it needs an external JVM process this package can't spin up itself - go test ./... (no tags)
// skips this file entirely, matching the Lightsail load test's own "needs external infra" shape.
//
// To run: start a JVM server with SQL wire over WebSocket, e.g.
//
//	./gradlew :kdb-service:runService --args="--listen-sql-ws kdb-ws://127.0.0.1:17444/kdb?bind=true --namespace demo/interop"
//
// then:
//
//	KDB_JVM_SQL_WS_URI=ws://127.0.0.1:17444/kdb go test -tags interop ./go/kdb/interop/... -run JvmServer -v
package interop

import (
	"os"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/ws"
	"github.com/limidus/kdb/go/kdb/wire"
)

func TestGoClientAgainstRealJvmServer(t *testing.T) {
	uri := os.Getenv("KDB_JVM_SQL_WS_URI")
	if uri == "" {
		t.Skip("KDB_JVM_SQL_WS_URI not set - see file doc comment for how to start a JVM server to test against")
	}
	namespace := os.Getenv("KDB_JVM_SQL_NAMESPACE")
	if namespace == "" {
		namespace = "demo/interop"
	}

	transport := ws.NewTransport(core.DefaultConnectOptions())
	conn, err := transport.Connect(uri)
	if err != nil {
		t.Fatalf("connect to %s: %v", uri, err)
	}
	defer conn.Close()

	c := wire.NewCodec(wire.EncodingJSON)
	nodeID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	corr := 1
	req := func(msg wire.Message) wire.Message {
		t.Helper()
		frame, err := c.Encode(msg)
		if err != nil {
			t.Fatalf("encode %T: %v", msg, err)
		}
		if err := conn.Send(frame); err != nil {
			t.Fatalf("send %T: %v", msg, err)
		}
		resp, ok := <-conn.Incoming()
		if !ok {
			t.Fatalf("connection closed while awaiting response to %T", msg)
		}
		decoded, err := c.Decode(resp)
		if err != nil {
			t.Fatalf("decode response to %T: %v (raw=%q)", msg, err, string(resp))
		}
		corr++
		return decoded
	}

	ackMsg := req(wire.HandshakeMessage{
		H: wire.Header{MessageType: wire.MsgHandshake, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: corr},
		Request: wire.HandshakePayload{
			NodeID:     nodeID.String(),
			Namespaces: []string{namespace},
			ClientMode: wire.ClientSQL,
		},
	})
	ack, ok := ackMsg.(wire.HandshakeAckMessage)
	if !ok {
		t.Fatalf("expected HandshakeAckMessage, got %T", ackMsg)
	}
	if !ack.Response.Accepted {
		reason := "unknown"
		if ack.Response.RejectionReason != nil {
			reason = *ack.Response.RejectionReason
		}
		t.Fatalf("JVM server rejected handshake: %s", reason)
	}

	sbMsg := req(wire.SessionBeginMessage{
		H:               wire.Header{MessageType: wire.MsgSessionBegin, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: corr},
		Namespace:       namespace,
		ReadConsistency: "READ_COMMITTED",
	})
	sb, ok := sbMsg.(wire.SessionBeginAckMessage)
	if !ok {
		t.Fatalf("expected SessionBeginAckMessage, got %T", sbMsg)
	}
	if sb.SessionID == "" {
		t.Fatal("expected a non-empty session id")
	}

	marker := "go-jvm-interop-" + nodeID.String()
	insertMsg := req(wire.SqlExecMessage{
		H:         wire.Header{MessageType: wire.MsgSqlExec, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: corr},
		Namespace: namespace,
		SessionID: sb.SessionID,
		SQL:       `INSERT INTO users (_doc) VALUES ('{"marker":"` + marker + `"}')`,
	})
	insertResult, ok := insertMsg.(wire.SqlResultMessage)
	if !ok {
		t.Fatalf("expected SqlResultMessage, got %T", insertMsg)
	}
	if insertResult.Error != nil {
		t.Fatalf("INSERT against a real JVM server failed: %s", *insertResult.Error)
	}
	if insertResult.RowsAffected != 1 {
		t.Fatalf("expected 1 row affected, got %d", insertResult.RowsAffected)
	}

	selectMsg := req(wire.SqlExecMessage{
		H:         wire.Header{MessageType: wire.MsgSqlExec, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: corr},
		Namespace: namespace,
		SessionID: sb.SessionID,
		SQL:       `SELECT _doc FROM users`,
	})
	selectResult, ok := selectMsg.(wire.SqlResultMessage)
	if !ok {
		t.Fatalf("expected SqlResultMessage, got %T", selectMsg)
	}
	if selectResult.Error != nil {
		t.Fatalf("SELECT against a real JVM server failed: %s", *selectResult.Error)
	}
	found := false
	for _, row := range selectResult.Rows {
		for _, cell := range row {
			if strings.Contains(cell, marker) {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("expected a row containing marker %q among %d rows, found none", marker, len(selectResult.Rows))
	}
}
