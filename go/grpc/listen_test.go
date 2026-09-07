package kdbgrpc_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	kdbgrpc "github.com/limidus/kdb/go/grpc"
	kdbwirev1 "github.com/limidus/kdb/go/grpc/gen/kdbwire/v1"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/wire"
)

const testNamespace = "app/data"

// conn is a gRPC client stream plus the wire codec, so a test can talk KDB frames.
type conn struct {
	stream kdbwirev1.KdbWire_ConnectClient
	codec  wire.Codec
	nextID int
}

func listen(t *testing.T) (*kdbgrpc.Listener, *grpc.ClientConn) {
	t.Helper()
	rt, err := embed.OpenMemoryRuntime("demo", testNamespace, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Close)

	ln, err := kdbgrpc.Listen("grpc://127.0.0.1:0", server.NewKdbServerRuntime(rt), kdbgrpc.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })

	cc, err := grpc.NewClient(ln.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(wire.DefaultMaxFrameBytes+64),
			grpc.MaxCallSendMsgSize(wire.DefaultMaxFrameBytes+64),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	return ln, cc
}

func open(t *testing.T, cc *grpc.ClientConn) *conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	stream, err := kdbwirev1.NewKdbWireClient(cc).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return &conn{stream: stream, codec: wire.NewCodec(wire.EncodingJSON)}
}

func (c *conn) id() int { c.nextID++; return c.nextID }

func (c *conn) send(t *testing.T, msg wire.Message) {
	t.Helper()
	frame, err := c.codec.Encode(msg)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.stream.Send(&kdbwirev1.Frame{Payload: frame}); err != nil {
		t.Fatalf("send: %v", err)
	}
}

func (c *conn) recv(t *testing.T) wire.Message {
	t.Helper()
	f, err := c.stream.Recv()
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	msg, err := c.codec.Decode(f.GetPayload())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return msg
}

func (c *conn) handshake(t *testing.T) {
	t.Helper()
	c.send(t, wire.HandshakeMessage{
		H: wire.Header{MessageType: wire.MsgHandshake, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.id()},
		Request: wire.HandshakePayload{
			NodeID:     "grpc-test-client",
			Namespaces: []string{testNamespace},
			ClientMode: wire.ClientSQL,
		},
	})
	ack, ok := c.recv(t).(wire.HandshakeAckMessage)
	if !ok || !ack.Response.Accepted {
		t.Fatalf("handshake not accepted: %+v", ack)
	}
}

func (c *conn) beginSession(t *testing.T) string {
	t.Helper()
	c.send(t, wire.SessionBeginMessage{
		H:               wire.Header{MessageType: wire.MsgSessionBegin, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.id()},
		Namespace:       testNamespace,
		ReadConsistency: "READ_COMMITTED",
	})
	ack, ok := c.recv(t).(wire.SessionBeginAckMessage)
	if !ok || ack.SessionID == "" {
		t.Fatalf("session not begun: %+v", ack)
	}
	return ack.SessionID
}

func (c *conn) sql(t *testing.T, sessionID, statement string) wire.SqlResultMessage {
	t.Helper()
	c.send(t, wire.SqlExecMessage{
		H:         wire.Header{MessageType: wire.MsgSqlExec, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.id()},
		SessionID: sessionID,
		Namespace: testNamespace,
		SQL:       statement,
	})
	res, ok := c.recv(t).(wire.SqlResultMessage)
	if !ok {
		t.Fatalf("expected a SQL result, got %T", res)
	}
	return res
}

// commit flushes the session's buffered DML. DML is buffered server-side until an explicit
// TxCommit, so this is required for a write to become visible - and it exercises MsgTxCommit
// over this transport at the same time.
func (c *conn) commit(t *testing.T, sessionID string) {
	t.Helper()
	c.send(t, wire.TxCommitMessage{
		H:         wire.Header{MessageType: wire.MsgTxCommit, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.id()},
		Namespace: testNamespace,
		SessionID: sessionID,
	})
	res, ok := c.recv(t).(wire.SqlResultMessage)
	if !ok || res.Error != nil {
		t.Fatalf("commit failed: %+v", res)
	}
}

// The whole point, end to end: handshake, session, write, read - over HTTP/2, with no part of the
// protocol reimplemented for this transport.
func TestHandshakeSessionAndQueryOverGRPC(t *testing.T) {
	_, cc := listen(t)
	c := open(t, cc)
	c.handshake(t)
	session := c.beginSession(t)

	if res := c.sql(t, session, `INSERT INTO t (name) VALUES ('ada')`); res.Error != nil {
		t.Fatalf("insert failed: %s", *res.Error)
	}
	c.commit(t, session)
	res := c.sql(t, session, `SELECT * FROM t`)
	if res.Error != nil {
		t.Fatalf("select failed: %s", *res.Error)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(res.Rows))
	}
	if !strings.Contains(strings.Join(res.Rows[0], " "), "ada") {
		t.Fatalf("row did not contain the inserted document: %v", res.Rows[0])
	}
}

// One stream is one connection, and a connection's sessions die with it. A session id from a
// closed stream must not be usable on a new one - otherwise a dropped client would leave its
// sessions, and the document locks they hold, alive for the life of the process.
func TestSessionsDieWithTheStream(t *testing.T) {
	_, cc := listen(t)

	first := open(t, cc)
	first.handshake(t)
	stale := first.beginSession(t)
	if err := first.stream.CloseSend(); err != nil {
		t.Fatalf("close send: %v", err)
	}
	// Wait for the server to finish tearing the connection down.
	for {
		if _, err := first.stream.Recv(); err != nil {
			break
		}
	}

	second := open(t, cc)
	second.handshake(t)
	res := second.sql(t, stale, `SELECT * FROM t`)
	if res.Error == nil {
		t.Fatal("a session from a closed stream was still usable on a new one")
	}
}

// gRPC defaults to a 4 MiB receive limit and a KDB frame may be 16 MiB, so a large scan or
// snapshot response would be refused by the transport - and the failure would look like a KDB
// error rather than a transport limit. Listen raises both directions explicitly; this is the
// regression test for that, sized above gRPC's default and below the protocol's own bound.
func TestFrameLargerThanTheGrpcDefaultIsCarried(t *testing.T) {
	_, cc := listen(t)
	c := open(t, cc)
	c.handshake(t)
	session := c.beginSession(t)

	// ~6 MiB of document, comfortably over gRPC's 4 MiB default.
	big := strings.Repeat("x", 6<<20)
	res := c.sql(t, session, `INSERT INTO t (blob) VALUES ('`+big+`')`)
	if res.Error != nil {
		t.Fatalf("a 6 MiB frame was refused: %s", *res.Error)
	}
	c.commit(t, session)
	back := c.sql(t, session, `SELECT COUNT(*) AS n FROM t`)
	if back.Error != nil {
		t.Fatalf("reading it back failed: %s", *back.Error)
	}
	if len(back.Rows) != 1 || back.Rows[0][0] != "1" {
		t.Fatalf("count after the large insert = %v, want one row reading 1", back.Rows)
	}
}

// A grpcs:// address with no TLS settings is refused at Listen rather than quietly served in
// plaintext - a listener that silently downgrades is the worst available outcome.
func TestSecureSchemeRequiresTLS(t *testing.T) {
	rt, err := embed.OpenMemoryRuntime("demo", testNamespace, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	if ln, err := kdbgrpc.Listen("grpcs://127.0.0.1:0", server.NewKdbServerRuntime(rt), kdbgrpc.Options{}); err == nil {
		ln.Close()
		t.Fatal("grpcs:// was served without TLS settings")
	}
}
