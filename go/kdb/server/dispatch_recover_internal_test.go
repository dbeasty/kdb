package server

import (
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/wire"
)

// panickingParser stands in for whatever panics next.
//
// The specific bug that motivated the backstop - readIdentifier panicking on `SELECT 1` - is
// fixed at the source, so the end-to-end test can no longer reach it. That is exactly why this
// test injects a panic instead of relying on a real one: the property worth protecting is "no
// single request can end the process", not "that one statement is safe", and a backstop with no
// remaining trigger is a backstop nothing checks.
type panickingParser struct{ msg string }

func (p panickingParser) Parse(string) (sql.Statement, error) {
	panic(p.msg)
}

// newTestHandler builds a handler that has already handshaked and opened a session, and
// returns the session id.
//
// Both steps are necessary rather than ceremonial: handleSqlExec looks the session up before it
// ever reaches the parser, so a handler without one answers "unknown session" and the injected
// panic never fires - a test that would pass while proving nothing.
func newTestHandler(t *testing.T, parser sql.Parser) (*sqlWireConnHandler, string) {
	t.Helper()
	runtime, err := embed.OpenMemoryRuntime("demo", "app/data", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	h := newSqlWireConnHandler(wire.NewCodec(wire.EncodingJSON), NewKdbServerRuntime(runtime))

	ack, ok := h.dispatch(wire.HandshakeMessage{
		H:       wire.Header{MessageType: wire.MsgHandshake, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 1},
		Request: wire.HandshakePayload{NodeID: "test", ClientMode: wire.ClientSQL},
	}).(wire.HandshakeAckMessage)
	if !ok || !ack.Response.Accepted {
		t.Fatalf("handshake rejected: %+v", ack)
	}

	begun, ok := h.dispatch(wire.SessionBeginMessage{
		H:               wire.Header{MessageType: wire.MsgSessionBegin, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 2},
		Namespace:       "app/data",
		ReadConsistency: "READ_COMMITTED",
	}).(wire.SessionBeginAckMessage)
	if !ok || begun.SessionID == "" {
		errMsg := ""
		if begun.Error != nil {
			errMsg = *begun.Error
		}
		t.Fatalf("session begin rejected: %s", errMsg)
	}

	// Injected after the setup dispatches, so the handshake and session begin above run against
	// the real parser.
	h.parser = parser
	return h, begun.SessionID
}

func TestDispatchRecoveringTurnsAPanicIntoATypedReply(t *testing.T) {
	h, sessionID := newTestHandler(t, panickingParser{msg: "boom"})

	msg := wire.SqlExecMessage{
		H: wire.Header{
			MessageType:     wire.MsgSqlExec,
			ProtocolVersion: wire.KdbWireProtocolVersion,
			CorrelationID:   4242,
		},
		Namespace: "app/data",
		SessionID: sessionID,
		SQL:       "SELECT anything",
	}

	// The call must return rather than unwind - without the recover this takes the test binary
	// down, which is precisely what it did to the server process.
	reply := h.dispatchRecovering(msg)
	if reply == nil {
		t.Fatal("expected a reply, got nil - a dropped reply leaves the caller waiting out its own deadline")
	}

	result, ok := reply.(wire.SqlResultMessage)
	if !ok {
		t.Fatalf("got %T, want wire.SqlResultMessage", reply)
	}
	// The correlation id is what lets the client match this to the call it is blocked on. A
	// reply carrying the wrong one is indistinguishable from an unrelated frame, and the caller
	// hangs anyway.
	if result.H.CorrelationID != 4242 {
		t.Fatalf("correlation id = %d, want 4242", result.H.CorrelationID)
	}
	if result.Error == nil || !strings.Contains(*result.Error, "internal server error") {
		t.Fatalf("expected an internal-error message, got %+v", result.Error)
	}
	if result.ErrorCode == nil || *result.ErrorCode != wire.ErrorCodeInternal {
		t.Fatalf("expected ErrorCodeInternal, got %+v", result.ErrorCode)
	}
	// Namespace and session are echoed so a client with several sessions can tell which one
	// failed.
	if result.Namespace != "app/data" || result.SessionID != sessionID {
		t.Fatalf("reply lost its namespace/session: %+v", result)
	}
}

// TestDispatchRecoveringLeavesTheHandlerUsable pins the resynchronization property: a recovered
// panic must not poison the connection, or one bad statement still costs the caller everything
// it was going to do next.
func TestDispatchRecoveringLeavesTheHandlerUsable(t *testing.T) {
	h, sessionID := newTestHandler(t, panickingParser{msg: "boom"})

	for i := 1; i <= 3; i++ {
		reply := h.dispatchRecovering(wire.SqlExecMessage{
			H:         wire.Header{MessageType: wire.MsgSqlExec, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: i},
			Namespace: "app/data",
			SessionID: sessionID,
			SQL:       "SELECT anything",
		})
		result, ok := reply.(wire.SqlResultMessage)
		if !ok {
			t.Fatalf("attempt %d: got %T, want wire.SqlResultMessage", i, reply)
		}
		if result.H.CorrelationID != i {
			t.Fatalf("attempt %d: correlation id = %d", i, result.H.CorrelationID)
		}
	}

	// With a working parser again, the same handler and the same session still serve normally -
	// a recovered panic must not have poisoned either.
	h.parser = sql.DefaultParser{}
	reply := h.dispatchRecovering(wire.SqlExecMessage{
		H:         wire.Header{MessageType: wire.MsgSqlExec, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 99},
		Namespace: "app/data",
		SessionID: sessionID,
		SQL:       `CREATE TABLE players (name VARCHAR NOT NULL)`,
	})
	result, ok := reply.(wire.SqlResultMessage)
	if !ok {
		t.Fatalf("got %T, want wire.SqlResultMessage", reply)
	}
	if result.Error != nil {
		t.Fatalf("handler stopped serving after a recovered panic: %s", *result.Error)
	}
}

// TestInternalErrorReplyPreservesCorrelationForEveryRequestKind checks the fallback for message
// types that are not SqlExec - a panic in an upsert or a commit has to be answerable too.
func TestInternalErrorReplyPreservesCorrelationForEveryRequestKind(t *testing.T) {
	cases := []struct {
		name              string
		msg               wire.Message
		wantNS, wantSess  string
		wantCorrelationID int
	}{
		{
			name: "sqlExec",
			msg: wire.SqlExecMessage{
				H:         wire.Header{MessageType: wire.MsgSqlExec, CorrelationID: 1},
				Namespace: "ns", SessionID: "s1",
			},
			wantNS: "ns", wantSess: "s1", wantCorrelationID: 1,
		},
		{
			name: "txCommit",
			msg: wire.TxCommitMessage{
				H:         wire.Header{MessageType: wire.MsgTxCommit, CorrelationID: 2},
				Namespace: "ns", SessionID: "s2",
			},
			wantNS: "ns", wantSess: "s2", wantCorrelationID: 2,
		},
		{
			name: "upsert",
			msg: wire.UpsertMessage{
				H:         wire.Header{MessageType: wire.MsgUpsert, CorrelationID: 3},
				Namespace: "ns", SessionID: "s3",
			},
			wantNS: "ns", wantSess: "s3", wantCorrelationID: 3,
		},
		{
			name: "documentGet",
			msg: wire.DocumentGetMessage{
				H:         wire.Header{MessageType: wire.MsgDocumentGet, CorrelationID: 4},
				Namespace: "ns",
			},
			wantNS: "ns", wantSess: "", wantCorrelationID: 4,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reply, ok := internalErrorReply(tc.msg).(wire.SqlResultMessage)
			if !ok {
				t.Fatalf("internalErrorReply returned %T", internalErrorReply(tc.msg))
			}
			if reply.H.CorrelationID != tc.wantCorrelationID {
				t.Fatalf("correlation id = %d, want %d", reply.H.CorrelationID, tc.wantCorrelationID)
			}
			if reply.Namespace != tc.wantNS || reply.SessionID != tc.wantSess {
				t.Fatalf("namespace/session = %q/%q, want %q/%q",
					reply.Namespace, reply.SessionID, tc.wantNS, tc.wantSess)
			}
			if reply.ErrorCode == nil || *reply.ErrorCode != wire.ErrorCodeInternal {
				t.Fatalf("expected ErrorCodeInternal, got %+v", reply.ErrorCode)
			}
		})
	}
}
