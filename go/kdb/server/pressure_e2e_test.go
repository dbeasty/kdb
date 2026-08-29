package server

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/wire"
)

// listenUnderPressure starts a real wire listener whose runtime is pinned into the given pressure
// zone, and returns a connected raw client.
func listenUnderPressure(t *testing.T, usedFraction float64) (*KdbServerRuntime, *rawWireClient) {
	t.Helper()
	srv := newTestRuntime(t)
	srv.SetMemoryLimit(testBudget, 0.85)
	t.Cleanup(func() { srv.memGuard.Stop() })
	srv.memGuard.Stop()
	srv.memGuard.observe(float64(testBudget) * usedFraction)

	ln, err := ListenSqlWire("tcp://127.0.0.1:0?bind=true", srv)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return srv, dialRawWireClient(t, "tcp://"+ln.Addr().String())
}

// The end-to-end claim: a client whose write is shed gets a typed, actionable answer over the
// wire - not a hung connection, not an opaque string. This is the whole difference between a
// server that degrades and one that appears to have died.
func TestWireClientGetsTypedBusyUnderPressure(t *testing.T) {
	_, client := listenUnderPressure(t, 0.90) // ZoneHigh: writes shed, reads served

	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	reply := client.request(t, wire.UpsertMessage{
		H:         wire.Header{MessageType: wire.MsgUpsert, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: client.nextCorrelation()},
		Namespace: "app/data",
		DocID:     docID.String(),
		JSON:      `{"v":1}`,
	})
	result, ok := reply.(wire.UpsertResultMessage)
	if !ok {
		t.Fatalf("expected UpsertResultMessage, got %T", reply)
	}
	if result.Error == nil {
		t.Fatal("expected the write to be refused under ZoneHigh pressure")
	}
	if result.ErrorCode == nil || *result.ErrorCode != wire.ErrorCodeBusy {
		t.Fatalf("expected a BUSY error code, got %v", result.ErrorCode)
	}
	if result.RetryAfterMs == nil || *result.RetryAfterMs <= 0 {
		t.Fatalf("a BUSY response must tell the client when to retry, got %v", result.RetryAfterMs)
	}
}

// The refusal must come from the frame boundary, before the body was ever decoded (§5.4). A
// deliberately malformed body proves it: if the server had decoded the payload it would have
// failed with a decode error instead of the pressure rejection.
func TestPressureRejectionHappensBeforeTheBodyIsDecoded(t *testing.T) {
	srv, _ := listenUnderPressure(t, 0.90)

	// Build a frame whose header says Upsert but whose body is not valid JSON at all.
	frame, err := wire.EncodeFrameOnly(wire.Header{
		MessageType:     wire.MsgUpsert,
		ProtocolVersion: wire.KdbWireProtocolVersion,
		CorrelationID:   4242,
	}, []byte("this is definitively not a valid upsert payload"))
	if err != nil {
		t.Fatal(err)
	}
	header, ok, err := wire.PeekHeader(frame, wire.DefaultMaxFrameBytes)
	if err != nil || !ok {
		t.Fatalf("peek: ok=%v err=%v", ok, err)
	}

	admit := srv.frameAdmitter(wire.NewCodec(wire.EncodingJSON))
	rejection, shedErr := admit(header)
	if shedErr == nil {
		t.Fatal("expected the write to be shed at the frame boundary")
	}
	if rejection == nil {
		t.Fatal("a shed request must carry a typed reply, or the client is left hanging")
	}
	// The reply is well-formed and correlated, built entirely from the header.
	decoded, err := wire.NewCodec(wire.EncodingJSON).Decode(rejection)
	if err != nil {
		t.Fatalf("the rejection frame must be decodable: %v", err)
	}
	result, ok := decoded.(wire.UpsertResultMessage)
	if !ok {
		t.Fatalf("expected UpsertResultMessage, got %T", decoded)
	}
	if decoded.Header().CorrelationID != 4242 {
		t.Errorf("rejection must be correlated to the request, got %d", decoded.Header().CorrelationID)
	}
	if result.ErrorCode == nil || *result.ErrorCode != wire.ErrorCodeBusy {
		t.Errorf("expected BUSY, got %v", result.ErrorCode)
	}
}

// Point reads keep working while writes are shed - the priority order that makes a pressured
// server diagnosable rather than opaque.
func TestReadsStillServedWhileWritesAreShed(t *testing.T) {
	srv, client := listenUnderPressure(t, 0.90)

	admit := srv.frameAdmitter(wire.NewCodec(wire.EncodingJSON))
	readHeader := wire.Header{MessageType: wire.MsgDocumentGet, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 1}
	if rejection, err := admit(readHeader); err != nil || rejection != nil {
		t.Errorf("point reads must never be shed at the frame boundary, got err=%v", err)
	}

	// And a real session still establishes end to end, which is what an operator needs in order
	// to investigate a node that is shedding.
	client.handshake(t, wire.ClientSQL, "app/data")
	ack := client.sessionBegin(t, "app/data", "READ_COMMITTED")
	if ack.SessionID == "" {
		reason := ""
		if ack.Error != nil {
			reason = *ack.Error
		}
		t.Errorf("session establishment must survive memory pressure - refusing it turns a recoverable overload into an opaque one (error: %q)", reason)
	}
}

// Control traffic is never shed: refusing the messages a client needs to establish or unwind a
// session turns a recoverable overload into a stuck one.
func TestControlTrafficIsNeverShed(t *testing.T) {
	srv, _ := listenUnderPressure(t, 0.99) // Critical - the most aggressive zone
	admit := srv.frameAdmitter(wire.NewCodec(wire.EncodingJSON))
	for _, msgType := range []wire.MessageType{wire.MsgHandshake, wire.MsgSessionBegin, wire.MsgTxRollback} {
		h := wire.Header{MessageType: msgType, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 1}
		if rejection, err := admit(h); err != nil || rejection != nil {
			t.Errorf("%v must never be shed, got err=%v", msgType, err)
		}
	}
}

func TestOpClassForMessage(t *testing.T) {
	for _, tc := range []struct {
		msgType wire.MessageType
		want    OpClass
		ok      bool
	}{
		{wire.MsgDocumentGet, ClassPointRead, true},
		{wire.MsgSqlExec, ClassScan, true},
		{wire.MsgUpsert, ClassWrite, true},
		{wire.MsgTxCommit, ClassWrite, true},
		{wire.MsgCommitPush, ClassReplication, true},
		{wire.MsgHandshake, 0, false},
		{wire.MsgSessionBegin, 0, false},
	} {
		got, ok := opClassForMessage(tc.msgType)
		if ok != tc.ok || (ok && got != tc.want) {
			t.Errorf("%v: got (%v, %v), want (%v, %v)", tc.msgType, got, ok, tc.want, tc.ok)
		}
	}
}

// When the server is healthy nothing is shed - the admitter must be invisible in the common case.
func TestNothingShedInNormalZone(t *testing.T) {
	srv, _ := listenUnderPressure(t, 0.10)
	admit := srv.frameAdmitter(wire.NewCodec(wire.EncodingJSON))
	for _, msgType := range []wire.MessageType{
		wire.MsgDocumentGet, wire.MsgSqlExec, wire.MsgUpsert, wire.MsgTxCommit, wire.MsgCommitPush,
	} {
		h := wire.Header{MessageType: msgType, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 1}
		if rejection, err := admit(h); err != nil || rejection != nil {
			t.Errorf("%v must be admitted in ZoneNormal, got err=%v", msgType, err)
		}
	}
}
