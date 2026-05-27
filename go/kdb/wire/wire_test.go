package wire_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/wire"
)

func testHeader(correlation int, msgType wire.MessageType) wire.Header {
	return wire.Header{
		MessageType:     msgType,
		ProtocolVersion: wire.KdbWireProtocolVersion,
		CorrelationID:   correlation,
	}
}

func repeatHex(b byte) codec.Hash {
	h, _ := codec.HashFromHex(strings.Repeat(fmt.Sprintf("%02x", b), 32))
	return h
}

func TestFrameRoundtripHandshake(t *testing.T) {
	c := wire.NewCodec(wire.EncodingKdbBinary)
	msg := wire.HandshakeMessage{
		H: testHeader(1, wire.MsgHandshake),
		Request: wire.HandshakePayload{
			NodeID:     "node-a",
			Namespaces: []string{"app/data"},
			LocalHeads: map[string]string{"app/data": repeatHex(0).Hex()},
			ClientMode: wire.ClientStreamReadOnly,
		},
	}
	frame, err := c.Encode(msg)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := c.Decode(frame)
	if err != nil {
		t.Fatal(err)
	}
	back, ok := decoded.(wire.HandshakeMessage)
	if !ok {
		t.Fatalf("expected HandshakeMessage, got %T", decoded)
	}
	if back.Request.NodeID != "node-a" {
		t.Fatalf("nodeId: %q", back.Request.NodeID)
	}
	if back.Request.ClientMode != wire.ClientStreamReadOnly {
		t.Fatalf("clientMode: %v", back.Request.ClientMode)
	}
}

func TestFrameRoundtripDeltaCommit(t *testing.T) {
	c := wire.NewCodec(wire.EncodingKdbBinary)
	hash := repeatHex(0xaa)
	parent := repeatHex(0xbb)
	msg := wire.DeltaCommitMessage{
		H: testHeader(2, wire.MsgDeltaCommit),
		Payload: wire.DeltaCommitPayload{
			Namespace:       "app/events",
			CommitHash:      hash,
			ParentHash:      parent,
			TimestampMicros: 1_700_000_000_000_000,
		},
	}
	decoded, err := c.Decode(mustEncode(t, c, msg))
	if err != nil {
		t.Fatal(err)
	}
	back := decoded.(wire.DeltaCommitMessage)
	if back.Payload.CommitHash != hash || back.Payload.ParentHash != parent {
		t.Fatalf("hashes mismatch")
	}
}

func TestFrameRoundtripDeltaCommitWithOps(t *testing.T) {
	c := wire.NewCodec(wire.EncodingKdbBinary)
	docID, _ := codec.UUIDFromString("550e8400-e29b-41d4-a716-446655440000")
	msg := wire.DeltaCommitMessage{
		H: testHeader(20, wire.MsgDeltaCommit),
		Payload: wire.DeltaCommitPayload{
			Namespace:       "app/data",
			CommitHash:      repeatHex(0x11),
			ParentHash:      repeatHex(0x22),
			TimestampMicros: 1_700_000_000_000_001,
			Operations:      []document.Op{document.WriteOp{DocID: docID, Patch: `{"k":"v"}`}},
		},
	}
	decoded, err := c.Decode(mustEncode(t, c, msg))
	if err != nil {
		t.Fatal(err)
	}
	back := decoded.(wire.DeltaCommitMessage)
	if len(back.Payload.Operations) != 1 {
		t.Fatalf("ops: %d", len(back.Payload.Operations))
	}
	op, ok := back.Payload.Operations[0].(document.WriteOp)
	if !ok || op.Patch != `{"k":"v"}` {
		t.Fatalf("write op: %+v", back.Payload.Operations[0])
	}
}

func TestRejectOversizedFrame(t *testing.T) {
	err := wire.ValidateFrameLength(wire.DefaultMaxFrameBytes+1, wire.DefaultMaxFrameBytes)
	var fe *wire.FrameTooLargeError
	if err == nil {
		t.Fatal("expected error")
	}
	if !asError(err, &fe) {
		t.Fatalf("expected FrameTooLargeError, got %T", err)
	}
}

func TestRejectTruncatedFrame(t *testing.T) {
	c := wire.NewCodec(wire.EncodingKdbBinary)
	_, err := c.Decode([]byte{0, 0, 0, 8})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNegotiatePrefersBinary(t *testing.T) {
	n := wire.NewHandshakeNegotiator()
	local := wire.HandshakePayload{
		NodeID:             "a",
		Namespaces:         []string{"ns"},
		PreferredEncodings: []wire.PayloadEncoding{wire.EncodingKdbBinary, wire.EncodingJSON},
		ClientMode:         wire.ClientStreamReadOnly,
	}
	remote := local
	remote.NodeID = "b"
	ack, err := n.Negotiate(local, remote)
	if err != nil || ack.NegotiatedEncoding != wire.EncodingKdbBinary || !ack.Accepted {
		t.Fatalf("ack: %+v err=%v", ack, err)
	}
}

func TestNegotiateFailsNoCommon(t *testing.T) {
	n := wire.NewHandshakeNegotiator()
	local := wire.HandshakePayload{
		NodeID:             "a",
		Namespaces:         []string{"ns"},
		PreferredEncodings: []wire.PayloadEncoding{wire.EncodingJSON},
		ClientMode:         wire.ClientStreamReadOnly,
	}
	remote := local
	remote.PreferredEncodings = []wire.PayloadEncoding{wire.EncodingKdbBinary}
	_, err := n.Negotiate(local, remote)
	var encErr *kdberr.EncodingNegotiationFailureError
	if !asError(err, &encErr) {
		t.Fatalf("expected EncodingNegotiationFailureError, got %T", err)
	}
}

func TestVersionRejectFuture(t *testing.T) {
	n := wire.NewHandshakeNegotiator()
	local := wire.HandshakePayload{NodeID: "a", Namespaces: []string{"ns"}, ClientMode: wire.ClientStreamReadOnly}
	remote := local
	remote.ProtocolVersion = 99
	_, err := n.Negotiate(local, remote)
	var verErr *kdberr.UnsupportedProtocolVersionError
	if !asError(err, &verErr) {
		t.Fatalf("expected UnsupportedProtocolVersionError, got %T", err)
	}
}

func TestCompactionNoticeRoundtrip(t *testing.T) {
	c := wire.NewCodec(wire.EncodingKdbBinary)
	msg := wire.CompactionNoticeMessage{
		H: testHeader(3, wire.MsgCompactionNotice),
		Intent: wire.CompactionIntent{
			NamespaceID: "app/data", Boundary: repeatHex(0xcc), IssuedAtMillis: 1000,
		},
	}
	back := decodedAs[wire.CompactionNoticeMessage](t, c, msg)
	if back.Intent.Boundary != repeatHex(0xcc) {
		t.Fatalf("boundary mismatch")
	}
}

func TestPositionAckRoundtrip(t *testing.T) {
	c := wire.NewCodec(wire.EncodingKdbBinary)
	hash := repeatHex(0xff)
	msg := wire.PositionAckMessage{H: testHeader(5, wire.MsgPositionAck), Namespace: "app/data", CommitHash: hash}
	back := decodedAs[wire.PositionAckMessage](t, c, msg)
	if back.CommitHash != hash {
		t.Fatalf("hash mismatch")
	}
}

func TestJSONEncodingRoundtrip(t *testing.T) {
	c := wire.NewCodec(wire.EncodingJSON)
	msg := wire.HandshakeMessage{
		H: testHeader(6, wire.MsgHandshake),
		Request: wire.HandshakePayload{
			NodeID: "n", Namespaces: []string{"x"}, ClientMode: wire.ClientStreamWriteBack,
		},
	}
	back := decodedAs[wire.HandshakeMessage](t, c, msg)
	if back.Request.ClientMode != wire.ClientStreamWriteBack {
		t.Fatalf("mode: %v", back.Request.ClientMode)
	}
}

func mustEncode(t *testing.T, c *wire.DefaultCodec, msg wire.Message) []byte {
	t.Helper()
	b, err := c.Encode(msg)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func decodedAs[T wire.Message](t *testing.T, c *wire.DefaultCodec, msg wire.Message) T {
	t.Helper()
	decoded, err := c.Decode(mustEncode(t, c, msg))
	if err != nil {
		t.Fatal(err)
	}
	back, ok := decoded.(T)
	if !ok {
		t.Fatalf("expected %T, got %T", *new(T), decoded)
	}
	return back
}

func asError[T error](err error, target *T) bool {
	if err == nil {
		return false
	}
	if e, ok := err.(T); ok {
		*target = e
		return true
	}
	return false
}
