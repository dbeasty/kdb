package wire_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
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

func TestCommitPushAckRoundtrip(t *testing.T) {
	c := wire.NewCodec(wire.EncodingKdbBinary)
	head := repeatHex(0xab)
	msg := wire.CommitPushAckMessage{
		H: testHeader(7, wire.MsgCommitPushAck), Namespace: "app/data", AppliedCommits: 3, HeadHex: head.Hex(),
	}
	back := decodedAs[wire.CommitPushAckMessage](t, c, msg)
	if back.AppliedCommits != 3 || back.HeadHex != head.Hex() || back.Namespace != "app/data" {
		t.Fatalf("commitPushAck mismatch: %+v", back)
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

func strPtr(s string) *string { return &s }

func TestFrameRoundtripSqlClientHandshake(t *testing.T) {
	c := wire.NewCodec(wire.EncodingJSON)
	msg := wire.HandshakeMessage{
		H: testHeader(20, wire.MsgHandshake),
		Request: wire.HandshakePayload{
			NodeID:     "sql-client-1",
			Namespaces: []string{"app/data"},
			ClientMode: wire.ClientSQL,
		},
	}
	back := decodedAs[wire.HandshakeMessage](t, c, msg)
	if back.Request.ClientMode != wire.ClientSQL {
		t.Fatalf("clientMode: %v", back.Request.ClientMode)
	}
}

func TestFrameRoundtripSessionBegin(t *testing.T) {
	c := wire.NewCodec(wire.EncodingJSON)
	msg := wire.SessionBeginMessage{
		H:               testHeader(21, wire.MsgSessionBegin),
		Namespace:       "app/data",
		SessionID:       strPtr("sess-1"),
		ReadConsistency: "SNAPSHOT",
		BaseVersionHex:  strPtr(repeatHex(0).Hex()),
	}
	back := decodedAs[wire.SessionBeginMessage](t, c, msg)
	if back.Namespace != "app/data" || back.ReadConsistency != "SNAPSHOT" {
		t.Fatalf("session begin: %+v", back)
	}
	if back.SessionID == nil || *back.SessionID != "sess-1" {
		t.Fatalf("sessionId: %+v", back.SessionID)
	}
}

func TestFrameRoundtripSessionBeginAck(t *testing.T) {
	c := wire.NewCodec(wire.EncodingJSON)
	msg := wire.SessionBeginAckMessage{
		H:               testHeader(22, wire.MsgSessionBeginAck),
		Namespace:       "app/data",
		SessionID:       "sess-1",
		HeadHex:         repeatHex(0).Hex(),
		ReadConsistency: "SNAPSHOT",
	}
	back := decodedAs[wire.SessionBeginAckMessage](t, c, msg)
	if back.SessionID != "sess-1" || back.HeadHex != repeatHex(0).Hex() {
		t.Fatalf("session begin ack: %+v", back)
	}
}

func TestFrameRoundtripSqlExec(t *testing.T) {
	c := wire.NewCodec(wire.EncodingJSON)
	msg := wire.SqlExecMessage{
		H:              testHeader(23, wire.MsgSqlExec),
		Namespace:      "app/data",
		SessionID:      "sess-1",
		SQL:            "SELECT * FROM users",
		ParametersJSON: strPtr(`{"1":"x"}`),
	}
	back := decodedAs[wire.SqlExecMessage](t, c, msg)
	if back.SQL != "SELECT * FROM users" || back.SessionID != "sess-1" {
		t.Fatalf("sql exec: %+v", back)
	}
	if back.ParametersJSON == nil || *back.ParametersJSON != `{"1":"x"}` {
		t.Fatalf("parametersJson: %+v", back.ParametersJSON)
	}
}

func TestFrameRoundtripSqlResult(t *testing.T) {
	c := wire.NewCodec(wire.EncodingJSON)
	msg := wire.SqlResultMessage{
		H:                 testHeader(24, wire.MsgSqlResult),
		Namespace:         "app/data",
		SessionID:         "sess-1",
		Columns:           []string{"id", "name"},
		Rows:              [][]string{{"1", "alice"}},
		RowsAffected:      1,
		ResolvedCommitHex: repeatHex(0xaa).Hex(),
		ReadOnly:          false,
		GeneratedIDs:      []string{"g1"},
	}
	back := decodedAs[wire.SqlResultMessage](t, c, msg)
	if len(back.Columns) != 2 || back.Columns[1] != "name" {
		t.Fatalf("columns: %+v", back.Columns)
	}
	if len(back.Rows) != 1 || back.Rows[0][1] != "alice" {
		t.Fatalf("rows: %+v", back.Rows)
	}
	if back.Error != nil {
		t.Fatalf("expected no error, got %v", *back.Error)
	}
}

func TestFrameRoundtripSqlResultError(t *testing.T) {
	c := wire.NewCodec(wire.EncodingJSON)
	msg := wire.SqlResultMessage{
		H:         testHeader(25, wire.MsgSqlResult),
		Namespace: "app/data",
		SessionID: "sess-1",
		ReadOnly:  true,
		Error:     strPtr("kdb server: commit/query not yet implemented in Go port"),
	}
	back := decodedAs[wire.SqlResultMessage](t, c, msg)
	if back.Error == nil || *back.Error != "kdb server: commit/query not yet implemented in Go port" {
		t.Fatalf("error: %+v", back.Error)
	}
}

func TestFrameRoundtripTxCommit(t *testing.T) {
	c := wire.NewCodec(wire.EncodingJSON)
	msg := wire.TxCommitMessage{
		H:                testHeader(26, wire.MsgTxCommit),
		Namespace:        "app/data",
		SessionID:        "sess-1",
		TransactionBytes: []byte{1, 2, 3, 4},
	}
	back := decodedAs[wire.TxCommitMessage](t, c, msg)
	if len(back.TransactionBytes) != 4 || back.TransactionBytes[2] != 3 {
		t.Fatalf("transactionBytes: %+v", back.TransactionBytes)
	}
}

func TestFrameRoundtripTxRollback(t *testing.T) {
	c := wire.NewCodec(wire.EncodingJSON)
	msg := wire.TxRollbackMessage{
		H:         testHeader(27, wire.MsgTxRollback),
		Namespace: "app/data",
		SessionID: "sess-1",
	}
	back := decodedAs[wire.TxRollbackMessage](t, c, msg)
	if back.SessionID != "sess-1" {
		t.Fatalf("rollback: %+v", back)
	}
}

func TestFrameRoundtripDocumentGet(t *testing.T) {
	c := wire.NewCodec(wire.EncodingJSON)
	msg := wire.DocumentGetMessage{
		H:         testHeader(30, wire.MsgDocumentGet),
		Namespace: "app/data",
		DocID:     "11111111-1111-4111-8111-111111111111",
	}
	back := decodedAs[wire.DocumentGetMessage](t, c, msg)
	if back.DocID != msg.DocID || back.Namespace != msg.Namespace {
		t.Fatalf("document get: %+v", back)
	}
}

func TestFrameRoundtripDocumentGetResult(t *testing.T) {
	c := wire.NewCodec(wire.EncodingJSON)
	msg := wire.DocumentGetResultMessage{
		H:         testHeader(31, wire.MsgDocumentGetResult),
		Namespace: "app/data",
		DocID:     "11111111-1111-4111-8111-111111111111",
		JSON:      strPtr(`{"v":"x"}`),
		CommitHex: repeatHex(0xcc).Hex(),
	}
	back := decodedAs[wire.DocumentGetResultMessage](t, c, msg)
	if back.JSON == nil || *back.JSON != `{"v":"x"}` {
		t.Fatalf("document get result json: %+v", back.JSON)
	}
	if back.CommitHex != msg.CommitHex {
		t.Fatalf("commitHex: %+v", back)
	}
	if back.Error != nil {
		t.Fatalf("expected no error, got %v", *back.Error)
	}
}

// A point read shed under load carries the same "whether and when to retry" answer a write
// does. Writes have had it since Component 51; reads only gained it alongside the conflict
// pacing work, so the encoding is newer than the rest of this message.
func TestFrameRoundtripDocumentGetResultClassifiedError(t *testing.T) {
	c := wire.NewCodec(wire.EncodingJSON)
	code := wire.ErrorCodeBusy
	retry := 50
	msg := wire.DocumentGetResultMessage{
		H:            testHeader(33, wire.MsgDocumentGetResult),
		Namespace:    "app/data",
		DocID:        "11111111-1111-4111-8111-111111111111",
		Error:        strPtr("kdb server: busy (retry after 50ms): write queue is full"),
		ErrorCode:    &code,
		RetryAfterMs: &retry,
	}
	back := decodedAs[wire.DocumentGetResultMessage](t, c, msg)
	if back.ErrorCode == nil || *back.ErrorCode != wire.ErrorCodeBusy {
		t.Fatalf("errorCode: %v", back.ErrorCode)
	}
	if back.RetryAfterMs == nil || *back.RetryAfterMs != retry {
		t.Fatalf("retryAfterMs: %v", back.RetryAfterMs)
	}
	if back.Error == nil || *back.Error != *msg.Error {
		t.Fatalf("prose error must survive for a client that only reads it: %v", back.Error)
	}
}

func TestFrameRoundtripDocumentGetResultNotFound(t *testing.T) {
	c := wire.NewCodec(wire.EncodingJSON)
	msg := wire.DocumentGetResultMessage{
		H:         testHeader(32, wire.MsgDocumentGetResult),
		Namespace: "app/data",
		DocID:     "11111111-1111-4111-8111-111111111111",
		CommitHex: repeatHex(0xcc).Hex(),
	}
	back := decodedAs[wire.DocumentGetResultMessage](t, c, msg)
	if back.JSON != nil {
		t.Fatalf("expected nil json for not-found, got %v", *back.JSON)
	}
}

func TestFrameRoundtripUpsert(t *testing.T) {
	c := wire.NewCodec(wire.EncodingJSON)
	msg := wire.UpsertMessage{
		H:         testHeader(33, wire.MsgUpsert),
		Namespace: "app/data",
		DocID:     "11111111-1111-4111-8111-111111111111",
		JSON:      `{"v":"upserted"}`,
	}
	back := decodedAs[wire.UpsertMessage](t, c, msg)
	if back.JSON != msg.JSON || back.DocID != msg.DocID {
		t.Fatalf("upsert: %+v", back)
	}
}

func TestFrameRoundtripUpsertResult(t *testing.T) {
	c := wire.NewCodec(wire.EncodingJSON)
	msg := wire.UpsertResultMessage{
		H:         testHeader(34, wire.MsgUpsertResult),
		Namespace: "app/data",
		CommitHex: repeatHex(0xdd).Hex(),
	}
	back := decodedAs[wire.UpsertResultMessage](t, c, msg)
	if back.CommitHex != msg.CommitHex {
		t.Fatalf("upsert result: %+v", back)
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
