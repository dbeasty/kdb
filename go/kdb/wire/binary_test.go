package wire_test

import (
	"encoding/binary"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/wire"
)

func binaryCodec(features ...string) *wire.DefaultCodec {
	if len(features) == 0 {
		features = []string{wire.WireFeatureBinary, wire.WireFeatureBinary2}
	}
	return wire.NewCodec(wire.EncodingJSON).WithWireFeatures(features)
}

func strp(s string) *string { return &s }
func intp(v int) *int       { return &v }
func codep(c wire.ErrorCode) *wire.ErrorCode {
	return &c
}

// binaryMessages is one fully populated and one zero-valued instance of every message with a
// binary form, so both "every field set" and "every field absent" are covered.
func binaryMessages() []wire.Message {
	h := func(t wire.MessageType) wire.Header { return testHeader(77, t) }
	return []wire.Message{
		wire.SessionBeginMessage{H: h(wire.MsgSessionBegin), Namespace: "app/data", SessionID: strp(""), ReadConsistency: "STRONG", BaseVersionHex: strp("ab")},
		wire.SessionBeginMessage{H: h(wire.MsgSessionBegin)},
		wire.SessionBeginAckMessage{H: h(wire.MsgSessionBeginAck), Namespace: "n", SessionID: "s1", HeadHex: "cd", ReadConsistency: "SNAPSHOT", Error: strp("denied")},
		wire.SessionBeginAckMessage{H: h(wire.MsgSessionBeginAck)},
		wire.SqlExecMessage{H: h(wire.MsgSqlExec), Namespace: "n", SessionID: "s", SQL: "SELECT * FROM t WHERE a = ?", ParametersJSON: strp(`[1,"x"]`), PageSize: 500, Continuation: "opaque\x00token\xff"},
		wire.SqlExecMessage{H: h(wire.MsgSqlExec)},
		wire.SqlResultMessage{
			H: h(wire.MsgSqlResult), Namespace: "n", SessionID: "s", Columns: []string{"id", "body", ""},
			Rows:         [][]string{{"1", `{"a":"q\"uote"}`, ""}, {"2", "", "é"}, {}},
			RowsAffected: 3, ResolvedCommitHex: "ef", ReadOnly: true, Error: strp(""), GeneratedIDs: []string{"g1", ""},
			ErrorCode: codep(wire.ErrorCodeBusy), RetryAfterMs: intp(0), ContinuationToken: "tok",
			CellKinds: []wire.CellKind{wire.CellKindLong, wire.CellKindJSON, wire.CellKindNull, wire.CellKindLong, wire.CellKindString, wire.CellKindString},
		},
		wire.SqlResultMessage{H: h(wire.MsgSqlResult), RowsAffected: -1, RetryAfterMs: intp(-5)},
		wire.TxCommitMessage{H: h(wire.MsgTxCommit), Namespace: "n", SessionID: "s", TransactionBytes: []byte{0, 1, 2, 255}},
		wire.TxCommitMessage{H: h(wire.MsgTxCommit)},
		wire.TxRollbackMessage{H: h(wire.MsgTxRollback), Namespace: "n", SessionID: "s"},
		wire.ConflictReportMessage{H: h(wire.MsgConflictReport), Namespace: "n", ReportBytes: []byte(`{"x":1}`), ErrorCode: codep(wire.ErrorCodeConflict), RetryAfterMs: intp(12)},
		wire.ConflictReportMessage{H: h(wire.MsgConflictReport)},
		wire.DocumentGetMessage{H: h(wire.MsgDocumentGet), Namespace: "n", DocID: "d", MinCommit: "aa"},
		wire.DocumentGetResultMessage{H: h(wire.MsgDocumentGetResult), Namespace: "n", DocID: "d", JSON: strp(`{"k":"v\n"}`), CommitHex: "bb", Error: strp("e"), ErrorCode: codep(wire.ErrorCodeResourceExhausted), RetryAfterMs: intp(3)},
		wire.DocumentGetResultMessage{H: h(wire.MsgDocumentGetResult), JSON: strp("")},
		wire.DocumentGetResultMessage{H: h(wire.MsgDocumentGetResult)},
		wire.UpsertMessage{H: h(wire.MsgUpsert), Namespace: "n", DocID: "d", JSON: `{"k":1}`, SessionID: "s"},
		wire.UpsertResultMessage{H: h(wire.MsgUpsertResult), Namespace: "n", CommitHex: "cc", Error: strp("x"), ErrorCode: codep(wire.ErrorCodeUnavailable), RetryAfterMs: intp(9)},
		wire.UpsertResultMessage{H: h(wire.MsgUpsertResult)},
		wire.LockAcquireMessage{H: h(wire.MsgLockAcquire), Namespace: "n", SessionID: "s", DocID: "d", TTLMillis: 30000},
		wire.LockRenewMessage{H: h(wire.MsgLockRenew), Namespace: "n", SessionID: "s", DocID: "d", TTLMillis: 1},
		wire.LockReleaseMessage{H: h(wire.MsgLockRelease), Namespace: "n", SessionID: "s", DocID: "d"},
		wire.LockResultMessage{H: h(wire.MsgLockResult), Namespace: "n", SessionID: "s", DocID: "d", Granted: true, Fence: 1<<63 + 5, ExpiresAtMillis: 1750000000000, HolderSessionID: strp(""), Error: strp("e"), ErrorCode: codep(wire.ErrorCodeBusy)},
		wire.LockResultMessage{H: h(wire.MsgLockResult)},
		wire.SearchMessage{
			H: h(wire.MsgSearch), Namespace: "n", SessionID: "s",
			Text:   &wire.SearchTextArm{Index: "body", Query: "q \"x\"", Depth: 20, MinScore: floatp(0), Weight: floatp(0.25)},
			Vector: &wire.SearchVectorArm{Index: "emb", Vector: []float64{0, -1.5, 3e300}, Depth: 5},
			Fusion: "weighted", Limit: 10, IncludeJSON: true, AtCommitHex: "ab",
		},
		wire.SearchMessage{H: h(wire.MsgSearch), Text: &wire.SearchTextArm{}},
		wire.SearchResultMessage{
			H: h(wire.MsgSearchResult), Namespace: "n", ResolvedCommitHex: "cd",
			Hits:  []wire.SearchHit{{DocID: "a", Score: 0.5, JSON: strp(`{"a":1}`)}, {DocID: "b"}},
			Error: strp("x"), ErrorCode: codep(wire.ErrorCodeBusy), RetryAfterMs: intp(4),
		},
		wire.SearchResultMessage{H: h(wire.MsgSearchResult)},
		wire.HistoryListMessage{H: h(wire.MsgHistoryList), Namespace: "n", From: "HEAD~3", Skip: 2, Limit: 50},
		wire.HistoryResultMessage{
			H: h(wire.MsgHistoryResult), Namespace: "n", ResolvedHex: "ee",
			Commits: []wire.HistoryCommit{
				{CommitHex: "c1", ParentHexes: []string{"p1", "p2"}, TransactionID: "t", TimestampMicros: 1700000000000000, AuthorNodeID: "a", TreeHex: "tr", Message: "m", OperationCount: -1, Stubbed: true},
				{CommitHex: "c2"},
			},
		},
		wire.HistoryResultMessage{H: h(wire.MsgHistoryResult), Error: strp("no"), ErrorCode: codep(wire.ErrorCodeBusy), RetryAfterMs: intp(1)},
		wire.RevertMessage{H: h(wire.MsgRevert), Namespace: "n", To: "HEAD~1"},
		wire.RevertResultMessage{H: h(wire.MsgRevertResult), Namespace: "n", CommitHex: "c", TargetHex: "t", Restored: 3, Removed: 1, Error: strp(""), ErrorCode: codep(wire.ErrorCodeConflict), RetryAfterMs: intp(0)},
		wire.TxCommitMultiMessage{H: h(wire.MsgTxCommitMulti), SessionID: "s", Parts: []wire.TxCommitMultiPart{{Namespace: "a", TransactionBytes: []byte{1, 2}}, {Namespace: "b"}}},
		wire.TxCommitMultiResultMessage{H: h(wire.MsgTxCommitMultiResult), GroupID: "g", Parts: []wire.TxCommitMultiResultPart{{Namespace: "a", CommitHex: "c"}}},
		wire.TxCommitMultiResultMessage{H: h(wire.MsgTxCommitMultiResult), FailedNamespace: "b", ConflictReport: []byte(`{}`), Error: strp("conflict"), ErrorCode: codep(wire.ErrorCodeConflict), RetryAfterMs: intp(7)},
		deltaCommitSample(h(wire.MsgDeltaCommit)),
		wire.DeltaCommitMessage{H: h(wire.MsgDeltaCommit)},
	}
}

func floatp(v float64) *float64 { return &v }

func deltaCommitSample(h wire.Header) wire.DeltaCommitMessage {
	id := codec.UUID{MSB: -1, LSB: 42}
	key := ""
	return wire.DeltaCommitMessage{H: h, Payload: wire.DeltaCommitPayload{
		Namespace: "n", CommitHash: repeatHex(1), ParentHash: repeatHex(2), TimestampMicros: 1700000000000001,
		Operations: []document.Op{
			document.WriteOp{DocID: id, Patch: `{"a":"\n"}`, Replace: true},
			document.WriteOp{DocID: id, Patch: ""},
			document.DeleteOp{DocID: id},
			document.FileWriteOp{Path: "f/x", BlobHash: repeatHex(3)},
			document.SchemaMigrationOp{MigrationID: id, MigrationPayload: "p"},
			document.AttachmentPutOp{DocID: id, AttachmentID: codec.UUID{MSB: 5, LSB: 6}, Name: "a.png", Scheme: 1, ContentHash: repeatHex(5), Size: 1 << 40, Mime: "image/png"},
			document.AttachmentPutOp{DocID: id, AttachmentID: codec.UUID{MSB: 7}, Name: "é", Scheme: 1, ContentHash: codec.Hash{}, Size: 0, Mime: ""},
			document.AttachmentDeleteOp{DocID: id, AttachmentID: codec.UUID{MSB: 5, LSB: 6}},
		},
		IndexHints: []wire.IndexHint{
			{IndexID: id, FieldName: "f", IndexType: "HASH", Action: "ADD", DocID: id, Key: &key, CommitHash: repeatHex(4)},
			{IndexID: id},
		},
		SchemaDeltaBytes: []byte{9, 8},
	}}
}

func TestBinaryFramesRoundTripEveryMessage(t *testing.T) {
	c := binaryCodec()
	for _, msg := range binaryMessages() {
		frame, err := c.Encode(msg)
		if err != nil {
			t.Fatalf("%T: encode: %v", msg, err)
		}
		if tag := frame[wire.FrameHeaderSize]; tag != 2 {
			t.Fatalf("%T: tag %d, want 2", msg, tag)
		}
		// A JSON-only codec decodes it too: decoding goes by the tag, never by what was negotiated.
		got, err := wire.NewCodec(wire.EncodingJSON).Decode(frame)
		if err != nil {
			t.Fatalf("%T: decode: %v", msg, err)
		}
		want := msg
		hdr := msg.Header()
		hdr.PayloadLength = len(frame) - wire.FrameHeaderSize
		want = withHeader(want, hdr)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%T round trip:\n got %#v\nwant %#v", msg, got, want)
		}
	}
}

// withHeader returns msg with its header replaced, so a decoded message (whose header carries the
// payload length) compares equal to the one that was encoded.
func withHeader(msg wire.Message, h wire.Header) wire.Message {
	v := reflect.New(reflect.TypeOf(msg)).Elem()
	v.Set(reflect.ValueOf(msg))
	v.FieldByName("H").Set(reflect.ValueOf(h))
	return v.Interface().(wire.Message)
}

func TestBinaryCodecKeepsJSONForMessagesWithoutABinaryForm(t *testing.T) {
	frame, err := binaryCodec().Encode(wire.PositionAckMessage{H: testHeader(1, wire.MsgPositionAck), Namespace: "n", CommitHash: repeatHex(1)})
	if err != nil {
		t.Fatal(err)
	}
	if tag := frame[wire.FrameHeaderSize]; tag != 1 {
		t.Fatalf("tag %d, want the JSON envelope's 1", tag)
	}
	if _, err := binaryCodec().Decode(frame); err != nil {
		t.Fatal(err)
	}
}

func TestJSONCodecNeverWritesBinary(t *testing.T) {
	for _, msg := range binaryMessages() {
		frame, err := wire.NewCodec(wire.EncodingJSON).Encode(msg)
		if err != nil {
			t.Fatal(err)
		}
		if tag := frame[wire.FrameHeaderSize]; tag != 1 {
			t.Fatalf("%T: tag %d from a codec with no features", msg, tag)
		}
	}
}

func bigResult(rows int) wire.SqlResultMessage {
	m := wire.SqlResultMessage{H: testHeader(5, wire.MsgSqlResult), Namespace: "app/data", Columns: []string{"id", "name", "body"}, ReadOnly: true}
	for i := 0; i < rows; i++ {
		id := strconv.Itoa(i)
		m.Rows = append(m.Rows, []string{id, "user-" + id, `{"name":"user-` + id + `","tags":["a","b"],"score":` + id + `}`})
	}
	return m
}

func TestZstdFramesCompressLargeBodiesOnly(t *testing.T) {
	c := binaryCodec(wire.WireFeatureBinary, wire.WireFeatureZstd)
	small, err := c.Encode(bigResult(2))
	if err != nil {
		t.Fatal(err)
	}
	if tag := small[wire.FrameHeaderSize]; tag != 2 {
		t.Fatalf("small body: tag %d, want 2 (uncompressed)", tag)
	}
	msg := bigResult(2000)
	plain, err := binaryCodec().Encode(msg)
	if err != nil {
		t.Fatal(err)
	}
	packed, err := c.Encode(msg)
	if err != nil {
		t.Fatal(err)
	}
	if tag := packed[wire.FrameHeaderSize]; tag != 3 {
		t.Fatalf("large body: tag %d, want 3", tag)
	}
	if len(packed) >= len(plain)/2 {
		t.Fatalf("zstd frame %d bytes, uncompressed %d", len(packed), len(plain))
	}
	got, err := wire.NewCodec(wire.EncodingJSON).Decode(packed)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.(wire.SqlResultMessage).Rows, msg.Rows) {
		t.Fatal("rows differ after a zstd round trip")
	}
}

func TestPooledBinaryEncodeMatchesUnpooled(t *testing.T) {
	c := binaryCodec(wire.WireFeatureBinary, wire.WireFeatureZstd)
	for _, msg := range append(binaryMessages(), bigResult(2000)) {
		want, err := c.Encode(msg)
		if err != nil {
			t.Fatal(err)
		}
		got, release, err := c.EncodePooled(msg)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(want) {
			t.Fatalf("%T: pooled frame differs", msg)
		}
		release()
	}
}

// A field this build does not know is skipped, whatever its wire type: that is what lets a later
// release add fields without a new tag.
func TestBinaryDecodeSkipsUnknownFields(t *testing.T) {
	msg := wire.UpsertResultMessage{H: testHeader(3, wire.MsgUpsertResult), Namespace: "n", CommitHex: "cc"}
	frame, err := binaryCodec().Encode(msg)
	if err != nil {
		t.Fatal(err)
	}
	var extra []byte
	extra = binary.AppendUvarint(extra, 99<<3|0)
	extra = binary.AppendUvarint(extra, 1<<40)
	extra = binary.AppendUvarint(extra, 100<<3|2)
	extra = binary.AppendUvarint(extra, 3)
	extra = append(extra, "abc"...)
	extra = binary.AppendUvarint(extra, 101<<3|1)
	extra = append(extra, 1, 2, 3, 4, 5, 6, 7, 8)
	extra = binary.AppendUvarint(extra, 102<<3|5)
	extra = append(extra, 1, 2, 3, 4)
	frame = append(frame, extra...)
	binary.LittleEndian.PutUint32(frame, uint32(len(frame)))
	got, err := binaryCodec().Decode(frame)
	if err != nil {
		t.Fatal(err)
	}
	if m := got.(wire.UpsertResultMessage); m.Namespace != "n" || m.CommitHex != "cc" {
		t.Fatalf("got %#v", m)
	}
}

// Every prefix of every binary body, and every single-byte corruption of one, must decode or fail
// cleanly - a body comes off the network.
func TestBinaryDecodeNeverPanicsOnDamagedBodies(t *testing.T) {
	c := binaryCodec()
	for _, msg := range append(binaryMessages(), bigResult(3)) {
		frame, err := c.Encode(msg)
		if err != nil {
			t.Fatal(err)
		}
		for n := wire.FrameHeaderSize + 1; n <= len(frame); n++ {
			cut := append([]byte(nil), frame[:n]...)
			binary.LittleEndian.PutUint32(cut, uint32(n))
			_, _ = c.Decode(cut)
		}
		for i := wire.FrameHeaderSize + 1; i < len(frame); i++ {
			for _, b := range []byte{0x00, 0x7f, 0x80, 0xff} {
				bad := append([]byte(nil), frame...)
				bad[i] = b
				_, _ = c.Decode(bad)
			}
		}
	}
}

func FuzzBinaryDecode(f *testing.F) {
	c := binaryCodec()
	for _, msg := range binaryMessages() {
		frame, _ := c.Encode(msg)
		f.Add(uint16(msg.Header().MessageType), frame[wire.FrameHeaderSize+1:])
	}
	f.Fuzz(func(t *testing.T, msgType uint16, body []byte) {
		frame := make([]byte, wire.FrameHeaderSize+1, wire.FrameHeaderSize+1+len(body))
		frame = append(frame, body...)
		binary.LittleEndian.PutUint32(frame, uint32(len(frame)))
		binary.LittleEndian.PutUint16(frame[4:], msgType)
		frame[wire.FrameHeaderSize] = 2
		_, _ = c.Decode(frame)
	})
}

func TestBinaryDecodeRejectsWrongWireType(t *testing.T) {
	var body []byte
	body = binary.AppendUvarint(body, 1<<3|0) // namespace as a varint
	body = binary.AppendUvarint(body, 7)
	frame := make([]byte, wire.FrameHeaderSize, 64)
	frame = append(frame, 2)
	frame = append(frame, body...)
	binary.LittleEndian.PutUint32(frame, uint32(len(frame)))
	binary.LittleEndian.PutUint16(frame[4:], uint16(wire.MsgUpsertResult))
	if _, err := binaryCodec().Decode(frame); err == nil {
		t.Fatal("a string field sent as a varint decoded")
	}
}

func TestNegotiateWireFeatures(t *testing.T) {
	cases := []struct {
		offered, want []string
	}{
		{nil, nil},
		{[]string{"something-new"}, nil},
		{[]string{wire.WireFeatureZstd}, nil},
		{[]string{wire.WireFeatureBinary}, []string{wire.WireFeatureBinary}},
		{[]string{"x", wire.WireFeatureZstd, wire.WireFeatureBinary}, []string{wire.WireFeatureBinary, wire.WireFeatureZstd}},
	}
	for _, tc := range cases {
		if got := wire.NegotiateWireFeatures(tc.offered); !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("NegotiateWireFeatures(%v) = %v, want %v", tc.offered, got, tc.want)
		}
	}
	if c := wire.NewCodec(wire.EncodingJSON).WithWireFeatures([]string{wire.WireFeatureZstd}); c.BinaryFrames() {
		t.Fatal("zstd alone enabled binary frames")
	}
}

func TestHandshakeCarriesWireFeatures(t *testing.T) {
	c := wire.NewCodec(wire.EncodingJSON)
	hs := wire.HandshakeMessage{H: testHeader(1, wire.MsgHandshake), Request: wire.HandshakePayload{ClientMode: wire.ClientSQL, WireFeatures: wire.SupportedWireFeatures}}
	frame, err := c.Encode(hs)
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.Decode(frame)
	if err != nil {
		t.Fatal(err)
	}
	if f := got.(wire.HandshakeMessage).Request.WireFeatures; !reflect.DeepEqual(f, wire.SupportedWireFeatures) {
		t.Fatalf("offer: %v", f)
	}
	ack := wire.HandshakeAckMessage{H: testHeader(1, wire.MsgHandshake), Response: wire.HandshakeAckPayload{Accepted: true, NegotiatedEncoding: wire.EncodingJSON, WireFeatures: []string{wire.WireFeatureBinary}}}
	frame, err = c.Encode(ack)
	if err != nil {
		t.Fatal(err)
	}
	got, err = c.Decode(frame)
	if err != nil {
		t.Fatal(err)
	}
	if f := got.(wire.HandshakeAckMessage).Response.WireFeatures; !reflect.DeepEqual(f, []string{wire.WireFeatureBinary}) {
		t.Fatalf("ack: %v", f)
	}
	// Without features the key is left out, so the frame is byte for byte what older builds sent.
	frame, _ = c.Encode(wire.HandshakeMessage{H: testHeader(1, wire.MsgHandshake)})
	if strings.Contains(string(frame), "wireFeatures") {
		t.Fatal("an empty offer still wrote the wireFeatures key")
	}
}

// Clients that shipped with binary-frames decode only the core load-path bodies, so the rest of
// the set goes binary only under binary-frames-2.
func TestTheRestOfTheSetNeedsBinaryFrames2(t *testing.T) {
	core := binaryCodec(wire.WireFeatureBinary)
	full := binaryCodec(wire.WireFeatureBinary, wire.WireFeatureBinary2)
	for _, msg := range []wire.Message{
		wire.LockResultMessage{H: testHeader(1, wire.MsgLockResult), Namespace: "n"},
		wire.SearchResultMessage{H: testHeader(1, wire.MsgSearchResult), Namespace: "n"},
		deltaCommitSample(testHeader(1, wire.MsgDeltaCommit)),
	} {
		frame, err := core.Encode(msg)
		if err != nil {
			t.Fatal(err)
		}
		if tag := frame[wire.FrameHeaderSize]; tag != 1 {
			t.Fatalf("%T under binary-frames alone: tag %d, want JSON", msg, tag)
		}
		if frame, _ = full.Encode(msg); frame[wire.FrameHeaderSize] != 2 {
			t.Fatalf("%T under binary-frames-2 is not binary", msg)
		}
	}
	if wire.NewCodec(wire.EncodingJSON).WithWireFeatures([]string{wire.WireFeatureBinary2}).BinaryFrames() {
		t.Fatal("binary-frames-2 without binary-frames enabled binary frames")
	}
	if got := wire.NegotiateWireFeatures([]string{wire.WireFeatureBinary2}); got != nil {
		t.Fatalf("binary-frames-2 alone negotiated %v", got)
	}
}
