package wire_test

import (
	"bytes"
	"runtime"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/wire"
)

func mustUUID(t *testing.T, s string) codec.UUID {
	t.Helper()
	id, err := codec.UUIDFromString(s)
	if err != nil {
		t.Fatalf("uuid %q: %v", s, err)
	}
	return id
}

// roundTrip encodes msg, decodes the resulting frame, and returns the decoded message. Both
// encodings are exercised: the envelope body is identical JSON either way, but the tag byte and
// the negotiated-encoding paths differ, and a codec that only ever worked for one of them would
// otherwise pass every test in this file.
func roundTrip(t *testing.T, msg wire.Message) wire.Message {
	t.Helper()
	var last wire.Message
	for _, enc := range []wire.PayloadEncoding{wire.EncodingKdbBinary, wire.EncodingJSON} {
		c := wire.NewCodec(enc)
		frame, err := c.Encode(msg)
		if err != nil {
			t.Fatalf("encode (%v): %v", enc, err)
		}
		// Every frame the codec produces must survive its own header decoder and satisfy the
		// prefix-matches-buffer invariant the transports rely on.
		header, err := wire.DecodeHeader(frame)
		if err != nil {
			t.Fatalf("decode header (%v): %v", enc, err)
		}
		if header.MessageType != msg.Header().MessageType {
			t.Fatalf("messageType (%v): got %v, want %v", enc, header.MessageType, msg.Header().MessageType)
		}
		if header.CorrelationID != msg.Header().CorrelationID {
			t.Fatalf("correlationId (%v): got %d, want %d", enc, header.CorrelationID, msg.Header().CorrelationID)
		}
		if got := wire.FrameHeaderSize + header.PayloadLength; got != len(frame) {
			t.Fatalf("frame length (%v): header implies %d bytes, buffer is %d", enc, got, len(frame))
		}
		decoded, err := c.Decode(frame)
		if err != nil {
			t.Fatalf("decode (%v): %v", enc, err)
		}
		last = decoded
	}
	return last
}

func TestRoundTripHandshakeAck(t *testing.T) {
	reason := "namespace not served here"
	msg := wire.HandshakeAckMessage{
		H: testHeader(3, wire.MsgHandshake),
		Response: wire.HandshakeAckPayload{
			Accepted:           false,
			NegotiatedEncoding: wire.EncodingJSON,
			ProtocolVersion:    wire.KdbWireProtocolVersion,
			RemoteHeads:        map[string]string{"app/data": repeatHex(0x11).Hex()},
			RejectionReason:    &reason,
		},
	}
	back, ok := roundTrip(t, msg).(wire.HandshakeAckMessage)
	if !ok {
		t.Fatal("wrong message type")
	}
	if back.Response.Accepted {
		t.Fatal("accepted flag flipped")
	}
	if back.Response.NegotiatedEncoding != wire.EncodingJSON {
		t.Fatalf("negotiatedEncoding: %v", back.Response.NegotiatedEncoding)
	}
	if back.Response.RejectionReason == nil || *back.Response.RejectionReason != reason {
		t.Fatalf("rejectionReason: %v", back.Response.RejectionReason)
	}
	if back.Response.RemoteHeads["app/data"] != repeatHex(0x11).Hex() {
		t.Fatalf("remoteHeads: %v", back.Response.RemoteHeads)
	}
}

// Every op kind has to survive the opDto detour, including the two (fileWrite, schemaMigration)
// that no other test in the package constructs - a delta commit carrying one of those was
// decoded by a branch nothing exercised.
func TestRoundTripDeltaCommitAllOpKinds(t *testing.T) {
	docA := mustUUID(t, "11111111-1111-4111-8111-111111111111")
	docB := mustUUID(t, "22222222-2222-4222-8222-222222222222")
	migration := mustUUID(t, "33333333-3333-4333-8333-333333333333")
	blob := repeatHex(0x5a)

	msg := wire.DeltaCommitMessage{
		H: testHeader(9, wire.MsgDeltaCommit),
		Payload: wire.DeltaCommitPayload{
			Namespace:       "app/data",
			CommitHash:      repeatHex(0xcc),
			ParentHash:      repeatHex(0xbb),
			TimestampMicros: 1_700_000_000_000_000,
			Operations: []document.Op{
				document.WriteOp{DocID: docA, Patch: `{"title":"x"}`},
				document.DeleteOp{DocID: docB},
				document.FileWriteOp{Path: "assets/logo.png", BlobHash: blob},
				document.SchemaMigrationOp{MigrationID: migration, MigrationPayload: `{"add":"col"}`},
			},
			SchemaDeltaBytes: []byte{0x00, 0x01, 0xff, 0x7f, 0x80},
		},
	}
	back, ok := roundTrip(t, msg).(wire.DeltaCommitMessage)
	if !ok {
		t.Fatal("wrong message type")
	}
	if len(back.Payload.Operations) != 4 {
		t.Fatalf("ops: got %d, want 4", len(back.Payload.Operations))
	}
	if w, ok := back.Payload.Operations[0].(document.WriteOp); !ok || w.DocID != docA || w.Patch != `{"title":"x"}` {
		t.Fatalf("writeOp: %#v", back.Payload.Operations[0])
	}
	if d, ok := back.Payload.Operations[1].(document.DeleteOp); !ok || d.DocID != docB {
		t.Fatalf("deleteOp: %#v", back.Payload.Operations[1])
	}
	f, ok := back.Payload.Operations[2].(document.FileWriteOp)
	if !ok || f.Path != "assets/logo.png" || f.BlobHash != blob {
		t.Fatalf("fileWriteOp: %#v", back.Payload.Operations[2])
	}
	s, ok := back.Payload.Operations[3].(document.SchemaMigrationOp)
	if !ok || s.MigrationID != migration || s.MigrationPayload != `{"add":"col"}` {
		t.Fatalf("schemaMigrationOp: %#v", back.Payload.Operations[3])
	}
	// []byte fields are the ones that broke cross-language interop before (Go base64-encodes
	// them by default, Kotlin expects a number array), so assert the exact bytes rather than
	// just a non-empty slice.
	if !bytes.Equal(back.Payload.SchemaDeltaBytes, []byte{0x00, 0x01, 0xff, 0x7f, 0x80}) {
		t.Fatalf("schemaDeltaBytes: %v", back.Payload.SchemaDeltaBytes)
	}
	if back.Payload.TimestampMicros != 1_700_000_000_000_000 {
		t.Fatalf("timestampMicros: %d", back.Payload.TimestampMicros)
	}
}

func TestRoundTripDeltaCommitIndexHints(t *testing.T) {
	key := "title:hello"
	hint := wire.IndexHint{
		IndexID:    mustUUID(t, "44444444-4444-4444-8444-444444444444"),
		FieldName:  "title",
		IndexType:  "HASH",
		Action:     "INSERT",
		DocID:      mustUUID(t, "55555555-5555-4555-8555-555555555555"),
		Key:        &key,
		CommitHash: repeatHex(0x0e),
	}
	msg := wire.DeltaCommitMessage{
		H: testHeader(10, wire.MsgDeltaCommit),
		Payload: wire.DeltaCommitPayload{
			Namespace:  "app/data",
			CommitHash: repeatHex(0xcc),
			ParentHash: repeatHex(0xbb),
			IndexHints: []wire.IndexHint{hint},
		},
	}
	back := roundTrip(t, msg).(wire.DeltaCommitMessage)
	if len(back.Payload.IndexHints) != 1 {
		t.Fatalf("hints: got %d, want 1", len(back.Payload.IndexHints))
	}
	got := back.Payload.IndexHints[0]
	if got.IndexID != hint.IndexID || got.DocID != hint.DocID || got.CommitHash != hint.CommitHash {
		t.Fatalf("hint identity fields: %#v", got)
	}
	if got.FieldName != "title" || got.IndexType != "HASH" || got.Action != "INSERT" {
		t.Fatalf("hint descriptor fields: %#v", got)
	}
	if got.Key == nil || *got.Key != key {
		t.Fatalf("hint key: %v", got.Key)
	}

	// A nil Key is the common case (a delete hint carries no key) and must stay nil rather than
	// decoding as an empty string, which a consumer cannot tell apart from "indexed under ''".
	hint.Key = nil
	msg.Payload.IndexHints = []wire.IndexHint{hint}
	if k := roundTrip(t, msg).(wire.DeltaCommitMessage).Payload.IndexHints[0].Key; k != nil {
		t.Fatalf("nil hint key came back as %q", *k)
	}
}

func TestRoundTripCommitFetch(t *testing.T) {
	since := repeatHex(0x77)
	msg := wire.CommitFetchMessage{
		H:          testHeader(11, wire.MsgCommitFetch),
		Namespace:  "app/data",
		SinceHash:  &since,
		MaxCommits: 250,
	}
	back := roundTrip(t, msg).(wire.CommitFetchMessage)
	if back.Namespace != "app/data" || back.MaxCommits != 250 {
		t.Fatalf("scalars: %#v", back)
	}
	if back.SinceHash == nil || *back.SinceHash != since {
		t.Fatalf("sinceHash: %v", back.SinceHash)
	}

	// "No since hash" means fetch from the beginning; decoding it as the zero hash would mean
	// fetch from a commit that does not exist.
	msg.SinceHash = nil
	if h := roundTrip(t, msg).(wire.CommitFetchMessage).SinceHash; h != nil {
		t.Fatalf("nil sinceHash came back as %s", h.Hex())
	}
}

func buildTestCommit(t *testing.T, message string) document.Commit {
	t.Helper()
	c, err := document.BuildCommit(
		[]codec.Hash{repeatHex(0x01)},
		"app/data",
		mustUUID(t, "66666666-6666-4666-8666-666666666666"),
		codec.TimestampFromEpochMicros(1_700_000_000_000_000),
		mustUUID(t, "77777777-7777-4777-8777-777777777777"),
		[]document.Op{document.WriteOp{
			DocID: mustUUID(t, "88888888-8888-4888-8888-888888888888"),
			Patch: `{"n":1}`,
		}},
		repeatHex(0x02),
		nil,
		message,
	)
	if err != nil {
		t.Fatalf("build commit: %v", err)
	}
	return c
}

// CommitPush carries its commits through EncodeCommits/DecodeCommits, a hand-rolled
// length-prefixed framing inside the envelope that had no test of its own at all.
func TestRoundTripCommitPush(t *testing.T) {
	commits := []document.Commit{buildTestCommit(t, "first"), buildTestCommit(t, "second")}
	msg := wire.CommitPushMessage{
		H:         testHeader(12, wire.MsgCommitPush),
		Namespace: "app/data",
		Commits:   commits,
	}
	back := roundTrip(t, msg).(wire.CommitPushMessage)
	if back.Namespace != "app/data" {
		t.Fatalf("namespace: %q", back.Namespace)
	}
	if len(back.Commits) != 2 {
		t.Fatalf("commits: got %d, want 2", len(back.Commits))
	}
	for i, want := range commits {
		if back.Commits[i].Hash != want.Hash {
			t.Fatalf("commit %d hash: got %s, want %s", i, back.Commits[i].Hash.Hex(), want.Hash.Hex())
		}
		if back.Commits[i].Message != want.Message {
			t.Fatalf("commit %d message: %q", i, back.Commits[i].Message)
		}
	}
}

func TestRoundTripCommitPushEmpty(t *testing.T) {
	msg := wire.CommitPushMessage{
		H:         testHeader(13, wire.MsgCommitPush),
		Namespace: "app/data",
	}
	if got := roundTrip(t, msg).(wire.CommitPushMessage).Commits; len(got) != 0 {
		t.Fatalf("empty push decoded %d commits", len(got))
	}
}

func TestEncodeDecodeCommitsRejectsTruncatedPayload(t *testing.T) {
	payload, err := wire.EncodeCommits([]document.Commit{buildTestCommit(t, "only")})
	if err != nil {
		t.Fatal(err)
	}
	// Cut the last byte: the count and the per-commit length prefix still claim more than is
	// there, which must be an error rather than a silently short commit list.
	if _, err := wire.DecodeCommits(payload[:len(payload)-1]); err == nil {
		t.Fatal("expected an error for a truncated commit payload")
	}
	// A payload too short to even hold the count decodes as no commits, not as a panic.
	got, err := wire.DecodeCommits([]byte{0x01, 0x02})
	if err != nil {
		t.Fatalf("short payload: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("short payload decoded %d commits", len(got))
	}
	// A count larger than the payload can back must be rejected, not used to size an allocation
	// and then walked off the end.
	claimsThousands := []byte{0xff, 0xff, 0x00, 0x00}
	if _, err := wire.DecodeCommits(claimsThousands); err == nil {
		t.Fatal("expected an error for a count with no commit bodies behind it")
	}
}

// The count field is peer-controlled and was used directly as the slice capacity, so four bytes
// of input reserved ~800 GiB before the loop discovered there were no commit bodies behind it.
// On a machine with a memory limit - the container and systemd deployments both have one - that
// is a remote kill, not a slow decode, so assert on the allocation rather than just the error.
func TestDecodeCommitsDoesNotAllocateFromDeclaredCount(t *testing.T) {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	if _, err := wire.DecodeCommits([]byte{0xff, 0xff, 0xff, 0xff}); err == nil {
		t.Fatal("expected an error for a count with no commit bodies behind it")
	}

	runtime.ReadMemStats(&after)
	const limit = 8 << 20 // generous next to the 858 GiB the unbounded version reserved
	if grew := after.TotalAlloc - before.TotalAlloc; grew > limit {
		t.Fatalf("decoding a 4-byte payload allocated %d bytes", grew)
	}
}

func TestRoundTripDagDiff(t *testing.T) {
	msg := wire.DagDiffMessage{
		H:          testHeader(14, wire.MsgDagDiff),
		Namespace:  "app/data",
		LocalHead:  repeatHex(0xa1),
		RemoteHead: repeatHex(0xa2),
	}
	back := roundTrip(t, msg).(wire.DagDiffMessage)
	if back.LocalHead != msg.LocalHead || back.RemoteHead != msg.RemoteHead {
		t.Fatalf("heads: %#v", back)
	}
	if back.Namespace != "app/data" {
		t.Fatalf("namespace: %q", back.Namespace)
	}
}

func TestRoundTripTransactionReplay(t *testing.T) {
	msg := wire.TransactionReplayMessage{
		H:                testHeader(15, wire.MsgTransactionReplay),
		Namespace:        "app/data",
		BaseVersion:      repeatHex(0xb1),
		TransactionBytes: []byte{0xde, 0xad, 0x00, 0xbe, 0xef},
	}
	back := roundTrip(t, msg).(wire.TransactionReplayMessage)
	if back.BaseVersion != msg.BaseVersion {
		t.Fatalf("baseVersion: %s", back.BaseVersion.Hex())
	}
	if !bytes.Equal(back.TransactionBytes, msg.TransactionBytes) {
		t.Fatalf("transactionBytes: %v", back.TransactionBytes)
	}
}

func TestRoundTripConflictReport(t *testing.T) {
	msg := wire.ConflictReportMessage{
		H:           testHeader(16, wire.MsgConflictReport),
		Namespace:   "app/data",
		ReportBytes: []byte{0x01, 0x00, 0xfe},
	}
	back := roundTrip(t, msg).(wire.ConflictReportMessage)
	if !bytes.Equal(back.ReportBytes, msg.ReportBytes) {
		t.Fatalf("reportBytes: %v", back.ReportBytes)
	}
}

func TestRoundTripIceArchiveNotice(t *testing.T) {
	msg := wire.IceArchiveNoticeMessage{
		H:               testHeader(17, wire.MsgIceArchiveNotice),
		Namespace:       "app/data",
		OriginalHash:    repeatHex(0xc1),
		ArchiveLocation: "s3://cold/app-data/2026-08.tar",
		BundleHash:      repeatHex(0xc2),
	}
	back := roundTrip(t, msg).(wire.IceArchiveNoticeMessage)
	if back.OriginalHash != msg.OriginalHash || back.BundleHash != msg.BundleHash {
		t.Fatalf("hashes: %#v", back)
	}
	if back.ArchiveLocation != msg.ArchiveLocation {
		t.Fatalf("archiveLocation: %q", back.ArchiveLocation)
	}
}

func TestRoundTripSnapshotRequest(t *testing.T) {
	anchor := repeatHex(0xd1)
	msg := wire.SnapshotRequestMessage{
		H:          testHeader(18, wire.MsgSnapshotRequest),
		Namespace:  "app/data",
		AnchorHash: &anchor,
	}
	back := roundTrip(t, msg).(wire.SnapshotRequestMessage)
	if back.AnchorHash == nil || *back.AnchorHash != anchor {
		t.Fatalf("anchorHash: %v", back.AnchorHash)
	}
	msg.AnchorHash = nil
	if h := roundTrip(t, msg).(wire.SnapshotRequestMessage).AnchorHash; h != nil {
		t.Fatalf("nil anchorHash came back as %s", h.Hex())
	}
}

func TestRoundTripSnapshotResponse(t *testing.T) {
	msg := wire.SnapshotResponseMessage{
		H:             testHeader(19, wire.MsgSnapshotResponse),
		Namespace:     "app/data",
		AnchorHash:    repeatHex(0xd2),
		SnapshotBytes: []byte{0x28, 0xb5, 0x2f, 0xfd, 0x00},
		Compressed:    true,
	}
	back := roundTrip(t, msg).(wire.SnapshotResponseMessage)
	if !back.Compressed {
		t.Fatal("compressed flag lost")
	}
	if !bytes.Equal(back.SnapshotBytes, msg.SnapshotBytes) {
		t.Fatalf("snapshotBytes: %v", back.SnapshotBytes)
	}
	if back.AnchorHash != msg.AnchorHash {
		t.Fatalf("anchorHash: %s", back.AnchorHash.Hex())
	}
}

func TestRoundTripSchemaPush(t *testing.T) {
	msg := wire.SchemaPushMessage{
		H:           testHeader(20, wire.MsgSchemaPush),
		Namespace:   "app/data",
		SchemaBytes: []byte{0x7b, 0x7d},
		Revision:    42,
	}
	back := roundTrip(t, msg).(wire.SchemaPushMessage)
	if back.Revision != 42 {
		t.Fatalf("revision: %d", back.Revision)
	}
	if !bytes.Equal(back.SchemaBytes, msg.SchemaBytes) {
		t.Fatalf("schemaBytes: %v", back.SchemaBytes)
	}
}

// Empty and nil byte slices must not be confused: a schema push with no bytes is a real message
// shape, and the jsonByteArray encoding added for Kotlin compatibility has to handle both.
func TestRoundTripEmptyByteFields(t *testing.T) {
	msg := wire.SchemaPushMessage{
		H:           testHeader(21, wire.MsgSchemaPush),
		Namespace:   "app/data",
		SchemaBytes: []byte{},
		Revision:    1,
	}
	if got := roundTrip(t, msg).(wire.SchemaPushMessage).SchemaBytes; len(got) != 0 {
		t.Fatalf("empty schemaBytes decoded as %v", got)
	}
	msg.SchemaBytes = nil
	if got := roundTrip(t, msg).(wire.SchemaPushMessage).SchemaBytes; len(got) != 0 {
		t.Fatalf("nil schemaBytes decoded as %v", got)
	}
}

// The three exported string/lookup helpers are what operators and logs read; a wrong name here
// is a debugging trap rather than a data bug, but MessageTypeFromCode gates the decoder.
func TestMessageTypeCodesAndNames(t *testing.T) {
	for _, tc := range []struct {
		mt   wire.MessageType
		name string
	}{
		{wire.MsgHandshake, "HANDSHAKE"},
		{wire.MsgDeltaCommit, "DELTA_COMMIT"},
		{wire.MsgCommitFetch, "COMMIT_FETCH"},
		{wire.MsgCommitPush, "COMMIT_PUSH"},
		{wire.MsgDagDiff, "DAG_DIFF"},
		{wire.MsgTransactionReplay, "TRANSACTION_REPLAY"},
		{wire.MsgConflictReport, "CONFLICT_REPORT"},
		{wire.MsgCompactionNotice, "COMPACTION_NOTICE"},
		{wire.MsgIceArchiveNotice, "ICE_ARCHIVE_NOTICE"},
		{wire.MsgSnapshotRequest, "SNAPSHOT_REQUEST"},
		{wire.MsgSnapshotResponse, "SNAPSHOT_RESPONSE"},
		{wire.MsgPositionAck, "POSITION_ACK"},
		{wire.MsgSchemaPush, "SCHEMA_PUSH"},
		{wire.MsgSessionBegin, "SESSION_BEGIN"},
		{wire.MsgSqlExec, "SQL_EXEC"},
		{wire.MsgSqlResult, "SQL_RESULT"},
		{wire.MsgTxCommit, "TX_COMMIT"},
		{wire.MsgTxRollback, "TX_ROLLBACK"},
		{wire.MsgSessionBeginAck, "SESSION_BEGIN_ACK"},
		{wire.MsgDocumentGet, "DOCUMENT_GET"},
		{wire.MsgDocumentGetResult, "DOCUMENT_GET_RESULT"},
		{wire.MsgUpsert, "UPSERT"},
		{wire.MsgUpsertResult, "UPSERT_RESULT"},
		{wire.MsgCommitPushAck, "COMMIT_PUSH_ACK"},
	} {
		if tc.mt.String() != tc.name {
			t.Errorf("%#x: name is %q, want %q", uint16(tc.mt), tc.mt.String(), tc.name)
		}
		got, ok := wire.MessageTypeFromCode(uint16(tc.mt))
		if !ok {
			t.Errorf("%s (%#x) is not recognized by MessageTypeFromCode", tc.name, uint16(tc.mt))
			continue
		}
		if got != tc.mt {
			t.Errorf("%s: code %#x maps back to %v", tc.name, uint16(tc.mt), got)
		}
	}
	if _, ok := wire.MessageTypeFromCode(0x00); ok {
		t.Error("code 0x00 should not be a known message type")
	}
	if _, ok := wire.MessageTypeFromCode(0x19); ok {
		t.Error("code 0x19 is unassigned and should not be recognized")
	}
	if wire.MessageType(0x19).String() != "UNKNOWN" {
		t.Errorf("unassigned type names itself %q", wire.MessageType(0x19).String())
	}
}

func TestClientModeAndEncodingNames(t *testing.T) {
	for _, m := range []wire.ClientMode{
		wire.ClientStreamReadOnly, wire.ClientStreamWriteBack, wire.ClientFullPeer, wire.ClientSQL,
	} {
		if got := wire.ClientModeFromName(m.String()); got != m {
			t.Errorf("%v round-tripped through %q as %v", m, m.String(), got)
		}
	}
	// An unrecognized name falls back to the least-privileged mode, not to whatever the zero
	// value of a future enum happens to be.
	if got := wire.ClientModeFromName("SOMETHING_ELSE"); got != wire.ClientStreamReadOnly {
		t.Errorf("unknown client mode fell back to %v", got)
	}
	for _, e := range []wire.PayloadEncoding{wire.EncodingKdbBinary, wire.EncodingJSON} {
		if got := wire.PayloadEncodingFromName(e.String()); got != e {
			t.Errorf("%v round-tripped through %q as %v", e, e.String(), got)
		}
	}
}

// DecodePayloadEnvelope and Summary back kdb-inspect's frame dumps, which is the tooling an
// operator reaches for when something on the wire is already wrong.
func TestInspectEnvelopeAndSummary(t *testing.T) {
	c := wire.NewCodec(wire.EncodingKdbBinary)
	for _, tc := range []struct {
		name string
		msg  wire.Message
		want string
	}{
		{"handshake", wire.HandshakeMessage{
			H:       testHeader(1, wire.MsgHandshake),
			Request: wire.HandshakePayload{NodeID: "node-a", ClientMode: wire.ClientFullPeer},
		}, "handshake node=node-a"},
		{"deltaCommit", wire.DeltaCommitMessage{
			H: testHeader(2, wire.MsgDeltaCommit),
			Payload: wire.DeltaCommitPayload{
				Namespace: "app/data", CommitHash: repeatHex(1), ParentHash: repeatHex(2),
			},
		}, "deltaCommit ns=app/data"},
		{"commitFetch", wire.CommitFetchMessage{
			H: testHeader(3, wire.MsgCommitFetch), Namespace: "app/data",
		}, "commitFetch"},
		{"positionAck", wire.PositionAckMessage{
			H: testHeader(4, wire.MsgPositionAck), Namespace: "app/data", CommitHash: repeatHex(3),
		}, "positionAck"},
		{"schemaPush", wire.SchemaPushMessage{
			H: testHeader(5, wire.MsgSchemaPush), Namespace: "app/data", SchemaBytes: []byte{1},
		}, "schemaPush"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frame, err := c.Encode(tc.msg)
			if err != nil {
				t.Fatal(err)
			}
			header, err := wire.DecodeHeader(frame)
			if err != nil {
				t.Fatal(err)
			}
			env, err := wire.DecodePayloadEnvelope(frame, header)
			if err != nil {
				t.Fatalf("decode envelope: %v", err)
			}
			if got := env.Summary(); got != tc.want {
				t.Fatalf("summary: got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDecodeRejectsUnsupportedEncodingTag(t *testing.T) {
	c := wire.NewCodec(wire.EncodingKdbBinary)
	frame, err := c.Encode(wire.PositionAckMessage{
		H: testHeader(6, wire.MsgPositionAck), Namespace: "app/data", CommitHash: repeatHex(4),
	})
	if err != nil {
		t.Fatal(err)
	}
	frame[wire.FrameHeaderSize] = 9 // the encoding tag byte; only 0 and 1 are defined
	if _, err := c.Decode(frame); err == nil {
		t.Fatal("expected an error for an unsupported encoding tag")
	}
}

func TestDecodeRejectsInvalidPayloadJSON(t *testing.T) {
	c := wire.NewCodec(wire.EncodingKdbBinary)
	frame, err := c.Encode(wire.PositionAckMessage{
		H: testHeader(7, wire.MsgPositionAck), Namespace: "app/data", CommitHash: repeatHex(5),
	})
	if err != nil {
		t.Fatal(err)
	}
	frame[wire.FrameHeaderSize+1] = '?' // corrupt the first byte of the JSON body
	if _, err := c.Decode(frame); err == nil {
		t.Fatal("expected an error for a corrupt payload body")
	}
}

// A hash field carrying something that is not a hash must fail the decode rather than reach a
// handler as a zero hash, which would silently mean "the root commit".
func TestDecodeRejectsMalformedHashInPayload(t *testing.T) {
	c := wire.NewCodec(wire.EncodingKdbBinary)
	frame, err := c.Encode(wire.PositionAckMessage{
		H: testHeader(8, wire.MsgPositionAck), Namespace: "app/data", CommitHash: repeatHex(6),
	})
	if err != nil {
		t.Fatal(err)
	}
	body := string(frame[wire.FrameHeaderSize+1:])
	corrupted := strings.Replace(body, repeatHex(6).Hex(), strings.Repeat("z", 64), 1)
	if corrupted == body {
		t.Fatal("test setup: commit hash hex not found in the encoded body")
	}
	rebuilt, err := wire.EncodeFrameOnly(
		wire.Header{
			MessageType:     wire.MsgPositionAck,
			ProtocolVersion: wire.KdbWireProtocolVersion,
			CorrelationID:   8,
		},
		append([]byte{0}, []byte(corrupted)...),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Decode(rebuilt); err == nil {
		t.Fatal("expected an error for a non-hex commit hash")
	}
}

func TestCodecReportsItsEncoding(t *testing.T) {
	if got := wire.NewCodec(wire.EncodingJSON).Encoding(); got != wire.EncodingJSON {
		t.Fatalf("Encoding(): %v", got)
	}
	if got := wire.NewCodec(wire.EncodingKdbBinary).Encoding(); got != wire.EncodingKdbBinary {
		t.Fatalf("Encoding(): %v", got)
	}
}

func TestEncodeFrameOnlyAndDecodeHeaderViaCodec(t *testing.T) {
	c := wire.NewCodec(wire.EncodingKdbBinary)
	payload := []byte{0, '{', '}'}
	frame, err := c.EncodeFrameOnly(wire.Header{
		MessageType:     wire.MsgPositionAck,
		ProtocolVersion: wire.KdbWireProtocolVersion,
		CorrelationID:   99,
	}, payload)
	if err != nil {
		t.Fatal(err)
	}
	header, err := c.DecodeHeader(frame)
	if err != nil {
		t.Fatal(err)
	}
	if header.CorrelationID != 99 || header.MessageType != wire.MsgPositionAck {
		t.Fatalf("header: %#v", header)
	}
	if header.PayloadLength != len(payload) {
		t.Fatalf("payloadLength: got %d, want %d", header.PayloadLength, len(payload))
	}
	// An oversized payload is refused at encode time rather than producing a frame no peer
	// would accept.
	if _, err := c.EncodeFrameOnly(wire.Header{MessageType: wire.MsgPositionAck},
		make([]byte, wire.DefaultMaxFrameBytes)); err == nil {
		t.Fatal("expected an error encoding an oversized frame")
	}
}
