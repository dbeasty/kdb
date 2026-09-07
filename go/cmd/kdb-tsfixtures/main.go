// Command genfixtures emits the golden fixtures the TypeScript client's conformance tests
// check themselves against.
//
// The point is differential testing, not documentation. @kdb/client is a third, hand-written
// implementation of a wire format that already has two (go/kdb/wire and Kotlin's kdb-wire), and
// "wire-compatible" is a claim that has to be proven rather than asserted. Every value below is
// produced by the real Go encoder - the same code paths the Go client uses in production - so a
// TS decoder that disagrees with any of it disagrees with a live server too.
//
// The content-hash fixtures matter most. That hash is computed locally and never crosses the
// wire (go/kdb/client/client.go:640-650), so nothing at runtime would ever tell a TS client its
// canonical encoder had drifted: conditional replaces would just fail forever with what looks
// like a phantom concurrent write.
//
// Run from the go module directory:
//
//	cd go && go run ./cmd/kdb-tsfixtures
//
// which rewrites ../packages/kdb-client/test/golden/*.json. Pass -out to write elsewhere.
package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/wire"
)

type frameFixture struct {
	Name          string `json:"name"`
	Kind          string `json:"kind"`
	TypeCode      int    `json:"typeCode"`
	CorrelationID int    `json:"correlationId"`
	FrameHex      string `json:"frameHex"`
	EnvelopeJSON  string `json:"envelopeJson"`
}

type hashFixture struct {
	Name        string `json:"name"`
	DocID       string `json:"docId"`
	JSON        string `json:"json"`
	EncodedHex  string `json:"encodedHex"`
	ContentHash string `json:"contentHash"`
}

type transactionFixture struct {
	Name     string `json:"name"`
	JSON     string `json:"json"`
	Bytes    []byte `json:"-"`
	WireForm []int  `json:"wireForm"`
}

func main() {
	outDir := flag.String(
		"out",
		filepath.Join("..", "packages", "kdb-client", "test", "golden"),
		"directory to write the fixture files into",
	)
	flag.Parse()
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fail(err)
	}
	dir := *outDir

	writeJSON(filepath.Join(dir, "frames.json"), buildFrames())
	writeJSON(filepath.Join(dir, "hashes.json"), buildHashes())
	writeJSON(filepath.Join(dir, "transactions.json"), buildTransactions())
	fmt.Println("wrote fixtures to", dir)
}

func buildFrames() []frameFixture {
	c := wire.NewCodec(wire.EncodingJSON)
	token := "alice:secret"
	rejection := "bad token"
	sqlError := "no such table"
	docJSON := `{"name":"ada","score":42}`

	cases := []struct {
		name string
		msg  wire.Message
	}{
		{"handshake", wire.HandshakeMessage{
			H: hdr(wire.MsgHandshake, 1),
			Request: wire.HandshakePayload{
				NodeID: "kdb-client-ts", ClientMode: wire.ClientSQL, Token: &token,
			},
		}},
		{"handshakeAckAccepted", wire.HandshakeAckMessage{
			H: hdr(wire.MsgHandshake, 1),
			Response: wire.HandshakeAckPayload{
				Accepted: true, NegotiatedEncoding: wire.EncodingJSON,
				ProtocolVersion: wire.KdbWireProtocolVersion,
				RemoteHeads:     map[string]string{},
			},
		}},
		{"handshakeAckRejected", wire.HandshakeAckMessage{
			H: hdr(wire.MsgHandshake, 2),
			Response: wire.HandshakeAckPayload{
				Accepted: false, NegotiatedEncoding: wire.EncodingJSON,
				ProtocolVersion: wire.KdbWireProtocolVersion,
				RemoteHeads:     map[string]string{},
				RejectionReason: &rejection,
			},
		}},
		{"sessionBegin", wire.SessionBeginMessage{
			H: hdr(wire.MsgSessionBegin, 3), Namespace: "app/users",
			ReadConsistency: "READ_COMMITTED",
		}},
		{"sqlExec", wire.SqlExecMessage{
			H: hdr(wire.MsgSqlExec, 5), Namespace: "app/users", SessionID: "sess-1",
			SQL: "SELECT * FROM users WHERE id = ?", ParametersJSON: strptr(`["u1"]`),
		}},
		{"sqlResultRows", wire.SqlResultMessage{
			H: hdr(wire.MsgSqlResult, 5), Namespace: "app/users", SessionID: "sess-1",
			Columns: []string{"id", "name"}, Rows: [][]string{{"u1", "ada"}, {"u2", "grace"}},
			ReadOnly: true, GeneratedIDs: []string{},
		}},
		{"sqlResultError", wire.SqlResultMessage{
			H: hdr(wire.MsgSqlResult, 6), Namespace: "app/users", SessionID: "sess-1",
			Columns: []string{}, Rows: [][]string{}, Error: &sqlError,
			GeneratedIDs: []string{},
		}},
		{"documentGet", wire.DocumentGetMessage{
			H: hdr(wire.MsgDocumentGet, 7), Namespace: "app/users",
			DocID: "6f9619ff-8b86-d011-b42d-00cf4fc964ff",
		}},
		{"documentGetResultFound", wire.DocumentGetResultMessage{
			H: hdr(wire.MsgDocumentGetResult, 7), Namespace: "app/users",
			DocID: "6f9619ff-8b86-d011-b42d-00cf4fc964ff", JSON: &docJSON,
			CommitHex: "00" + hex.EncodeToString(make([]byte, 31)),
		}},
		{"documentGetResultMissing", wire.DocumentGetResultMessage{
			H: hdr(wire.MsgDocumentGetResult, 8), Namespace: "app/users",
			DocID:     "6f9619ff-8b86-d011-b42d-00cf4fc964ff",
			CommitHex: "00" + hex.EncodeToString(make([]byte, 31)),
		}},
		{"upsert", wire.UpsertMessage{
			H: hdr(wire.MsgUpsert, 9), Namespace: "app/users",
			DocID: "6f9619ff-8b86-d011-b42d-00cf4fc964ff", JSON: docJSON, SessionID: "sess-1",
		}},
		{"lockAcquire", wire.LockAcquireMessage{
			H: hdr(wire.MsgLockAcquire, 11), Namespace: "app/users", SessionID: "sess-1",
			DocID: "6f9619ff-8b86-d011-b42d-00cf4fc964ff", TTLMillis: 30000,
		}},
		{"lockResultGranted", wire.LockResultMessage{
			H: hdr(wire.MsgLockResult, 11), Namespace: "app/users", SessionID: "sess-1",
			DocID: "6f9619ff-8b86-d011-b42d-00cf4fc964ff", Granted: true, Fence: 7,
			ExpiresAtMillis: 1750000000000,
		}},
	}

	out := make([]frameFixture, 0, len(cases))
	for _, tc := range cases {
		frame, err := c.Encode(tc.msg)
		if err != nil {
			fail(fmt.Errorf("%s: %w", tc.name, err))
		}
		header, err := wire.DecodeHeader(frame)
		if err != nil {
			fail(fmt.Errorf("%s: %w", tc.name, err))
		}
		// Payload is [encoding tag][JSON]; the envelope is everything after the tag.
		envelope := string(frame[wire.FrameHeaderSize+1:])
		var probe struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal([]byte(envelope), &probe); err != nil {
			fail(fmt.Errorf("%s: %w", tc.name, err))
		}
		out = append(out, frameFixture{
			Name:          tc.name,
			Kind:          probe.Kind,
			TypeCode:      int(header.MessageType),
			CorrelationID: header.CorrelationID,
			FrameHex:      hex.EncodeToString(frame),
			EnvelopeJSON:  envelope,
		})
	}
	return out
}

// buildHashes covers the cases most likely to expose a divergent canonical encoder: non-ASCII
// (where a character count would differ from the UTF-8 byte count appendLebString actually
// writes), embedded quotes, an empty object, and a body long enough to push the LEB128 length
// prefix past one byte.
func buildHashes() []hashFixture {
	long := `{"pad":"` + repeat("x", 400) + `"}`
	cases := []struct {
		name  string
		docID string
		json  string
	}{
		{"simple", "6f9619ff-8b86-d011-b42d-00cf4fc964ff", `{"name":"ada"}`},
		{"emptyObject", "6f9619ff-8b86-d011-b42d-00cf4fc964ff", `{}`},
		{"nonAscii", "6f9619ff-8b86-d011-b42d-00cf4fc964ff", `{"name":"Ada Lovelace ✨","city":"Köln"}`},
		{"embeddedQuotes", "00000000-0000-0000-0000-000000000000", `{"quote":"she said \"hello\""}`},
		{"nested", "ffffffff-ffff-ffff-ffff-ffffffffffff", `{"a":{"b":[1,2,3]},"c":null}`},
		{"multiByteLength", "6f9619ff-8b86-d011-b42d-00cf4fc964ff", long},
	}

	reg := document.WireRegistry()
	out := make([]hashFixture, 0, len(cases))
	for _, tc := range cases {
		id, err := codec.UUIDFromString(tc.docID)
		if err != nil {
			fail(fmt.Errorf("%s: %w", tc.name, err))
		}
		doc := document.Document{ID: id, JSON: tc.json}
		encoded, err := codec.EncodeBytes(doc.ToDocumentBodyValue(), document.DocumentBodyType, reg)
		if err != nil {
			fail(fmt.Errorf("%s: %w", tc.name, err))
		}
		hash, err := document.ComputeContentHash(doc)
		if err != nil {
			fail(fmt.Errorf("%s: %w", tc.name, err))
		}
		out = append(out, hashFixture{
			Name:        tc.name,
			DocID:       tc.docID,
			JSON:        tc.json,
			EncodedHex:  hex.EncodeToString(encoded),
			ContentHash: hash.Hex(),
		})
	}
	return out
}

// buildTransactions pins the transactionBytes encoding, including the case that matters most
// for compatibility: a transaction with no preconditions must not carry a "preconditions" key
// at all (transaction_codec.go tags it omitempty precisely so an older peer sees what it always
// saw).
func buildTransactions() []transactionFixture {
	docID := mustUUID("6f9619ff-8b86-d011-b42d-00cf4fc964ff")
	txID := mustUUID("11111111-2222-3333-4444-555555555555")
	author := mustUUID("99999999-8888-7777-6666-555555555555")
	base := mustHash("00" + hex.EncodeToString(make([]byte, 31)))
	hash := mustHash("11" + hex.EncodeToString(make([]byte, 31)))

	cases := []struct {
		name string
		tx   document.Transaction
	}{
		{"writeNoPreconditions", document.Transaction{
			ID: txID, BaseVersion: base, Timestamp: codec.TimestampFromEpochMicros(1700000000000000),
			AuthorNodeID: author,
			Operations:   []document.Op{document.WriteOp{DocID: docID, Patch: `{"n":1}`}},
		}},
		{"writeExpectAbsent", document.Transaction{
			ID: txID, BaseVersion: base, Timestamp: codec.TimestampFromEpochMicros(1700000000000000),
			AuthorNodeID: author,
			Operations:   []document.Op{document.WriteOp{DocID: docID, Patch: `{"n":1}`}},
			Preconditions: []document.Precondition{
				{OpIndex: 0, Kind: document.ExpectAbsent},
			},
		}},
		{"writeExpectContentHash", document.Transaction{
			ID: txID, BaseVersion: base, Timestamp: codec.TimestampFromEpochMicros(1700000000000000),
			AuthorNodeID: author,
			Operations:   []document.Op{document.WriteOp{DocID: docID, Patch: `{"n":2}`}},
			Preconditions: []document.Precondition{
				{OpIndex: 0, Kind: document.ExpectContentHash, ContentHash: hash},
			},
		}},
		{"multiWrite", document.Transaction{
			ID: txID, BaseVersion: base, Timestamp: codec.TimestampFromEpochMicros(1700000000000000),
			AuthorNodeID: author,
			Operations: []document.Op{
				document.WriteOp{DocID: docID, Patch: `{"n":1}`},
				document.WriteOp{DocID: mustUUID("00000000-0000-0000-0000-000000000001"), Patch: `{"n":2}`},
			},
		}},
	}

	out := make([]transactionFixture, 0, len(cases))
	for _, tc := range cases {
		encoded, err := wire.EncodeTransaction(tc.tx)
		if err != nil {
			fail(fmt.Errorf("%s: %w", tc.name, err))
		}
		wireForm := make([]int, len(encoded))
		for i, b := range encoded {
			wireForm[i] = int(b)
		}
		out = append(out, transactionFixture{
			Name: tc.name, JSON: string(encoded), WireForm: wireForm,
		})
	}
	return out
}

func hdr(t wire.MessageType, correlation int) wire.Header {
	return wire.Header{
		MessageType:     t,
		ProtocolVersion: wire.KdbWireProtocolVersion,
		CorrelationID:   correlation,
	}
}

func strptr(s string) *string { return &s }

func repeat(s string, n int) string {
	out := make([]byte, 0, len(s)*n)
	for i := 0; i < n; i++ {
		out = append(out, s...)
	}
	return string(out)
}

func mustUUID(s string) codec.UUID {
	u, err := codec.UUIDFromString(s)
	if err != nil {
		fail(err)
	}
	return u
}

func mustHash(s string) codec.Hash {
	h, err := codec.HashFromHex(s)
	if err != nil {
		fail(err)
	}
	return h
}

func writeJSON(path string, value any) {
	body, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		fail(err)
	}
	if err := os.WriteFile(path, append(body, '\n'), 0o644); err != nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "genfixtures:", err)
	os.Exit(1)
}
