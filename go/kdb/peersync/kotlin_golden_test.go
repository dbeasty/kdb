package peersync

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage/attachstore"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
	"github.com/limidus/kdb/go/kdb/wire"
)

// Golden frames for Kotlin's peer sync v2 (kdb-peer-sync SyncV2GoldenTest): Kotlin must decode
// each sync frame to the same message and encode that message to the same bytes, and a Kotlin host
// serving the same blob must answer the same BLOB_FETCH with the same frame. Set
// KDB_PRINT_GOLDEN=1 to print the frames after a deliberate change.

func goldenHash(b byte) codec.Hash {
	var h codec.Hash
	for i := range h.Bytes {
		h.Bytes[i] = b
	}
	return h
}

func goldenHeader(cid int, t wire.MessageType) wire.Header {
	return wire.Header{MessageType: t, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: cid}
}

func mustGoldenUUID(s string) codec.UUID {
	u, err := codec.UUIDFromString(s)
	if err != nil {
		panic(err)
	}
	return u
}

var (
	goldenDoc = mustGoldenUUID("6f9619ff-8b86-d011-b42d-00cf4fc964ff")
	goldenAtt = mustGoldenUUID("0b6f3c1e-2a4d-4e8f-9a1b-3c5d7e9f1a2b")
)

// goldenCommit is a commit carrying a document write, an attachment put and an attachment
// delete, with an attachment root - what Kotlin must decode to the same hash.
func goldenCommit(t *testing.T) document.Commit {
	root := goldenHash(9)
	c, err := document.BuildCommitWithAttachments([]codec.Hash{goldenHash(1)}, "app/data", codec.UUID{MSB: 1, LSB: 2},
		codec.TimestampFromEpochMicros(1_760_000_000_123_456), codec.UUID{MSB: 3, LSB: 4},
		[]document.Op{
			document.WriteOp{DocID: goldenDoc, Patch: `{"id":"6f9619ff-8b86-d011-b42d-00cf4fc964ff","n":1}`},
			document.AttachmentPutOp{DocID: goldenDoc, AttachmentID: goldenAtt, Name: "photo.png", Scheme: document.AttachmentHashSchemeChunked1,
				ContentHash: goldenHash(7), Size: 3 << 20, Mime: "image/png"},
			document.AttachmentDeleteOp{DocID: goldenDoc, AttachmentID: codec.UUID{MSB: 8, LSB: 9}},
		}, goldenHash(2), nil, "golden", &root)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func syncV2GoldenSamples(t *testing.T) []wire.Message {
	user, token := "peer", "t0k"
	c := goldenCommit(t)
	return []wire.Message{
		wire.SyncHelloMessage{H: goldenHeader(10001, wire.MsgSyncHello), NodeID: "node-a", Protocol: 2,
			Capabilities: []string{"branches", "tags", "zstd-pack", "attachments"}, Namespaces: []string{"app/data", "!app/x"},
			User: &user, Token: &token},
		wire.SyncHelloAckMessage{H: goldenHeader(10001, wire.MsgSyncHelloAck), Accepted: true, NodeID: "node-b", Protocol: 2,
			Capabilities: []string{"branches", "attachments"},
			Refs: []wire.NamespaceRefs{{Namespace: "app/data", Branches: map[string]string{"main": c.Hash.Hex()},
				Tags: map[string]string{"v1": goldenHash(1).Hex()}, Access: wire.AccessPull}}},
		wire.SyncHelloAckMessage{H: goldenHeader(10001, wire.MsgSyncHelloAck), NodeID: "node-b", Protocol: 2, Reason: "nope"},
		wire.RefsRequestMessage{H: goldenHeader(10002, wire.MsgRefsRequest), Namespaces: []string{"app/data"}},
		wire.RefsResultMessage{H: goldenHeader(10002, wire.MsgRefsResult),
			Refs: []wire.NamespaceRefs{{Namespace: "app/data", Branches: map[string]string{"main": goldenHash(1).Hex()}}}},
		wire.FetchRequestMessage{H: goldenHeader(10003, wire.MsgFetchRequest), Namespace: "app/data",
			Wants: []codec.Hash{c.Hash}, Haves: []codec.Hash{goldenHash(1)}, MaxBytes: 4 << 20},
		wire.PackPageMessage{H: goldenHeader(10003, wire.MsgPackPage), Namespace: "app/data", Commits: []document.Commit{c}, Done: true},
		wire.RefUpdateMessage{H: goldenHeader(10004, wire.MsgRefUpdate), Namespace: "app/data", Kind: wire.RefBranch, Ref: "main",
			OldHex: goldenHash(1).Hex(), NewHex: c.Hash.Hex(), Commits: []document.Commit{c}, More: true},
		wire.RefUpdateAckMessage{H: goldenHeader(10004, wire.MsgRefUpdateAck), Namespace: "app/data", Kind: wire.RefBranch, Ref: "main",
			Outcome: wire.RefConflict, HeadHex: goldenHash(1).Hex(), Stored: 1,
			ConflictBytes: []byte(`{"transactionId":"x","baseHash":"a","targetHash":"b","conflicts":[]}`)},
		wire.PeerErrorMessage{H: goldenHeader(10005, wire.MsgPeerError), Code: wire.ErrorCodeUnsupported, Message: "no"},
	}
}

var syncV2GoldenFrames = []string{
	"ca0000002600010011270000017b226b696e64223a2273796e6348656c6c6f222c2273796e6348656c6c6f223a7b226e6f64654964223a226e6f64652d61222c2270726f746f636f6c223a322c226361706162696c6974696573223a5b226272616e63686573222c2274616773222c227a7374642d7061636b222c226174746163686d656e7473225d2c226e616d65737061636573223a5b226170702f64617461222c22216170702f78225d2c2275736572223a2270656572222c22746f6b656e223a2274306b227d7d",
	"680100002700010011270000017b226b696e64223a2273796e6348656c6c6f41636b222c2273796e6348656c6c6f41636b223a7b226163636570746564223a747275652c226e6f64654964223a226e6f64652d62222c2270726f746f636f6c223a322c226361706162696c6974696573223a5b226272616e63686573222c226174746163686d656e7473225d2c2272656673223a5b7b226e616d657370616365223a226170702f64617461222c226272616e63686573223a7b226d61696e223a2233636535643662636438363833333935333830363036633861656365323965616136643935663665363334626534326462626434353935363632316563353634227d2c2274616773223a7b227631223a2230313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031227d2c22616363657373223a2270756c6c227d5d7d7d",
	"750000002700010011270000017b226b696e64223a2273796e6348656c6c6f41636b222c2273796e6348656c6c6f41636b223a7b226163636570746564223a66616c73652c226e6f64654964223a226e6f64652d62222c2270726f746f636f6c223a322c22726561736f6e223a226e6f7065227d7d",
	"4d0000002800010012270000017b226b696e64223a227265667352657175657374222c227265667352657175657374223a7b226e616d65737061636573223a5b226170702f64617461225d7d7d",
	"aa0000002900010012270000017b226b696e64223a2272656673526573756c74222c2272656673526573756c74223a7b2272656673223a5b7b226e616d657370616365223a226170702f64617461222c226272616e63686573223a7b226d61696e223a2230313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031227d7d5d7d7d",
	"f90000002a00010013270000017b226b696e64223a22666574636852657175657374222c22666574636852657175657374223a7b226e616d657370616365223a226170702f64617461222c2277616e7473223a5b2233636535643662636438363833333935333830363036633861656365323965616136643935663665363334626534326462626434353935363632316563353634225d2c226861766573223a5b2230313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031225d2c226d61784279746573223a343139343330347d7d",
	"840200002b00010013270000017b226b696e64223a227061636b50616765222c227061636b50616765223a7b226e616d657370616365223a226170702f64617461222c22636f6d6d6974735061796c6f6164223a2241514141414938424141434e4177454b415145424151454241514542415145424151454241514542415145424151454241514542415145424151454241676b49595842774c32526864474544447741414141414141414142414141414141414141414945425544697a2b36315141594142513841414141414141414141774141414141414141414542676f444145674244322b5747662b4c68744152744330417a302f4a5a50384343544e37496d6c6b496a6f694e6d59354e6a45355a6d59744f4749344e69316b4d4445784c5749304d6d51744d44426a5a6a526d597a6b324e475a6d49697769626949364d583045626745506235595a2f347547304247304c51445054386c6b2f774950433238384869704e546f2b61477a7864667038614b774d4a4358426f623352764c6e42755a77514541514141414155504277634842776348427763484277634842776348427763484277634842776348427763484277634842776347425141414d4141414141414142776b4a615731685a3255766347356e4253514244322b5747662b4c68744152744330417a302f4a5a503843447741414141414141414149414141414141414141416b48447749434167494341674943416749434167494341674943416749434167494341674943416749434167494343516b475a3239735a4756754367344243516b4a43516b4a43516b4a43516b4a43516b4a43516b4a43516b4a43516b4a43516b4a43516b4a43516b3d222c22646f6e65223a747275657d7d",
	"3b0300002c00010014270000017b226b696e64223a22726566557064617465222c22726566557064617465223a7b226e616d657370616365223a226170702f64617461222c226b696e64223a226272616e6368222c22726566223a226d61696e222c226f6c64486578223a2230313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031222c226e6577486578223a2233636535643662636438363833333935333830363036633861656365323965616136643935663665363334626534326462626434353935363632316563353634222c22636f6d6d6974735061796c6f6164223a2241514141414938424141434e4177454b415145424151454241514542415145424151454241514542415145424151454241514542415145424151454241676b49595842774c32526864474544447741414141414141414142414141414141414141414945425544697a2b36315141594142513841414141414141414141774141414141414141414542676f444145674244322b5747662b4c68744152744330417a302f4a5a50384343544e37496d6c6b496a6f694e6d59354e6a45355a6d59744f4749344e69316b4d4445784c5749304d6d51744d44426a5a6a526d597a6b324e475a6d49697769626949364d583045626745506235595a2f347547304247304c51445054386c6b2f774950433238384869704e546f2b61477a7864667038614b774d4a4358426f623352764c6e42755a77514541514141414155504277634842776348427763484277634842776348427763484277634842776348427763484277634842776347425141414d4141414141414142776b4a615731685a3255766347356e4253514244322b5747662b4c68744152744330417a302f4a5a503843447741414141414141414149414141414141414141416b48447749434167494341674943416749434167494341674943416749434167494341674943416749434167494343516b475a3239735a4756754367344243516b4a43516b4a43516b4a43516b4a43516b4a43516b4a43516b4a43516b4a43516b4a43516b4a43516b3d222c226d6f7265223a747275657d7d",
	"450100002d00010014270000017b226b696e64223a2272656655706461746541636b222c2272656655706461746541636b223a7b226e616d657370616365223a226170702f64617461222c226b696e64223a226272616e6368222c22726566223a226d61696e222c226f7574636f6d65223a22636f6e666c696374222c2268656164486578223a2230313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031222c2273746f726564223a312c22636f6e666c6963744279746573223a2265794a30636d46756332466a64476c76626b6c6b496a6f6965434973496d4a686332564959584e6f496a6f6959534973496e5268636d646c64456868633267694f694a6949697769593239755a6d78705933527a496a70625858303d227d7d",
	"620000002500010015270000017b226b696e64223a22706565724572726f72222c22706565724572726f72223a7b226e616d657370616365223a22222c22636f6465223a22554e535550504f52544544222c226d657373616765223a226e6f227d7d",
}

// goldenBlob is a 2.5 MiB blob: two whole chunks and a short third.
func goldenBlob() []byte {
	b := make([]byte, 5<<19)
	for i := range b {
		b[i] = byte(i*7 + i>>12)
	}
	return b
}

// blobFetchGolden is what a host holding goldenBlob, attached at a commit main still names, answers
// to two BLOB_FETCH requests: the whole blob, then from its second chunk at most one chunk plus an
// entry for a blob it does not hold. Each is the frame's length and SHA-256.
func blobFetchGoldenFrames(t *testing.T) [][]byte {
	d, err := dag.NewInMemoryCommitDag("app/data")
	if err != nil {
		t.Fatal(err)
	}
	store := attachstore.New(storio.NewInMemoryPlatformIO(), "app/data", nil, attachstore.Config{})
	info, err := store.Put(bytes.NewReader(goldenBlob()))
	if err != nil {
		t.Fatal(err)
	}
	head, _ := d.Head()
	c, err := d.AppendCommit(document.Transaction{Operations: []document.Op{
		document.WriteOp{DocID: goldenDoc, Patch: "{}"},
		document.AttachmentPutOp{DocID: goldenDoc, AttachmentID: goldenAtt, Name: "blob.bin", Scheme: document.AttachmentHashSchemeChunked1,
			ContentHash: info.Hash, Size: info.Size},
	}}, head, document.EmptyDocumentTree(), nil, "")
	if err != nil {
		t.Fatal(err)
	}
	env := IngestEnv{DAG: d, NamespaceID: "app/data", Attachments: store}
	table, err := d.AttachmentTableAt(c.Hash)
	if err != nil {
		t.Fatal(err)
	}
	serve := func(cid int, entries []wire.BlobFetchEntry) []byte {
		out := wire.BlobFetchResultMessage{H: goldenHeader(cid, wire.MsgBlobFetchResult), Namespace: "app/data"}
		budget := int64(blobFetchPageBytes)
		for i, e := range entries {
			part, used := serveBlobEntry(env, e, budget, &table)
			part.Index = i
			budget -= used
			out.Parts = append(out.Parts, part)
		}
		frame, err := wire.NewCodec(wire.EncodingJSON).Encode(out)
		if err != nil {
			t.Fatal(err)
		}
		return frame
	}
	entry := wire.BlobFetchEntry{DocID: goldenDoc, AttachmentID: goldenAtt, CommitHex: c.Hash.Hex(), Hash: info.Hash}
	second := entry
	second.Offset, second.MaxBytes = document.AttachmentChunkBytes, 1
	missing := entry
	missing.Hash = goldenHash(5)
	return [][]byte{serve(20, []wire.BlobFetchEntry{entry}), serve(21, []wire.BlobFetchEntry{second, missing})}
}

var blobFetchGolden = []string{
	"cdcdca2b753a88122500887dba0820046c9314c7d98e4310c1f9bbfd12d38dcf", // 2621728 bytes
	"84aa833f5b7d8048859ef81ee26997677b7e95c9f624a0013b1fa17e7c66cd14", // 1049048 bytes
}

func TestKotlinSyncV2GoldenFrames(t *testing.T) {
	print := os.Getenv("KDB_PRINT_GOLDEN") != ""
	codec := wire.NewCodec(wire.EncodingJSON)
	samples := syncV2GoldenSamples(t)
	for i, m := range samples {
		frame, err := codec.Encode(m)
		if err != nil {
			t.Fatal(err)
		}
		got := hex.EncodeToString(frame)
		if print {
			t.Logf("sync %d: %s", i, got)
			continue
		}
		if i >= len(syncV2GoldenFrames) || got != syncV2GoldenFrames[i] {
			t.Errorf("%T: frame changed:\n %s", m, got)
		}
		// And the golden decodes back to the message.
		if _, err := codec.Decode(frame); err != nil {
			t.Errorf("%T: %v", m, err)
		}
	}
	for i, frame := range blobFetchGoldenFrames(t) {
		sum := sha256.Sum256(frame)
		got := hex.EncodeToString(sum[:])
		if print {
			t.Logf("blob %d: %d %s", i, len(frame), got)
			continue
		}
		if i >= len(blobFetchGolden) || got != blobFetchGolden[i] {
			t.Errorf("BLOB_FETCH_RESULT %d changed: %d bytes, sha256 %s", i, len(frame), got)
		}
	}
	if len(samples) != len(syncV2GoldenFrames) && !print {
		t.Errorf("%d samples, %d golden frames", len(samples), len(syncV2GoldenFrames))
	}
}
