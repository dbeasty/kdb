package document

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

func TestFromJSONAssignsFreshID(t *testing.T) {
	d, err := FromJSON(`{"a":1}`)
	if err != nil {
		t.Fatal(err)
	}
	if d.JSON != `{"a":1}` {
		t.Fatalf("json: %q", d.JSON)
	}
}

func TestFromJSONWithIDPreservesID(t *testing.T) {
	id, _ := codec.UUIDFromString("12345678-1234-4123-8123-123456789012")
	d, err := FromJSONWithID(id, `{"x":true}`)
	if err != nil {
		t.Fatal(err)
	}
	if d.ID != id {
		t.Fatal("id mismatch")
	}
}

func TestDocumentBodyRoundTrip(t *testing.T) {
	id, _ := codec.RandomUUID()
	d, err := FromJSONWithID(id, `{"k":"v"}`)
	if err != nil {
		t.Fatal(err)
	}
	back, err := FromDocumentBodyValue(d.ToDocumentBodyValue())
	if err != nil {
		t.Fatal(err)
	}
	if back.ID != d.ID || back.JSON != d.JSON {
		t.Fatal("round trip mismatch")
	}
}

func TestContentHashDeterministic(t *testing.T) {
	id, _ := codec.RandomUUID()
	a := Document{ID: id, JSON: `{"a":1}`}
	b := Document{ID: id, JSON: `{"a":1}`}
	ha, err := a.ContentHash()
	if err != nil {
		t.Fatal(err)
	}
	hb, err := b.ContentHash()
	if err != nil {
		t.Fatal(err)
	}
	if ha.Hex() != hb.Hex() {
		t.Fatal("hash mismatch")
	}
}

func TestMergeRootLevelOverwrite(t *testing.T) {
	d, err := FromJSON(`{"a":1,"b":2}`)
	if err != nil {
		t.Fatal(err)
	}
	m, err := d.Merge(`{"b":99,"c":3}`)
	if err != nil {
		t.Fatal(err)
	}
	if m.JSON != `{"a":1,"b":99,"c":3}` {
		t.Fatalf("got %q", m.JSON)
	}
}

func TestDocumentTreeWithAndWithout(t *testing.T) {
	id, _ := codec.RandomUUID()
	h := codec.Hash{Bytes: [32]byte{3}}
	with, err := EmptyDocumentTree().With(id, h)
	if err != nil {
		t.Fatal(err)
	}
	without, err := with.Without(id)
	if err != nil {
		t.Fatal(err)
	}
	if without.TreeHash.Hex() != EmptyDocumentTree().TreeHash.Hex() {
		t.Fatal("expected empty tree hash")
	}
}

func TestDocumentTreeHashDeterministic(t *testing.T) {
	id1, _ := codec.UUIDFromString("11111111-1111-4111-8111-111111111111")
	id2, _ := codec.UUIDFromString("22222222-2222-4222-8222-222222222222")
	h1 := codec.Hash{Bytes: [32]byte{1}}
	h2 := codec.Hash{Bytes: [32]byte{2}}
	a, err := BuildDocumentTree(map[codec.UUID]codec.Hash{id1: h1, id2: h2})
	if err != nil {
		t.Fatal(err)
	}
	b, err := BuildDocumentTree(map[codec.UUID]codec.Hash{id2: h2, id1: h1})
	if err != nil {
		t.Fatal(err)
	}
	if a.TreeHash.Hex() != b.TreeHash.Hex() {
		t.Fatal("tree hash order sensitivity")
	}
}

func TestCommitHashDeterministic(t *testing.T) {
	treeHex := ""
	for i := 0; i < 32; i++ {
		treeHex += "01"
	}
	tree, _ := codec.HashFromHex(treeHex)
	c := sampleCommit(t, tree)
	h1, err := ComputeCommitHash(c)
	if err != nil {
		t.Fatal(err)
	}
	h2, err := ComputeCommitHash(c)
	if err != nil {
		t.Fatal(err)
	}
	if h1.Hex() != h2.Hex() {
		t.Fatal("commit hash not deterministic")
	}
}

func TestOpRoundTripAllTypes(t *testing.T) {
	id, _ := codec.RandomUUID()
	hash := codec.Hash{Bytes: [32]byte{7}}
	ops := []Op{
		WriteOp{DocID: id, Patch: `{"p":1}`},
		DeleteOp{DocID: id},
		FileWriteOp{Path: "p/x", BlobHash: hash},
		SchemaMigrationOp{MigrationID: id, MigrationPayload: `{"m":1}`},
	}
	for _, op := range ops {
		back, err := OpFromValue(op.toValue())
		if err != nil {
			t.Fatal(err)
		}
		if !opEqual(op, back) {
			t.Fatalf("round trip failed for %T", op)
		}
	}
}

func TestFromDocumentBodyBadUUID(t *testing.T) {
	bad := codec.RecordValue{Fields: map[int]codec.Value{
		2: codec.StringValue{V: "{}"},
	}}
	if _, err := FromDocumentBodyValue(bad); err == nil {
		t.Fatal("expected error")
	}
}

func TestMergeCommitTwoParents(t *testing.T) {
	p1 := codec.Hash{Bytes: [32]byte{1}}
	p2 := codec.Hash{Bytes: [32]byte{2}}
	tx, _ := codec.RandomUUID()
	author, _ := codec.RandomUUID()
	c := Commit{
		ParentHashes:     []codec.Hash{p1, p2},
		NamespaceID:      "ns",
		TransactionID:    tx,
		Timestamp:        codec.Timestamp{EpochMillis: 1},
		AuthorNodeID:     author,
		DocumentTreeHash: codec.Hash{Bytes: [32]byte{4}},
		Message:          "m",
	}
	realHash, err := ComputeCommitHash(c)
	if err != nil {
		t.Fatal(err)
	}
	c.Hash = realHash
	bytes, err := c.ToPayloadBytes()
	if err != nil {
		t.Fatal(err)
	}
	back, err := FromPayloadBytes(bytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(back.ParentHashes) != 2 {
		t.Fatal("expected 2 parents")
	}
	if !commitsEqual(c, back) {
		t.Fatal("round trip mismatch")
	}
}

func TestCommitStubPreservesOriginalHash(t *testing.T) {
	oh := codec.Hash{Bytes: [32]byte{9}}
	stub := CommitStub{
		OriginalHash: oh, ArchiveLocation: "s3://x",
		StubbedAt: codec.Timestamp{EpochMillis: 5},
	}
	reg := WireRegistry()
	blob, err := codec.EncodeBytes(stub.ToValue(), CommitStubWireType, reg)
	if err != nil {
		t.Fatal(err)
	}
	round, err := codec.DecodeBytes(blob, CommitStubWireType, reg)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := CommitStubFromValue(round)
	if err != nil {
		t.Fatal(err)
	}
	if s2.OriginalHash != oh {
		t.Fatal("hash mismatch")
	}
}

func TestFromJSONArrayRootThrows(t *testing.T) {
	if _, err := FromJSON(`[1,2,3]`); err == nil {
		t.Fatal("expected error")
	}
}

func sampleCommit(t *testing.T, treeHash codec.Hash) Commit {
	t.Helper()
	tx, _ := codec.RandomUUID()
	author, _ := codec.RandomUUID()
	docID, _ := codec.RandomUUID()
	c := Commit{
		ParentHashes:     []codec.Hash{{Bytes: [32]byte{2}}},
		NamespaceID:      "n",
		TransactionID:    tx,
		Timestamp:        codec.TimestampNow(),
		AuthorNodeID:     author,
		Operations:       []Op{WriteOp{DocID: docID, Patch: "{}"}},
		DocumentTreeHash: treeHash,
		Message:          "hi",
	}
	h, err := ComputeCommitHash(c)
	if err != nil {
		t.Fatal(err)
	}
	c.Hash = h
	return c
}

func opEqual(a, b Op) bool {
	switch av := a.(type) {
	case WriteOp:
		bv, ok := b.(WriteOp)
		return ok && av.DocID == bv.DocID && av.Patch == bv.Patch
	case DeleteOp:
		bv, ok := b.(DeleteOp)
		return ok && av.DocID == bv.DocID
	case FileWriteOp:
		bv, ok := b.(FileWriteOp)
		return ok && av.Path == bv.Path && av.BlobHash == bv.BlobHash
	case SchemaMigrationOp:
		bv, ok := b.(SchemaMigrationOp)
		return ok && av.MigrationID == bv.MigrationID && av.MigrationPayload == bv.MigrationPayload
	}
	return false
}

func commitsEqual(a, b Commit) bool {
	if a.Hash != b.Hash || a.NamespaceID != b.NamespaceID || a.Message != b.Message {
		return false
	}
	if len(a.ParentHashes) != len(b.ParentHashes) {
		return false
	}
	for i := range a.ParentHashes {
		if a.ParentHashes[i] != b.ParentHashes[i] {
			return false
		}
	}
	if len(a.Operations) != len(b.Operations) {
		return false
	}
	for i := range a.Operations {
		if !opEqual(a.Operations[i], b.Operations[i]) {
			return false
		}
	}
	return a.TransactionID == b.TransactionID &&
		a.AuthorNodeID == b.AuthorNodeID &&
		a.DocumentTreeHash == b.DocumentTreeHash &&
		a.Timestamp.EpochMillis == b.Timestamp.EpochMillis &&
		a.Timestamp.MicroRemainder == b.Timestamp.MicroRemainder
}
