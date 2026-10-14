package document

import (
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

func attachHash(b byte) codec.Hash {
	var h codec.Hash
	for i := range h.Bytes {
		h.Bytes[i] = b
	}
	return h
}

func att(id byte, name string, content byte) Attachment {
	return Attachment{ID: codec.UUID{MSB: int64(id), LSB: 1}, Name: name,
		AttachmentRef: AttachmentRef{Scheme: AttachmentHashSchemeChunked1, ContentHash: attachHash(content), Size: int64(content), Mime: "x/y"}}
}

func cnt(t *testing.T, tab AttachmentTable, doc codec.UUID) int {
	t.Helper()
	n, err := tab.Count(doc)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func get(t *testing.T, tab AttachmentTable, doc, id codec.UUID) (Attachment, bool) {
	t.Helper()
	a, ok, err := tab.Get(doc, id)
	if err != nil {
		t.Fatal(err)
	}
	return a, ok
}

func byName(t *testing.T, tab AttachmentTable, doc codec.UUID, name string) (Attachment, bool) {
	t.Helper()
	a, ok, err := tab.GetByName(doc, name)
	if err != nil {
		t.Fatal(err)
	}
	return a, ok
}

func TestAttachmentOpsRoundTripThroughACommit(t *testing.T) {
	id := codec.UUID{MSB: 1, LSB: 2}
	attID := codec.UUID{MSB: 3, LSB: 4}
	ops := []Op{
		WriteOp{DocID: id, Patch: `{"a":1}`},
		AttachmentPutOp{DocID: id, AttachmentID: attID, Name: "photo.png", Scheme: 1, ContentHash: attachHash(7), Size: 1 << 40, Mime: "image/png"},
		AttachmentPutOp{DocID: id, AttachmentID: attID, Name: "é ☃", Scheme: 1, ContentHash: attachHash(8), Size: 0, Mime: ""},
		AttachmentDeleteOp{DocID: id, AttachmentID: attID},
	}
	root := attachHash(9)
	c, err := BuildCommitWithAttachments(nil, "ns", id, codec.Timestamp{EpochMillis: 5}, id, ops, attachHash(1), nil, "m", &root)
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.ToPayloadBytes()
	if err != nil {
		t.Fatal(err)
	}
	back, err := FromPayloadBytes(b)
	if err != nil {
		t.Fatal(err)
	}
	if back.Hash != c.Hash || !reflect.DeepEqual(back.Operations, ops) || back.AttachmentTreeHash == nil || *back.AttachmentTreeHash != root {
		t.Fatalf("round trip:\n got %+v\nwant %+v", back, c)
	}
}

// A commit with no attachment table hashes exactly as it did before the field existed, and the
// root is part of the hash when present.
func TestAttachmentRootIsInTheCommitHashOnlyWhenSet(t *testing.T) {
	id := codec.UUID{MSB: 3, LSB: 4}
	ops := []Op{WriteOp{DocID: id, Patch: `{}`}}
	plain, err := BuildCommit(nil, "ns", id, codec.Timestamp{EpochMillis: 1}, id, ops, attachHash(1), nil, "")
	if err != nil {
		t.Fatal(err)
	}
	none, err := BuildCommitWithAttachments(nil, "ns", id, codec.Timestamp{EpochMillis: 1}, id, ops, attachHash(1), nil, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if plain.Hash != none.Hash {
		t.Fatal("a nil attachment root changed the commit hash")
	}
	root := attachHash(2)
	with, err := BuildCommitWithAttachments(nil, "ns", id, codec.Timestamp{EpochMillis: 1}, id, ops, attachHash(1), nil, "", &root)
	if err != nil {
		t.Fatal(err)
	}
	if with.Hash == plain.Hash {
		t.Fatal("the attachment root is not covered by the commit hash")
	}
	if got, _ := ComputeCommitHash(with); got != with.Hash {
		t.Fatal("ComputeCommitHash disagrees with BuildCommitWithAttachments")
	}
}

// A document holds several attachments, each found by ID or by name; replacing one leaves the
// others; names are unique within a document.
func TestAttachmentTableHoldsSeveralPerDocument(t *testing.T) {
	doc, other := codec.UUID{MSB: 1}, codec.UUID{MSB: 2}
	var tab AttachmentTable
	var err error
	for _, a := range []Attachment{att(1, "a.png", 1), att(2, "b.pdf", 2), att(3, "c.txt", 3)} {
		if tab, err = tab.Put(doc, a); err != nil {
			t.Fatal(err)
		}
	}
	if tab, err = tab.Put(other, att(1, "a.png", 9)); err != nil {
		t.Fatal(err) // the same name and ID on another document is fine
	}
	if cnt(t, tab, doc) != 3 || tab.Len() != 4 || tab.Documents() != 2 {
		t.Fatalf("count %d len %d docs %d", cnt(t, tab, doc), tab.Len(), tab.Documents())
	}
	if a, ok := byName(t, tab, doc, "b.pdf"); !ok || a.ID != att(2, "", 0).ID {
		t.Fatal("lookup by name")
	}
	if a, ok := get(t, tab, doc, att(3, "", 0).ID); !ok || a.Name != "c.txt" {
		t.Fatal("lookup by id")
	}
	// Replace one: its bytes and name change, its ID stays, the others are untouched.
	replaced, err := tab.Put(doc, att(2, "b-v2.pdf", 7))
	if err != nil {
		t.Fatal(err)
	}
	if a, _ := get(t, replaced, doc, att(2, "", 0).ID); a.Name != "b-v2.pdf" || a.ContentHash != attachHash(7) {
		t.Fatalf("replaced: %+v", a)
	}
	if _, ok := byName(t, replaced, doc, "b.pdf"); ok {
		t.Fatal("the old name still resolves")
	}
	if a, _ := get(t, replaced, doc, att(1, "", 0).ID); a.ContentHash != attachHash(1) || cnt(t, replaced, doc) != 3 {
		t.Fatal("replacing one attachment changed another")
	}
	// A name another attachment of the document has is refused.
	if _, err := tab.Put(doc, att(4, "a.png", 4)); err == nil {
		t.Fatal("a duplicate name was accepted")
	}
	if _, err := tab.Put(doc, Attachment{Name: "x", AttachmentRef: att(0, "", 1).AttachmentRef}); err == nil {
		t.Fatal("an attachment with no id was accepted")
	}
	for _, bad := range []string{"", strings.Repeat("x", 256), "a\nb", string([]byte{0xff})} {
		if _, err := tab.Put(doc, att(5, bad, 5)); err == nil {
			t.Fatalf("name %q was accepted", bad)
		}
	}
	// Deleting one keeps the rest; deleting all empties the document.
	one, _ := tab.Delete(doc, att(1, "", 0).ID)
	if cnt(t, one, doc) != 2 {
		t.Fatal("delete removed more than one")
	}
	none, _ := one.DeleteAll(doc)
	if cnt(t, none, doc) != 0 || none.Documents() != 1 {
		t.Fatal("DeleteAll")
	}
	// The root depends on contents, not on the order they were set in.
	var rev AttachmentTable
	for _, a := range []Attachment{att(3, "c.txt", 3), att(1, "a.png", 1), att(2, "b.pdf", 2)} {
		rev, _ = rev.Put(doc, a)
	}
	rev, _ = rev.Put(other, att(1, "a.png", 9))
	if *rev.Root() != *tab.Root() {
		t.Fatal("same contents, different roots")
	}
	if renamed, _ := tab.Put(doc, att(1, "renamed.png", 1)); *renamed.Root() == *tab.Root() {
		t.Fatal("a rename did not change the root")
	}
}

func TestAttachmentTableApply(t *testing.T) {
	a := codec.UUID{MSB: 1}
	idA, idB := codec.UUID{MSB: 10}, codec.UUID{MSB: 11}
	put := func(id codec.UUID, name string, h byte) Op {
		return AttachmentPutOp{DocID: a, AttachmentID: id, Name: name, Scheme: 1, ContentHash: attachHash(h), Size: int64(h), Mime: "x"}
	}
	t1, err := AttachmentTable{}.Apply([]Op{put(idA, "one", 1), put(idB, "two", 2), WriteOp{DocID: a, Patch: "{}"}})
	if err != nil {
		t.Fatal(err)
	}
	if cnt(t, t1, a) != 2 {
		t.Fatalf("count %d", cnt(t, t1, a))
	}
	t2, _ := t1.Apply([]Op{DeleteOp{DocID: a}})
	if cnt(t, t2, a) != 0 || t2.Root() != nil {
		t.Fatal("a document delete left attachments")
	}
	t3, _ := t1.Apply([]Op{AttachmentDeleteOp{DocID: a, AttachmentID: idA}, put(idA, "again", 3)})
	if got, ok := get(t, t3, a, idA); !ok || got.ContentHash != attachHash(3) || got.Name != "again" {
		t.Fatal("a put after a delete in the same ops was lost")
	}
	if _, err := (AttachmentTable{}).Apply([]Op{put(idA, "x", 1), put(idB, "x", 2)}); err == nil {
		t.Fatal("two attachments with one name were accepted")
	}
	bad := put(idA, "x", 1).(AttachmentPutOp)
	bad.Scheme = 9
	if _, err := (AttachmentTable{}).Apply([]Op{bad}); err == nil {
		t.Fatal("an unknown hash scheme was accepted")
	}
}

func TestPackedAttachmentTablesRoundTrip(t *testing.T) {
	doc, other := codec.UUID{MSB: 1}, codec.UUID{MSB: 2}
	var tab AttachmentTable
	tab, _ = tab.Put(doc, att(1, "a.png", 1))
	tab, _ = tab.Put(doc, att(2, "b ☃", 2))
	tab, _ = tab.Put(other, att(3, "c", 3))
	packed, err := AppendPackedAttachmentTables(nil, []AttachmentTable{tab, {}})
	if err != nil {
		t.Fatal(err)
	}
	back, err := ParsePackedAttachmentTables(packed)
	if err != nil {
		t.Fatal(err)
	}
	if len(back) != 1 || *back[0].Root() != *tab.Root() || back[0].Len() != 3 {
		t.Fatal("packed tables did not round-trip")
	}
	if _, err := ParsePackedAttachmentTables(packed[:len(packed)-2]); err == nil {
		t.Fatal("truncated tables accepted")
	}
}

// Content hashes: the chunked Merkle scheme, against an independent reading of its definition.
func TestAttachmentContentHashScheme(t *testing.T) {
	leaf := func(b []byte) [32]byte { return sha256.Sum256(append([]byte{0}, b...)) }
	node := func(l, r [32]byte) [32]byte { return sha256.Sum256(append(append([]byte{1}, l[:]...), r[:]...)) }
	ident := func(tree [32]byte, size int) codec.Hash {
		b := []byte{2, 0, 0, 0, 0, 0, 0, 0, 0}
		for i := 0; i < 8; i++ {
			b[1+i] = byte(uint64(size) >> (56 - 8*i))
		}
		return codec.Hash{Bytes: sha256.Sum256(append(b, tree[:]...))}
	}
	data := make([]byte, 3*AttachmentChunkBytes+5)
	for i := range data {
		data[i] = byte(i * 7)
	}
	c := func(i int) []byte { return data[i*AttachmentChunkBytes : min((i+1)*AttachmentChunkBytes, len(data))] }
	// Four leaves: RFC 6962 splits 4 as 2+2.
	want := ident(node(node(leaf(c(0)), leaf(c(1))), node(leaf(c(2)), leaf(c(3)))), len(data))
	if got := AttachmentContentHash(data); got != want {
		t.Fatal("four-chunk content hash differs from the definition")
	}
	// Three leaves: 2+1.
	three := data[:3*AttachmentChunkBytes]
	if got := AttachmentContentHash(three); got != ident(node(node(leaf(c(0)), leaf(c(1))), leaf(c(2))), len(three)) {
		t.Fatal("three-chunk content hash differs")
	}
	if got := AttachmentContentHash(nil); got != ident(leaf(nil), 0) {
		t.Fatal("empty content hash differs")
	}
	if got := AttachmentContentHash([]byte("hello")); got != ident(leaf([]byte("hello")), 5) {
		t.Fatal("single-chunk content hash differs")
	}
	if got := AttachmentContentHash([]byte("hello")).Hex(); got != attachmentHelloGolden {
		t.Fatalf("content hash of \"hello\" is %s; pinned %s - other implementations depend on it", got, attachmentHelloGolden)
	}
}

// Every chunk of a blob can be proven against the root on its own.
func TestMerkleInclusionProofs(t *testing.T) {
	for n := 1; n <= 17; n++ {
		leaves := make([]codec.Hash, n)
		var m MerkleBuilder
		for i := range leaves {
			leaves[i] = ChunkLeafHash([]byte{byte(i), byte(n)})
			m.Add(leaves[i])
		}
		tree := MerkleTreeHash(leaves)
		if m.Root() != tree {
			t.Fatalf("n=%d: streaming builder disagrees with the recursive tree hash", n)
		}
		for i := range leaves {
			proof := MerkleInclusionProof(leaves, i)
			if !VerifyMerkleInclusion(leaves[i], i, n, proof, tree) {
				t.Fatalf("n=%d i=%d: valid proof rejected", n, i)
			}
			if VerifyMerkleInclusion(ChunkLeafHash([]byte("forged")), i, n, proof, tree) {
				t.Fatalf("n=%d i=%d: forged leaf accepted", n, i)
			}
			if n > 1 && VerifyMerkleInclusion(leaves[i], (i+1)%n, n, proof, tree) && leaves[i] != leaves[(i+1)%n] {
				t.Fatalf("n=%d i=%d: proof accepted at the wrong index", n, i)
			}
		}
	}
}

// The ref and set hashes are fixed encodings other implementations must reproduce; pin them.
func TestAttachmentRefAndSetHashesAreStable(t *testing.T) {
	r := AttachmentRef{Scheme: 1, ContentHash: attachHash(0xab), Size: 1234567, Mime: "image/png"}
	if got := r.Hash().Hex(); got != attachmentRefGolden {
		t.Fatalf("ref hash %s, want %s - changing it breaks every attachment root", got, attachmentRefGolden)
	}
	set := AttachmentSet{{ID: codec.UUID{MSB: 1, LSB: 2}, Name: "a.png", AttachmentRef: r}}
	if got := set.Hash().Hex(); got != attachmentSetGolden {
		t.Fatalf("set hash %s, want %s", got, attachmentSetGolden)
	}
}

func TestAttachmentCommitGoldenBytes(t *testing.T) {
	id, _ := codec.UUIDFromString("6f9619ff-8b86-d011-b42d-00cf4fc964ff")
	attID, _ := codec.UUIDFromString("0b6f3c1e-2a4d-4e8f-9a1b-3c5d7e9f1a2b")
	ops := []Op{
		WriteOp{DocID: id, Patch: `{"a":1}`},
		AttachmentPutOp{DocID: id, AttachmentID: attID, Name: "photo.png", Scheme: 1, ContentHash: attachHash(0xab), Size: 1234567, Mime: "image/png"},
		AttachmentDeleteOp{DocID: id, AttachmentID: attID},
	}
	root := attachHash(0xcd)
	c, err := BuildCommitWithAttachments([]codec.Hash{attachHash(1)}, "app/data", id, codec.Timestamp{EpochMillis: 1750000000000, MicroRemainder: 5}, id, ops, attachHash(2), nil, "m", &root)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := c.ToPayloadBytes()
	if got := hex.EncodeToString(b); got != attachmentCommitGolden || c.Hash.Hex() != attachmentCommitGoldenHash {
		t.Fatalf("commit encoding changed:\n got %s %s", got, c.Hash.Hex())
	}
}
