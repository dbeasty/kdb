package document

import (
	"reflect"
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

func TestAttachmentOpsRoundTripThroughACommit(t *testing.T) {
	id := codec.UUID{MSB: 1, LSB: 2}
	ops := []Op{
		WriteOp{DocID: id, Patch: `{"a":1}`},
		AttachmentPutOp{DocID: id, ContentHash: attachHash(7), Size: 1 << 40, Mime: "image/png"},
		AttachmentPutOp{DocID: id, ContentHash: attachHash(8), Size: 0, Mime: ""},
		AttachmentDeleteOp{DocID: id},
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

func TestAttachmentTablePutGetDeleteAndRoot(t *testing.T) {
	a, b := codec.UUID{MSB: 1}, codec.UUID{MSB: 2}
	refA := AttachmentRef{ContentHash: attachHash(1), Size: 10, Mime: "text/plain"}
	refB := AttachmentRef{ContentHash: attachHash(2), Size: 20, Mime: "image/png"}

	var zero AttachmentTable
	if zero.Root() != nil || zero.Len() != 0 {
		t.Fatal("the zero table is not empty")
	}
	t1, err := zero.Put(a, refA)
	if err != nil {
		t.Fatal(err)
	}
	t2, err := t1.Put(b, refB)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := t2.Get(a); !ok || got != refA {
		t.Fatalf("get a: %+v %v", got, ok)
	}
	if _, ok := t1.Get(b); ok {
		t.Fatal("Put changed the table it was called on")
	}
	// The root depends on contents, not on the order they were set in.
	u1, _ := zero.Put(b, refB)
	u2, _ := u1.Put(a, refA)
	if *u2.Root() != *t2.Root() {
		t.Fatal("same contents, different roots")
	}
	// Changing only the mime changes the root.
	t3, _ := t2.Put(a, AttachmentRef{ContentHash: refA.ContentHash, Size: refA.Size, Mime: "application/octet-stream"})
	if *t3.Root() == *t2.Root() {
		t.Fatal("a mime change did not change the root")
	}
	t4, err := t2.Delete(a)
	if err != nil {
		t.Fatal(err)
	}
	t5, _ := t4.Delete(b)
	if t5.Root() != nil || t5.Len() != 0 {
		t.Fatal("deleting every attachment did not empty the table")
	}
	if t6, _ := t5.Delete(a); t6.Len() != 0 {
		t.Fatal("deleting a missing attachment did something")
	}
}

// DeleteOp removes the document's attachment; AttachmentPutOp after a delete in the same ops sets
// it again; operations naming no attachment leave the table alone.
func TestAttachmentTableApply(t *testing.T) {
	a, b := codec.UUID{MSB: 1}, codec.UUID{MSB: 2}
	put := func(id codec.UUID, h byte) Op {
		return AttachmentPutOp{DocID: id, ContentHash: attachHash(h), Size: int64(h), Mime: "x"}
	}
	t1, err := AttachmentTable{}.Apply([]Op{put(a, 1), put(b, 2), WriteOp{DocID: a, Patch: "{}"}})
	if err != nil {
		t.Fatal(err)
	}
	if t1.Len() != 2 {
		t.Fatalf("len %d", t1.Len())
	}
	t2, _ := t1.Apply([]Op{DeleteOp{DocID: a}})
	if _, ok := t2.Get(a); ok || t2.Len() != 1 {
		t.Fatal("a document delete left its attachment")
	}
	t3, _ := t2.Apply([]Op{AttachmentDeleteOp{DocID: b}, put(b, 3)})
	if r, ok := t3.Get(b); !ok || r.ContentHash != attachHash(3) {
		t.Fatal("a put after a delete in the same ops was lost")
	}
	if _, err := (AttachmentTable{}).Apply([]Op{AttachmentPutOp{DocID: a, Size: -1}}); err == nil {
		t.Fatal("a negative size was accepted")
	}
	if TouchesAttachments([]Op{WriteOp{DocID: a}}, true) || TouchesAttachments([]Op{DeleteOp{DocID: a}}, false) {
		t.Fatal("TouchesAttachments reported a change that cannot happen")
	}
	if !TouchesAttachments([]Op{DeleteOp{DocID: a}}, true) || !TouchesAttachments([]Op{AttachmentDeleteOp{DocID: a}}, false) {
		t.Fatal("TouchesAttachments missed a change")
	}
}

// The ref hash is a fixed encoding other implementations must reproduce; pin it.
func TestAttachmentRefHashIsStable(t *testing.T) {
	r := AttachmentRef{ContentHash: attachHash(0xab), Size: 1234567, Mime: "image/png"}
	if got := r.Hash().Hex(); got != attachmentRefGolden {
		t.Fatalf("ref hash %s, want %s - changing it breaks every attachment root", got, attachmentRefGolden)
	}
}
