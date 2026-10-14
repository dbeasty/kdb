package dag

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

func (b *dagBuilder) mergeCommit(name string, p0, p1 codec.Hash, tree document.DocumentTree, ops ...document.Op) document.Commit {
	b.t.Helper()
	tx := b.tx(p0)
	tx.Operations = ops
	c, err := b.dag.AppendMergeCommitOnto(nil, tx, p0, p1, tree, nil, name)
	if err != nil {
		b.t.Fatalf("merge %s: %v", name, err)
	}
	return c
}

func treeWith(t *testing.T, ids ...codec.UUID) document.DocumentTree {
	tree := document.EmptyDocumentTree()
	for _, id := range ids {
		var err error
		if tree, err = tree.With(id, h(99)); err != nil {
			t.Fatal(err)
		}
	}
	return tree
}

func named(id codec.UUID, attachmentID codec.UUID, name string, content byte) document.AttachmentPutOp {
	op := putOp(id, content)
	op.AttachmentID, op.Name = attachmentID, name
	return op
}

// The replica session's repro: one side writes a document's JSON, the other attaches to it. The
// merge keeps the attachment whichever side is the first parent.
func TestMergeKeepsBothSidesAttachments(t *testing.T) {
	for _, attachFirst := range []bool{false, true} {
		b := newDagBuilder(t)
		x := codec.UUID{MSB: 1}
		base := b.attachCommit("base", b.root, document.WriteOp{DocID: x, Patch: "{}"})
		l := b.attachCommit("L", base.Hash, document.WriteOp{DocID: x, Patch: `{"a":1}`})
		hc := b.attachCommit("H", base.Hash, named(x, attID, "d.txt", 1))
		p0, p1 := l.Hash, hc.Hash
		if attachFirst {
			p0, p1 = p1, p0
		}
		m := b.mergeCommit("M", p0, p1, treeWith(t, x), document.WriteOp{DocID: x, Patch: `{"a":1}`})
		a, ok := abyName(t, tableAt(t, b.dag, m.Hash), x, "d.txt")
		if !ok || a.ContentHash != h(1) {
			t.Fatalf("attachFirst=%v: merge lost d.txt (%+v)", attachFirst, a)
		}
		if !attachFirst && (m.AttachmentTreeHash == nil || *m.AttachmentTreeHash != *hc.AttachmentTreeHash) {
			t.Fatal("merge root is not the attaching side's table")
		}
		// The table follows from the merge's first parent and its own operations alone, which is
		// what a node that receives the commit derives.
		first, _ := tableAt(t, b.dag, p0).Apply(m.Operations)
		if *first.Root() != *m.AttachmentTreeHash {
			t.Fatal("first parent + merge ops does not give the merge's root")
		}
	}
}

func TestMergeAttachmentConflicts(t *testing.T) {
	b := newDagBuilder(t)
	x := codec.UUID{MSB: 1}
	k1, k2, k3, k4 := codec.UUID{MSB: 10}, codec.UUID{MSB: 11}, codec.UUID{MSB: 12}, codec.UUID{MSB: 13}
	base := b.attachCommit("base", b.root, document.WriteOp{DocID: x, Patch: "{}"},
		named(x, k1, "keep", 1), named(x, k2, "both", 2), named(x, k3, "gone", 3))
	// Ours: replaces "both", removes "gone" (theirs changes it: a version beats a removal), adds
	// "new" under k4. Theirs: replaces "both" differently, changes "gone", adds "new" under a
	// smaller ID than k4, so k4's is the one renamed.
	k0 := codec.UUID{MSB: 9}
	ours := b.attachCommit("ours", base.Hash, named(x, k2, "both", 20),
		document.AttachmentDeleteOp{DocID: x, AttachmentID: k3}, named(x, k4, "new", 40))
	theirs := b.attachCommit("theirs", base.Hash, named(x, k2, "both", 21), named(x, k3, "gone", 30), named(x, k0, "new", 50))

	m1 := b.mergeCommit("m1", ours.Hash, theirs.Hash, treeWith(t, x))
	m2 := b.mergeCommit("m2", theirs.Hash, ours.Hash, treeWith(t, x))
	t1, t2 := tableAt(t, b.dag, m1.Hash), tableAt(t, b.dag, m2.Hash)
	if *t1.Root() != *t2.Root() {
		t.Fatal("the merged table depends on the parent order")
	}
	want := map[string]byte{"keep": 1, "gone": 30, "new": 50}
	for name, content := range want {
		a, ok := abyName(t, t1, x, name)
		if !ok || a.ContentHash != h(content) {
			t.Errorf("%s: %+v %v", name, a, ok)
		}
	}
	if a, _ := aget(t, t1, x, k2); a.ContentHash != h(20) && a.ContentHash != h(21) {
		t.Errorf("both: %+v", a)
	}
	if a, ok := aget(t, t1, x, k4); !ok || a.Name != "new~"+k4.String()[:8] {
		t.Errorf("k4 renamed to %q", a.Name)
	}

	// A document the merged tree no longer has keeps no attachments.
	gone := b.mergeCommit("m3", ours.Hash, theirs.Hash, treeWith(t), document.DeleteOp{DocID: x})
	if acount(t, tableAt(t, b.dag, gone.Hash), x) != 0 {
		t.Fatal("a deleted document kept attachments through a merge")
	}
}
