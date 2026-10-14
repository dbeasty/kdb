package dag

import (
	"path/filepath"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// attachCommit appends a commit with ops on parent.
func (b *dagBuilder) attachCommit(name string, parent codec.Hash, ops ...document.Op) document.Commit {
	b.t.Helper()
	tx := b.tx(parent)
	tx.Operations = ops
	c, err := b.dag.AppendCommitDetached(tx, parent, document.EmptyDocumentTree(), nil, name)
	if err != nil {
		b.t.Fatalf("commit %s: %v", name, err)
	}
	b.names[c.Hash] = name
	return c
}

// attID is the attachment ID every putOp uses unless a test says otherwise.
var attID = codec.UUID{MSB: 77, LSB: 77}

func putOp(id codec.UUID, content byte) document.AttachmentPutOp {
	return document.AttachmentPutOp{DocID: id, AttachmentID: attID, Name: "file", Scheme: document.AttachmentHashSchemeChunked1,
		ContentHash: h(content), Size: int64(content) * 10, Mime: "application/octet-stream"}
}

func tableAt(t *testing.T, d *InMemoryCommitDag, c codec.Hash) document.AttachmentTable {
	t.Helper()
	tab, err := d.AttachmentTableAt(c)
	if err != nil {
		t.Fatal(err)
	}
	return tab
}

func aget(t *testing.T, tab document.AttachmentTable, doc, id codec.UUID) (document.Attachment, bool) {
	t.Helper()
	a, ok, err := tab.Get(doc, id)
	if err != nil {
		t.Fatal(err)
	}
	return a, ok
}

func abyName(t *testing.T, tab document.AttachmentTable, doc codec.UUID, name string) (document.Attachment, bool) {
	t.Helper()
	a, ok, err := tab.GetByName(doc, name)
	if err != nil {
		t.Fatal(err)
	}
	return a, ok
}

func acount(t *testing.T, tab document.AttachmentTable, doc codec.UUID) int {
	t.Helper()
	n, err := tab.Count(doc)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// The DAG derives every commit's attachment root from its parent's table and its operations, and
// a commit that touches no attachment carries its parent's root unchanged.
func TestCommitsCarryTheirAttachmentRoot(t *testing.T) {
	b := newDagBuilder(t)
	a, c := codec.UUID{MSB: 1}, codec.UUID{MSB: 2}

	plain := b.attachCommit("plain", b.root, document.WriteOp{DocID: a, Patch: "{}"})
	if plain.AttachmentTreeHash != nil {
		t.Fatal("a commit with no attachments carries a root")
	}
	one := b.attachCommit("one", plain.Hash, putOp(a, 1))
	if one.AttachmentTreeHash == nil {
		t.Fatal("an attachment put left no root")
	}
	if ref, ok := aget(t, tableAt(t, b.dag, one.Hash), a, attID); !ok || ref.ContentHash != h(1) || ref.Size != 10 {
		t.Fatalf("table at one: %+v %v", ref, ok)
	}
	json := b.attachCommit("json-only", one.Hash, document.WriteOp{DocID: a, Patch: `{"x":1}`})
	if json.AttachmentTreeHash == nil || *json.AttachmentTreeHash != *one.AttachmentTreeHash {
		t.Fatal("a JSON-only commit did not inherit its parent's root")
	}
	two := b.attachCommit("two", json.Hash, putOp(c, 2), putOp(a, 3))
	tab := tableAt(t, b.dag, two.Hash)
	if tab.Len() != 2 {
		t.Fatalf("len %d", tab.Len())
	}
	if ref, _ := aget(t, tab, a, attID); ref.ContentHash != h(3) {
		t.Fatal("the replaced attachment did not replace")
	}
	// Deleting the document takes its attachment; detaching the other empties the table.
	del := b.attachCommit("delete-a", two.Hash, document.DeleteOp{DocID: a})
	if _, ok := aget(t, tableAt(t, b.dag, del.Hash), a, attID); ok {
		t.Fatal("a document delete left its attachment")
	}
	empty := b.attachCommit("detach-c", del.Hash, document.AttachmentDeleteOp{DocID: c, AttachmentID: attID})
	if empty.AttachmentTreeHash != nil {
		t.Fatal("an empty table still has a root")
	}
	// Earlier commits still answer with their own tables.
	if ref, _ := aget(t, tableAt(t, b.dag, one.Hash), a, attID); ref.ContentHash != h(1) {
		t.Fatal("the table at an older commit changed")
	}
}

// A table that is not cached is rebuilt from the operations, and checked against the roots the
// commits recorded.
func TestAttachmentTablesRebuildFromHistory(t *testing.T) {
	b := newDagBuilder(t)
	a := codec.UUID{MSB: 1}
	var last document.Commit
	parent := b.root
	for i := byte(1); i <= 5; i++ {
		last = b.attachCommit("c", parent, putOp(a, i))
		parent = last.Hash
	}
	want := tableAt(t, b.dag, last.Hash)
	b.dag.mu.Lock()
	b.dag.attachments = attachmentCache{}
	b.dag.mu.Unlock()
	got := tableAt(t, b.dag, last.Hash)
	if *got.Root() != *want.Root() {
		t.Fatal("the rebuilt table differs")
	}
	if ref, _ := aget(t, got, a, attID); ref.ContentHash != h(5) {
		t.Fatal("rebuilt to the wrong version")
	}
}

// The resident tier, the graph file and a packed checkpoint all carry the root, so a commit read
// back from any of them re-encodes to its own hash.
func TestAttachmentRootSurvivesEveryCommitStore(t *testing.T) {
	skipWithoutMmap(t)
	b := newDagBuilder(t)
	b.dag.SetGraphSettings(GraphSettings{FileEnabled: true, RebuildCommits: 1 << 30})
	b.dag.ConfigureGraphFile(filepath.Join(t.TempDir(), "graph"), false)
	t.Cleanup(b.dag.CloseGraphFile)
	a := codec.UUID{MSB: 1}
	one := b.attachCommit("one", b.root, putOp(a, 1))
	two := b.attachCommit("two", one.Hash, document.WriteOp{DocID: a, Patch: "{}"})

	check := func(where string, d *InMemoryCommitDag) {
		t.Helper()
		for _, want := range []document.Commit{one, two} {
			got, err := d.GetCommitOrThrow(want.Hash)
			if err != nil {
				t.Fatalf("%s: %v", where, err)
			}
			if got.AttachmentTreeHash == nil || *got.AttachmentTreeHash != *want.AttachmentTreeHash {
				t.Fatalf("%s: lost the attachment root", where)
			}
			if rh, err := document.ComputeCommitHash(got); err != nil || rh != want.Hash {
				t.Fatalf("%s: the commit no longer reproduces its hash", where)
			}
		}
	}
	check("resident", b.dag)
	if err := b.dag.RebuildGraphFile(); err != nil {
		t.Fatal(err)
	}
	if st := b.dag.GraphFileStats(); st.ResidentCommits != 0 {
		t.Fatalf("commits still resident after a rebuild: %+v", st)
	}
	check("graph file", b.dag)
	if !b.dag.HasAttachments() {
		t.Fatal("HasAttachments is false for a DAG holding attachment roots")
	}

	// A packed checkpoint (no graph file, so every commit is packed into it).
	pb := newDagBuilder(t)
	one = pb.attachCommit("one", pb.root, putOp(a, 1))
	two = pb.attachCommit("two", one.Hash, document.WriteOp{DocID: a, Patch: "{}"})
	state, _, _, err := pb.dag.CheckpointSnapshotWithGraph(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !PackedCommitsHaveAttachments(state.PackedCommits) {
		t.Fatal("PackedCommitsHaveAttachments missed them")
	}
	fresh, _ := NewInMemoryCommitDag("ns")
	if err := fresh.RestoreCheckpoint(state); err != nil {
		t.Fatal(err)
	}
	for _, want := range []document.Commit{one, two} {
		got, ok := fresh.GetCommit(want.Hash)
		if !ok || got.AttachmentTreeHash == nil || *got.AttachmentTreeHash != *want.AttachmentTreeHash {
			t.Fatal("checkpoint: lost the attachment root")
		}
	}
}

func TestSquashRefusesAWindowWithAttachments(t *testing.T) {
	b := newDagBuilder(t)
	one := b.attachCommit("one", b.root, putOp(codec.UUID{MSB: 1}, 1))
	two := b.attachCommit("two", one.Hash, document.WriteOp{DocID: codec.UUID{MSB: 2}, Patch: "{}"})
	b.dag.mu.Lock()
	br := b.dag.branches[mainBranch]
	br.HeadHash = two.Hash
	b.dag.branches[mainBranch] = br
	b.dag.mu.Unlock()
	if _, err := b.dag.Squash([]codec.Hash{one.Hash}, b.root, document.EmptyDocumentTree(), nil, "s"); err == nil {
		t.Fatal("squashed away a commit with attachments")
	}
}
