package dag

import (
	"errors"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

func TestAppendAndHeadMovesLinear(t *testing.T) {
	dag, err := NewInMemoryCommitDag("ns")
	if err != nil {
		t.Fatal(err)
	}
	root, err := dag.Head()
	if err != nil {
		t.Fatal(err)
	}
	txID, _ := codec.RandomUUID()
	author, _ := codec.RandomUUID()
	tx := document.Transaction{
		ID: txID, BaseVersion: root, Operations: nil,
		Timestamp: codec.TimestampNow(), AuthorNodeID: author,
	}
	tree := document.EmptyDocumentTree()
	c, err := dag.AppendCommit(tx, root, tree, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	head, err := dag.Head()
	if err != nil {
		t.Fatal(err)
	}
	if c.Hash != head {
		t.Fatal("head did not advance")
	}
	if _, ok := dag.GetCommit(c.Hash); !ok {
		t.Fatal("commit not stored")
	}
}

func TestPutCommitIdempotent(t *testing.T) {
	dag, err := NewInMemoryCommitDag("ns")
	if err != nil {
		t.Fatal(err)
	}
	root, _ := dag.Head()
	txID, _ := codec.RandomUUID()
	author, _ := codec.RandomUUID()
	tx := document.Transaction{
		ID: txID, BaseVersion: root,
		Timestamp: codec.TimestampNow(), AuthorNodeID: author,
	}
	c, err := dag.AppendCommit(tx, root, document.EmptyDocumentTree(), nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := dag.PutCommit(c, true); err != nil {
		t.Fatal(err)
	}
	if err := dag.PutCommit(c, true); err != nil {
		t.Fatal(err)
	}
	head, _ := dag.Head()
	if c.Hash != head {
		t.Fatal("head changed")
	}
}

func TestDiffMaps(t *testing.T) {
	dag, err := NewInMemoryCommitDag("ns")
	if err != nil {
		t.Fatal(err)
	}
	id1, _ := codec.RandomUUID()
	id2, _ := codec.RandomUUID()
	id3, _ := codec.RandomUUID()
	h1 := codec.Hash{Bytes: [32]byte{1}}
	h2 := codec.Hash{Bytes: [32]byte{2}}
	h1m := codec.Hash{Bytes: [32]byte{3}}
	h3 := codec.Hash{Bytes: [32]byte{4}}

	root, _ := dag.Head()
	tx1 := newTx(root)
	treeA, err := document.BuildDocumentTree(map[codec.UUID]codec.Hash{id1: h1, id2: h2})
	if err != nil {
		t.Fatal(err)
	}
	ca, err := dag.AppendCommit(tx1, root, treeA, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	tx2 := newTx(ca.Hash)
	treeB, err := document.BuildDocumentTree(map[codec.UUID]codec.Hash{id1: h1m, id3: h3})
	if err != nil {
		t.Fatal(err)
	}
	cb, err := dag.AppendCommit(tx2, ca.Hash, treeB, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	d, err := dag.Diff(ca.Hash, cb.Hash)
	if err != nil {
		t.Fatal(err)
	}
	mod := d.Modified()
	if len(mod) != 1 || mod[0].DocID != id1 {
		t.Fatal("modified")
	}
	added := d.Added()
	if len(added) != 1 || added[0].DocID != id3 {
		t.Fatal("added")
	}
	removed := d.Removed()
	if len(removed) != 1 || removed[0].DocID != id2 {
		t.Fatal("removed")
	}
}

func TestEmptyDiffSameCommit(t *testing.T) {
	dag, err := NewInMemoryCommitDag("ns")
	if err != nil {
		t.Fatal(err)
	}
	h, _ := dag.Head()
	d, err := dag.Diff(h, h)
	if err != nil {
		t.Fatal(err)
	}
	if !d.IsEmpty() {
		t.Fatal("expected empty diff")
	}
}

func TestStubShowsInWalk(t *testing.T) {
	dag, err := NewInMemoryCommitDag("ns")
	if err != nil {
		t.Fatal(err)
	}
	genesis, _ := dag.Head()
	c1, err := dag.AppendCommit(newTx(genesis), genesis, document.EmptyDocumentTree(), nil, "")
	if err != nil {
		t.Fatal(err)
	}
	c2, err := dag.AppendCommit(newTx(c1.Hash), c1.Hash, document.EmptyDocumentTree(), nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dag.StubCommit(genesis, "ice://bucket/obj"); err != nil {
		t.Fatal(err)
	}
	w := dag.Walk(c2.Hash, nil, 10)
	found := false
	for _, e := range w {
		if _, ok := e.(StubbedEntry); ok {
			found = true
		}
	}
	if !found {
		t.Fatal("expected stub in walk")
	}
}

func TestCompactionBlockedWhenBranchHeadInSquash(t *testing.T) {
	dag, err := NewInMemoryCommitDag("ns")
	if err != nil {
		t.Fatal(err)
	}
	head, _ := dag.Head()
	_, err = dag.Squash([]codec.Hash{head}, head, document.EmptyDocumentTree(), nil, "")
	var safety *CompactionSafetyError
	if !errors.As(err, &safety) {
		t.Fatalf("expected CompactionSafetyError, got %v", err)
	}
}

func TestMissingParentRejected(t *testing.T) {
	dag, err := NewInMemoryCommitDag("ns")
	if err != nil {
		t.Fatal(err)
	}
	bogus := codec.Hash{Bytes: [32]byte{0x7f}}
	tx, _ := codec.RandomUUID()
	author, _ := codec.RandomUUID()
	bad, err := document.BuildCommit(
		[]codec.Hash{bogus}, "ns", tx, codec.TimestampNow(), author,
		nil, document.EmptyDocumentTree().TreeHash, nil, "",
	)
	if err != nil {
		t.Fatal(err)
	}
	err = dag.PutCommit(bad, true)
	var cons *ConsistencyError
	if !errors.As(err, &cons) {
		t.Fatalf("expected ConsistencyError, got %v", err)
	}
}

func newTx(base codec.Hash) document.Transaction {
	id, _ := codec.RandomUUID()
	author, _ := codec.RandomUUID()
	return document.Transaction{
		ID: id, BaseVersion: base,
		Timestamp: codec.TimestampNow(), AuthorNodeID: author,
	}
}
