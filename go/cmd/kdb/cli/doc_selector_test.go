package cli_test

import (
	"testing"

	"github.com/limidus/kdb/go/cmd/kdb/cli"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	storagemem "github.com/limidus/kdb/go/kdb/storage/mem"
)

func TestResolveDocSelector_prefixUnique(t *testing.T) {
	ns := "demo/users"
	d, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatal(err)
	}
	store := storagemem.NewInMemoryStorageAdapter()
	head, _ := d.Head()
	genesis, _ := d.GetCommitOrThrow(head)

	id, _ := codec.UUIDFromString("aaaaaaaa-0000-0000-0000-000000000000")
	doc, _ := document.FromJSONWithID(id, `{"v":1}`)
	_ = store.PutDocument(ns, doc)
	tree, _ := store.CommitTree(ns, genesis.DocumentTreeHash)
	d.PutDocumentTree(tree)

	got, err := cli.ResolveDocSelectorForTest(ns, store, tree.TreeHash, "aaaaaaaa")
	if err != nil {
		t.Fatal(err)
	}
	if got != id {
		t.Fatalf("got %s", got.String())
	}
}

func TestResolveDocSelector_prefixAmbiguous(t *testing.T) {
	ns := "demo/users"
	d, _ := dag.NewInMemoryCommitDag(ns)
	store := storagemem.NewInMemoryStorageAdapter()
	head, _ := d.Head()
	genesis, _ := d.GetCommitOrThrow(head)

	id1, _ := codec.UUIDFromString("bbbbbbbb-0000-0000-0000-000000000000")
	id2, _ := codec.UUIDFromString("bbbbbbbb-1111-1111-1111-111111111111")
	doc1, _ := document.FromJSONWithID(id1, `{"v":1}`)
	doc2, _ := document.FromJSONWithID(id2, `{"v":2}`)
	_ = store.PutDocument(ns, doc1)
	_ = store.PutDocument(ns, doc2)
	tree, _ := store.CommitTree(ns, genesis.DocumentTreeHash)
	d.PutDocumentTree(tree)

	_, err := cli.ResolveDocSelectorForTest(ns, store, tree.TreeHash, "bbbbbbbb")
	if err == nil {
		t.Fatal("expected ambiguity error")
	}
}

