package storage_test

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	storagemem "github.com/limidus/kdb/go/kdb/storage/mem"
)

func TestInMemoryPutAndCommitTree(t *testing.T) {
	ns := "demo/users"
	d, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatal(err)
	}
	store := storagemem.NewInMemoryStorageAdapter()
	head, err := d.Head()
	if err != nil {
		t.Fatal(err)
	}
	commit, err := d.GetCommitOrThrow(head)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := document.FromJSON(`{"userId":"u1"}`)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutDocument(ns, doc); err != nil {
		t.Fatal(err)
	}
	tree, err := store.CommitTree(ns, commit.DocumentTreeHash)
	if err != nil {
		t.Fatal(err)
	}
	d.PutDocumentTree(tree)
	got, err := store.GetDocument(ns, doc.ID, tree.TreeHash)
	if err != nil || got == nil {
		t.Fatalf("get doc: %v %v", got, err)
	}
}

func TestInMemoryBlobRoundtrip(t *testing.T) {
	store := storagemem.NewInMemoryStorageAdapter()
	payload := []byte("hello-blob")
	h, err := store.WriteBlob(payload)
	if err != nil {
		t.Fatal(err)
	}
	out, err := store.ReadBlob(h)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != string(payload) {
		t.Fatalf("blob: %q", out)
	}
	_ = codec.Hash{}
}
