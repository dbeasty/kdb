package embed_test

import (
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
)

func TestPutJSONDocument_injectsIDAndCommits(t *testing.T) {
	ns := "demo/users"
	rt, err := embed.OpenMemoryRuntime("demo", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	genesis, err := rt.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	result, err := embed.PutJSONDocument(rt, ns, `{"name":"Ada"}`)
	if err != nil {
		t.Fatal(err)
	}
	if result.DocID == (codec.UUID{}) {
		t.Fatal("expected doc id")
	}
	if result.Commit == (codec.Hash{}) {
		t.Fatal("expected commit hash")
	}
	head, err := rt.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	if head == genesis {
		t.Fatal("expected head to advance")
	}
	commit, err := rt.DAG.GetCommitOrThrow(head)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := rt.Storage.GetDocument(ns, result.DocID, commit.DocumentTreeHash)
	if err != nil {
		t.Fatal(err)
	}
	if doc == nil {
		t.Fatal("document not found")
	}
	if !strings.Contains(doc.JSON, `"id":`) {
		t.Fatalf("json %q", doc.JSON)
	}
	if !strings.Contains(doc.JSON, `"name":"Ada"`) {
		t.Fatalf("json %q", doc.JSON)
	}
}

func TestPutJSONDocument_preservesExplicitID(t *testing.T) {
	ns := "demo/users"
	rt, err := embed.OpenMemoryRuntime("demo", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	docID, err := codec.UUIDFromString("00000000-0000-0000-0000-000000000001")
	if err != nil {
		t.Fatal(err)
	}
	result, err := embed.PutJSONDocument(rt, ns, `{"id":"00000000-0000-0000-0000-000000000001","v":1}`)
	if err != nil {
		t.Fatal(err)
	}
	if result.DocID != docID {
		t.Fatalf("doc id %+v", result.DocID)
	}
}
