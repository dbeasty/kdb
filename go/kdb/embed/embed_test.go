package embed_test

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
)

func TestOpenMemoryRuntime(t *testing.T) {
	ns := "demo/users"
	rt, err := embed.OpenMemoryRuntime("demo", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	if rt.Catalog != "demo" || rt.DefaultNamespace != ns {
		t.Fatalf("runtime: %+v", rt)
	}
	head, err := rt.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	commit, err := rt.DAG.GetCommitOrThrow(head)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := document.FromJSON(`{"userId":"u1"}`)
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Storage.PutDocument(ns, doc); err != nil {
		t.Fatal(err)
	}
	tree, err := rt.Storage.CommitTree(ns, commit.DocumentTreeHash)
	if err != nil {
		t.Fatal(err)
	}
	rt.DAG.PutDocumentTree(tree)
}

func TestOpenMemoryRuntimeWithSchema(t *testing.T) {
	sch, err := schema.Build([]schema.Field{
		schema.MustField("userId", schema.StringType{}, true, true, false),
	}, 1, codec.TimestampNow(), "")
	if err != nil {
		t.Fatal(err)
	}
	rt, err := embed.OpenMemoryRuntime("demo", "demo/users", sch)
	if err != nil {
		t.Fatal(err)
	}
	if rt.Schema.IsNone() {
		t.Fatal("expected schema")
	}
}
