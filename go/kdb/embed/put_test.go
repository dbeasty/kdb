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

// TestPutJSONDocument_rejectsNonStringID is the regression test for the finding recorded in
// docs/kdb-finish-up-plan.md as 1-G3: a document whose "id" field is present but isn't a JSON
// string used to be silently accepted anyway, writing under a fresh random id the caller never
// asked for and had no way to learn about (`kdb put` reported success under an id the user never
// specified). It must now be a clear error instead.
func TestPutJSONDocument_rejectsNonStringID(t *testing.T) {
	ns := "demo/users"
	rt, err := embed.OpenMemoryRuntime("demo", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := embed.PutJSONDocument(rt, ns, `{"id":12345,"v":1}`); err == nil {
		t.Fatal("expected an error for a numeric \"id\" field, got none")
	}
}

// TestPutJSONDocument_rejectsEmptyID is 1-G3's other half: an explicitly empty "id" field is
// also a caller mistake worth surfacing, not a silent fresh-random-id substitution.
func TestPutJSONDocument_rejectsEmptyID(t *testing.T) {
	ns := "demo/users"
	rt, err := embed.OpenMemoryRuntime("demo", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := embed.PutJSONDocument(rt, ns, `{"id":"","v":1}`); err == nil {
		t.Fatal("expected an error for an empty \"id\" field, got none")
	}
}
