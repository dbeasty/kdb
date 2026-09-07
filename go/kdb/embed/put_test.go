package embed_test

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
)

// TestPutJSONDocument_storesBodyByteExactAndCommits pins kdb-spec-layer16 §9.4: a body without an
// "id" is stored exactly as given - nothing injected, nothing reordered - and the minted identity
// is reported only through PutResult.DocID.
func TestPutJSONDocument_storesBodyByteExactAndCommits(t *testing.T) {
	ns := "demo/users"
	rt, err := embed.OpenMemoryRuntime("demo", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	genesis, err := rt.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	const body = `{"zeta":1,"name":"Ada","alpha":{"b":2,"a":1}}`
	result, err := embed.PutJSONDocument(rt, ns, body)
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
	if doc.JSON != body {
		t.Fatalf("stored body was rewritten:\n got %s\nwant %s", doc.JSON, body)
	}
}

// TestPutJSONDocument_derivesIDFromNonUUIDString: a natural-key "id" is accepted (it used to be
// rejected as "invalid uuid") and maps to the spec's derived UUID, with the body kept verbatim.
func TestPutJSONDocument_derivesIDFromNonUUIDString(t *testing.T) {
	ns := "demo/users"
	rt, err := embed.OpenMemoryRuntime("demo", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	const body = `{"id":"order-1","total":42}`
	result, err := embed.PutJSONDocument(rt, ns, body)
	if err != nil {
		t.Fatal(err)
	}
	if want := codec.DerivedUUID("order-1"); result.DocID != want {
		t.Fatalf("doc id %s, want derived %s", result.DocID, want)
	}
	head, _ := rt.DAG.Head()
	commit, _ := rt.DAG.GetCommitOrThrow(head)
	doc, err := rt.Storage.GetDocument(ns, result.DocID, commit.DocumentTreeHash)
	if err != nil || doc == nil {
		t.Fatalf("doc=%v err=%v", doc, err)
	}
	if doc.JSON != body {
		t.Fatalf("stored body was rewritten: %s", doc.JSON)
	}
	// A second put under the same natural key lands on the same document.
	again, err := embed.PutJSONDocument(rt, ns, `{"id":"order-1","total":43}`)
	if err != nil {
		t.Fatal(err)
	}
	if again.DocID != result.DocID {
		t.Fatalf("same natural key resolved to different ids: %s vs %s", again.DocID, result.DocID)
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
