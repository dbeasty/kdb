package server

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
)

// Regression test: NewKdbServerRuntime used to type-assert Runtime.DAG directly to
// *dag.InMemoryCommitDag, which only ever matches embed.OpenMemoryRuntime's DAG. A file-backed
// runtime (embed.OpenFileRuntime, the --data-dir CLI mode in cmd/kdb-service) wraps its DAG in
// *embed.PersistingCommitDAG for durability, so the assertion always failed and every commit
// against a file-backed KdbServerRuntime returned "commit requires an InMemoryCommitDag" - the
// Go native server's durable mode could not write at all. This proves both that commits now
// succeed against a file-backed runtime, and that they're actually durable (survive a reopen of
// the same data directory), not just accepted in memory.
func TestKdbServerRuntimeCommitsAndPersistsAgainstFileBackedRuntime(t *testing.T) {
	dataDir := t.TempDir()
	ns := "app/data"

	rt, err := embed.OpenFileRuntime(dataDir, "app", ns, schema.None())
	if err != nil {
		t.Fatalf("OpenFileRuntime: %v", err)
	}
	srv := NewKdbServerRuntime(rt)

	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	base, err := rt.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	tx := document.Transaction{
		ID:          mustRandomUUID(t),
		BaseVersion: base,
		Operations:  []document.Op{document.WriteOp{DocID: docID, Patch: `{"v":"durable"}`}},
		Timestamp:   codec.TimestampNow(),
	}
	commit, err := srv.Commit(ns, tx, "sess-1", auth.Principal{})
	if err != nil {
		t.Fatalf("Commit against a file-backed runtime should succeed, got: %v", err)
	}
	rt.Close()

	// Reopen the same data directory fresh - if the commit was actually persisted to the delta
	// log (not just applied to the in-memory delegate DAG), replaying it on open must reproduce
	// the exact same commit hash and document content.
	reopened, err := embed.OpenFileRuntime(dataDir, "app", ns, schema.None())
	if err != nil {
		t.Fatalf("reopen OpenFileRuntime: %v", err)
	}
	defer reopened.Close()

	head, err := reopened.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	if head != commit.Hash {
		t.Fatalf("expected reopened head to be the persisted commit %s, got %s - commit was not durably written", commit.Hash.Hex(), head.Hex())
	}
	headCommit, ok := reopened.DAG.GetCommit(head)
	if !ok {
		t.Fatalf("head commit %s missing after reopen", head.Hex())
	}
	doc, err := reopened.Storage.GetDocumentOrThrow(ns, docID, headCommit.DocumentTreeHash)
	if err != nil {
		t.Fatalf("document missing after reopen: %v", err)
	}
	if doc.JSON != `{"v":"durable"}` {
		t.Fatalf("expected persisted document content, got %q", doc.JSON)
	}
}

// The same regression, via Upsert - the other write path that funnels through commitWith.
func TestKdbServerRuntimeUpsertsAgainstFileBackedRuntime(t *testing.T) {
	dataDir := t.TempDir()
	ns := "app/data"

	rt, err := embed.OpenFileRuntime(dataDir, "app", ns, schema.None())
	if err != nil {
		t.Fatalf("OpenFileRuntime: %v", err)
	}
	srv := NewKdbServerRuntime(rt)
	defer rt.Close()

	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := srv.Upsert(ns, docID, `{"v":"upserted"}`, auth.Principal{}); err != nil {
		t.Fatalf("Upsert against a file-backed runtime should succeed, got: %v", err)
	}
}
