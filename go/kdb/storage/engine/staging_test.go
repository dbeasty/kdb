package engine_test

// Covers ServerEngine's staging redesign specifically (see
// pending_shard.go / server_engine.go): the WIP feature this reconciles
// with (transaction write-phase rollback) was originally tested only
// against mem.InMemoryStorageAdapter, which already staged writes.
// ServerEngine didn't - Phase 2's gap-fix work made its writes visible
// immediately for performance - so it needs its own coverage of the
// "not visible until commit" and "DiscardPending rolls back" properties.

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
	"github.com/limidus/kdb/go/kdb/storage/io"
)

func TestServerEnginePutDocument_NotVisibleUntilCommitTree(t *testing.T) {
	e := newInMemoryServerEngine(t)
	doc, err := document.FromJSON(`{"v":"a"}`)
	if err != nil {
		t.Fatal(err)
	}

	if err := e.PutDocument("ns", doc); err != nil {
		t.Fatal(err)
	}
	got, err := e.GetDocument("ns", doc.ID, document.EmptyDocumentTree().TreeHash)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("expected doc not visible before CommitTree, got %+v", got)
	}

	tree, err := e.CommitTree("ns", document.EmptyDocumentTree().TreeHash)
	if err != nil {
		t.Fatal(err)
	}
	got, err = e.GetDocument("ns", doc.ID, tree.TreeHash)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.JSON != doc.JSON {
		t.Fatalf("expected doc visible after CommitTree, got %+v", got)
	}
}

func TestServerEngineDiscardPending_RollsBackStagedWrites(t *testing.T) {
	e := newInMemoryServerEngine(t)
	existing, err := document.FromJSON(`{"v":"existing"}`)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.PutDocument("ns", existing); err != nil {
		t.Fatal(err)
	}
	committed, err := e.CommitTree("ns", document.EmptyDocumentTree().TreeHash)
	if err != nil {
		t.Fatal(err)
	}

	// Stage a new put and a delete of the already-committed doc.
	staged, err := document.FromJSON(`{"v":"staged"}`)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.PutDocument("ns", staged); err != nil {
		t.Fatal(err)
	}
	if err := e.DeleteDocument("ns", existing.ID); err != nil {
		t.Fatal(err)
	}

	if err := e.DiscardPending("ns"); err != nil {
		t.Fatal(err)
	}

	// The staged put must not be visible, and the staged delete must not have applied -
	// discarding restores exactly the last-committed state.
	got, err := e.GetDocument("ns", staged.ID, committed.TreeHash)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("expected discarded staged put to stay invisible, got %+v", got)
	}
	got, err = e.GetDocument("ns", existing.ID, committed.TreeHash)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("expected discarded staged delete to leave the pre-existing doc intact")
	}

	// A subsequent CommitTree (no new pending writes) must reflect exactly the
	// pre-discard committed state, not anything discarded.
	tree, err := e.CommitTree("ns", document.EmptyDocumentTree().TreeHash)
	if err != nil {
		t.Fatal(err)
	}
	if tree.Size() != 1 {
		t.Fatalf("tree size=%d after discard+commit, want 1 (only the pre-existing doc)", tree.Size())
	}
}

func newInMemoryServerEngine(t *testing.T) *engine.ServerEngine {
	t.Helper()
	shim := io.NewInMemoryPlatformIO()
	cfg := storage.StorageEngineConfig{GlobalMemoryBudgetBytes: 1 << 20, IOShim: shim}
	return engine.NewServerEngine("ns", cfg, nil)
}
