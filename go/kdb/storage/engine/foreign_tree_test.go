package engine

import (
	"errors"
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/io"
)

func foreignTree(t *testing.T, n int) (document.DocumentTree, map[codec.UUID]string) {
	t.Helper()
	entries := map[codec.UUID]codec.Hash{}
	bodies := map[codec.UUID]string{}
	for i := 0; i < n; i++ {
		id, _ := codec.RandomUUID()
		body := fmt.Sprintf(`{"foreign":%d}`, i)
		h, err := (document.Document{ID: id, JSON: body}).ContentHash()
		if err != nil {
			t.Fatal(err)
		}
		entries[id], bodies[id] = h, body
	}
	tree, err := document.BuildDocumentTree(entries)
	if err != nil {
		t.Fatal(err)
	}
	return tree, bodies
}

func foreignEngine(s storage.HistoryStrategy) *ServerEngine {
	return NewServerEngine("ns", storage.StorageEngineConfig{
		GlobalMemoryBudgetBytes: 1 << 30, IOShim: io.NewInMemoryPlatformIO(), HistoryStrategy: s,
	}, nil)
}

// A foreign tree resolves by hash with its documents, and the live tree does not move.
func TestStoreForeignTreeResolvesWithoutTouchingTheLiveTree(t *testing.T) {
	e := foreignEngine(storage.HistoryStrategyObjects)
	live := e.LiveTree().TreeHash
	tree, bodies := foreignTree(t, 20)
	if err := e.StoreForeignTree("ns", tree, bodies); err != nil {
		t.Fatal(err)
	}
	if e.LiveTree().TreeHash != live {
		t.Fatal("storing a foreign tree moved the live tree")
	}
	for id, body := range bodies {
		doc, err := e.GetDocument("ns", id, tree.TreeHash)
		if err != nil || doc == nil || doc.JSON != body {
			t.Fatalf("document %s at the foreign tree: %v %v", id, doc, err)
		}
	}
	e.treesByHash = newBoundedTreeStore(1 << 20) // drop the cache: the tree object must resolve it
	got, ok, err := e.TreeAt(tree.TreeHash)
	if err != nil || !ok || got.TreeHash != tree.TreeHash {
		t.Fatalf("the foreign tree did not resolve from its object: %v %v", ok, err)
	}
}

// A body that does not match its content hash refuses the whole tree, before anything is written.
func TestStoreForeignTreeRejectsATamperedBody(t *testing.T) {
	e := foreignEngine(storage.HistoryStrategyObjects)
	tree, bodies := foreignTree(t, 5)
	for id := range bodies {
		bodies[id] = `{"tampered":true}`
		break
	}
	if err := e.StoreForeignTree("ns", tree, bodies); err == nil {
		t.Fatal("a tampered body was accepted")
	}
	if _, ok, _ := e.TreeAt(tree.TreeHash); ok {
		t.Fatal("a refused tree resolves")
	}
}

// Without tree objects there is nowhere durable to keep the tree.
func TestStoreForeignTreeNeedsTheObjectsStrategy(t *testing.T) {
	e := foreignEngine(storage.HistoryStrategyReplay)
	tree, bodies := foreignTree(t, 3)
	if err := e.StoreForeignTree("ns", tree, bodies); !errors.Is(err, ErrForeignTreeNeedsObjects) {
		t.Fatalf("want ErrForeignTreeNeedsObjects, got %v", err)
	}
}
