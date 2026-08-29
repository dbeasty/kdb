package mem

import (
	"errors"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// A scan whose batch callback stops early (executor hit its LIMIT or row budget) must stop the
// underlying walk - the pre-streaming implementation materialized the entire namespace's entry
// map before the first batch, which is exactly what admission control could never see or bound.
func TestScanDocumentsStopsOnBatchError(t *testing.T) {
	a := NewInMemoryStorageAdapter()
	ns := "ns"
	tree := document.EmptyDocumentTree()
	for i := 0; i < 1000; i++ {
		id, err := codec.RandomUUID()
		if err != nil {
			t.Fatal(err)
		}
		doc := document.Document{ID: id, JSON: `{"i":1}`}
		if err := a.PutDocument(ns, doc); err != nil {
			t.Fatal(err)
		}
		var err2 error
		tree, err2 = a.CommitTree(ns, tree.TreeHash)
		if err2 != nil {
			t.Fatal(err2)
		}
	}

	stop := errors.New("enough")
	batches := 0
	err := a.ScanDocuments(ns, tree.TreeHash, 100, func(batch []document.Document) error {
		batches++
		return stop
	})
	if !errors.Is(err, stop) {
		t.Fatalf("expected the batch error back, got %v", err)
	}
	if batches != 1 {
		t.Fatalf("scan delivered %d batches after the first said stop", batches)
	}
}

// Streaming must not change what a full scan sees: every document, each exactly once.
func TestScanDocumentsStreamsAllDocuments(t *testing.T) {
	a := NewInMemoryStorageAdapter()
	ns := "ns"
	tree := document.EmptyDocumentTree()
	want := map[codec.UUID]bool{}
	for i := 0; i < 300; i++ {
		id, err := codec.RandomUUID()
		if err != nil {
			t.Fatal(err)
		}
		want[id] = true
		if err := a.PutDocument(ns, document.Document{ID: id, JSON: `{"i":1}`}); err != nil {
			t.Fatal(err)
		}
		var err2 error
		tree, err2 = a.CommitTree(ns, tree.TreeHash)
		if err2 != nil {
			t.Fatal(err2)
		}
	}
	seen := map[codec.UUID]bool{}
	err := a.ScanDocuments(ns, tree.TreeHash, 64, func(batch []document.Document) error {
		for _, d := range batch {
			if seen[d.ID] {
				t.Fatalf("document %s delivered twice", d.ID)
			}
			seen[d.ID] = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(seen) != len(want) {
		t.Fatalf("scan saw %d documents, want %d", len(seen), len(want))
	}
}
