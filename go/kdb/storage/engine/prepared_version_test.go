package engine

import (
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
)

// TestPreparedVersionsOfOneDocumentDoNotReplaceEachOther: writers to a hot document prepare their
// versions one after another before any of them commits. Each commit must still find its own
// prepared hashes; with one slot per document, the second prepare replaced the first, and the
// first commit's lookup then took the slot from the second as well.
func TestPreparedVersionsOfOneDocumentDoNotReplaceEachOther(t *testing.T) {
	e := foreignEngine(storage.HistoryStrategyObjects)
	id, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	docs := make([]document.Document, maxPreparedPerDocument)
	for i := range docs {
		docs[i] = document.Document{ID: id, JSON: fmt.Sprintf(`{"v":%d}`, i)}
		e.PrepareVersion("ns", docs[i])
	}
	// Committed in another order than prepared: the queue does not promise the prepare order.
	for _, i := range []int{1, 0, 3, 2} {
		h, spine, err := e.versionHashes(docs[i])
		if err != nil {
			t.Fatal(err)
		}
		want, _ := docs[i].ContentHash()
		if h != want {
			t.Fatalf("version %d: hash %x, want %x", i, h, want)
		}
		if spine == nil {
			t.Errorf("version %d: its prepared spine was lost", i)
		}
	}
	if _, ok := e.prepared[id]; ok {
		t.Errorf("committed versions left behind: %d", len(e.prepared[id]))
	}
}

// TestPreparedVersionsPerDocumentAreBounded: versions that never commit (a stale premerge) do not
// pile up - past the bound the oldest is dropped, and its commit hashes under the gate as before.
func TestPreparedVersionsPerDocumentAreBounded(t *testing.T) {
	e := foreignEngine(storage.HistoryStrategyObjects)
	id, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	var docs []document.Document
	for i := 0; i <= maxPreparedPerDocument; i++ {
		d := document.Document{ID: id, JSON: fmt.Sprintf(`{"v":%d}`, i)}
		docs = append(docs, d)
		e.PrepareVersion("ns", d)
	}
	if got := len(e.prepared[id]); got != maxPreparedPerDocument {
		t.Fatalf("%d prepared versions held, want %d", got, maxPreparedPerDocument)
	}
	h, spine, err := e.versionHashes(docs[0])
	if err != nil {
		t.Fatal(err)
	}
	if want, _ := docs[0].ContentHash(); h != want || spine != nil {
		t.Errorf("oldest version: hash ok=%v spine=%v, want its hash computed and no spine", h == want, spine)
	}
	if _, spine, _ := e.versionHashes(docs[maxPreparedPerDocument]); spine == nil {
		t.Error("newest version lost its prepared spine")
	}
}
