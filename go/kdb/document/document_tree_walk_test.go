package document

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

func treeWithDocs(t *testing.T, n int) DocumentTree {
	t.Helper()
	tree := EmptyDocumentTree()
	for i := 0; i < n; i++ {
		id, err := codec.RandomUUID()
		if err != nil {
			t.Fatal(err)
		}
		var err2 error
		tree, err2 = tree.With(id, codec.Hash{Bytes: [32]byte{byte(i), byte(i >> 8)}})
		if err2 != nil {
			t.Fatal(err2)
		}
	}
	return tree
}

// Walk must visit every entry exactly once, matching MaterializedEntries - it exists so scans
// can stream the trie without the O(namespace) flat-map allocation, not to change what a scan
// sees.
func TestWalkVisitsEveryEntryOnce(t *testing.T) {
	tree := treeWithDocs(t, 500)
	want := tree.MaterializedEntries()
	got := make(map[codec.UUID]codec.Hash)
	tree.Walk(func(id codec.UUID, h codec.Hash) bool {
		if _, dup := got[id]; dup {
			t.Fatalf("Walk visited %s twice", id)
		}
		got[id] = h
		return true
	})
	if len(got) != len(want) {
		t.Fatalf("Walk visited %d entries, MaterializedEntries has %d", len(got), len(want))
	}
	for id, h := range want {
		if got[id] != h {
			t.Errorf("Walk hash for %s = %v, want %v", id, got[id], h)
		}
	}
}

// Early stop is the point: a scan that has what it needs must be able to stop walking without
// visiting - or paying for - the rest of the namespace.
func TestWalkStopsEarly(t *testing.T) {
	tree := treeWithDocs(t, 1000)
	visited := 0
	tree.Walk(func(codec.UUID, codec.Hash) bool {
		visited++
		return visited < 10
	})
	if visited != 10 {
		t.Fatalf("Walk visited %d entries after being told to stop at 10", visited)
	}
}

// Walk must also work on a flat-map-backed tree (wire decode path), where no trie exists yet.
func TestWalkOnMaterializedTree(t *testing.T) {
	entries := make(map[codec.UUID]codec.Hash)
	for i := 0; i < 50; i++ {
		id, err := codec.RandomUUID()
		if err != nil {
			t.Fatal(err)
		}
		entries[id] = codec.Hash{Bytes: [32]byte{byte(i)}}
	}
	tree, err := BuildDocumentTree(entries)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	tree.Walk(func(codec.UUID, codec.Hash) bool { n++; return true })
	if n != len(entries) {
		t.Fatalf("Walk visited %d of %d entries on an Entries-backed tree", n, len(entries))
	}
}
