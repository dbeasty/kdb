package engine

import (
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// treeOfSize builds a distinct tree holding n documents. Distinct because the pin tests turn
// on identity: two trees with the same content share a hash and are one entry, not two.
func treeOfSize(t *testing.T, salt, n int) document.DocumentTree {
	t.Helper()
	tree := document.EmptyDocumentTree()
	for i := 0; i < n; i++ {
		id, err := codec.RandomUUID()
		if err != nil {
			t.Fatal(err)
		}
		h, err := codec.HashFromBytes(document.SHA256Digest([]byte(fmt.Sprintf("%d-%d", salt, i))))
		if err != nil {
			t.Fatal(err)
		}
		next, err := tree.With(id, h)
		if err != nil {
			t.Fatal(err)
		}
		tree = next
	}
	return tree
}

// TestPinnedTreeSurvivesEvictionPressure is the store-level half of the guarantee
// KdbServerRuntime.runTransaction depends on: a queued writer's base tree must still be there
// when the writer reaches the front, however many newer trees the writers ahead of it publish.
func TestPinnedTreeSurvivesEvictionPressure(t *testing.T) {
	s := newBoundedTreeStore(8 << 10)
	base := treeOfSize(t, 0, 4)
	s.Put(base)
	s.Pin(base.TreeHash)

	// Far more than the budget can hold, which is what a burst of concurrent commits looks
	// like from in here.
	for i := 1; i <= 50; i++ {
		s.Put(treeOfSize(t, i, 4))
	}
	if _, ok := s.Peek(base.TreeHash); !ok {
		t.Fatal("a pinned tree was evicted; a queued writer would have to rebuild its base version")
	}

	s.Unpin(base.TreeHash)
	if s.PinnedCount() != 0 {
		t.Fatalf("expected no pins after release, got %d", s.PinnedCount())
	}
	// Released, it is ordinary history again and goes the way of everything else.
	for i := 51; i <= 60; i++ {
		s.Put(treeOfSize(t, i, 4))
	}
	if _, ok := s.Peek(base.TreeHash); ok {
		t.Fatal("a released tree is still held; the pin outlived its Unpin")
	}
}

// TestUnpinEvictsWithoutWaitingForTheNextPut covers the case a Put-only eviction policy gets
// wrong. When a burst of writers pins more than the budget the store is deliberately over it,
// and once the burst drains nothing necessarily inserts again - a namespace can go straight
// from writing to serving reads. Evicting at Unpin is what returns the memory then.
func TestUnpinEvictsWithoutWaitingForTheNextPut(t *testing.T) {
	first := treeOfSize(t, 1, 4)
	// A budget narrower than a single tree, so any two released trees are over it. Pinned trees
	// do not count against the budget at all - see boundedTreeStore.pinnedBytes - which is why
	// all four below can be held under it at once.
	s := newBoundedTreeStore(treeSizeBytes(first) / 2)
	pinned := []document.DocumentTree{first, treeOfSize(t, 2, 4), treeOfSize(t, 3, 4), treeOfSize(t, 4, 4)}
	for _, tr := range pinned {
		s.Put(tr)
		s.Pin(tr.TreeHash)
	}
	for _, tr := range pinned {
		if _, ok := s.Peek(tr.TreeHash); !ok {
			t.Fatal("a pinned tree was evicted; pins are supposed to be exempt from the budget")
		}
	}

	// Releasing puts a tree back under the budget's authority. The first release is still held
	// by the floor every LRU here keeps - one tree survives whatever the budget says - so it
	// takes a second release for eviction to have something to choose between, and then the
	// least recently released goes, with no Put anywhere in sight.
	s.Unpin(pinned[0].TreeHash)
	s.Unpin(pinned[1].TreeHash)
	if _, ok := s.Peek(pinned[0].TreeHash); ok {
		t.Fatal("an unpinned over-budget tree is still resident; Unpin did not reconsider eviction")
	}
	for _, tr := range pinned[2:] {
		if _, ok := s.Peek(tr.TreeHash); !ok {
			t.Fatal("releasing one pin evicted a tree another holder still pins")
		}
	}
}

// TestTreePinsAreCounted covers concurrent writers sharing one base version: the first to
// finish must not unpin it out from under the others.
func TestTreePinsAreCounted(t *testing.T) {
	s := newBoundedTreeStore(8 << 10)
	base := treeOfSize(t, 0, 4)
	s.Put(base)
	s.Pin(base.TreeHash)
	s.Pin(base.TreeHash)
	s.Unpin(base.TreeHash)

	for i := 1; i <= 50; i++ {
		s.Put(treeOfSize(t, i, 4))
	}
	if _, ok := s.Peek(base.TreeHash); !ok {
		t.Fatal("one writer releasing dropped a base version another writer still holds")
	}
	s.Unpin(base.TreeHash)
	if s.PinnedCount() != 0 {
		t.Fatalf("expected no pins after both releases, got %d", s.PinnedCount())
	}
}

// TestPinBeforeResidencyProtectsTheRebuild is the case that makes the pin safe to take at the
// front of a transaction: the base tree may already have been evicted by then, and the pin has
// to protect the rebuild's output rather than requiring something to protect up front.
func TestPinBeforeResidencyProtectsTheRebuild(t *testing.T) {
	s := newBoundedTreeStore(8 << 10)
	base := treeOfSize(t, 0, 4)

	s.Pin(base.TreeHash) // nothing resident under this hash yet
	s.Put(base)
	for i := 1; i <= 50; i++ {
		s.Put(treeOfSize(t, i, 4))
	}
	if _, ok := s.Peek(base.TreeHash); !ok {
		t.Fatal("a tree pinned before it was stored was evicted anyway")
	}
}
