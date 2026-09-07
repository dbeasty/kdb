package engine

import (
	"container/list"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// treeBytesPerNode approximates what one trie node costs resident: a
// 32-byte hash, a pointer, and a 16-entry child array, plus allocator
// overhead.
const treeBytesPerNode = 176

// treeSpineNodes is the depth of the document trie. It is fixed at 32 with
// no path compression (see document/document_tree_trie.go), so every
// document's path is 32 internal nodes deep whatever else the tree holds -
// which is why a tree of a single document still costs several kilobytes.
const treeSpineNodes = 32

// treeSizeBytes approximates a tree's resident cost.
//
// Deliberately an approximation. Trees share subtrees with the versions
// they were derived from, so summing per-tree sizes overcounts what is
// actually held, and there is no cheap way to know how much of a given
// tree is shared with another still in the cache. What matters is that the
// budget is denominated in bytes, so it composes with the engine's other
// budgets rather than being a count that means something different for
// every namespace.
func treeSizeBytes(t document.DocumentTree) int64 {
	return int64(treeBytesPerNode) * int64(treeSpineNodes+t.Size())
}

// boundedTreeStore holds document trees under a byte budget, evicting the
// least recently used.
//
// The current tree is deliberately not this store's problem: ServerEngine
// serves it from latestTree, an atomic snapshot outside the store, so
// nothing here can slow a normal read or evict the tree the write path is
// building on. Everything in here is history, and everything in here can
// be obtained again - by object lookup under the objects strategy, or by
// folding the log under replay.
type boundedTreeStore struct {
	mu     sync.Mutex
	trees  map[codec.Hash]*treeEntry
	lru    *list.List
	bytes  int64
	budget int64
}

type treeEntry struct {
	tree document.DocumentTree
	size int64
	el   *list.Element
}

// newBoundedTreeStore returns a store bounded to budgetBytes. A budget <= 0
// means unbounded, which is what a runtime with nowhere to re-read a tree
// from needs.
func newBoundedTreeStore(budgetBytes int64) *boundedTreeStore {
	return &boundedTreeStore{
		trees:  make(map[codec.Hash]*treeEntry),
		lru:    list.New(),
		budget: budgetBytes,
	}
}

func (s *boundedTreeStore) Get(h codec.Hash) (document.DocumentTree, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.trees[h]
	if !ok {
		return document.DocumentTree{}, false
	}
	s.lru.MoveToFront(e.el)
	return e.tree, true
}

func (s *boundedTreeStore) Put(t document.DocumentTree) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.trees[t.TreeHash]; ok {
		s.lru.MoveToFront(e.el)
		return
	}
	e := &treeEntry{tree: t, size: treeSizeBytes(t)}
	e.el = s.lru.PushFront(t.TreeHash)
	s.trees[t.TreeHash] = e
	s.bytes += e.size
	s.evictLocked()
}

// evictLocked drops least-recently-used trees until the store is within
// budget, always keeping at least one. Keeping one matters: a tree larger
// than the whole budget would otherwise be evicted the instant it was
// stored, so every read of it would miss and rebuild it again.
func (s *boundedTreeStore) evictLocked() {
	if s.budget <= 0 {
		return
	}
	for s.bytes > s.budget && s.lru.Len() > 1 {
		el := s.lru.Back()
		if el == nil {
			return
		}
		h := el.Value.(codec.Hash)
		if e, ok := s.trees[h]; ok {
			s.bytes -= e.size
			delete(s.trees, h)
		}
		s.lru.Remove(el)
	}
}

// SetBudget changes the ceiling and evicts down to it.
func (s *boundedTreeStore) SetBudget(b int64) {
	s.mu.Lock()
	s.budget = b
	s.evictLocked()
	s.mu.Unlock()
}

// ResidentBytes is what the store currently holds. For tests and
// reporting; not on any hot path.
func (s *boundedTreeStore) ResidentBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bytes
}

// Len is how many trees are held. For tests.
func (s *boundedTreeStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.trees)
}

// Peek returns a tree without promoting it to most-recently-used. Used by
// the rebuild path while probing for an ancestor to fold forward from -
// see ServerEngine.CachedTree.
func (s *boundedTreeStore) Peek(h codec.Hash) (document.DocumentTree, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.trees[h]
	if !ok {
		return document.DocumentTree{}, false
	}
	return e.tree, true
}
