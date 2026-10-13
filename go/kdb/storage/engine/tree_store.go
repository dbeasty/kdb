package engine

import (
	"container/list"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// Trie cost constants (document/document_tree_slab.go). The trie's records live in a slab: an
// internal node is a 100-byte record, a leaf an 84-byte one. Leaves are path-compressed - a leaf
// sits at the first depth where its subtree holds one entry - so a tree of n random ids is about
// log16(n)+1 levels deep, has about 0.37 internal nodes per leaf, and costs about
// treeBytesPerDoc per document all told including chunk slack
// (TestSlabTreeMemoryPerDocument measures ~131).
const (
	treeBytesPerNode = 100
	treeBytesPerLeaf = 84
	treeBytesPerDoc  = 130
	// treeEntryOverhead is the store's own cost per tree: its entry, map slot and list element.
	treeEntryOverhead = 192
)

// treeSizeBytes is what a tree costs if nothing else holds any of it: every node, charged as
// an independent copy. Used for a tree whose relationship to the others is unknown - one
// rebuilt for a history read, or handed in from outside.
func treeSizeBytes(t document.DocumentTree) int64 {
	return treeEntryOverhead + treeBytesPerDoc*int64(t.Size())
}

// treePathBytes is what one changed document adds to a tree that shares everything else with
// its parent: a new leaf and a new copy of every internal node on its path.
func treePathBytes(size int) int64 {
	depth := int64(1) // the root
	for n := size; n > 16; n /= 16 {
		depth++
	}
	return depth*treeBytesPerNode + treeBytesPerLeaf
}

// treeDeltaBytes is what a tree derived from a resident parent by changes edits actually adds
// to memory: the paths it rewrote, nothing else. Capped at treeSizeBytes - a commit that touches
// most of a small tree has shared nothing worth counting.
//
// Charging this rather than treeSizeBytes is what lets the store hold history at all on a large
// namespace. Every tree used to be charged 176 x (32 + size) as if it were a private copy, so at
// a million documents a single tree "cost" 176 MB, far over the history budget, and the store
// held exactly one - every read at any older commit rebuilt its tree. In fact consecutive trees
// share all but the changed paths, so a run of them costs the live tree plus the sum of their
// deltas.
//
// It is an estimate, and it can undercount in one case: a tree's unique nodes are the paths
// changed between it and the trees still resident, so once its neighbours are evicted it holds
// more than its own delta. The store evicts least recently used first, which for trees
// published commit by commit keeps the retained set a contiguous run back from the live tree,
// where the sum of deltas is within a factor of two of the truth (a node is unique to at most
// the tree that introduced it and the one that replaced it). A history read that promotes one
// old tree out of that run is the exception.
func treeDeltaBytes(size, changes int) int64 {
	full := treeEntryOverhead + treeBytesPerDoc*int64(size)
	if changes < 1 {
		changes = 1
	}
	d := treeEntryOverhead + int64(changes)*treePathBytes(size)
	return min(d, full)
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
	mu    sync.Mutex
	trees map[codec.Hash]*treeEntry
	lru   *list.List
	pins  map[codec.Hash]int
	bytes int64
	// pinnedBytes is the part of bytes held by pinned trees, and the budget is applied to the
	// difference rather than to the total.
	//
	// Two reasons. The budget can only govern what it is able to evict, and a pinned tree is by
	// definition not that - measuring against the total just means that once pins alone exceed
	// the budget, every insert walks the whole LRU and throws out every unpinned tree, which
	// costs a rebuild for the readers that wanted them and saves nothing. And the sizes here
	// are worst-case: treeSizeBytes charges each tree as if it were an independent copy, while
	// a pinned tree is a writer's base version, one or a few commits behind the live tree and
	// therefore sharing nearly every node with it. Counting a set that is almost entirely
	// shared at full price is what would make a namespace report gigabytes it does not hold.
	pinnedBytes int64
	budget      int64
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
		pins:   make(map[codec.Hash]int),
		budget: budgetBytes,
	}
}

// Trees is a snapshot of every tree held, pinned or not.
func (s *boundedTreeStore) Trees() []document.DocumentTree {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]document.DocumentTree, 0, len(s.trees))
	for _, e := range s.trees {
		out = append(out, e.tree)
	}
	return out
}

// Replace swaps the tree held under t's hash for t - an equal tree, as a rebase onto a new tree
// segment returns - keeping its charge, position and pins. A no-op if nothing is held under it.
func (s *boundedTreeStore) Replace(t document.DocumentTree) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.trees[t.TreeHash]; ok {
		e.tree = t
	}
}

func (s *boundedTreeStore) Get(h codec.Hash) (document.DocumentTree, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.trees[h]
	if !ok {
		return document.DocumentTree{}, false
	}
	// A pinned entry has no LRU position to promote; it re-enters at the front when released.
	if e.el != nil {
		s.lru.MoveToFront(e.el)
	}
	return e.tree, true
}

func (s *boundedTreeStore) Put(t document.DocumentTree) {
	s.PutCharged(t, treeSizeBytes(t))
}

// PutCharged stores t charging it size bytes against the budget instead of treeSizeBytes - for a
// caller that knows how much of t it shares with trees already held (see treeDeltaBytes).
func (s *boundedTreeStore) PutCharged(t document.DocumentTree, size int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.trees[t.TreeHash]; ok {
		// Already held. A pinned entry has no LRU position at all, so there is nothing to
		// promote - it re-enters at the front when its last pin is released.
		if e.el != nil {
			s.lru.MoveToFront(e.el)
		}
		return
	}
	e := &treeEntry{tree: t, size: size}
	s.trees[t.TreeHash] = e
	s.bytes += e.size
	// A pin taken before the tree was resident - the ordinary case for a writer whose base was
	// already evicted when it pinned - lands the entry straight onto the pinned side, out of
	// the LRU, exactly as if it had been pinned after the fact.
	if s.pins[t.TreeHash] > 0 {
		s.pinnedBytes += e.size
	} else {
		e.el = s.lru.PushFront(t.TreeHash)
	}
	s.evictLocked()
}

// evictLocked drops least-recently-used trees until the evictable part of the store is within
// budget, always keeping at least one. Keeping one matters: a tree larger than the whole budget
// would otherwise be evicted the instant it was stored, so every read of it would miss and
// rebuild it again.
//
// Pinned trees are neither in the list nor in the comparison, so the store's total footprint
// can exceed the budget by whatever the in-flight writers hold. That is deliberate: the budget
// can only govern what it is able to evict, and the pinned set is bounded by write concurrency
// instead - see pinnedBytes.
func (s *boundedTreeStore) evictLocked() {
	if s.budget <= 0 {
		return
	}
	// Only unpinned trees are in the list, so this consumes every entry it looks at and costs
	// what it frees. An earlier version left pinned entries in place and skipped over them,
	// which turned a burst of concurrent writers - every one of them pinning its base - into an
	// O(pinned) walk under this mutex on every single commit, and cost more than the rebuilds
	// the pinning was there to avoid.
	for s.bytes-s.pinnedBytes > s.budget && s.lru.Len() > 1 {
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

// Pin marks h un-evictable until a matching Unpin, whether or not the tree
// is resident yet: a pin taken before the tree is rebuilt still protects the
// Put that lands it, which is what makes it safe to pin a base version whose
// tree may already have been evicted.
//
// Counted rather than boolean, because concurrent writers routinely share a
// base version - one of them finishing must not unpin it for the others.
func (s *boundedTreeStore) Pin(h codec.Hash) {
	s.mu.Lock()
	s.pins[h]++
	if s.pins[h] == 1 {
		if e, ok := s.trees[h]; ok {
			s.pinnedBytes += e.size
			if e.el != nil {
				s.lru.Remove(e.el)
				e.el = nil
			}
		}
	}
	s.mu.Unlock()
}

// Unpin releases one pin taken by Pin, and reconsiders the store for
// eviction. Evicting here rather than waiting for the next Put is what keeps
// a burst of writers from leaving their base trees resident indefinitely:
// under a steady read workload nothing else inserts into this store, so an
// entry that became evictable after the last Put would simply stay.
func (s *boundedTreeStore) Unpin(h codec.Hash) {
	s.mu.Lock()
	if n, ok := s.pins[h]; ok {
		if n <= 1 {
			delete(s.pins, h)
			// Re-enters at the front: it was in use a moment ago, which is exactly what the
			// front of an LRU means, and the writer that just released it is the most recent
			// thing to have touched it.
			if e, ok := s.trees[h]; ok {
				s.pinnedBytes -= e.size
				e.el = s.lru.PushFront(h)
			}
		} else {
			s.pins[h] = n - 1
		}
	}
	s.evictLocked()
	s.mu.Unlock()
}

// PinnedCount is how many distinct trees are pinned right now. For tests and
// reporting - a number that only ever grows is a leaked release.
func (s *boundedTreeStore) PinnedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pins)
}

// SetBudget changes the ceiling and evicts down to it.
func (s *boundedTreeStore) SetBudget(b int64) {
	s.mu.Lock()
	s.budget = b
	s.evictLocked()
	s.mu.Unlock()
}

// ResidentBytes is what the store currently holds, pinned trees included - the honest total,
// not the part the budget governs. For tests and reporting; not on any hot path.
func (s *boundedTreeStore) ResidentBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bytes
}

// EvictableBytes is the part of ResidentBytes the budget actually governs - what the store
// would be able to give back if asked. This, not ResidentBytes, is what the memory arbiter
// should read: pinned trees are a writer's working set, they are bounded by write concurrency
// rather than by any cache policy, and they are charged at treeSizeBytes' worst case even
// though a base version shares nearly every node with the live tree. Reporting them as demand
// would have one busy namespace claim a share of the pool it does not hold.
func (s *boundedTreeStore) EvictableBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bytes - s.pinnedBytes
}

// PinnedBytes is the part of ResidentBytes held by pinned trees. For tests and reporting.
func (s *boundedTreeStore) PinnedBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pinnedBytes
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
