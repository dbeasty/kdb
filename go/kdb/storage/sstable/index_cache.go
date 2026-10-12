package sstable

import (
	"container/list"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
)

// indexEntryBytes is roughly what one parsed footer-index entry costs resident: a 32-byte key
// and a 32-byte BlockHandle in a map whose slots are, on average, somewhat under-filled. An
// estimate for budgeting, not an exact count - the same kind of estimate the engine's other
// budgeted stores make.
const indexEntryBytes = 128

// indexFixedBytes is the per-table overhead charged on top of the entries, so a cache of many
// empty or tiny tables still accounts for something.
const indexFixedBytes = 512

// DefaultIndexCacheBytes is the index-cache budget of an LsmBlobStore nothing has configured.
// The engine replaces it with a share of the namespace's hot tier (see
// storage.ResolvedSSTableIndexCacheBytes).
const DefaultIndexCacheBytes int64 = 16 << 20

// tableIndexCache holds the parsed footer index of recently probed tables, least recently used
// first out.
//
// It exists because an SSTable is immutable once written: its index never changes, so parsing
// it again on every probe - which is what LsmBlobStore.Lookup did, after first reading the whole
// file to find its size - bought nothing and cost time proportional to the table. Issue #43's
// crash-recovery open spent 28 seconds doing exactly that.
//
// Not safe on its own: every method must be called with the owning LsmBlobStore's mu held.
// Sharing that lock with the table list is what lets a compaction drop the indexes of the
// tables it replaces in the same critical section as the swap, so a lookup that loaded an index
// just before the swap cannot re-insert it after (see LsmBlobStore.indexFor).
type tableIndexCache struct {
	budget  int64
	used    int64
	byName  map[string]*list.Element
	recency *list.List // front is most recently used; values are *cachedIndex
}

type cachedIndex struct {
	segment  string
	index    map[codec.Hash]BlockHandle
	bytes    int64
	lastUsed time.Time
}

// indexHotWindow is how long after its last probe an index is exempt from eviction.
//
// A lookup probes the tables newest to oldest. When the indexes of two tables together exceed the
// budget, evicting by recency alone drops the index the *next* lookup needs: every probe then
// re-reads and re-parses a whole table footer (a text index, ~100 ms and ~100 MB of garbage for a
// quarter-million entries), and a scan of a few hundred thousand documents after a restart ran for
// eleven minutes instead of one second. Indexes in active use are therefore kept even over budget
// and age out once idle, so the budget still bounds what an idle namespace holds.
var indexHotWindow = 5 * time.Second

func newTableIndexCache(budget int64) tableIndexCache {
	return tableIndexCache{budget: budget, byName: make(map[string]*list.Element), recency: list.New()}
}

func indexCost(index map[codec.Hash]BlockHandle) int64 {
	return indexFixedBytes + int64(len(index))*indexEntryBytes
}

func (c *tableIndexCache) get(segment string) (map[codec.Hash]BlockHandle, bool) {
	el, ok := c.byName[segment]
	if !ok {
		return nil, false
	}
	c.recency.MoveToFront(el)
	entry := el.Value.(*cachedIndex)
	entry.lastUsed = time.Now()
	return entry.index, true
}

// put records index and evicts least recently used indexes until the cache is back within its
// budget - except the one just added, which is always kept. A table whose index alone exceeds
// the budget would otherwise be re-read and re-parsed on every probe, which is the defect this
// cache exists to remove; holding one over-budget index is the cheaper failure.
func (c *tableIndexCache) put(segment string, index map[codec.Hash]BlockHandle) {
	if el, ok := c.byName[segment]; ok {
		c.recency.MoveToFront(el)
		return
	}
	entry := &cachedIndex{segment: segment, index: index, bytes: indexCost(index), lastUsed: time.Now()}
	c.byName[segment] = c.recency.PushFront(entry)
	c.used += entry.bytes
	c.evict()
}

func (c *tableIndexCache) evict() {
	now := time.Now()
	for c.used > c.budget && c.recency.Len() > 1 {
		back := c.recency.Back()
		// Least recently used is at the back, so the first hot entry means all the rest are hot.
		if now.Sub(back.Value.(*cachedIndex).lastUsed) < indexHotWindow {
			return
		}
		c.removeElement(back)
	}
}

func (c *tableIndexCache) remove(segment string) {
	if el, ok := c.byName[segment]; ok {
		c.removeElement(el)
	}
}

func (c *tableIndexCache) removeElement(el *list.Element) {
	entry := c.recency.Remove(el).(*cachedIndex)
	delete(c.byName, entry.segment)
	c.used -= entry.bytes
}

func (c *tableIndexCache) setBudget(budget int64) {
	c.budget = budget
	c.evict()
}

// SetIndexCacheBudget bounds the memory the store spends on parsed table indexes. Shrinking
// evicts at once. Safe to call while the store is serving lookups.
func (s *LsmBlobStore) SetIndexCacheBudget(budgetBytes int64) {
	if budgetBytes <= 0 {
		budgetBytes = DefaultIndexCacheBytes
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.indexes.setBudget(budgetBytes)
}

// IndexCacheBytes reports the estimated bytes held by cached table indexes.
func (s *LsmBlobStore) IndexCacheBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.indexes.used
}

// indexFor returns h's parsed footer index, from the cache when it is there.
//
// The read and parse happen outside the lock, so a slow disk never stalls other lookups; two
// lookups missing on the same table at once both read it, and the second insert is a no-op. The
// result is cached only if h is still registered when the parse finishes: a compaction that
// swapped h out in the meantime has already dropped its index, and re-inserting it would leak
// an index nothing can reach. Read errors are not cached, so a table that could not be read is
// tried again on the next probe, as it always has been.
func (s *LsmBlobStore) indexFor(h Handle) (map[codec.Hash]BlockHandle, error) {
	s.mu.Lock()
	if index, ok := s.indexes.get(h.SegmentName); ok {
		s.mu.Unlock()
		return index, nil
	}
	s.mu.Unlock()

	index, err := NewDefaultReader(s.io, h).Index()
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	if s.registeredLocked(h.SegmentName) {
		s.indexes.put(h.SegmentName, index)
	}
	s.mu.Unlock()
	return index, nil
}

func (s *LsmBlobStore) registeredLocked(segment string) bool {
	for _, t := range s.tables {
		if t.SegmentName == segment {
			return true
		}
	}
	return false
}
