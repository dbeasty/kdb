package engine

import (
	"container/list"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// docHashShardCount mirrors doc_shard.go's rationale: independent lock
// domains so lookups/inserts for different content hashes proceed in
// parallel instead of serializing behind one mutex.
const docHashShardCount = 64

// minDocHashShardBudgetBytes keeps a shard able to hold something even
// when the configured budget divided 64 ways rounds to almost nothing. A
// shard that can hold no entries at all would evict every version the
// instant it was written, turning every read of live data into a cold
// load - far worse than slightly overshooting a very small budget.
const minDocHashShardBudgetBytes int64 = 64 << 10

type docHashEntry struct {
	doc  document.Document
	size int64
	// el is this entry's position in the shard's recency list. The list
	// holds codec.Hash values, back = least recently used.
	el *list.Element
}

type docHashShard struct {
	mu     sync.Mutex
	docs   map[codec.Hash]*docHashEntry
	lru    *list.List
	pins   map[codec.Hash]int
	bytes  int64
	budget int64
}

// shardedDocByHashStore holds document versions keyed by content hash
// rather than by document ID, so a version referenced by an older
// committed DocumentTree stays retrievable via GetDocument/ScanDocuments
// at that tree's hash - see server_engine.go's treesByHash and the
// atCommit fix it backs.
//
// It used to hold *every* version ever committed and never delete one,
// which made a namespace's resident memory the arithmetic sum of every
// version of every document it had ever held. A single 1.4MB document
// rewritten 463 times - one long-lived append-only aggregate, nothing
// exotic - cost 329MB resident, and reopening the store rebuilt all of it
// (docs/benchmarks/open-cost.md). On a container sized for the live data
// that is fatal, and it gets worse with every write rather than settling.
//
// So the store is bounded, and the bound is drawn in the one place it can
// be drawn safely: versions in the *current* tree are pinned and never
// evicted, everything older is evictable. That keeps every read of live
// data in memory - the overwhelmingly common case, and the only case that
// is latency-critical - while history falls back to being re-read from
// the delta log on demand (ServerEngine.coldLoader). Pinning is what makes
// eviction safe against the write path too: a version is staged into the
// tree before its commit reaches the delta log, so an unpinned-by-default
// store could drop a version that was not durable anywhere yet.
type shardedDocByHashStore struct {
	shards [docHashShardCount]*docHashShard
}

// newShardedDocByHashStore returns a store bounded to budgetBytes in
// total, divided evenly across shards. A budget <= 0 means unbounded,
// which is the pre-existing behavior and what in-memory targets with no
// delta log to fall back on still need.
func newShardedDocByHashStore(budgetBytes int64) *shardedDocByHashStore {
	perShard := int64(0)
	if budgetBytes > 0 {
		perShard = budgetBytes / docHashShardCount
		if perShard < minDocHashShardBudgetBytes {
			perShard = minDocHashShardBudgetBytes
		}
	}
	s := &shardedDocByHashStore{}
	for i := range s.shards {
		s.shards[i] = &docHashShard{
			docs:   make(map[codec.Hash]*docHashEntry),
			lru:    list.New(),
			pins:   make(map[codec.Hash]int),
			budget: perShard,
		}
	}
	return s
}

func (s *shardedDocByHashStore) shardFor(h codec.Hash) *docHashShard {
	// Content hashes are SHA256 output, already uniformly distributed - any
	// byte works as a cheap shard selector (mirrors mem/blob_shard.go).
	idx := int(h.Bytes[0]) % docHashShardCount
	return s.shards[idx]
}

// entrySizeBytes approximates what one cached version costs. The JSON text
// dominates; the constant covers the Document header, the map entry, and
// the recency-list node, and exists so that a store of many tiny documents
// is still bounded by something rather than counting as free.
func entrySizeBytes(doc document.Document) int64 {
	return int64(len(doc.JSON)) + 96
}

func (s *shardedDocByHashStore) Put(h codec.Hash, doc document.Document) {
	sh := s.shardFor(h)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if e, ok := sh.docs[h]; ok {
		sh.lru.MoveToFront(e.el)
		return
	}
	e := &docHashEntry{doc: doc, size: entrySizeBytes(doc)}
	e.el = sh.lru.PushFront(h)
	sh.docs[h] = e
	sh.bytes += e.size
	sh.evictLocked()
}

// evictLocked drops least-recently-used unpinned entries until the shard
// is within budget, or until nothing evictable is left. Overshooting the
// budget because everything in the shard is pinned is deliberate: pinned
// versions are the live tree, and dropping one would lose data that no
// other tier holds yet.
func (sh *docHashShard) evictLocked() {
	if sh.budget <= 0 {
		return
	}
	el := sh.lru.Back()
	for sh.bytes > sh.budget && el != nil {
		prev := el.Prev()
		h := el.Value.(codec.Hash)
		if sh.pins[h] == 0 {
			if e, ok := sh.docs[h]; ok {
				sh.bytes -= e.size
				delete(sh.docs, h)
			}
			sh.lru.Remove(el)
		}
		el = prev
	}
}

func (s *shardedDocByHashStore) Get(h codec.Hash) (document.Document, bool) {
	sh := s.shardFor(h)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, ok := sh.docs[h]
	if !ok {
		return document.Document{}, false
	}
	sh.lru.MoveToFront(e.el)
	return e.doc, true
}

// Pin marks h as un-evictable until a matching Unpin. Counted, because the
// same content hash is reachable from several document IDs at once when
// their content is identical, and one of them being overwritten must not
// unpin the others.
func (s *shardedDocByHashStore) Pin(h codec.Hash) {
	sh := s.shardFor(h)
	sh.mu.Lock()
	sh.pins[h]++
	sh.mu.Unlock()
}

// Unpin releases one pin taken by Pin, and reconsiders the shard for
// eviction.
//
// Evicting here rather than waiting for the next Put matters more than it
// looks: a shard only sees an insert every docHashShardCount writes, so an
// entry that became evictable right after the last insert into its shard
// would simply stay resident. With one document rewritten many times that
// left one large version stranded in each of the 64 shards - 81MB against
// a 32MB budget, still growing with history, which is exactly the failure
// the budget exists to prevent.
func (s *shardedDocByHashStore) Unpin(h codec.Hash) {
	sh := s.shardFor(h)
	sh.mu.Lock()
	if n, ok := sh.pins[h]; ok {
		if n <= 1 {
			delete(sh.pins, h)
		} else {
			sh.pins[h] = n - 1
		}
	}
	sh.evictLocked()
	sh.mu.Unlock()
}

// ResidentBytes is the total size of every version currently held. For
// tests and for reporting; not on any hot path.
func (s *shardedDocByHashStore) ResidentBytes() int64 {
	var total int64
	for _, sh := range s.shards {
		sh.mu.Lock()
		total += sh.bytes
		sh.mu.Unlock()
	}
	return total
}

// SetBudget sets the store's total byte ceiling, divided evenly across
// shards, and evicts down to it immediately. A budget <= 0 restores
// unbounded behavior.
//
// Separate from the constructor because bounding this store is only safe
// once something can serve what it drops: see ServerEngine.SetColdLoader,
// the sole caller, which installs the delta-log fallback and the budget
// together.
func (s *shardedDocByHashStore) SetBudget(budgetBytes int64) {
	perShard := int64(0)
	if budgetBytes > 0 {
		perShard = budgetBytes / docHashShardCount
		if perShard < minDocHashShardBudgetBytes {
			perShard = minDocHashShardBudgetBytes
		}
	}
	for _, sh := range s.shards {
		sh.mu.Lock()
		sh.budget = perShard
		sh.evictLocked()
		sh.mu.Unlock()
	}
}
