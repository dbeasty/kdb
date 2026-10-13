package sstable

import "sync"

// blockEntryOverhead is what one cached block costs beyond its own bytes: the map slot, the
// entry with its two list links, the slice header and the key's string header. An estimate for
// budgeting, like indexEntryBytes, so a cache of many tiny blocks still accounts for something.
const blockEntryOverhead = 128

// BlockCache holds decoded SSTable blocks, least recently used first out, bounded by bytes.
//
// It is the analogue of Cassandra's ChunkCache: what it caches is the *decompressed* block, keyed
// by the file it came from and the block's offset in that file, so a repeated lookup of a hot key
// costs a map probe instead of a disk read, a CRC and a zstd decode.
//
// Keys are (segment name, offset). An SSTable's segment name embeds a random UUID minted when the
// table is written (see DefaultWriter.Finish) and a table is immutable once sealed, so a name
// always denotes the same bytes: an entry can go stale only if a name is reused, and none is.
// Compaction still drops the entries of the tables it replaces (RemoveSegment) - not for
// correctness but so a dead table's blocks do not sit in the budget until LRU pushes them out.
//
// Cached values are shared, not copied: Get hands every caller the same slice. Callers must treat
// it as read-only (see LsmBlobStore.Lookup), which every reader of the blob store already does -
// they decode it into fresh structures or convert it to a string.
//
// Safe for concurrent use.
type BlockCache struct {
	mu       sync.Mutex
	capacity int64
	used     int64
	entries  map[blockKey]*blockEntry
	// head is the most recently used entry and tail the least; nil when the cache is empty.
	head, tail *blockEntry

	hits, misses, evictions int64
}

type blockKey struct {
	segment string
	offset  int64
}

type blockEntry struct {
	key        blockKey
	value      []byte
	prev, next *blockEntry
}

// BlockCacheStats is a point-in-time view of a BlockCache, for tests and diagnostics.
type BlockCacheStats struct {
	Entries                 int
	UsedBytes, Capacity     int64
	Hits, Misses, Evictions int64
}

// NewBlockCache returns a cache that holds at most capacityBytes of blocks (each charged its
// length plus a fixed overhead). A capacity of zero or less caches nothing.
func NewBlockCache(capacityBytes int64) *BlockCache {
	return &BlockCache{capacity: capacityBytes, entries: make(map[blockKey]*blockEntry)}
}

func blockCost(v []byte) int64 { return int64(len(v)) + blockEntryOverhead }

// Get returns the cached block for (segment, offset), marking it most recently used. The slice
// is shared with every other caller and must not be modified.
func (c *BlockCache) Get(segment string, offset int64) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[blockKey{segment, offset}]
	if !ok {
		c.misses++
		return nil, false
	}
	c.hits++
	c.moveToFront(e)
	return e.value, true
}

// Put caches value for (segment, offset) and evicts least recently used blocks until the cache
// is back within its capacity. The cache keeps value itself, so the caller must not modify it
// afterwards. A block larger than the whole capacity is not cached: admitting it would flush
// everything else only for it to be evicted by the next insert.
func (c *BlockCache) Put(segment string, offset int64, value []byte) {
	if c == nil {
		return
	}
	cost := blockCost(value)
	c.mu.Lock()
	defer c.mu.Unlock()
	if cost > c.capacity {
		return
	}
	k := blockKey{segment, offset}
	if e, ok := c.entries[k]; ok {
		// Two lookups that missed on the same block at once both load it; the bytes are
		// identical (the table is immutable), so the second insert only refreshes recency.
		c.moveToFront(e)
		return
	}
	e := &blockEntry{key: k, value: value}
	c.entries[k] = e
	c.pushFront(e)
	c.used += cost
	c.evictLocked()
}

// RemoveSegment drops every cached block of segment. For a table that compaction has replaced:
// its blocks can no longer be asked for, and would otherwise hold budget until aged out.
// Linear in the number of cached blocks, which is fine at compaction frequency.
func (c *BlockCache) RemoveSegment(segment string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for e := c.head; e != nil; {
		next := e.next
		if e.key.segment == segment {
			c.removeLocked(e)
		}
		e = next
	}
}

// SetCapacity re-bounds the cache, evicting at once when it shrinks. Safe while serving.
func (c *BlockCache) SetCapacity(capacityBytes int64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.capacity = capacityBytes
	c.evictLocked()
}

// UsedBytes is what the cache currently charges against its capacity.
func (c *BlockCache) UsedBytes() int64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.used
}

// Stats reports the cache's size and counters.
func (c *BlockCache) Stats() BlockCacheStats {
	if c == nil {
		return BlockCacheStats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return BlockCacheStats{
		Entries: len(c.entries), UsedBytes: c.used, Capacity: c.capacity,
		Hits: c.hits, Misses: c.misses, Evictions: c.evictions,
	}
}

func (c *BlockCache) evictLocked() {
	for c.used > c.capacity && c.tail != nil {
		c.removeLocked(c.tail)
		c.evictions++
	}
}

func (c *BlockCache) removeLocked(e *blockEntry) {
	c.unlink(e)
	delete(c.entries, e.key)
	c.used -= blockCost(e.value)
}

func (c *BlockCache) pushFront(e *blockEntry) {
	e.prev = nil
	e.next = c.head
	if c.head != nil {
		c.head.prev = e
	}
	c.head = e
	if c.tail == nil {
		c.tail = e
	}
}

func (c *BlockCache) unlink(e *blockEntry) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		c.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		c.tail = e.prev
	}
	e.prev, e.next = nil, nil
}

func (c *BlockCache) moveToFront(e *blockEntry) {
	if c.head == e {
		return
	}
	c.unlink(e)
	c.pushFront(e)
}
