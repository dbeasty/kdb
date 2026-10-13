package delta

import (
	"sync"
	"sync/atomic"

	"github.com/limidus/kdb/go/kdb/storage/bufpool"
)

// FrameCache is a bounded, byte-budgeted LRU of decompressed delta frames - the payloads
// DefaultReader.ReadCommitAt and ReadDocumentAt decode - keyed by the segment incarnation and the
// frame's offset in it. It is KDB's analogue of Cassandra's ChunkCache, which holds decompressed
// SSTable chunks keyed by (file, position) so a hot chunk is inflated once rather than per read.
//
// Without it every cold read of a version paid a frame read, a CRC over the compressed body and a
// zstd decode of the whole commit, however recently the same commit had been read; with a cold
// read for a version a hundred-document commit wrote, that was the same ~20 KB inflated a hundred
// times over to serve the hundred documents.
//
// # Validity
//
// A frame never changes once written: sealed segments are immutable, and the active one only grows
// at its tail, past every frame already in it. What can happen to a frame is that its segment is
// deleted (truncation, retention, repair rewriting it) and another file takes the name. So the key
// is not the segment's sequence number but its generation (storage.SegmentGenerationer), which the
// shim changes whenever the segment is deleted, restored, or its namespace closed: frames of a
// gone segment are simply never asked for again and age out of the LRU. Only complete frames whose
// CRC matched are ever inserted, so a torn or damaged frame is not cached; it fails again on the
// next read, as it would without a cache.
//
// # Layout
//
// Pointer-free apart from one slice header per 1 MiB of arena, so the garbage collector does not
// scan what it holds: the lesson of #159 and of the cold-frame index (#175). Each shard keeps
//
//   - an arena of fixed 256-byte pages in 1 MiB chunks, grown on demand up to the shard's budget;
//   - a page table (next page of the same frame, as a uint32), so a frame occupies any free pages
//     rather than a contiguous run and the arena never fragments;
//   - an entry table with an intrusive doubly-linked LRU ring (uint32 indexes), and a map from
//     key to entry index whose keys and values are plain integers.
//
// A hit copies the frame out into a pooled buffer under the shard lock. Copying a frame is far
// cheaper than inflating it, and it is what makes eviction safe: nothing outside the lock ever
// refers to arena bytes, so a page can be reused the moment its frame is evicted, with no
// reference counts or epochs.
//
// A nil *FrameCache caches nothing, so callers need not check.
type FrameCache struct {
	shards [frameCacheShards]frameShard
	budget atomic.Int64

	hits, misses, inserts, evictions atomic.Int64
}

const (
	framePageSize    = 256
	frameChunkPages  = 4096 // 1 MiB of pages per arena chunk
	frameCacheShards = 8
	noPage           = ^uint32(0)
	// frameEntryOverhead is what a cached frame costs besides its pages: its entry (32 bytes) and
	// its map slot (key, value and the map's own per-slot overhead, ~40 bytes).
	frameEntryOverhead = 72
	// framePageCost is a page plus its page-table slot.
	framePageCost = framePageSize + 4
)

type frameKey struct {
	gen uint64
	off int64
}

type frameEntry struct {
	key        frameKey
	first      uint32 // first page, noPage for an empty frame
	length     uint32
	prev, next uint32 // LRU ring; entry 0 is the sentinel
}

type frameShard struct {
	mu          sync.Mutex
	budgetBytes int64
	index       map[frameKey]uint32
	entries     []frameEntry
	freeEntries []uint32
	chunks      [][]byte
	pageNext    []uint32
	freePages   []uint32
	capacity    int   // pages allocated in chunks
	slack       int64 // chunk backing arrays beyond capacity (extendChunk's headroom)
	usedPages   int
}

// NewFrameCache returns a cache holding at most about budgetBytes of frames and bookkeeping, or
// nil - a cache that holds nothing - when budgetBytes <= 0.
func NewFrameCache(budgetBytes int64) *FrameCache {
	if budgetBytes <= 0 {
		return nil
	}
	c := &FrameCache{}
	for i := range c.shards {
		s := &c.shards[i]
		s.index = make(map[frameKey]uint32)
		s.entries = []frameEntry{{}} // the LRU sentinel
	}
	c.SetBudget(budgetBytes)
	return c
}

// SetBudget re-cuts the cache to budgetBytes, evicting least recently used frames and releasing
// arena memory when it shrinks. budgetBytes <= 0 empties it. Safe while the cache is in use.
func (c *FrameCache) SetBudget(budgetBytes int64) {
	if c == nil {
		return
	}
	if budgetBytes < 0 {
		budgetBytes = 0
	}
	c.budget.Store(budgetBytes)
	per := budgetBytes / frameCacheShards
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		s.budgetBytes = per
		// Evict until what the live frames need fits, then give back arena past what that leaves.
		for s.liveBytes() > per && s.evictOne() {
			c.evictions.Add(1)
		}
		limit := int((per - int64(len(s.index))*frameEntryOverhead) / framePageCost)
		if limit < s.usedPages {
			limit = s.usedPages
		}
		if s.capacity > limit {
			s.shrinkArena(limit)
		}
		s.mu.Unlock()
	}
}

// Budget reports the budget the cache was last cut to.
func (c *FrameCache) Budget() int64 {
	if c == nil {
		return 0
	}
	return c.budget.Load()
}

// Bytes reports the memory the cache holds: its arena (with the chunks' growth headroom, which
// stays within what the budget leaves room for), page table and entry bookkeeping. What the
// memory budget is charged with.
func (c *FrameCache) Bytes() int64 {
	if c == nil {
		return 0
	}
	var n int64
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		n += s.usedBytes(0, 0) + s.slack
		s.mu.Unlock()
	}
	return n
}

// FrameCacheStats are a cache's counters since it was made.
type FrameCacheStats struct {
	Hits, Misses, Inserts, Evictions int64
	Frames                           int
}

// Stats reports the cache's counters, for tests and reporting.
func (c *FrameCache) Stats() FrameCacheStats {
	if c == nil {
		return FrameCacheStats{}
	}
	st := FrameCacheStats{Hits: c.hits.Load(), Misses: c.misses.Load(), Inserts: c.inserts.Load(), Evictions: c.evictions.Load()}
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		st.Frames += len(s.index)
		s.mu.Unlock()
	}
	return st
}

func (c *FrameCache) shard(k frameKey) *frameShard {
	h := (k.gen*0x9E3779B97F4A7C15 ^ uint64(k.off)) * 0xBF58476D1CE4E5B9
	return &c.shards[h>>61]
}

// get copies the cached frame for k into a pooled buffer, which the caller returns with
// bufpool.Put once done with it, and marks the frame most recently used.
func (c *FrameCache) get(k frameKey) (*[]byte, bool) {
	if c == nil {
		return nil, false
	}
	s := c.shard(k)
	s.mu.Lock()
	ei, ok := s.index[k]
	if !ok {
		s.mu.Unlock()
		c.misses.Add(1)
		return nil, false
	}
	s.unlink(ei)
	s.pushFront(ei)
	e := s.entries[ei]
	bp := bufpool.Get(int(e.length))
	dst := *bp
	for p, at := e.first, 0; at < len(dst); p = s.pageNext[p] {
		at += copy(dst[at:], s.page(p))
	}
	s.mu.Unlock()
	c.hits.Add(1)
	return bp, true
}

// put caches payload under k, evicting least recently used frames to make room. A frame too large
// for a quarter of its shard, or for the buffer pool a hit copies into, is not cached: one huge
// commit must not flush everything else.
func (c *FrameCache) put(k frameKey, payload []byte) {
	if c == nil || len(payload) > bufpool.MaxPooled {
		return
	}
	pages := (len(payload) + framePageSize - 1) / framePageSize
	s := c.shard(k)
	s.mu.Lock()
	defer s.mu.Unlock()
	if pages > s.budgetPages()/4 {
		return
	}
	if ei, ok := s.index[k]; ok {
		s.unlink(ei)
		s.pushFront(ei)
		return
	}
	for s.usedBytes(pages, 1) > s.budgetBytes || len(s.freePages) < pages {
		if len(s.freePages) < pages && s.grow(pages-len(s.freePages)) {
			continue
		}
		if !s.evictOne() {
			return
		}
		c.evictions.Add(1)
	}
	first := noPage
	prev := noPage
	for at := 0; at < len(payload); {
		p := s.freePages[len(s.freePages)-1]
		s.freePages = s.freePages[:len(s.freePages)-1]
		at += copy(s.page(p), payload[at:])
		s.pageNext[p] = noPage
		if prev == noPage {
			first = p
		} else {
			s.pageNext[prev] = p
		}
		prev = p
	}
	s.usedPages += pages
	var ei uint32
	if n := len(s.freeEntries); n > 0 {
		ei = s.freeEntries[n-1]
		s.freeEntries = s.freeEntries[:n-1]
	} else {
		ei = uint32(len(s.entries))
		s.entries = append(s.entries, frameEntry{})
	}
	s.entries[ei] = frameEntry{key: k, first: first, length: uint32(len(payload))}
	s.pushFront(ei)
	s.index[k] = ei
	c.inserts.Add(1)
}

// usedBytes is what the shard holds, plus the cost of extraPages more pages (beyond any free
// already allocated) and extraEntries more entries.
func (s *frameShard) usedBytes(extraPages, extraEntries int) int64 {
	capacity := s.capacity
	if need := s.usedPages + extraPages; need > capacity {
		capacity = need
	}
	return int64(capacity)*framePageCost + int64(len(s.index)+extraEntries)*frameEntryOverhead
}

// liveBytes is what the cached frames themselves need, not counting free arena pages.
func (s *frameShard) liveBytes() int64 {
	return int64(s.usedPages)*framePageCost + int64(len(s.index))*frameEntryOverhead
}

func (s *frameShard) budgetPages() int {
	return int(s.budgetBytes / framePageCost)
}

func (s *frameShard) page(p uint32) []byte {
	off := int(p%frameChunkPages) * framePageSize
	return s.chunks[p/frameChunkPages][off : off+framePageSize]
}

// extendChunk returns chunk lengthened to pages pages. Its backing array grows geometrically, up to
// a whole chunk, so filling a chunk a few pages at a time copies it a constant number of times
// rather than once per step - the step is one page in a small shard, and copying the partial chunk
// on every step cost a frame cache ~27 KB of allocation per frame cached (a host serving a 50k
// commit catch-up allocated 1.4 GB here).
//
// maxPages bounds the headroom: the pages the budget leaves room for in this chunk.
func extendChunk(chunk []byte, pages, maxPages int) []byte {
	n := pages * framePageSize
	if n <= cap(chunk) {
		return chunk[:n]
	}
	grown := make([]byte, n, max(n, min(maxPages*framePageSize, 2*cap(chunk))))
	copy(grown, chunk)
	return grown
}

// slackBytes is the chunks' backing arrays beyond the pages allocated: extendChunk's headroom.
func (s *frameShard) slackBytes() int64 {
	var n int64
	for _, c := range s.chunks {
		n += int64(cap(c) - len(c))
	}
	return n
}

// grow adds at least want pages to the arena if the budget allows, reporting whether it added any.
func (s *frameShard) grow(want int) bool {
	limit := s.budgetPages()
	// Pages the budget leaves room for once every entry's overhead is paid.
	room := limit - s.capacity - (len(s.index)+1)*frameEntryOverhead/framePageCost
	if room <= 0 {
		return false
	}
	if want < 64 && room >= 64*16 {
		// Grow in steps rather than a page at a time, but only where a step is small next to the
		// budget: free pages count against it, so a coarse step in a small shard would evict frames
		// to pay for pages nothing uses yet.
		want = 64
	}
	if want > room {
		want = room
	}
	old := s.capacity
	newCap := old + want
	for s.capacity < newCap {
		ci := s.capacity / frameChunkPages
		inChunk := s.capacity % frameChunkPages
		take := frameChunkPages - inChunk
		if take > newCap-s.capacity {
			take = newCap - s.capacity
		}
		// Headroom only where the budget is large enough for coarse steps (see above): the budget
		// does not charge it, so it is kept within what the budget leaves room for. A small
		// shard's partial chunk is small, and copying it on each step stays cheap.
		maxPages := inChunk + take
		if room >= 64*16 {
			maxPages = min(frameChunkPages, old+room-ci*frameChunkPages)
		}
		if ci == len(s.chunks) {
			s.chunks = append(s.chunks, extendChunk(nil, take, maxPages))
		} else {
			s.chunks[ci] = extendChunk(s.chunks[ci], inChunk+take, maxPages)
		}
		s.capacity += take
	}
	s.slack = s.slackBytes()
	for p := old; p < newCap; p++ {
		s.pageNext = append(s.pageNext, noPage)
	}
	// Free pages are taken from the end, so push the new ones highest first: low pages are used
	// first, which keeps a later shrink's relocations few.
	for p := newCap - 1; p >= old; p-- {
		s.freePages = append(s.freePages, uint32(p))
	}
	return true
}

// shrinkArena releases every page at or above limit, moving frames that use one into free pages
// below it. The caller has already evicted down to fewer than limit used pages.
func (s *frameShard) shrinkArena(limit int) {
	below := s.freePages[:0]
	for _, p := range s.freePages {
		if int(p) < limit {
			below = append(below, p)
		}
	}
	s.freePages = below
	for ei := s.entries[0].next; ei != 0; ei = s.entries[ei].next {
		e := &s.entries[ei]
		prev := noPage
		for p := e.first; p != noPage; {
			next := s.pageNext[p]
			if int(p) >= limit {
				q := s.freePages[len(s.freePages)-1]
				s.freePages = s.freePages[:len(s.freePages)-1]
				copy(s.page(q), s.page(p))
				s.pageNext[q] = next
				if prev == noPage {
					e.first = q
				} else {
					s.pageNext[prev] = q
				}
				p = q
			}
			prev = p
			p = next
		}
	}
	nChunks := (limit + frameChunkPages - 1) / frameChunkPages
	for i := nChunks; i < len(s.chunks); i++ {
		s.chunks[i] = nil
	}
	s.chunks = s.chunks[:nChunks]
	if tail := limit % frameChunkPages; tail != 0 {
		last := make([]byte, tail*framePageSize)
		copy(last, s.chunks[nChunks-1])
		s.chunks[nChunks-1] = last
	}
	s.pageNext = s.pageNext[:limit]
	s.capacity = limit
	s.slack = s.slackBytes()
}

// evictOne drops the least recently used frame, reporting false when there is none.
func (s *frameShard) evictOne() bool {
	ei := s.entries[0].prev
	if ei == 0 {
		return false
	}
	e := s.entries[ei]
	s.unlink(ei)
	for p := e.first; p != noPage; {
		next := s.pageNext[p]
		s.freePages = append(s.freePages, p)
		s.usedPages--
		p = next
	}
	delete(s.index, e.key)
	s.entries[ei] = frameEntry{}
	s.freeEntries = append(s.freeEntries, ei)
	return true
}

func (s *frameShard) unlink(ei uint32) {
	e := &s.entries[ei]
	s.entries[e.prev].next = e.next
	s.entries[e.next].prev = e.prev
	e.prev, e.next = 0, 0
}

func (s *frameShard) pushFront(ei uint32) {
	head := s.entries[0].next
	s.entries[ei].prev = 0
	s.entries[ei].next = head
	s.entries[head].prev = ei
	s.entries[0].next = ei
}
