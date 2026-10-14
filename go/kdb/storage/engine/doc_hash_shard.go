package engine

import (
	"sync"
	"unsafe"

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

// Arena geometry. A shard's bodies live in chunks that start small and double up to
// bodyArenaMaxChunk, so a namespace of a few documents costs a few kilobytes rather than 64
// shards times a megabyte. A body larger than bodyArenaLargeBody gets a chunk of its own,
// sized exactly, which is released the moment that body is: a 1.4 MB document rewritten a few
// hundred times must not leave megabyte-sized holes waiting for a compaction.
const (
	bodyArenaMinChunk  = 4 << 10
	bodyArenaMaxChunk  = 128 << 10
	bodyArenaLargeBody = bodyArenaMaxChunk / 4
	// bodyArenaCompactMin is the smallest arena worth compacting: below it the copy costs more
	// than the bytes it would give back.
	bodyArenaCompactMin = 64 << 10
)

// bodyArenaDeadFraction is the share of a shard's appended bytes that may be dead (evicted or
// superseded bodies still occupying a chunk) before the shard compacts. A compaction copies the
// live bytes once, so with a quarter dead each compaction pays for itself after at most three
// live bytes copied per dead byte reclaimed - Cassandra's memtable slab trades the same way.
const bodyArenaDeadFraction = 0.25

// noSlot marks an absent slot link: the end of the free list, or an entry that is not on the
// recency ring because it is pinned.
const noSlot = ^uint32(0)

// slotFree marks a bodyRec that holds nothing (it is on the free list).
const slotFree = ^uint32(0)

// bodyRec is one resident version. It holds no pointers - the body lives in the shard's arena,
// addressed by chunk and offset, and the recency ring is threaded through these records by
// index - so a shard's slot array is a single allocation the collector never scans, however
// many versions it holds. The previous entry carried a Go string, a *list.Element and (on that
// element) a boxed hash: three pointers per version, all of them marked on every GC cycle.
type bodyRec struct {
	hash  codec.Hash
	id    codec.UUID
	chunk uint32 // slotFree when the record is unused
	off   uint32
	n     uint32
	pins  int32
	// prev/next thread the recency ring (head = most recently used) while the entry is
	// unpinned, and are noSlot while it is pinned. A free record reuses next as the free list.
	prev, next uint32
}

// slotReferenced, in a record's off, marks an unpinned version read since the evictor last passed
// it (second-chance / CLOCK eviction). Get sets it instead of moving the record to the front of
// the recency ring: with live bodies bounded every live version is unpinned, so every read of a
// hot document rewrote the ring under the shard lock, and heavy concurrent reads of overlapping
// documents ran 29% slower than with the versions pinned. off never needs the bit - it is an
// offset inside one chunk, below bodyArenaMaxChunk, and 0 for a large body's own chunk.
const slotReferenced = 1 << 31

// bodyRecBytes is what one slot costs in the slot array.
const bodyRecBytes = int64(unsafe.Sizeof(bodyRec{}))

// docIndexEntryBytes is what the budget charges for one version's share of the slot index: a
// 4-byte bucket at the index's typical load. See slotIndex.
const docIndexEntryBytes = 8

// docPinnedEntryBytes approximates one entry of map[codec.Hash]int32, the placeholder-pin map:
// a 36-byte slot plus its control byte, at the swiss table's 7/8 maximum load and the
// power-of-two growth that leaves it between half and fully loaded. Checked against the heap by
// TestDocArenaAccountingMatchesHeap rather than derived, which is why it is a constant.
const docPinnedEntryBytes = 56

// docHashEntryOverhead is what the budget charges a resident version on top of its body: its
// slot record and its index bucket. The budget is applied to live bytes (body plus this), not to
// the arena's footprint, because eviction can only ever give back live bytes; the footprint
// follows them down at the next chunk release or compaction. ResidentBytes reports the footprint.
const docHashEntryOverhead = bodyRecBytes + docIndexEntryBytes

type docHashShard struct {
	mu sync.Mutex
	// index maps a content hash to its slot, and holds no pointers. See slotIndex.
	index     slotIndex
	slots     []bodyRec
	freeHead  uint32
	usedSlots int
	// head is the most recently used unpinned slot, or noSlot when nothing is evictable; the
	// least recently used is slots[head].prev.
	head   uint32
	lruLen int

	// chunks hold the bodies. A chunk is append-only and its bytes are never written again once
	// a body occupies them: compaction copies live bodies into new chunks and drops the old
	// ones, never rewrites in place. That is the whole of the aliasing argument in Get - see
	// docArenaReturnsAlias.
	chunks    [][]byte
	chunkLive []int64 // live body bytes per chunk; a chunk at zero that is not cur is released
	cur       int     // chunk appends go to, or -1
	nextChunk int     // size of the next small chunk (doubles up to bodyArenaMaxChunk)
	arenaCap  int64   // sum of cap(chunks[i]) over held chunks
	arenaUsed int64   // bytes appended into held chunks, live or dead
	arenaLive int64   // bytes of bodies still referenced by a slot
	compacted int64   // compactions run, for tests and reporting

	// pinned holds the pin counts of versions that are pinned but not loaded - the live tree's
	// versions that nothing has read yet, which after an open is nearly all of them. Kept apart
	// from the slots because a placeholder needs only its count: key and value hold no
	// pointers and are never scanned. A version moves to the slots, taking its count, when it
	// is stored.
	pinned map[codec.Hash]int32
	// pinnedPeak is pinned's high-water length: Go maps do not shrink when entries are deleted,
	// so the footprint follows the peak, not the current length.
	pinnedPeak int
	// bytes is the live charge the budget is applied to: the sum over resident versions of
	// len(body) + docHashEntryOverhead.
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
//
// Layout (docs/benchmarks/2026-10-13-live-body-arena.md): each shard keeps its bodies in an
// append-only byte arena and everything else in pointer-free records, so the collector's cost
// for this store no longer grows with the number of versions it holds. The resident set is the
// same set it always was; only its representation changed.
type shardedDocByHashStore struct {
	shards [docHashShardCount]*docHashShard
}

// newShardedDocByHashStore returns a store bounded to budgetBytes in
// total, divided evenly across shards. A budget <= 0 means unbounded,
// which is the pre-existing behavior and what in-memory targets with no
// delta log to fall back on still need.
func newShardedDocByHashStore(budgetBytes int64) *shardedDocByHashStore {
	perShard := perShardDocBudget(budgetBytes)
	s := &shardedDocByHashStore{}
	for i := range s.shards {
		s.shards[i] = &docHashShard{
			pinned:    make(map[codec.Hash]int32),
			freeHead:  noSlot,
			head:      noSlot,
			cur:       -1,
			nextChunk: bodyArenaMinChunk,
			budget:    perShard,
		}
	}
	return s
}

func perShardDocBudget(budgetBytes int64) int64 {
	if budgetBytes <= 0 {
		return 0
	}
	perShard := budgetBytes / docHashShardCount
	if perShard < minDocHashShardBudgetBytes {
		perShard = minDocHashShardBudgetBytes
	}
	return perShard
}

func (s *shardedDocByHashStore) shardFor(h codec.Hash) *docHashShard {
	// Content hashes are SHA256 output, already uniformly distributed - any
	// byte works as a cheap shard selector (mirrors mem/blob_shard.go).
	idx := int(h.Bytes[0]) % docHashShardCount
	return s.shards[idx]
}

// entrySizeBytes is what the budget charges one resident version: its body and the fixed cost
// of the record and index entry that locate it. Tiny documents therefore still count for
// something, so a store of many of them is bounded too.
func entrySizeBytes(doc document.Document) int64 {
	return int64(len(doc.JSON)) + docHashEntryOverhead
}

// Put stores doc under h, copying its body into the shard's arena. A version already resident
// is only promoted.
func (s *shardedDocByHashStore) Put(h codec.Hash, doc document.Document) {
	sh := s.shardFor(h)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if i, ok := sh.index.find(sh.slots, &h); ok {
		if sh.slots[i].pins == 0 {
			sh.lruMoveToFront(i)
		}
		return
	}
	chunk, off := sh.appendBody(doc.JSON)
	i := sh.allocSlot()
	r := &sh.slots[i]
	*r = bodyRec{hash: h, id: doc.ID, chunk: chunk, off: off, n: uint32(len(doc.JSON)), prev: noSlot, next: noSlot}
	// Pinned before it was stored (the engine pins a version before its commit is written)
	// carries its count across; otherwise this is a new, evictable entry.
	if n, ok := sh.pinned[h]; ok {
		r.pins = n
		delete(sh.pinned, h)
		// Placeholders drain as the live tree is read in; a full read pass empties the map, and
		// a Go map keeps its peak size (56 B a slot at a million documents) until rebuilt.
		if sh.pinnedPeak > docShrinkMinEntries && len(sh.pinned)*2 < sh.pinnedPeak {
			sh.shrinkPinnedLocked()
		}
	}
	if r.pins == 0 {
		sh.lruPushFront(i)
	}
	sh.index.insert(sh.slots, &h, i)
	sh.bytes += entrySizeBytes(doc)
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
	evicted := false
	for sh.bytes > sh.budget && sh.head != noSlot {
		tail := sh.slots[sh.head].prev
		if r := &sh.slots[tail]; r.off&slotReferenced != 0 {
			// Read since the last pass: a second chance. Clearing the bit bounds the loop - every
			// rotation clears one - and making the tail the head moves it to the front in O(1).
			r.off &^= slotReferenced
			sh.head = tail
			continue
		}
		sh.removeSlot(tail)
		evicted = true
	}
	if evicted {
		sh.maybeCompactLocked()
	}
}

// removeSlot drops a resident, unpinned version entirely: off the ring, out of the index, its
// body counted dead, its record back on the free list.
func (sh *docHashShard) removeSlot(i uint32) {
	r := &sh.slots[i]
	sh.lruRemove(i)
	sh.index.remove(sh.slots, &r.hash)
	sh.bytes -= int64(r.n) + docHashEntryOverhead
	sh.releaseBody(r.chunk, r.n)
	*r = bodyRec{chunk: slotFree, prev: noSlot, next: sh.freeHead}
	sh.freeHead = i
	sh.usedSlots--
}

// Get returns the version stored under h. See docArenaReturnsAlias for what the returned body
// string refers to.
func (s *shardedDocByHashStore) Get(h codec.Hash) (document.Document, bool) {
	sh := s.shardFor(h)
	sh.mu.Lock()
	i, ok := sh.index.find(sh.slots, &h)
	if !ok {
		sh.mu.Unlock()
		return document.Document{}, false
	}
	r := &sh.slots[i]
	if r.pins == 0 && r.off&slotReferenced == 0 {
		// Written only when not set already: a store on every read of a hot version bounces its
		// cache line between cores, which is most of what the ring update cost.
		r.off |= slotReferenced
	}
	doc := document.Document{ID: r.id, JSON: sh.bodyString(r.chunk, r.off&^slotReferenced, r.n)}
	sh.mu.Unlock()
	return doc, true
}

// Contains reports whether h is resident, without touching its recency or reading its body.
func (s *shardedDocByHashStore) Contains(h codec.Hash) bool {
	sh := s.shardFor(h)
	sh.mu.Lock()
	_, ok := sh.index.find(sh.slots, &h)
	sh.mu.Unlock()
	return ok
}

// docArenaReturnsAlias selects how Get hands a body out. true returns a string that aliases the
// arena chunk; false copies it out (one allocation per read).
//
// Aliasing is safe here for a reason specific to Go, not because of any epoch scheme of our
// own: a chunk's bytes are written exactly once and never again - appends only ever write past
// every existing body, and compaction copies live bodies into *new* chunks and simply drops
// its references to the old ones. Nothing is ever freed, pooled or reused by this code; a
// dropped chunk is reclaimed by the garbage collector, which treats the returned string's
// interior pointer as a reference to the whole chunk. So a reader holding a body across any
// number of compactions keeps exactly the bytes it was given, and the chunk stays allocated
// for as long as it does. The collector is the reader epoch.
//
// The cost is retention, not safety: while a caller holds one body from a chunk the store has
// dropped, the whole chunk (up to bodyArenaMaxChunk) stays allocated. Callers hold a body for
// the length of one request; one that kept bodies (or substrings of them) for the life of the
// process would pin chunks this way. Never add a chunk pool or free list here: recycling a
// chunk is the one change that would turn this from retention into corruption.
// See docs/benchmarks/2026-10-13-live-body-arena.md for the A/B that chose this.
const docArenaReturnsAlias = true

// bodyString returns the body at (chunk, off, n). Caller holds sh.mu.
func (sh *docHashShard) bodyString(chunk, off, n uint32) string {
	if n == 0 {
		return ""
	}
	b := sh.chunks[chunk][off : off+n : off+n]
	if docArenaReturnsAlias {
		return unsafe.String(&b[0], len(b))
	}
	return string(b)
}

// Pin marks h as un-evictable until a matching Unpin. Counted, because the
// same content hash is reachable from several document IDs at once when
// their content is identical, and one of them being overwritten must not
// unpin the others.
func (s *shardedDocByHashStore) Pin(h codec.Hash) {
	sh := s.shardFor(h)
	sh.mu.Lock()
	if i, ok := sh.index.find(sh.slots, &h); ok {
		r := &sh.slots[i]
		if r.pins == 0 {
			sh.lruRemove(i)
		}
		r.pins++
	} else {
		sh.pinned[h]++
		if len(sh.pinned) > sh.pinnedPeak {
			sh.pinnedPeak = len(sh.pinned)
		}
	}
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
	if i, ok := sh.index.find(sh.slots, &h); ok && sh.slots[i].pins > 0 {
		r := &sh.slots[i]
		r.pins--
		if r.pins == 0 {
			sh.lruPushFront(i)
		}
	} else if n, ok := sh.pinned[h]; ok {
		if n <= 1 {
			delete(sh.pinned, h) // a placeholder nobody holds any more
		} else {
			sh.pinned[h] = n - 1
		}
	}
	sh.evictLocked()
	sh.mu.Unlock()
}

// ResidentBytes is what the store actually occupies: every arena chunk it holds (live bodies,
// dead ones awaiting compaction, and the unfilled tail of the current chunk), the slot arrays,
// and its two indexes at their high-water size. This is the honest footprint, which is what
// the memory arbiter should see; the budget itself is applied to LiveBytes. Takes every shard's
// lock once and walks nothing.
func (s *shardedDocByHashStore) ResidentBytes() int64 {
	var total int64
	for _, sh := range s.shards {
		sh.mu.Lock()
		total += sh.footprintLocked()
		sh.mu.Unlock()
	}
	return total
}

func (sh *docHashShard) footprintLocked() int64 {
	return sh.arenaCap +
		int64(cap(sh.slots))*bodyRecBytes +
		int64(cap(sh.chunks))*int64(unsafe.Sizeof([]byte(nil))) +
		int64(cap(sh.chunkLive))*8 +
		sh.index.bytes() +
		int64(sh.pinnedPeak)*docPinnedEntryBytes
}

// LiveBytes is the charge the budget is applied to: resident bodies plus the fixed per-entry
// overhead.
func (s *shardedDocByHashStore) LiveBytes() int64 {
	var total int64
	for _, sh := range s.shards {
		sh.mu.Lock()
		total += sh.bytes
		sh.mu.Unlock()
	}
	return total
}

// DocArenaStats is a breakdown of the version store, for metrics and tests.
type DocArenaStats struct {
	Versions       int64 // resident versions
	PinnedVersions int64 // resident versions with a pin (the loaded part of the live tree)
	Placeholders   int64 // pinned but not loaded
	BodyLiveBytes  int64 // bytes of resident bodies
	ArenaBytes     int64 // bytes of arena chunks held (live + dead + unfilled tail)
	ArenaDeadBytes int64 // appended but no longer referenced, awaiting release or compaction
	IndexBytes     int64 // slot arrays, chunk tables and both indexes
	ResidentBytes  int64 // ArenaBytes + IndexBytes
	LiveBytes      int64 // what the budget is applied to
	Compactions    int64
}

// Stats returns a breakdown of the store. Takes each shard's lock once.
func (s *shardedDocByHashStore) Stats() DocArenaStats {
	var st DocArenaStats
	for _, sh := range s.shards {
		sh.mu.Lock()
		st.Versions += int64(sh.usedSlots)
		st.PinnedVersions += int64(sh.usedSlots - sh.lruLen)
		st.Placeholders += int64(len(sh.pinned))
		st.BodyLiveBytes += sh.arenaLive
		st.ArenaBytes += sh.arenaCap
		st.ArenaDeadBytes += sh.arenaUsed - sh.arenaLive
		fp := sh.footprintLocked()
		st.IndexBytes += fp - sh.arenaCap
		st.ResidentBytes += fp
		st.LiveBytes += sh.bytes
		st.Compactions += sh.compacted
		sh.mu.Unlock()
	}
	return st
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
	perShard := perShardDocBudget(budgetBytes)
	for _, sh := range s.shards {
		sh.mu.Lock()
		sh.budget = perShard
		sh.evictLocked()
		sh.mu.Unlock()
	}
}

// --- slots and the recency ring ---------------------------------------------------------------

func (sh *docHashShard) allocSlot() uint32 {
	sh.usedSlots++
	if sh.freeHead != noSlot {
		i := sh.freeHead
		sh.freeHead = sh.slots[i].next
		return i
	}
	sh.slots = append(sh.slots, bodyRec{})
	return uint32(len(sh.slots) - 1)
}

func (sh *docHashShard) lruPushFront(i uint32) {
	r := &sh.slots[i]
	if sh.head == noSlot {
		r.prev, r.next = i, i
	} else {
		h := &sh.slots[sh.head]
		tail := h.prev
		r.next = sh.head
		r.prev = tail
		sh.slots[tail].next = i
		h.prev = i
	}
	sh.head = i
	sh.lruLen++
}

// lruRemove takes i off the ring if it is on it (an entry is on the ring exactly when it is
// resident and unpinned).
func (sh *docHashShard) lruRemove(i uint32) {
	r := &sh.slots[i]
	if r.next == noSlot {
		return
	}
	if r.next == i { // the only entry
		sh.head = noSlot
	} else {
		sh.slots[r.prev].next = r.next
		sh.slots[r.next].prev = r.prev
		if sh.head == i {
			sh.head = r.next
		}
	}
	r.prev, r.next = noSlot, noSlot
	sh.lruLen--
}

func (sh *docHashShard) lruMoveToFront(i uint32) {
	if sh.head == i {
		return
	}
	sh.lruRemove(i)
	sh.lruPushFront(i)
}

// --- the arena --------------------------------------------------------------------------------

// appendBody copies body into the arena and returns where it landed.
func (sh *docHashShard) appendBody(body string) (chunk, off uint32) {
	n := len(body)
	if n == 0 {
		return 0, 0
	}
	if n > bodyArenaLargeBody {
		c := sh.newChunk(n)
		sh.chunks[c] = append(sh.chunks[c], body...)
		sh.accountAppend(c, n)
		return uint32(c), 0
	}
	if sh.cur < 0 || cap(sh.chunks[sh.cur])-len(sh.chunks[sh.cur]) < n {
		size := max(sh.nextChunk, n)
		if sh.nextChunk < sh.maxChunk() {
			sh.nextChunk *= 2
		}
		old := sh.cur
		sh.cur = sh.newChunk(size)
		// The chunk that stops being current may already be entirely dead; it was kept only
		// because it was still being appended to.
		if old >= 0 && sh.chunkLive[old] == 0 {
			sh.dropChunk(old)
		}
	}
	b := sh.chunks[sh.cur]
	o := len(b)
	sh.chunks[sh.cur] = append(b, body...) // within capacity: never reallocates
	sh.accountAppend(sh.cur, n)
	return uint32(sh.cur), uint32(o)
}

func (sh *docHashShard) accountAppend(c, n int) {
	sh.chunkLive[c] += int64(n)
	sh.arenaUsed += int64(n)
	sh.arenaLive += int64(n)
}

// newChunk allocates a chunk of capacity size, reusing a released chunk's table position if
// there is one (the position, never the memory).
// maxChunk is the largest chunk this shard grows to: bodyArenaMaxChunk, but no more than a quarter
// of the shard's budget. Chunks double up to it, and a shard always holds a partly filled current
// chunk; at a small budget (64 KiB a shard under a 4 MiB namespace budget) 128 KiB chunks left
// the arena holding several times what the budget allowed - the version store's heap grew with
// writes past its bound, which a CI runner caught as 4x the writes costing 2.8x the memory.
func (sh *docHashShard) maxChunk() int {
	if sh.budget <= 0 {
		return bodyArenaMaxChunk
	}
	return int(min(int64(bodyArenaMaxChunk), max(int64(bodyArenaMinChunk), sh.budget/4)))
}

// compactMin is the smallest arena this shard compacts: bodyArenaCompactMin, but no more than half
// the shard's budget, so a small shard sheds dead bodies before they amount to its whole budget.
func (sh *docHashShard) compactMin() int64 {
	if sh.budget <= 0 {
		return bodyArenaCompactMin
	}
	return min(int64(bodyArenaCompactMin), max(int64(bodyArenaMinChunk), sh.budget/2))
}

func (sh *docHashShard) newChunk(size int) int {
	return sh.adoptChunk(make([]byte, 0, size))
}

func (sh *docHashShard) adoptChunk(b []byte) int {
	sh.arenaCap += int64(cap(b))
	sh.arenaUsed += int64(len(b))
	for c := range sh.chunks {
		if sh.chunks[c] == nil {
			sh.chunks[c] = b
			sh.chunkLive[c] = 0
			return c
		}
	}
	sh.chunks = append(sh.chunks, b)
	sh.chunkLive = append(sh.chunkLive, 0)
	return len(sh.chunks) - 1
}

// releaseBody marks n bytes of chunk dead, and releases the chunk if that was its last live
// body and nothing will append to it again.
func (sh *docHashShard) releaseBody(chunk, n uint32) {
	if n == 0 {
		return
	}
	sh.chunkLive[chunk] -= int64(n)
	sh.arenaLive -= int64(n)
	if sh.chunkLive[chunk] == 0 && int(chunk) != sh.cur {
		sh.dropChunk(int(chunk))
	}
}

// dropChunk forgets a chunk with no live bodies. Its memory is reclaimed by the collector once
// no reader still holds a body that aliases it.
func (sh *docHashShard) dropChunk(c int) {
	sh.arenaCap -= int64(cap(sh.chunks[c]))
	sh.arenaUsed -= int64(len(sh.chunks[c]))
	sh.chunks[c] = nil
	sh.chunkLive[c] = 0
	if c == sh.cur {
		sh.cur = -1
	}
}

// maybeCompactLocked compacts the shard once enough of its arena is dead.
func (sh *docHashShard) maybeCompactLocked() {
	dead := sh.arenaUsed - sh.arenaLive
	if sh.arenaUsed >= sh.compactMin() && float64(dead) >= bodyArenaDeadFraction*float64(sh.arenaUsed) {
		sh.compactLocked()
		return
	}
	// The slots and indexes can be oversized after a large eviction even when the arena is not
	// worth copying, e.g. after many tiny bodies were evicted.
	sh.maybeShrinkIndexesLocked()
}

// compactLocked copies every live small body into fresh chunks sized to hold them, carries
// each large body's dedicated chunk across as it is, and drops the old chunks. Old chunks are
// never written to: a reader that was handed a body aliasing one keeps it intact (see
// docArenaReturnsAlias).
func (sh *docHashShard) compactLocked() {
	oldChunks := sh.chunks
	sh.chunks = nil
	sh.chunkLive = nil
	sh.cur = -1
	sh.arenaCap, sh.arenaUsed, sh.arenaLive = 0, 0, 0
	// Size the first new chunk to the live small bodies, so a compacted shard is one chunk
	// rather than the doubling sequence again.
	var small int64
	for i := range sh.slots {
		if r := &sh.slots[i]; r.chunk != slotFree && r.n <= bodyArenaLargeBody {
			small += int64(r.n)
		}
	}
	sh.nextChunk = bodyArenaMinChunk
	for int64(sh.nextChunk) < small && sh.nextChunk < bodyArenaMaxChunk {
		sh.nextChunk *= 2
	}
	for i := range sh.slots {
		r := &sh.slots[i]
		if r.chunk == slotFree || r.n == 0 {
			continue
		}
		if r.n > bodyArenaLargeBody { // a dedicated chunk holding exactly this body
			c := sh.adoptChunk(oldChunks[r.chunk])
			sh.chunkLive[c] = int64(r.n)
			sh.arenaLive += int64(r.n)
			r.chunk = uint32(c)
			continue
		}
		off := r.off &^ slotReferenced
		b := oldChunks[r.chunk][off : off+r.n]
		r.chunk, r.off = sh.appendBody(unsafe.String(&b[0], len(b)))
	}
	sh.compacted++
	sh.maybeShrinkIndexesLocked()
}

// docShrinkMinEntries is the smallest slot array or index worth rebuilding to give memory back.
const docShrinkMinEntries = 256

// maybeShrinkIndexesLocked rebuilds the slot array and the placeholder map when they are mostly
// empty (the slot index shrinks by itself). Go maps and slices never give memory back on their own, so a shard that once held a
// million versions and now holds a thousand would otherwise keep the million's footprint.
func (sh *docHashShard) maybeShrinkIndexesLocked() {
	if sh.pinnedPeak > docShrinkMinEntries && len(sh.pinned)*2 < sh.pinnedPeak {
		sh.shrinkPinnedLocked()
	}
	if len(sh.slots) > docShrinkMinEntries && sh.usedSlots*2 < len(sh.slots) {
		sh.repackSlotsLocked()
	}
}

// shrinkPinnedLocked rebuilds the placeholder map at its current size. Amortised O(1): it runs
// only after the map has halved since the last rebuild.
func (sh *docHashShard) shrinkPinnedLocked() {
	m := make(map[codec.Hash]int32, len(sh.pinned))
	for k, v := range sh.pinned {
		m[k] = v
	}
	sh.pinned = m
	sh.pinnedPeak = len(m)
}

// repackSlotsLocked moves the used slot records to the front of a right-sized array, keeping
// every record's recency position.
func (sh *docHashShard) repackSlotsLocked() {
	remap := make([]uint32, len(sh.slots))
	packed := make([]bodyRec, 0, sh.usedSlots)
	for i := range sh.slots {
		if sh.slots[i].chunk == slotFree {
			remap[i] = noSlot
			continue
		}
		remap[i] = uint32(len(packed))
		packed = append(packed, sh.slots[i])
	}
	for i := range packed {
		r := &packed[i]
		if r.next != noSlot {
			r.prev, r.next = remap[r.prev], remap[r.next]
		}
	}
	sh.index.rebuild(packed, len(packed))
	if sh.head != noSlot {
		sh.head = remap[sh.head]
	}
	sh.slots = packed
	sh.freeHead = noSlot
}
