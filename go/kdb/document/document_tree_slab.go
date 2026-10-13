package document

import (
	"math/bits"
	"runtime"
	"runtime/metrics"
	"sync"
	"sync/atomic"

	"github.com/limidus/kdb/go/kdb/codec"
)

// The trie's nodes live in a slab: fixed-size records in chunks of plain structs, addressed by
// uint32 references instead of pointers. A record holds no pointer, so the chunks are allocated
// as no-scan memory and the garbage collector never traces the tree. At a million documents the
// pointer trie it replaces was ~2.8 heap objects per document, every one marked on every cycle.
//
// Sharing and reclamation. Trees are persistent: an update creates the O(depth) records on the
// changed path and shares every other subtree with the tree it came from, and historical trees
// (the engine's tree store, pinned readers) share records with the live one. So a record is
// freed by reference counting:
//
//   - every record counts the references to it: from parent records, plus one per tree handle
//     (trieTree) whose root it is;
//   - each DocumentTree carries a *trieTree, the one heap object per tree version the GC does
//     track. When the last DocumentTree copy naming it becomes unreachable, a runtime cleanup
//     releases its root: the count drops, and records reaching zero go back on the free list and
//     release their children.
//
// The Go GC thereby decides which roots are live - it already knows every place a DocumentTree
// is held, including lock-free reader snapshots - and the slab only counts edges between
// records, which change only on updates. A reader holding a tree holds its handle; while the
// handle is reachable its root's count is at least one, so every record reachable from it has
// a count of at least one and cannot be reused under the reader. Readers take no lock: a slot
// is written only while unpublished (fresh, or freed by every tree that could name it).
// Every method that reads a tree's records ends with runtime.KeepAlive on its handle, so the
// handle outlives the read even after the root reference has been copied out of it.
//
// One slab serves one family of trees - those derived from each other by With/Without - so
// unrelated namespaces never share a lock. Mutation (allocation, counts, frees) is under the
// slab's mutex; the hashing an update needs, which is most of its cost, is done before taking
// it (see trieEdit).

// tref addresses a record in a trieSlab: 0 is the empty subtree; the top bit selects the leaf
// pool; bit 30 marks a record local to an edit not yet placed in the slab.
type tref uint32

const (
	refLeaf      tref = 1 << 31
	refLocal     tref = 1 << 30
	refIndexMask tref = refLocal - 1
)

// nodeRec is an internal node: its hash and its 16 children (0 for an absent child). 96 bytes.
type nodeRec struct {
	hash  [32]byte
	child [16]tref
}

// leafRec is a compressed leaf: one entry standing for the whole subtree where it sits, with the
// hash that subtree's full-depth spine would have (foldLeaf at its depth). 80 bytes.
//
// Reference counts are not in the records but in a dense array beside them (slabPool.counts):
// a release decrements every child of a freed node and an update increments every sibling on
// its path, and at 4 bytes a count those land in a few megabytes of cache rather than one cache
// miss each across the whole slab.
type leafRec struct {
	hash    [32]byte
	id      [16]byte
	content codec.Hash
}

// Chunks double from 8 records up to 16,384, then stay there: a tree of a few documents (a wire
// decode, a test) costs a few hundred bytes, a large one costs no more than one partly used
// chunk of slack.
const (
	slabMinShift   = 3
	slabMaxShift   = 14
	slabGeomChunks = slabMaxShift - slabMinShift + 1
	slabGeomSlots  = (1 << slabMinShift) * (1<<slabGeomChunks - 1)
	slabMaxIndex   = uint32(refIndexMask)
)

func slabLocate(i uint32) (int, uint32) {
	if i < slabGeomSlots {
		k := bits.Len32(i>>slabMinShift+1) - 1
		return k, i - (1<<slabMinShift)*(1<<k-1)
	}
	j := i - slabGeomSlots
	return slabGeomChunks + int(j>>slabMaxShift), j & (1<<slabMaxShift - 1)
}

func slabChunkLen(k int) int {
	if k >= slabGeomChunks {
		return 1 << slabMaxShift
	}
	return 1 << (slabMinShift + k)
}

// slabPool is one record type's chunks. Slot 0 is never handed out. The chunk directory is
// replaced, never modified, when it grows, so a reader may index a directory it loaded at any
// time after the records it reads were published.
type slabPool[R any] struct {
	chunks atomic.Pointer[[][]R]
	counts atomic.Pointer[[][]uint32] // reference counts, chunked as the records are
	used   uint32   // slots below this have been handed out at some time; under the slab lock
	cap    uint32   // slots below this are addressable; under the slab lock
	free   []uint32 // freed slots; under the slab lock
}

func (p *slabPool[R]) dir() [][]R {
	if d := p.chunks.Load(); d != nil {
		return *d
	}
	return nil
}

// grow makes slots below n addressable, reporting how many slots it added. Under the slab lock.
func (p *slabPool[R]) grow(n uint32) (added uint32) {
	if p.cap >= n {
		return 0
	}
	before := p.cap
	d := p.dir()
	nd := make([][]R, len(d), len(d)+4)
	copy(nd, d)
	var cd [][]uint32
	if c := p.counts.Load(); c != nil {
		cd = *c
	}
	ncd := make([][]uint32, len(cd), len(cd)+4)
	copy(ncd, cd)
	for p.cap < n {
		l := slabChunkLen(len(nd))
		nd = append(nd, make([]R, l))
		ncd = append(ncd, make([]uint32, l))
		p.cap += uint32(l)
	}
	p.counts.Store(&ncd)
	p.chunks.Store(&nd)
	return p.cap - before
}

// reserve hands out n fresh consecutive slots, and reports how many slots the pool grew by to
// make room. Under the slab lock.
func (p *slabPool[R]) reserve(n uint32) (start, grew uint32) {
	if p.used == 0 {
		p.used = 1
	}
	start = p.used
	if uint64(start)+uint64(n) > uint64(slabMaxIndex) {
		panic("document: trie slab exhausted")
	}
	p.used += n
	return start, p.grow(p.used)
}

func (p *slabPool[R]) at(i uint32) *R {
	k, off := slabLocate(i)
	return &p.dir()[k][off]
}

// count is slot i's reference count. Under the slab lock, or by a bulk build on its own slots.
func (p *slabPool[R]) count(i uint32) *uint32 {
	k, off := slabLocate(i)
	return &(*p.counts.Load())[k][off]
}

type trieSlab struct {
	mu     sync.Mutex
	nodes  slabPool[nodeRec]
	leaves slabPool[leafRec]

	// Roots of dropped trees awaiting release, under their own lock so a cleanup never waits on
	// an update (see dropTrieRoot). pendingN mirrors len(pending) for a lock-free check.
	pendMu   sync.Mutex
	pending  []tref
	pendingN atomic.Int32

	reused uint64 // slots handed out again after being freed

	// For counted: records created, and whether slots were added, since the last GC cycle seen.
	created, cycleCreated uint64
	grewSinceCycle        bool
	gcCycle               uint64
	cycles                [1]metrics.Sample
}

const (
	nodeRecBytes = 96 + 4 // with its count
	leafRecBytes = 80 + 4
)

func newTrieSlab() *trieSlab { return &trieSlab{} }

// trieView is a snapshot of a slab's chunk directories, for reading the records of a tree held
// by the caller.
type trieView struct {
	nodes  [][]nodeRec
	leaves [][]leafRec
}

func (s *trieSlab) view() trieView {
	return trieView{nodes: s.nodes.dir(), leaves: s.leaves.dir()}
}

func (v *trieView) node(r tref) *nodeRec {
	k, off := slabLocate(uint32(r))
	return &v.nodes[k][off]
}

func (v *trieView) leaf(r tref) *leafRec {
	k, off := slabLocate(uint32(r &^ refLeaf))
	return &v.leaves[k][off]
}

func (v *trieView) hash(r tref) *[32]byte {
	switch {
	case r == 0:
		return &trieZero
	case r&refLeaf != 0:
		return &v.leaf(r).hash
	default:
		return &v.node(r).hash
	}
}

// slabPoison, set only by tests, overwrites freed records so that a read of a freed slot fails
// hash verification (and, under -race, is reported as a race with the freeing write).
var slabPoison atomic.Bool

var slabPoisonHash = [32]byte{0: 0xde, 1: 0xad, 31: 0xff}

// The methods below are called with s.mu held.

func (s *trieSlab) retain(r tref) {
	switch {
	case r == 0:
	case r&refLeaf != 0:
		*s.leaves.count(uint32(r &^ refLeaf))++
	default:
		*s.nodes.count(uint32(r))++
	}
}

func (s *trieSlab) release(r tref) {
	switch {
	case r == 0:
	case r&refLeaf != 0:
		i := uint32(r &^ refLeaf)
		rc := s.leaves.count(i)
		if *rc == 0 {
			panic("document: trie slab leaf released more often than retained")
		}
		if *rc--; *rc == 0 {
			if slabPoison.Load() {
				l := s.leaves.at(i)
				l.hash, l.content = slabPoisonHash, codec.Hash{Bytes: slabPoisonHash}
			}
			s.leaves.free = append(s.leaves.free, i)
		}
	default:
		rc := s.nodes.count(uint32(r))
		if *rc == 0 {
			panic("document: trie slab node released more often than retained")
		}
		if *rc--; *rc == 0 {
			n := s.nodes.at(uint32(r))
			children := n.child
			if slabPoison.Load() {
				n.hash = slabPoisonHash
			}
			s.nodes.free = append(s.nodes.free, uint32(r))
			for _, c := range children {
				s.release(c)
			}
		}
	}
}

func (s *trieSlab) allocNode() uint32 {
	s.counted()
	if n := len(s.nodes.free); n > 0 {
		s.reused++
		i := s.nodes.free[n-1]
		s.nodes.free = s.nodes.free[:n-1]
		return i
	}
	i, grew := s.nodes.reserve(1)
	s.grewSinceCycle = s.grewSinceCycle || grew > 0
	return i
}

func (s *trieSlab) allocLeaf() uint32 {
	s.counted()
	if n := len(s.leaves.free); n > 0 {
		s.reused++
		i := s.leaves.free[n-1]
		s.leaves.free = s.leaves.free[:n-1]
		return i
	}
	i, grew := s.leaves.reserve(1)
	s.grewSinceCycle = s.grewSinceCycle || grew > 0
	return i
}

// Reclamation waits on the collector: a dropped tree's records are released once the GC has found
// its handle unreachable. And the collector paces itself by heap growth, in which a slab's free
// slots count as live, while the records an update replaces are garbage it cannot see. So in a
// loop that allocates little besides the slab's own records (updates and nothing else), cycles
// come further apart the larger the slab gets, every version's records wait on them, and the
// slab would grow geometrically. The engine's write path allocates far more per update than the
// records it adds, so collections normally come first; this is the bound for when they do not.
// Go's own rule (GOGC=100) is to collect once allocation since the last cycle matches the live
// heap; the slab applies it at half that: once the records created since the last cycle - each
// update's replace about as many as it creates - exceed half the records that were live before
// it (and at least slabNudgeFloor bytes), and the slab has had to grow, a collection is
// requested in the background, one at a time process-wide. A slab then holds about 1.5 times
// what its trees need; a pointer trie's heap would reach twice at its GC goal.
const slabNudgeFloor = 4 << 20

const slabNudgeEvery = 4096 // records created between checks

var slabGCRunning atomic.Bool

func (s *trieSlab) counted() {
	s.created++
	if s.created%slabNudgeEvery != 0 {
		return
	}
	if s.cycles[0].Name == "" {
		s.cycles[0].Name = "/gc/cycles/total:gc-cycles"
	}
	metrics.Read(s.cycles[:])
	if c := s.cycles[0].Value.Uint64(); c != s.gcCycle {
		s.gcCycle, s.cycleCreated, s.grewSinceCycle = c, s.created, false
		return
	}
	since := s.created - s.cycleCreated
	inUse := uint64(s.nodes.used) + uint64(s.leaves.used) - uint64(len(s.nodes.free)+len(s.leaves.free))
	live := uint64(0)
	if inUse > since {
		live = inUse - since
	}
	const recBytes = (nodeRecBytes + leafRecBytes) / 2
	if s.grewSinceCycle && since*recBytes > max(slabNudgeFloor, live*recBytes/2) && slabGCRunning.CompareAndSwap(false, true) {
		s.cycleCreated, s.grewSinceCycle = s.created, false
		go func() {
			runtime.GC()
			slabGCRunning.Store(false)
		}()
	}
}

// trieTree is a handle on one tree in a slab: the only per-tree heap object, and what keeps the
// tree's records from being reused. Never construct one except through adopt.
type trieTree struct {
	slab *trieSlab
	root tref
}

type trieDrop struct {
	slab *trieSlab
	root tref
}

// dropTrieRoot runs (on the runtime's cleanup goroutine) once a handle is unreachable: it gives
// back the handle's reference to its root, freeing whatever only that tree held. A collection
// can drop a whole burst of versions at once, and taking the slab lock for each would hand it
// back and forth with a writer once per version; so the root is queued, and the queue is
// drained here only when the slab lock is free. A writer finding roots queued releases a few
// before each update (drainSome), so the queue cannot outrun the updates making it.
func dropTrieRoot(d trieDrop) {
	s := d.slab
	s.pendMu.Lock()
	s.pending = append(s.pending, d.root)
	s.pendingN.Store(int32(len(s.pending)))
	s.pendMu.Unlock()
	if s.mu.TryLock() {
		s.drain(-1)
		s.mu.Unlock()
	}
}

// drain releases up to limit queued roots (all of them when limit < 0). Under s.mu.
func (s *trieSlab) drain(limit int) {
	for limit != 0 && s.pendingN.Load() > 0 {
		var take [64]tref
		n := len(take)
		if limit > 0 && limit < n {
			n = limit
		}
		s.pendMu.Lock()
		n = min(n, len(s.pending))
		copy(take[:], s.pending[len(s.pending)-n:])
		s.pending = s.pending[:len(s.pending)-n]
		s.pendingN.Store(int32(len(s.pending)))
		s.pendMu.Unlock()
		for _, r := range take[:n] {
			s.release(r)
		}
		if limit > 0 {
			limit -= n
		}
	}
}

// drainPerUpdate is how many queued roots an update releases before placing its own records:
// more than the one version each update drops, so a backlog shrinks.
const drainPerUpdate = 4

// adopt wraps root, whose count the caller has already raised by one for this handle (under the
// lock), in a handle that gives that reference back when it becomes unreachable. A zero root is
// the empty tree, which has no handle.
func (s *trieSlab) adopt(root tref) *trieTree {
	if root == 0 {
		return nil
	}
	h := &trieTree{slab: s, root: root}
	runtime.AddCleanup(h, dropTrieRoot, trieDrop{slab: s, root: root})
	return h
}

// chunkBytes is the slab's chunk memory, read without the lock.
func (s *trieSlab) chunkBytes() int64 {
	var b int64
	for k := range s.nodes.dir() {
		b += int64(slabChunkLen(k)) * nodeRecBytes
	}
	for k := range s.leaves.dir() {
		b += int64(slabChunkLen(k)) * leafRecBytes
	}
	return b
}

// slabStats describes a slab's occupancy, for tests and measurement.
type slabStats struct {
	Nodes, Leaves         int // records in use (including those of queued roots)
	FreeNodes, FreeLeaves int
	Reused                int // slots handed out again after being freed
	Pending               int // dropped roots not yet released
	Bytes                 int // chunk memory
}

func (s *trieSlab) stats() slabStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := slabStats{
		FreeNodes:  len(s.nodes.free),
		FreeLeaves: len(s.leaves.free),
		Reused:     int(s.reused),
		Pending:    int(s.pendingN.Load()),
	}
	if s.nodes.used > 0 {
		st.Nodes = int(s.nodes.used) - 1 - st.FreeNodes
	}
	if s.leaves.used > 0 {
		st.Leaves = int(s.leaves.used) - 1 - st.FreeLeaves
	}
	st.Bytes = int(s.chunkBytes())
	return st
}
