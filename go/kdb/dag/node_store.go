package dag

import (
	"bytes"
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// nodeStore holds the DAG's commits in a form the garbage collector does not have to scan, in
// two tiers that share one record format (graph_record.go).
//
// The previous representation was map[codec.Hash]commitNode with a document.Commit as the
// value: a slice of parents, three strings, a slice of operations and a pointer to a schema hash
// per commit. Every one of those is a pointer the collector traces on every cycle, and the whole
// thing measured 438-523 bytes per commit (docs/kdb-mmap-plan.md, Phase 0).
//
// The resident tier is the memtable of this scheme: commit records in heap chunks of raw bytes,
// addressed through a hash -> slot map whose key and value hold no pointers. Neither is scanned.
// The segment tier is the same records on disk, mapped (graph_file.go). Because the bytes are the
// same, flushing a commit to disk copies its record, and one decoder reads both tiers.
//
// A commit lives in exactly one place. Attach moves commits out of the resident tier; put never
// adds a commit a segment shows; segments never share a commit. A segment record can be hidden -
// deleted from the DAG since the segment was written, or never in this DAG at all - and hidden is
// what keeps the files from ever being an authority. A merge drops hidden records for good, the
// way a Cassandra compaction drops tombstoned rows.
//
// Variable-length data lives beside the records: messages and extra parents of resident commits
// in side maps (a segment carries its own), and operations (bounded by the DAG's operations
// budget) for both tiers.
//
// Not safe for concurrent use; the owning InMemoryCommitDag's mu guards it. Every read copies out
// of the mapping before the lock is released, which is what lets a rewrite swap segments under the
// write lock without counting readers.
type nodeStore struct {
	index map[codec.Hash]uint32
	// chunks hold records in fixed-size blocks rather than one growing slice: a single slice
	// doubles its capacity as it fills, so up to half of it is empty, and every doubling copies
	// the whole thing at the moment the DAG is busiest admitting commits.
	//
	// Every chunk but the first is chunkSlots records. The first starts at minChunkSlots and
	// doubles to chunkSlots, so a namespace of a few commits costs kilobytes, not the full chunk
	// - a server holding thousands of small namespaces paid 1 MiB each for its first commit.
	chunks     [][]byte
	chunkSlots uint32 // records per full chunk; zero means DefaultGraphChunkSlots
	used       uint32 // slots ever handed out; free holds the ones since returned
	free       []uint32

	// namespaces interns Commit.NamespaceID for the resident tier (a record's ns index). A DAG
	// holds one namespace, so this is almost always a single entry; it is a table only so a
	// commit that names a different namespace still round-trips exactly.
	namespaces []string

	messages map[codec.Hash]string
	ops      map[codec.Hash][]document.Op
	extra    map[codec.Hash][]codec.Hash // parents beyond the second
	// attach is the attachment root of resident commits that have one (grFlagAttach).
	attachRoots map[codec.Hash]codec.Hash

	// segs is the segment tier, newest first.
	segs []*graphBase
	// hidden is segment records the DAG does not hold. Every entry names a record in some segment.
	hidden map[codec.Hash]struct{}
	// genOverride is a segment commit's generation where the DAG's differs from the record's - a
	// recompute after the segment was written. Pointer-free, like hidden.
	genOverride map[codec.Hash]uint32
	// segRecords is the total record count across segs.
	segRecords int

	// goMode holds commits the way the DAG always used to (GraphMemoryGo): a Go map of whole
	// document.Commit values, traced by the collector on every cycle. Kept selectable at startup
	// so the two can be run against each other, and as a fallback. In this mode goNodes is the
	// resident tier and every other field above is unused; segments are never attached.
	goMode  bool
	goNodes map[codec.Hash]goNode

	// frozen is how many slots a segment write is reading in place (freeze): until it ends
	// (unfreeze), no slot below it changes. A write to a frozen slot goes to a fresh slot instead
	// (copy-on-write), and a frozen slot let go of waits in frozenFreed. Zero when no write is
	// reading resident records. This is what lets a flush stream the resident tier without the
	// DAG's lock and without copying it - Cassandra's switched-out memtable.
	frozen      uint32
	frozenFreed []uint32
	// genEpoch counts generation changes to segment records, so a merge can tell whether any
	// happened while it ran (attachMerged).
	genEpoch uint64

	// recent is the last commit put, held whole, so the lookups that follow every append - the
	// head snapshot, the next transaction reading its parent, the causal timestamp - get back
	// the stored struct the way the Go map always handed it out, instead of decoding the record
	// (six allocations per write). One commit, so it costs the collector nothing worth counting.
	// Kept current by every path that changes a commit (put, replace, setOps, delete); cleared
	// whenever it might not be.
	recent   document.Commit
	recentOK bool
}

// goNode is one commit in GraphMemoryGo - exactly the old commitNode.
type goNode struct {
	commit     document.Commit
	generation uint32
}

// setGoMode switches the resident representation, moving any commits already held. Only ever
// done at startup (SetGraphSettings), when the DAG holds its genesis commit and nothing else.
func (s *nodeStore) setGoMode(on bool) {
	if on == s.goMode {
		return
	}
	var held []document.Commit
	var gens []uint32
	s.forEach(func(h codec.Hash) {
		c, _ := s.commit(h)
		held = append(held, c)
		gens = append(gens, s.generation(h))
	})
	chunkSlots := s.chunkSlots
	*s = newNodeStore()
	s.chunkSlots = chunkSlots
	s.goMode = on
	if on {
		s.goNodes = make(map[codec.Hash]goNode)
	}
	for i, c := range held {
		s.put(c, gens[i])
	}
}

// isResident reports whether h is held in memory (rather than in a segment).
func (s *nodeStore) isResident(h codec.Hash) bool {
	if s.goMode {
		_, ok := s.goNodes[h]
		return ok
	}
	_, ok := s.index[h]
	return ok
}

// forEachResident visits every commit held in memory.
func (s *nodeStore) forEachResident(fn func(codec.Hash)) {
	if s.goMode {
		for h := range s.goNodes {
			fn(h)
		}
		return
	}
	for h := range s.index {
		fn(h)
	}
}

func (s *nodeStore) slotsPerChunk() uint32 {
	if s.chunkSlots == 0 {
		return DefaultGraphChunkSlots
	}
	return s.chunkSlots
}

// setChunkSlots sets the full chunk size in records (zero is the default), repacking any records
// already held: SetGraphSettings runs after the genesis commit is stored. Slot numbers are
// unchanged, so the index stays valid.
func (s *nodeStore) setChunkSlots(n uint32) {
	if n == s.chunkSlots {
		return
	}
	old := *s
	s.chunks, s.chunkSlots = nil, n
	for i := uint32(0); i < old.used; i++ {
		s.ensureSlot(i)
		copy(s.at(i), old.at(i))
	}
}

func (s *nodeStore) at(i uint32) []byte {
	per := s.slotsPerChunk()
	c := s.chunks[i/per]
	off := int(i%per) * graphRecordSize
	return c[off : off+graphRecordSize]
}

// ensureSlot makes slot i addressable, allocating or growing a chunk as needed.
func (s *nodeStore) ensureSlot(i uint32) {
	per := s.slotsPerChunk()
	ci := int(i / per)
	for len(s.chunks) <= ci {
		if len(s.chunks) == 0 {
			s.chunks = append(s.chunks, make([]byte, min(per, minChunkSlots)*graphRecordSize))
		} else {
			s.chunks = append(s.chunks, make([]byte, per*graphRecordSize))
		}
	}
	if need := (int(i%per) + 1) * graphRecordSize; ci == 0 && len(s.chunks[0]) < need {
		// Only the first chunk is short. Double it, copying at most one chunk's records.
		n := min(max(2*len(s.chunks[0]), need), int(per)*graphRecordSize)
		grown := make([]byte, n)
		copy(grown, s.chunks[0])
		s.chunks[0] = grown
	}
}

func newNodeStore() nodeStore {
	return nodeStore{index: make(map[codec.Hash]uint32)}
}

// segPos is where the segment tier shows h, if it does. Each segment's bloom filter turns a
// miss - the common case: a write looks up its own new commit - into one cache line per segment.
func (s *nodeStore) segPos(h codec.Hash) (*graphBase, int, bool) {
	if len(s.segs) == 0 {
		return nil, 0, false
	}
	if _, gone := s.hidden[h]; gone {
		return nil, 0, false
	}
	return s.recordOf(h)
}

// recordOf is the segment with a record for h, hidden or not, and its position there.
func (s *nodeStore) recordOf(h codec.Hash) (*graphBase, int, bool) {
	key := bloomKeyHash(&h)
	for _, b := range s.segs {
		if !b.bloom.has(key) {
			continue
		}
		if pos, ok := b.find(h); ok {
			return b, pos, true
		}
	}
	return nil, 0, false
}

// inAnySegment reports whether any segment has a record for h, hidden or not.
func (s *nodeStore) inAnySegment(h codec.Hash) bool {
	_, _, ok := s.recordOf(h)
	return ok
}

// residentLen is how many commits the resident tier holds - what the flush threshold counts.
func (s *nodeStore) residentLen() int {
	if s.goMode {
		return len(s.goNodes)
	}
	return len(s.index)
}

// segVisible is how many commits the segment tier answers for.
func (s *nodeStore) segVisible() int { return s.segRecords - len(s.hidden) }

func (s *nodeStore) len() int { return s.residentLen() + s.segVisible() }

func (s *nodeStore) has(h codec.Hash) bool {
	if s.goMode {
		_, ok := s.goNodes[h]
		return ok
	}
	if _, ok := s.index[h]; ok {
		return true
	}
	_, _, ok := s.segPos(h)
	return ok
}

func (s *nodeStore) generation(h codec.Hash) uint32 {
	if s.goMode {
		return s.goNodes[h].generation
	}
	if i, ok := s.index[h]; ok {
		return recGeneration(s.at(i))
	}
	if b, pos, ok := s.segPos(h); ok {
		if g, ok := s.genOverride[h]; ok {
			return g
		}
		return b.generation(pos)
	}
	return 0
}

func (s *nodeStore) setGeneration(h codec.Hash, gen uint32) {
	if s.goMode {
		if n, ok := s.goNodes[h]; ok {
			n.generation = gen
			s.goNodes[h] = n
		}
		return
	}
	if i, ok := s.index[h]; ok {
		setRecGeneration(s.at(s.writable(h, i)), gen)
		return
	}
	if b, pos, ok := s.segPos(h); ok {
		s.setSegGeneration(h, b.generation(pos), gen)
	}
}

func (s *nodeStore) setSegGeneration(h codec.Hash, recorded, gen uint32) {
	s.genEpoch++
	if recorded == gen {
		delete(s.genOverride, h)
		return
	}
	if s.genOverride == nil {
		s.genOverride = make(map[codec.Hash]uint32)
	}
	s.genOverride[h] = gen
}

func (s *nodeStore) internNamespace(ns string) uint16 {
	for i, n := range s.namespaces {
		if n == ns {
			return uint16(i)
		}
	}
	s.namespaces = append(s.namespaces, ns)
	return uint16(len(s.namespaces) - 1)
}

// timestamp is a commit's timestamp without building the commit.
func (s *nodeStore) timestamp(h codec.Hash) (codec.Timestamp, bool) {
	if s.goMode {
		n, ok := s.goNodes[h]
		return n.commit.Timestamp, ok
	}
	if i, ok := s.index[h]; ok {
		r := s.at(i)
		return codec.Timestamp{EpochMillis: int64(le.Uint64(r[grEpochMs:])), MicroRemainder: int(int64(le.Uint64(r[grMicro:])))}, true
	}
	if b, pos, ok := s.segPos(h); ok {
		return b.timestamp(pos), true
	}
	return codec.Timestamp{}, false
}

func (s *nodeStore) setOps(h codec.Hash, ops []document.Op) {
	if s.recentOK && s.recent.Hash == h {
		s.recent.Operations = ops
	}
	if len(ops) > 0 {
		if s.ops == nil {
			s.ops = make(map[codec.Hash][]document.Op)
		}
		s.ops[h] = ops
	} else if s.ops != nil {
		delete(s.ops, h)
	}
}

// put stores c with generation gen, replacing any commit already held under its hash.
func (s *nodeStore) put(c document.Commit, gen uint32) {
	if s.goMode {
		s.goNodes[c.Hash] = goNode{commit: c, generation: gen}
		return
	}
	s.recent, s.recentOK = c, true
	if _, resident := s.index[c.Hash]; !resident {
		if b, pos, ok := s.segPos(c.Hash); ok {
			// Already on disk. A commit's hash covers everything a record carries, so the only
			// things that can differ are the ones held beside it.
			s.setOps(c.Hash, c.Operations)
			s.setSegGeneration(c.Hash, b.generation(pos), gen)
			return
		}
	}
	i, ok := s.index[c.Hash]
	if ok && i < s.frozen {
		s.frozenFreed = append(s.frozenFreed, i) // a segment write is reading it; re-put elsewhere
		ok = false
	}
	if !ok {
		i = s.newSlot()
		s.index[c.Hash] = i
	}
	encodeRecord(s.at(i), c, gen, s.internNamespace(c.NamespaceID))

	if len(c.ParentHashes) > 2 {
		if s.extra == nil {
			s.extra = make(map[codec.Hash][]codec.Hash)
		}
		s.extra[c.Hash] = append([]codec.Hash(nil), c.ParentHashes[2:]...)
	} else if s.extra != nil {
		delete(s.extra, c.Hash)
	}
	if c.AttachmentTreeHash != nil {
		if s.attachRoots == nil {
			s.attachRoots = make(map[codec.Hash]codec.Hash)
		}
		s.attachRoots[c.Hash] = *c.AttachmentTreeHash
	} else if s.attachRoots != nil {
		delete(s.attachRoots, c.Hash)
	}
	if c.Message != "" {
		if s.messages == nil {
			s.messages = make(map[codec.Hash]string)
		}
		s.messages[c.Hash] = c.Message
	} else if s.messages != nil {
		delete(s.messages, c.Hash)
	}
	s.setOps(c.Hash, c.Operations)
}

// replace swaps the commit held under its own hash and keeps the generation already derived for
// it, for the paths that put operations back on (or take them off) a commit whose shape has not
// changed. Reports false when the hash is not held.
func (s *nodeStore) replace(c document.Commit) bool {
	if s.goMode {
		n, ok := s.goNodes[c.Hash]
		if ok {
			n.commit = c
			s.goNodes[c.Hash] = n
		}
		return ok
	}
	if i, ok := s.index[c.Hash]; ok {
		s.put(c, recGeneration(s.at(i)))
		return true
	}
	if _, _, ok := s.segPos(c.Hash); ok {
		s.setOps(c.Hash, c.Operations)
		return true
	}
	return false
}

// newSlot hands out an empty slot. A free slot is never one a segment write is reading: slots let
// go of while frozen wait in frozenFreed.
func (s *nodeStore) newSlot() uint32 {
	// Not while frozen: a commit admitted during a flush must land above the freeze, where the
	// flush's attach looks for what stays resident (attachFlushed).
	if n := len(s.free); n > 0 && s.frozen == 0 {
		i := s.free[n-1]
		s.free = s.free[:n-1]
		return i
	}
	i := s.used
	s.used++
	s.ensureSlot(i)
	return i
}

// writable returns the slot h's record may be changed in: i itself, or - when a segment write is
// reading i - a fresh copy that the index now names.
func (s *nodeStore) writable(h codec.Hash, i uint32) uint32 {
	if i >= s.frozen {
		return i
	}
	j := s.newSlot() // may regrow the first chunk, so copy only after
	copy(s.at(j), s.at(i))
	s.index[h] = j
	s.frozenFreed = append(s.frozenFreed, i)
	return j
}

func (s *nodeStore) freeSlot(h codec.Hash, i uint32) {
	delete(s.index, h)
	if i < s.frozen {
		s.frozenFreed = append(s.frozenFreed, i)
		return
	}
	clear(s.at(i))
	s.free = append(s.free, i)
}

// freeze marks every slot handed out so far as being read by a segment write, and returns the
// chunks as they stand: the write reads its records from these, with no lock held. Nothing below
// the mark changes until unfreeze.
func (s *nodeStore) freeze() ([][]byte, uint32) {
	s.frozen = s.used
	return append([][]byte(nil), s.chunks...), s.slotsPerChunk()
}

// unfreeze ends a segment write's hold on the resident slots and frees the ones let go of
// meanwhile.
func (s *nodeStore) unfreeze() {
	if s.frozen == 0 && len(s.frozenFreed) == 0 {
		return
	}
	s.frozen = 0
	for _, i := range s.frozenFreed {
		clear(s.at(i))
		s.free = append(s.free, i)
	}
	s.frozenFreed = nil
}

func (s *nodeStore) delete(h codec.Hash) {
	if s.goMode {
		delete(s.goNodes, h)
		return
	}
	if s.recentOK && s.recent.Hash == h {
		s.recent, s.recentOK = document.Commit{}, false
	}
	if i, ok := s.index[h]; ok {
		s.freeSlot(h, i)
	}
	if b, _, ok := s.recordOf(h); ok {
		s.hide(b, h)
	}
	delete(s.messages, h)
	delete(s.ops, h)
	delete(s.extra, h)
	delete(s.attachRoots, h)
	delete(s.genOverride, h)
}

// hide marks b's record for h as one the DAG no longer holds.
func (s *nodeStore) hide(b *graphBase, h codec.Hash) {
	if s.hidden == nil {
		s.hidden = make(map[codec.Hash]struct{})
	}
	if _, already := s.hidden[h]; !already {
		s.hidden[h] = struct{}{}
		b.hiddenRecs++
	}
}

// recountHidden recomputes every segment's hidden-record count from the overlay.
func (s *nodeStore) recountHidden() {
	for _, b := range s.segs {
		b.hiddenRecs = 0
	}
	for h := range s.hidden {
		if b, _, ok := s.recordOf(h); ok {
			b.hiddenRecs++
		}
	}
}

// attachRoot is h's attachment root when the resident tier holds one, else nil.
func (s *nodeStore) attachRoot(h codec.Hash) *codec.Hash {
	r, ok := s.attachRoots[h]
	if !ok {
		return nil // before the copy below, which escapes: a miss must not allocate
	}
	out := r
	return &out
}

// commit rebuilds the document.Commit for h. It allocates the parent slice, so a path that only
// needs ancestry should use forEachParent instead.
func (s *nodeStore) commit(h codec.Hash) (document.Commit, bool) {
	if s.goMode {
		n, ok := s.goNodes[h]
		return n.commit, ok
	}
	if s.recentOK && s.recent.Hash == h && s.has(h) {
		return s.recent, true
	}
	if i, ok := s.index[h]; ok {
		r := s.at(i)
		return decodeRecord(r, s.namespaces[recNsIdx(r)], s.messages[h], s.extra[h], s.ops[h], s.attachRoot(h)), true
	}
	if b, pos, ok := s.segPos(h); ok {
		return b.commit(pos, s.ops[h]), true
	}
	return document.Commit{}, false
}

// treeHash is a commit's document tree hash without building the commit.
func (s *nodeStore) treeHash(h codec.Hash) (codec.Hash, bool) {
	if s.goMode {
		n, ok := s.goNodes[h]
		return n.commit.DocumentTreeHash, ok
	}
	if i, ok := s.index[h]; ok {
		return recTree(s.at(i)), true
	}
	if b, pos, ok := s.segPos(h); ok {
		return b.treeHash(pos), true
	}
	return codec.Hash{}, false
}

// forEachParent calls fn for each parent of h, in order. Stops early when fn returns false. A
// hash that is not held has no parents. Allocates nothing for a commit with two parents or fewer.
func (s *nodeStore) forEachParent(h codec.Hash, fn func(codec.Hash) bool) {
	var p0, p1 codec.Hash
	var n uint32
	var extra []codec.Hash
	if s.goMode {
		for _, p := range s.goNodes[h].commit.ParentHashes {
			if !fn(p) {
				return
			}
		}
		return
	}
	if i, ok := s.index[h]; ok {
		p0, p1, n = recParents01(s.at(i))
		if n > 2 {
			extra = s.extra[h]
		}
	} else if b, pos, ok := s.segPos(h); ok {
		b.forEachParent(pos, fn)
		return
	} else {
		return
	}
	if n > 0 && !fn(p0) {
		return
	}
	if n > 1 && !fn(p1) {
		return
	}
	for _, p := range extra {
		if !fn(p) {
			return
		}
	}
}

// forEach visits every commit's hash, in no particular order. fn must not add or remove commits.
func (s *nodeStore) forEach(fn func(codec.Hash)) {
	if s.goMode {
		for h := range s.goNodes {
			fn(h)
		}
		return
	}
	for h := range s.index {
		fn(h)
	}
	for _, b := range s.segs {
		for pos := 0; pos < b.count; pos++ {
			h := b.hashAt(pos)
			if _, gone := s.hidden[h]; gone {
				continue
			}
			fn(h)
		}
	}
}

// forEachHorizonCandidate visits every commit that could have a parent the DAG does not hold:
// all of the resident tier, and in each segment only the commits whose XPOS entry says a parent
// lies outside it. A parent inside the same segment is always held - unless it was deleted without
// leaving a stub, so if any hidden record is unstubbed every segment commit is visited, and the
// answer is the same either way.
func (s *nodeStore) forEachHorizonCandidate(stubbed func(codec.Hash) bool, fn func(codec.Hash)) {
	if s.goMode {
		s.forEach(fn)
		return
	}
	for h := range s.index {
		fn(h)
	}
	all := false
	for h := range s.hidden {
		if !stubbed(h) {
			all = true
			break
		}
	}
	for _, b := range s.segs {
		visit := func(pos int) {
			h := b.hashAt(pos)
			if _, gone := s.hidden[h]; !gone {
				fn(h)
			}
		}
		if all {
			for pos := 0; pos < b.count; pos++ {
				visit(pos)
			}
			continue
		}
		xp := b.externalParentPositions()
		for i := 0; i+4 <= len(xp); i += 4 {
			visit(int(le.Uint32(xp[i:])))
		}
	}
}

// beginResidentWrite freezes the resident slots for a write that reads their records in place,
// and returns how many slots the freeze covers; snapshotSlots then notes, a batch at a time,
// which of them the write takes.
func (s *nodeStore) beginResidentWrite(in *graphWriteInput) uint32 {
	in.chunks, in.per = s.freeze()
	// Sized up front: appending would grow it 1.25x at a time and allocate five times its size.
	in.resident = make([]residentEntry, 0, len(s.index))
	return s.frozen
}

// snapshotSlots notes the live commits in frozen slots [lo, hi): include's go to the write, with
// flags saying what the record alone cannot (entryHasOps, entryOwnsTx); the rest are held back.
// Unless shadowed, a commit some segment still has a (hidden) record for is held back too - two
// segments must never share a commit - and waits for the merge that drops that record; for a
// resident commit, having such a record is exactly being in hidden.
//
// The lock may be released between batches: a frozen record never changes, so a slot whose index
// entry no longer names it was freed or copied since the freeze, and the copy - in a slot above
// the freeze - stays resident for the next write. No commit admitted meanwhile is below it.
func (s *nodeStore) snapshotSlots(in *graphWriteInput, lo, hi uint32, include func(codec.Hash, []byte) bool, flags func(codec.Hash, []byte) uint32, shadowed bool) {
	for i := lo; i < hi; i++ {
		r := s.at(i)
		h := recHash(r)
		if j, ok := s.index[h]; !ok || j != i {
			continue
		}
		if _, hid := s.hidden[h]; !include(h, r) || (hid && !shadowed) {
			in.heldBack = append(in.heldBack, residentEntry{hash: h, slot: i})
			continue
		}
		in.resident = append(in.resident, residentEntry{hash: h, slot: i, flags: flags(h, r)})
		msg, extra, attach := s.messages[h], s.extra[h], s.attachRoot(h)
		if msg != "" || extra != nil || attach != nil {
			if in.side == nil {
				in.side = make(map[codec.Hash]residentSide)
			}
			in.side[h] = residentSide{msg: msg, extra: extra, attach: attach}
		}
	}
}

// residentForWrite is beginResidentWrite and every slot's snapshot, in one critical section.
func (s *nodeStore) residentForWrite(in *graphWriteInput, include func(codec.Hash, []byte) bool, flags func(codec.Hash, []byte) uint32, shadowed bool) {
	s.snapshotSlots(in, 0, s.beginResidentWrite(in), include, flags, shadowed)
}

// sortResident orders a write's resident entries by hash, as the format stores them. Run without
// the lock.
func (in *graphWriteInput) sortResident() {
	sort.Slice(in.resident, func(a, b int) bool {
		return bytes.Compare(in.resident[a].hash.Bytes[:], in.resident[b].hash.Bytes[:]) < 0
	})
}

// segmentsForWrite adds inputs (oldest first) to a merge: a reference on each mapping, held until
// the write releases it, and the part of the overlay - hidden records, generation overrides -
// that falls in them.
func (s *nodeStore) segmentsForWrite(in *graphWriteInput, inputs []*graphBase) {
	all := len(inputs) == len(s.segs)
	for _, b := range inputs {
		r, err := b.mapping.Acquire()
		if err != nil {
			continue
		}
		in.inputs = append(in.inputs, b)
		in.releases = append(in.releases, r.Release)
	}
	inInputs := func(h codec.Hash) bool {
		if all {
			return true
		}
		key := bloomKeyHash(&h)
		for _, b := range in.inputs {
			if b.bloom.has(key) {
				if _, ok := b.find(h); ok {
					return true
				}
			}
		}
		return false
	}
	in.hidden = make(map[codec.Hash]struct{})
	for h := range s.hidden {
		if inInputs(h) {
			in.hidden[h] = struct{}{}
		}
	}
	in.genOverride = make(map[codec.Hash]uint32)
	for h, g := range s.genOverride {
		if inInputs(h) {
			in.genOverride[h] = g
		}
	}
}

// attachFlushed adds nb, written from exactly in's resident entries (sorted, so entry i is record
// i), as the newest segment, and ends the freeze. The flushed commits leave the resident tier; one
// rewritten while the flush ran (a new generation, a re-put) keeps what changed as an override,
// and one deleted meanwhile becomes a hidden record.
//
// O(what stays resident), not O(what was flushed): the resident tier is rebuilt from the commits
// the flush held back and those admitted or rewritten since the freeze - all above it - instead of
// the flushed ones being deleted one at a time under the DAG's write lock. Reports false, changing
// nothing, if nb is not what the entries describe.
func (s *nodeStore) attachFlushed(nb *graphBase, in *graphWriteInput) bool {
	entries := in.resident
	if nb.count != len(entries) || (nb.count > 0 && (nb.hashAt(0) != entries[0].hash || nb.hashAt(nb.count-1) != entries[nb.count-1].hash)) {
		return false
	}
	flushed := func(h codec.Hash) (int, bool) {
		i := sort.Search(len(entries), func(i int) bool { return bytes.Compare(entries[i].hash.Bytes[:], h.Bytes[:]) >= 0 })
		return i, i < len(entries) && entries[i].hash == h
	}
	frozen := s.frozen
	// Flushed commits whose frozen slot was let go of: deleted (hidden now), or rewritten into a
	// slot above the freeze (found below). A frozen record keeps its bytes until the freeze ends.
	for _, i := range s.frozenFreed {
		h := recHash(s.at(i))
		if pos, ok := flushed(h); ok && entries[pos].slot == i {
			if _, still := s.index[h]; !still {
				s.hide(nb, h)
			}
		}
	}
	setOverride := func(h codec.Hash, g uint32) {
		if s.genOverride == nil {
			s.genOverride = make(map[codec.Hash]uint32)
		}
		s.genOverride[h] = g
	}
	old := *s
	*s = nodeStore{
		index: make(map[codec.Hash]uint32, len(in.heldBack)+int(old.used-frozen)), chunkSlots: old.chunkSlots,
		namespaces: old.namespaces, ops: old.ops,
		segs: append([]*graphBase{nb}, old.segs...), hidden: old.hidden, genOverride: old.genOverride,
		segRecords: old.segRecords + nb.count, genEpoch: old.genEpoch, recent: old.recent, recentOK: old.recentOK,
	}
	keep := func(h codec.Hash, from uint32) {
		j := s.newSlot()
		copy(s.at(j), old.at(from))
		s.index[h] = j
		if m, ok := old.messages[h]; ok {
			if s.messages == nil {
				s.messages = make(map[codec.Hash]string)
			}
			s.messages[h] = m
		}
		if x, ok := old.extra[h]; ok {
			if s.extra == nil {
				s.extra = make(map[codec.Hash][]codec.Hash)
			}
			s.extra[h] = x
		}
		if a, ok := old.attachRoots[h]; ok {
			if s.attachRoots == nil {
				s.attachRoots = make(map[codec.Hash]codec.Hash)
			}
			s.attachRoots[h] = a
		}
	}
	for _, e := range in.heldBack {
		if i, ok := old.index[e.hash]; ok && i == e.slot {
			keep(e.hash, i)
		}
	}
	for i := frozen; i < old.used; i++ {
		h := recHash(old.at(i))
		if j, ok := old.index[h]; !ok || j != i {
			continue // free
		}
		if pos, ok := flushed(h); ok {
			// Rewritten while the flush ran: nb has the commit; keep its newer generation.
			if g := recGeneration(old.at(i)); g != nb.generation(pos) {
				setOverride(h, g)
			}
			continue
		}
		keep(h, i)
	}
	return true
}

// attachMerged replaces the segments in replaced with nb, their merge, in time proportional to
// the overlay rather than to the records: nb holds exactly the records of replaced that were
// visible when the merge began, so only what changed since - commits deleted (they stay hidden,
// now in nb) and generations recomputed (epoch says whether any were) - needs reconciling. Reports
// false, changing nothing, when it cannot be sure of that; the caller then runs attach, which
// checks every record.
func (s *nodeStore) attachMerged(nb *graphBase, replaced []*graphBase, epoch uint64) bool {
	if s.genEpoch != epoch {
		return false
	}
	isReplaced := func(b *graphBase) bool {
		for _, r := range replaced {
			if r == b {
				return true
			}
		}
		return false
	}
	var kept []*graphBase
	records := nb.count
	for _, b := range s.segs {
		if !isReplaced(b) {
			kept = append(kept, b)
			records += b.count
		}
	}
	if len(kept)+len(replaced) != len(s.segs) {
		return false
	}
	inKept := func(h codec.Hash) bool {
		key := bloomKeyHash(&h)
		for _, b := range kept {
			if b.bloom.has(key) {
				if _, ok := b.find(h); ok {
					return true
				}
			}
		}
		return false
	}
	hidden := make(map[codec.Hash]struct{}, len(s.hidden))
	nbHidden := 0
	for h := range s.hidden {
		if inKept(h) {
			hidden[h] = struct{}{}
		} else if nb.mayHold(&h) {
			if _, ok := nb.find(h); ok {
				hidden[h] = struct{}{} // deleted while the merge ran
				nbHidden++
			}
		}
		// Otherwise it was hidden when the merge began, and the merge dropped it.
	}
	override := make(map[codec.Hash]uint32, len(s.genOverride))
	for h, g := range s.genOverride {
		if inKept(h) {
			override[h] = g
		} else if pos, ok := nb.find(h); ok && nb.generation(pos) != g {
			override[h] = g
		}
	}
	// Every commit the segments showed, they still show.
	if records-len(hidden) != s.segVisible() {
		return false
	}
	nb.hiddenRecs = nbHidden
	s.segs = append([]*graphBase{nb}, kept...)
	s.hidden = hidden
	s.genOverride = override
	s.segRecords = records
	for _, b := range replaced {
		b.close()
	}
	return true
}

// attach adds nb as the newest segment, replacing the segments in replaced (all of them, for a
// full rewrite; none, for a flush or an open). Every resident commit nb holds moves out of the
// resident tier; every nb record the DAG does not hold is hidden.
func (s *nodeStore) attach(nb *graphBase, replaced []*graphBase) {
	isReplaced := func(b *graphBase) bool {
		for _, r := range replaced {
			if r == b {
				return true
			}
		}
		return false
	}
	var kept []*graphBase
	for _, b := range s.segs {
		if !isReplaced(b) {
			kept = append(kept, b)
		}
	}
	// What the DAG shows from a replaced segment, before anything changes.
	replacedVisible := func(h codec.Hash) (uint32, bool) {
		if _, gone := s.hidden[h]; gone {
			return 0, false
		}
		for _, b := range replaced {
			if pos, ok := b.find(h); ok {
				if g, ok := s.genOverride[h]; ok {
					return g, true
				}
				return b.generation(pos), true
			}
		}
		return 0, false
	}
	inKept := func(h codec.Hash) bool {
		for _, b := range kept {
			if _, ok := b.find(h); ok {
				return true
			}
		}
		return false
	}

	hidden := make(map[codec.Hash]struct{})
	override := make(map[codec.Hash]uint32)
	for h := range s.hidden {
		if inKept(h) {
			hidden[h] = struct{}{}
		}
	}
	for h, g := range s.genOverride {
		if inKept(h) {
			override[h] = g
		}
	}
	setOverride := func(h codec.Hash, recorded, gen uint32) {
		if gen != recorded {
			override[h] = gen
		}
	}
	for pos := 0; pos < nb.count; pos++ {
		h := nb.hashAt(pos)
		fileGen := nb.generation(pos)
		if i, ok := s.index[h]; ok {
			setOverride(h, fileGen, recGeneration(s.at(i)))
			s.freeSlot(h, i)
			delete(s.messages, h)
			delete(s.extra, h)
			delete(s.attachRoots, h)
			continue
		}
		if gen, ok := replacedVisible(h); ok {
			setOverride(h, fileGen, gen)
			continue
		}
		hidden[h] = struct{}{}
	}
	// A commit a replaced segment showed must survive. A full rewrite writes every one of them, so
	// this finds nothing - but if it ever does, the commit goes back to the resident tier rather
	// than vanishing, which is the whole of "never an authority".
	for _, b := range replaced {
		for pos := 0; pos < b.count; pos++ {
			h := b.hashAt(pos)
			gen, ok := replacedVisible(h)
			if !ok {
				continue
			}
			if _, ok := nb.find(h); ok {
				continue
			}
			c := b.commit(pos, s.ops[h])
			saved := s.segs
			s.segs = nil // so put admits it to the resident tier
			s.put(c, gen)
			s.segs = saved
		}
	}

	s.segs = append([]*graphBase{nb}, kept...)
	s.hidden = hidden
	s.genOverride = override
	s.segRecords = 0
	for _, b := range s.segs {
		s.segRecords += b.count
	}
	for _, b := range replaced {
		b.close()
	}
	s.recountHidden()
	s.compactSlab()
	// A Go map never gives back its buckets, so an index that held a million commits before the
	// attach moved them out would keep costing what it cost then. Copying is O(what is left).
	s.index = shrunk(s.index)
	s.messages = shrunk(s.messages)
	s.extra = shrunk(s.extra)
	s.attachRoots = shrunk(s.attachRoots)
}

// shrunk returns m in a map sized for what it holds now.
func shrunk[K comparable, V any](m map[K]V) map[K]V {
	if m == nil {
		return nil
	}
	out := make(map[K]V, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// compactSlab gives back the chunks an attach emptied. Slots are renumbered, so this may only
// run where nothing holds a slot index - which, under the DAG's write lock, is everywhere.
func (s *nodeStore) compactSlab() {
	if s.frozen > 0 || uint32(len(s.free)) < s.slotsPerChunk() {
		return // a segment write is reading slots in place: numbers must not move under it
	}
	old := *s
	s.chunks, s.used, s.free = nil, 0, nil
	for h, i := range old.index {
		j := s.used
		s.used++
		s.ensureSlot(j)
		copy(s.at(j), old.at(i))
		s.index[h] = j
	}
}

// segmentOpsEvicted reports a segment commit whose record says it wrote operations and which has
// none resident - so reading them means going to the loader. Resident commits answer through the
// DAG's eviction set instead.
func (s *nodeStore) segmentOpsEvicted(h codec.Hash) bool {
	if s.goMode {
		return false
	}
	if _, resident := s.index[h]; resident {
		return false
	}
	b, pos, ok := s.segPos(h)
	if !ok || !b.hasOps(pos) {
		return false
	}
	return len(s.ops[h]) == 0
}

// attachTrusted installs segs (newest first) as the segment tier exactly as a checkpoint
// describes them: every record visible except hidden ones. For an open that restores from a
// checkpoint which references these segments instead of carrying their commits - the checkpoint,
// not the DAG, is what vouches for them, so nothing is hidden for being absent from the DAG.
// Any resident commit a segment also holds (the genesis commit a new DAG starts with) leaves the
// resident tier, keeping one commit in one place.
func (s *nodeStore) attachTrusted(segs []*graphBase, hidden []codec.Hash) {
	s.segs = segs
	s.hidden = make(map[codec.Hash]struct{}, len(hidden))
	for _, h := range hidden {
		if s.inAnySegment(h) {
			s.hidden[h] = struct{}{}
		}
	}
	s.genOverride = nil
	s.segRecords = 0
	for _, b := range segs {
		s.segRecords += b.count
	}
	for h, i := range s.index {
		if _, _, ok := s.segPos(h); ok {
			s.freeSlot(h, i)
			delete(s.messages, h)
			delete(s.extra, h)
			delete(s.attachRoots, h)
		}
	}
	s.recountHidden()
}

// detach drops every segment, putting nothing back: for a DAG that is closing.
func (s *nodeStore) detach() {
	for _, b := range s.segs {
		b.close()
	}
	s.segs, s.hidden, s.genOverride, s.segRecords = nil, nil, nil, 0
}
