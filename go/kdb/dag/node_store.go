package dag

import (
	"bytes"
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// nodeStore holds the DAG's commits in a form the garbage collector does not have to scan, in
// two tiers.
//
// The previous representation was map[codec.Hash]commitNode with a document.Commit as the
// value: a slice of parents, three strings, a slice of operations and a pointer to a schema hash
// per commit. Every one of those is a pointer the collector traces on every cycle, and the whole
// thing measured 438-523 bytes per commit (docs/kdb-mmap-plan.md, Phase 0).
//
// The resident tier keeps the same information in two pointer-free structures:
//
//   - index: hash -> slot, a map whose key and value contain no pointers, which Go does not scan
//   - slab:  fixed-width slot records in 4096-slot chunks, likewise not scanned
//
// The base tier, when a graph file is attached, is that file mapped: the same records on disk,
// costing page cache rather than heap. A commit lives in exactly one tier - attach moves commits
// out of the resident tier, and put never adds a commit the base already shows - except that a
// base record can be hidden: deleted from the DAG since the file was written, or never in this
// DAG at all. hidden is what keeps the file from ever being an authority.
//
// Anything that genuinely is a pointer lives in a side map holding only the commits that need it:
// messages and extra parents of resident commits (the base carries its own), and operations
// (bounded by the DAG's operations budget) for both tiers.
//
// Not safe for concurrent use; the owning InMemoryCommitDag's mu guards it exactly as it guarded
// the map this replaces. Every read copies out of the mapping before the lock is released, which
// is what lets a rebuild swap the base under the write lock without counting readers.
type nodeStore struct {
	index map[codec.Hash]uint32
	// chunks is the slab, in fixed-size blocks rather than one growing slice: a single slice
	// doubles its capacity as it fills, so up to half of it is empty, and every doubling copies
	// the whole thing at the moment the DAG is busiest admitting commits. The chunk table itself
	// holds a pointer per slotsPerChunk commits, which is negligible next to what it replaces.
	chunks []*[slotsPerChunk]slot
	used   uint32 // slots ever handed out; free holds the ones since returned
	free   []uint32

	// namespaces interns Commit.NamespaceID. A DAG holds one namespace, so this is almost always
	// a single entry; it is a table rather than a field only so a commit that names a different
	// namespace still round-trips exactly.
	namespaces []string

	messages map[codec.Hash]string
	ops      map[codec.Hash][]document.Op
	extra    map[codec.Hash][]codec.Hash // parents beyond the second

	base *graphBase
	// hidden is base records the DAG does not hold. Every entry names a base record.
	hidden map[codec.Hash]struct{}
	// genOverride is a base commit's generation where the DAG's differs from the file's - a
	// recompute after the file was written. Pointer-free, like hidden.
	genOverride map[codec.Hash]uint32
}

// slot is one commit's fixed-width record. It must stay free of pointers: that is the whole
// point of it, and TestSlotHasNoPointers pins it.
type slot struct {
	hash       codec.Hash
	parent0    codec.Hash
	parent1    codec.Hash
	schema     codec.Hash
	tree       codec.Hash
	tx         codec.UUID
	author     codec.UUID
	epochMs    int64
	micro      int64
	generation uint32
	nParents   uint32
	ns         uint16
	hasSchema  bool
	live       bool
}

const slotsPerChunk = 4096

func (s *nodeStore) at(i uint32) *slot {
	return &s.chunks[i/slotsPerChunk][i%slotsPerChunk]
}

func newNodeStore() nodeStore {
	return nodeStore{index: make(map[codec.Hash]uint32)}
}

// basePos is h's position in the base, if the base shows it.
func (s *nodeStore) basePos(h codec.Hash) (int, bool) {
	if s.base == nil {
		return 0, false
	}
	pos, ok := s.base.find(h)
	if !ok {
		return 0, false
	}
	if _, gone := s.hidden[h]; gone {
		return 0, false
	}
	return pos, true
}

// residentLen is how many commits the resident tier holds - what the rebuild threshold counts.
func (s *nodeStore) residentLen() int { return len(s.index) }

func (s *nodeStore) len() int {
	n := len(s.index)
	if s.base != nil {
		n += s.base.count - len(s.hidden)
	}
	return n
}

func (s *nodeStore) has(h codec.Hash) bool {
	if _, ok := s.index[h]; ok {
		return true
	}
	_, ok := s.basePos(h)
	return ok
}

func (s *nodeStore) generation(h codec.Hash) uint32 {
	if i, ok := s.index[h]; ok {
		return s.at(i).generation
	}
	if pos, ok := s.basePos(h); ok {
		if g, ok := s.genOverride[h]; ok {
			return g
		}
		return s.base.generation(pos)
	}
	return 0
}

func (s *nodeStore) setGeneration(h codec.Hash, gen uint32) {
	if i, ok := s.index[h]; ok {
		s.at(i).generation = gen
		return
	}
	if pos, ok := s.basePos(h); ok {
		s.setBaseGeneration(h, pos, gen)
	}
}

func (s *nodeStore) setBaseGeneration(h codec.Hash, pos int, gen uint32) {
	if s.base.generation(pos) == gen {
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

func (s *nodeStore) setOps(h codec.Hash, ops []document.Op) {
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
	if _, resident := s.index[c.Hash]; !resident {
		if pos, ok := s.basePos(c.Hash); ok {
			// Already in the base. A commit's hash covers everything the file carries, so the
			// only things that can differ are the ones held beside it.
			s.setOps(c.Hash, c.Operations)
			s.setBaseGeneration(c.Hash, pos, gen)
			return
		}
	}
	i, ok := s.index[c.Hash]
	if !ok {
		if n := len(s.free); n > 0 {
			i = s.free[n-1]
			s.free = s.free[:n-1]
		} else {
			i = s.used
			s.used++
			if int(i/slotsPerChunk) == len(s.chunks) {
				s.chunks = append(s.chunks, new([slotsPerChunk]slot))
			}
		}
		s.index[c.Hash] = i
	}
	sl := slot{
		hash:       c.Hash,
		tree:       c.DocumentTreeHash,
		tx:         c.TransactionID,
		author:     c.AuthorNodeID,
		epochMs:    c.Timestamp.EpochMillis,
		micro:      int64(c.Timestamp.MicroRemainder),
		generation: gen,
		nParents:   uint32(len(c.ParentHashes)),
		ns:         s.internNamespace(c.NamespaceID),
		live:       true,
	}
	if len(c.ParentHashes) > 0 {
		sl.parent0 = c.ParentHashes[0]
	}
	if len(c.ParentHashes) > 1 {
		sl.parent1 = c.ParentHashes[1]
	}
	if c.SchemaHash != nil {
		sl.hasSchema = true
		sl.schema = *c.SchemaHash
	}
	*s.at(i) = sl

	if len(c.ParentHashes) > 2 {
		if s.extra == nil {
			s.extra = make(map[codec.Hash][]codec.Hash)
		}
		s.extra[c.Hash] = append([]codec.Hash(nil), c.ParentHashes[2:]...)
	} else if s.extra != nil {
		delete(s.extra, c.Hash)
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
	if i, ok := s.index[c.Hash]; ok {
		s.put(c, s.at(i).generation)
		return true
	}
	if _, ok := s.basePos(c.Hash); ok {
		s.setOps(c.Hash, c.Operations)
		return true
	}
	return false
}

func (s *nodeStore) delete(h codec.Hash) {
	if i, ok := s.index[h]; ok {
		delete(s.index, h)
		*s.at(i) = slot{}
		s.free = append(s.free, i)
	}
	if s.base != nil {
		if _, inBase := s.base.find(h); inBase {
			if s.hidden == nil {
				s.hidden = make(map[codec.Hash]struct{})
			}
			s.hidden[h] = struct{}{}
		}
	}
	delete(s.messages, h)
	delete(s.ops, h)
	delete(s.extra, h)
	delete(s.genOverride, h)
}

func (s *nodeStore) commitFromSlot(sl *slot, ns, msg string, extra []codec.Hash, ops []document.Op) document.Commit {
	c := document.Commit{
		Hash:             sl.hash,
		NamespaceID:      ns,
		TransactionID:    sl.tx,
		Timestamp:        codec.Timestamp{EpochMillis: sl.epochMs, MicroRemainder: int(sl.micro)},
		AuthorNodeID:     sl.author,
		DocumentTreeHash: sl.tree,
		Message:          msg,
		Operations:       ops,
	}
	if sl.nParents > 0 {
		c.ParentHashes = make([]codec.Hash, 0, sl.nParents)
		c.ParentHashes = append(c.ParentHashes, sl.parent0)
		if sl.nParents > 1 {
			c.ParentHashes = append(c.ParentHashes, sl.parent1)
		}
		if sl.nParents > 2 {
			c.ParentHashes = append(c.ParentHashes, extra...)
		}
	}
	if sl.hasSchema {
		sc := sl.schema
		c.SchemaHash = &sc
	}
	return c
}

// commit rebuilds the document.Commit for h. It allocates the parent slice, so a path that only
// needs ancestry should use forEachParent instead.
func (s *nodeStore) commit(h codec.Hash) (document.Commit, bool) {
	if i, ok := s.index[h]; ok {
		sl := s.at(i)
		return s.commitFromSlot(sl, s.namespaces[sl.ns], s.messages[h], s.extra[h], s.ops[h]), true
	}
	if pos, ok := s.basePos(h); ok {
		sl, msg, extra := s.base.slotAt(pos)
		return s.commitFromSlot(&sl, s.base.ns, msg, extra, s.ops[h]), true
	}
	return document.Commit{}, false
}

// treeHash is a commit's document tree hash without building the commit.
func (s *nodeStore) treeHash(h codec.Hash) (codec.Hash, bool) {
	if i, ok := s.index[h]; ok {
		return s.at(i).tree, true
	}
	if pos, ok := s.basePos(h); ok {
		return s.base.treeHash(pos), true
	}
	return codec.Hash{}, false
}

// forEachParent calls fn for each parent of h, in order. Stops early when fn returns false. A
// hash that is not held has no parents. Allocates nothing for a resident commit or a base commit
// with two parents or fewer.
func (s *nodeStore) forEachParent(h codec.Hash, fn func(codec.Hash) bool) {
	var p0, p1 codec.Hash
	var n uint32
	var extra []codec.Hash
	if i, ok := s.index[h]; ok {
		sl := s.at(i)
		p0, p1, n = sl.parent0, sl.parent1, sl.nParents
		if n > 2 {
			extra = s.extra[h]
		}
	} else if pos, ok := s.basePos(h); ok {
		r := s.base.rec(pos)
		copy(p0.Bytes[:], r[grParent0:grParent0+32])
		copy(p1.Bytes[:], r[grParent1:grParent1+32])
		n = leU32(r[grNParents:])
		if n > 2 {
			_, _, extra = s.base.slotAt(pos)
		}
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

func leU32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

// forEach visits every commit's hash, in no particular order. fn must not add or remove commits.
func (s *nodeStore) forEach(fn func(codec.Hash)) {
	for h := range s.index {
		fn(h)
	}
	if s.base == nil {
		return
	}
	for pos := 0; pos < s.base.count; pos++ {
		h := s.base.hashAt(pos)
		if _, gone := s.hidden[h]; gone {
			continue
		}
		fn(h)
	}
}

// graphSnapshot is what a rebuild needs to write a new file without holding the DAG's lock: the
// resident commits destined for the file, sorted, and the base as it stood, kept alive by its own
// reference on the mapping.
type graphSnapshot struct {
	resident    []graphSource
	residentTx  []graphTx
	base        *graphBase // nil when there was none
	baseRegion  func()     // releases the snapshot's reference on base
	hidden      map[codec.Hash]struct{}
	genOverride map[codec.Hash]uint32
}

// snapshotForFile captures the resident commits include accepts, and a reference on the base.
// txOf names the transaction-index entry a commit owns (first writer wins), if it owns one.
func (s *nodeStore) snapshotForFile(include func(codec.Hash, *slot) bool, txOf func(codec.Hash, codec.UUID) bool) graphSnapshot {
	var snap graphSnapshot
	for h, i := range s.index {
		sl := s.at(i)
		if !include(h, sl) {
			continue
		}
		snap.resident = append(snap.resident, graphSource{s: *sl, msg: s.messages[h], extra: s.extra[h]})
		if txOf(h, sl.tx) {
			snap.residentTx = append(snap.residentTx, graphTx{key: txKey(sl.tx), hash: h})
		}
	}
	sort.Slice(snap.resident, func(a, b int) bool {
		return bytes.Compare(snap.resident[a].s.hash.Bytes[:], snap.resident[b].s.hash.Bytes[:]) < 0
	})
	sort.Slice(snap.residentTx, func(a, b int) bool {
		return bytes.Compare(snap.residentTx[a].key[:], snap.residentTx[b].key[:]) < 0
	})
	if s.base != nil {
		if r, err := s.base.mapping.Acquire(); err == nil {
			snap.base = s.base
			snap.baseRegion = r.Release
			snap.hidden = make(map[codec.Hash]struct{}, len(s.hidden))
			for h := range s.hidden {
				snap.hidden[h] = struct{}{}
			}
			snap.genOverride = make(map[codec.Hash]uint32, len(s.genOverride))
			for h, g := range s.genOverride {
				snap.genOverride[h] = g
			}
		}
	}
	return snap
}

func (snap *graphSnapshot) release() {
	if snap.baseRegion != nil {
		snap.baseRegion()
		snap.baseRegion = nil
	}
}

// sources merges the old base's visible records with the resident snapshot, in hash order.
func (snap *graphSnapshot) sources() (func() (graphSource, bool), func() (graphTx, bool)) {
	bi, ri := 0, 0
	nextBase := func() (graphSource, bool) {
		for b := snap.base; b != nil && bi < b.count; {
			pos := bi
			bi++
			h := b.hashAt(pos)
			if _, gone := snap.hidden[h]; gone {
				continue
			}
			sl, msg, extra := b.slotAt(pos)
			if g, ok := snap.genOverride[h]; ok {
				sl.generation = g
			}
			return graphSource{s: sl, msg: msg, extra: extra}, true
		}
		return graphSource{}, false
	}
	var pendBase *graphSource
	next := func() (graphSource, bool) {
		if pendBase == nil {
			if src, ok := nextBase(); ok {
				pendBase = &src
			}
		}
		haveRes := ri < len(snap.resident)
		switch {
		case pendBase == nil && !haveRes:
			return graphSource{}, false
		case pendBase == nil:
			ri++
			return snap.resident[ri-1], true
		case !haveRes:
			out := *pendBase
			pendBase = nil
			return out, true
		}
		c := bytes.Compare(pendBase.s.hash.Bytes[:], snap.resident[ri].s.hash.Bytes[:])
		if c < 0 {
			out := *pendBase
			pendBase = nil
			return out, true
		}
		if c == 0 {
			// Cannot happen while a commit lives in one tier, but the resident copy is the newer.
			pendBase = nil
		}
		ri++
		return snap.resident[ri-1], true
	}

	ti, rti := 0, 0
	nextBaseTx := func() (graphTx, bool) {
		for b := snap.base; b != nil && ti < b.txCount; {
			e := b.tidx[ti*graphTxEntrySize : (ti+1)*graphTxEntrySize]
			ti++
			var t graphTx
			copy(t.key[:], e[:16])
			copy(t.hash.Bytes[:], e[16:48])
			if _, gone := snap.hidden[t.hash]; gone {
				continue
			}
			return t, true
		}
		return graphTx{}, false
	}
	var pendTx *graphTx
	nextTx := func() (graphTx, bool) {
		if pendTx == nil {
			if t, ok := nextBaseTx(); ok {
				pendTx = &t
			}
		}
		haveRes := rti < len(snap.residentTx)
		switch {
		case pendTx == nil && !haveRes:
			return graphTx{}, false
		case pendTx == nil:
			rti++
			return snap.residentTx[rti-1], true
		case !haveRes:
			out := *pendTx
			pendTx = nil
			return out, true
		}
		if bytes.Compare(pendTx.key[:], snap.residentTx[rti].key[:]) <= 0 {
			// On a tie the base's entry is the older, so it wins - as it does in txIndex.
			out := *pendTx
			pendTx = nil
			return out, true
		}
		rti++
		return snap.residentTx[rti-1], true
	}
	return next, nextTx
}

// attach makes nb the base. Every resident commit the file holds moves out of the resident tier;
// every file record the DAG does not hold is hidden. The old base, if any, is closed: its last
// reference is dropped by whichever rebuild snapshot still holds it.
func (s *nodeStore) attach(nb *graphBase) {
	old := s.base
	oldVisible := func(h codec.Hash) bool {
		if old == nil {
			return false
		}
		if _, ok := old.find(h); !ok {
			return false
		}
		_, gone := s.hidden[h]
		return !gone
	}
	oldOverride := s.genOverride
	hidden := make(map[codec.Hash]struct{})
	var override map[codec.Hash]uint32
	setOverride := func(h codec.Hash, fileGen, gen uint32) {
		if gen == fileGen {
			return
		}
		if override == nil {
			override = make(map[codec.Hash]uint32)
		}
		override[h] = gen
	}
	for pos := 0; pos < nb.count; pos++ {
		h := nb.hashAt(pos)
		fileGen := nb.generation(pos)
		if i, ok := s.index[h]; ok {
			setOverride(h, fileGen, s.at(i).generation)
			delete(s.index, h)
			*s.at(i) = slot{}
			s.free = append(s.free, i)
			delete(s.messages, h)
			delete(s.extra, h)
			continue
		}
		if oldVisible(h) {
			gen := old.generation(mustFind(old, h))
			if g, ok := oldOverride[h]; ok {
				gen = g
			}
			setOverride(h, fileGen, gen)
			continue
		}
		hidden[h] = struct{}{}
	}
	// A commit the old base showed must survive into the new one. A rebuild writes every one of
	// them, so this finds nothing - but if it ever does, the commit goes back to the resident
	// tier rather than vanishing, which is the whole of "never an authority".
	if old != nil {
		for pos := 0; pos < old.count; pos++ {
			h := old.hashAt(pos)
			if !oldVisible(h) {
				continue
			}
			if _, ok := nb.find(h); ok {
				continue
			}
			sl, msg, extra := old.slotAt(pos)
			if g, ok := oldOverride[h]; ok {
				sl.generation = g
			}
			c := s.commitFromSlot(&sl, old.ns, msg, extra, s.ops[h])
			s.base = nil // so put admits it to the resident tier
			s.put(c, sl.generation)
		}
	}
	s.base = nb
	s.hidden = hidden
	s.genOverride = override
	if old != nil {
		old.close()
	}
	s.compactSlab()
	// A Go map never gives back its buckets, so an index that held a million commits before the
	// attach moved them out would keep costing what it cost then. Copying it is O(what is left).
	s.index = shrunk(s.index)
	s.messages = shrunk(s.messages)
	s.extra = shrunk(s.extra)
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

func mustFind(b *graphBase, h codec.Hash) int {
	pos, _ := b.find(h)
	return pos
}

// compactSlab gives back the chunks an attach emptied. Slots are renumbered, so this may only
// run where nothing holds a slot index - which, under the DAG's write lock, is everywhere.
func (s *nodeStore) compactSlab() {
	if len(s.free) < slotsPerChunk {
		return
	}
	var chunks []*[slotsPerChunk]slot
	var used uint32
	for h, i := range s.index {
		j := used
		used++
		if int(j/slotsPerChunk) == len(chunks) {
			chunks = append(chunks, new([slotsPerChunk]slot))
		}
		chunks[j/slotsPerChunk][j%slotsPerChunk] = *s.at(i)
		s.index[h] = j
	}
	s.chunks, s.used, s.free = chunks, used, nil
}

// detach drops the base, putting nothing back: for a DAG that is closing.
func (s *nodeStore) detach() {
	if s.base != nil {
		s.base.close()
		s.base = nil
	}
	s.hidden, s.genOverride = nil, nil
}
