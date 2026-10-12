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
// what keeps the files from ever being an authority. A full rewrite drops hidden records for good,
// the way a Cassandra compaction drops tombstoned rows.
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
	chunks []*[slotsPerChunk * graphRecordSize]byte
	used   uint32 // slots ever handed out; free holds the ones since returned
	free   []uint32

	// namespaces interns Commit.NamespaceID for the resident tier (a record's ns index). A DAG
	// holds one namespace, so this is almost always a single entry; it is a table only so a
	// commit that names a different namespace still round-trips exactly.
	namespaces []string

	messages map[codec.Hash]string
	ops      map[codec.Hash][]document.Op
	extra    map[codec.Hash][]codec.Hash // parents beyond the second

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
	*s = newNodeStore()
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

const slotsPerChunk = 4096

func (s *nodeStore) at(i uint32) []byte {
	c := s.chunks[i/slotsPerChunk]
	off := int(i%slotsPerChunk) * graphRecordSize
	return c[off : off+graphRecordSize]
}

func newNodeStore() nodeStore {
	return nodeStore{index: make(map[codec.Hash]uint32)}
}

// segPos is where the segment tier shows h, if it does.
func (s *nodeStore) segPos(h codec.Hash) (*graphBase, int, bool) {
	if len(s.segs) == 0 {
		return nil, 0, false
	}
	if _, gone := s.hidden[h]; gone {
		return nil, 0, false
	}
	for _, b := range s.segs {
		if pos, ok := b.find(h); ok {
			return b, pos, true
		}
	}
	return nil, 0, false
}

// inAnySegment reports whether any segment has a record for h, hidden or not.
func (s *nodeStore) inAnySegment(h codec.Hash) bool {
	for _, b := range s.segs {
		if _, ok := b.find(h); ok {
			return true
		}
	}
	return false
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
		return recGeneration(b.rec(pos))
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
		setRecGeneration(s.at(i), gen)
		return
	}
	if b, pos, ok := s.segPos(h); ok {
		s.setSegGeneration(h, recGeneration(b.rec(pos)), gen)
	}
}

func (s *nodeStore) setSegGeneration(h codec.Hash, recorded, gen uint32) {
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
	if s.goMode {
		s.goNodes[c.Hash] = goNode{commit: c, generation: gen}
		return
	}
	if _, resident := s.index[c.Hash]; !resident {
		if b, pos, ok := s.segPos(c.Hash); ok {
			// Already on disk. A commit's hash covers everything a record carries, so the only
			// things that can differ are the ones held beside it.
			s.setOps(c.Hash, c.Operations)
			s.setSegGeneration(c.Hash, recGeneration(b.rec(pos)), gen)
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
				s.chunks = append(s.chunks, new([slotsPerChunk * graphRecordSize]byte))
			}
		}
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

func (s *nodeStore) freeSlot(h codec.Hash, i uint32) {
	delete(s.index, h)
	clear(s.at(i))
	s.free = append(s.free, i)
}

func (s *nodeStore) delete(h codec.Hash) {
	if s.goMode {
		delete(s.goNodes, h)
		return
	}
	if i, ok := s.index[h]; ok {
		s.freeSlot(h, i)
	}
	if s.inAnySegment(h) {
		if s.hidden == nil {
			s.hidden = make(map[codec.Hash]struct{})
		}
		s.hidden[h] = struct{}{}
	}
	delete(s.messages, h)
	delete(s.ops, h)
	delete(s.extra, h)
	delete(s.genOverride, h)
}

// commit rebuilds the document.Commit for h. It allocates the parent slice, so a path that only
// needs ancestry should use forEachParent instead.
func (s *nodeStore) commit(h codec.Hash) (document.Commit, bool) {
	if s.goMode {
		n, ok := s.goNodes[h]
		return n.commit, ok
	}
	if i, ok := s.index[h]; ok {
		r := s.at(i)
		return decodeRecord(r, s.namespaces[recNsIdx(r)], s.messages[h], s.extra[h], s.ops[h]), true
	}
	if b, pos, ok := s.segPos(h); ok {
		return decodeRecord(b.rec(pos), b.ns, b.messageAt(pos), b.extraAt(pos), s.ops[h]), true
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
		return recTree(b.rec(pos)), true
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
		p0, p1, n = recParents01(b.rec(pos))
		if n > 2 {
			extra = b.extraAt(pos)
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

// graphSnapshot is what a write needs to produce a segment without holding the DAG's lock: the
// resident commits destined for it, sorted, and - for a full rewrite - the segments as they
// stood, each kept alive by the snapshot's own reference on its mapping.
type graphSnapshot struct {
	resident    []graphSource
	residentTx  []graphTx
	segs        []*graphBase // oldest first; nil for a flush
	releases    []func()
	hidden      map[codec.Hash]struct{}
	genOverride map[codec.Hash]uint32
}

// snapshotForSegment captures the resident commits include accepts. A full snapshot also takes a
// reference on every segment and copies the overlay that says which of their records still count.
// txOf names the transaction-index entry a commit owns (first writer wins), if it owns one.
func (s *nodeStore) snapshotForSegment(full bool, include func(codec.Hash, []byte) bool, txOf func(codec.Hash, codec.UUID) bool, hasOps func(codec.Hash) bool) graphSnapshot {
	var snap graphSnapshot
	for h, i := range s.index {
		r := s.at(i)
		// A flush may only write commits no segment has a record for - hidden ones included - or
		// two segments would share a commit. Those wait for the next full rewrite.
		if !include(h, r) || (!full && s.inAnySegment(h)) {
			continue
		}
		src := graphSource{msg: s.messages[h], extra: s.extra[h]}
		copy(src.rec[:], r)
		// Whether the commit wrote anything is not in the record until now: resident, it is
		// answered by the ops side map and the DAG's eviction set. On disk the record must say.
		setRecHasOps(src.rec[:], hasOps(h))
		snap.resident = append(snap.resident, src)
		if tx := recTx(r); txOf(h, tx) {
			snap.residentTx = append(snap.residentTx, graphTx{key: txKey(tx), hash: h})
		}
	}
	sort.Slice(snap.resident, func(a, b int) bool {
		return bytes.Compare(snap.resident[a].hash(), snap.resident[b].hash()) < 0
	})
	sort.Slice(snap.residentTx, func(a, b int) bool {
		return bytes.Compare(snap.residentTx[a].key[:], snap.residentTx[b].key[:]) < 0
	})
	if !full {
		return snap
	}
	for i := len(s.segs) - 1; i >= 0; i-- { // oldest first
		b := s.segs[i]
		r, err := b.mapping.Acquire()
		if err != nil {
			continue
		}
		snap.segs = append(snap.segs, b)
		snap.releases = append(snap.releases, r.Release)
	}
	snap.hidden = make(map[codec.Hash]struct{}, len(s.hidden))
	for h := range s.hidden {
		snap.hidden[h] = struct{}{}
	}
	snap.genOverride = make(map[codec.Hash]uint32, len(s.genOverride))
	for h, g := range s.genOverride {
		snap.genOverride[h] = g
	}
	return snap
}

func (snap *graphSnapshot) release() {
	for _, r := range snap.releases {
		r()
	}
	snap.releases = nil
}

// sources merges every input stream - each segment's visible records, then the resident tier -
// in hash order. Streams are disjoint, so the merge never has to choose between two records for
// one commit; if it ever did, the later stream (newer) would win.
func (snap *graphSnapshot) sources() (func() (graphSource, bool), func() (graphTx, bool)) {
	type stream struct {
		next func() (graphSource, bool)
		head *graphSource
	}
	var streams []*stream
	for _, b := range snap.segs {
		b, pos := b, 0
		streams = append(streams, &stream{next: func() (graphSource, bool) {
			for pos < b.count {
				p := pos
				pos++
				h := b.hashAt(p)
				if _, gone := snap.hidden[h]; gone {
					continue
				}
				src := graphSource{msg: b.messageAt(p), extra: b.extraAt(p)}
				copy(src.rec[:], b.rec(p))
				if g, ok := snap.genOverride[h]; ok {
					setRecGeneration(src.rec[:], g)
				}
				return src, true
			}
			return graphSource{}, false
		}})
	}
	ri := 0
	streams = append(streams, &stream{next: func() (graphSource, bool) {
		if ri >= len(snap.resident) {
			return graphSource{}, false
		}
		ri++
		return snap.resident[ri-1], true
	}})
	next := func() (graphSource, bool) {
		var best *stream
		for _, st := range streams {
			if st.head == nil {
				if src, ok := st.next(); ok {
					st.head = &src
				}
			}
			if st.head == nil {
				continue
			}
			if best == nil {
				best = st
				continue
			}
			switch c := bytes.Compare(st.head.hash(), best.head.hash()); {
			case c < 0:
				best = st
			case c == 0:
				best.head = nil // the newer stream's record wins; drop the older one
				best = st
			}
		}
		if best == nil {
			return graphSource{}, false
		}
		out := *best.head
		best.head = nil
		return out, true
	}

	type txStream struct {
		next func() (graphTx, bool)
		head *graphTx
	}
	var txs []*txStream
	for _, b := range snap.segs {
		b, i := b, 0
		txs = append(txs, &txStream{next: func() (graphTx, bool) {
			for i < b.txCount {
				e := b.tidx[i*graphTxEntrySize : (i+1)*graphTxEntrySize]
				i++
				var t graphTx
				copy(t.key[:], e[:16])
				copy(t.hash.Bytes[:], e[16:48])
				if _, gone := snap.hidden[t.hash]; gone {
					continue
				}
				return t, true
			}
			return graphTx{}, false
		}})
	}
	rti := 0
	txs = append(txs, &txStream{next: func() (graphTx, bool) {
		if rti >= len(snap.residentTx) {
			return graphTx{}, false
		}
		rti++
		return snap.residentTx[rti-1], true
	}})
	nextTx := func() (graphTx, bool) {
		var best *txStream
		for _, st := range txs {
			if st.head == nil {
				if t, ok := st.next(); ok {
					st.head = &t
				}
			}
			if st.head == nil {
				continue
			}
			// Strictly less: on a tie the earlier (older) stream keeps it - first writer wins, as
			// in txIndex. The writer drops the later duplicate.
			if best == nil || bytes.Compare(st.head.key[:], best.head.key[:]) < 0 {
				best = st
			}
		}
		if best == nil {
			return graphTx{}, false
		}
		out := *best.head
		best.head = nil
		return out, true
	}
	return next, nextTx
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
				return recGeneration(b.rec(pos)), true
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
		r := nb.rec(pos)
		h := recHash(r)
		if i, ok := s.index[h]; ok {
			setOverride(h, recGeneration(r), recGeneration(s.at(i)))
			s.freeSlot(h, i)
			delete(s.messages, h)
			delete(s.extra, h)
			continue
		}
		if gen, ok := replacedVisible(h); ok {
			setOverride(h, recGeneration(r), gen)
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
			c := decodeRecord(b.rec(pos), b.ns, b.messageAt(pos), b.extraAt(pos), s.ops[h])
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
	s.compactSlab()
	// A Go map never gives back its buckets, so an index that held a million commits before the
	// attach moved them out would keep costing what it cost then. Copying is O(what is left).
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

// compactSlab gives back the chunks an attach emptied. Slots are renumbered, so this may only
// run where nothing holds a slot index - which, under the DAG's write lock, is everywhere.
func (s *nodeStore) compactSlab() {
	if len(s.free) < slotsPerChunk {
		return
	}
	var chunks []*[slotsPerChunk * graphRecordSize]byte
	var used uint32
	for h, i := range s.index {
		j := used
		used++
		if int(j/slotsPerChunk) == len(chunks) {
			chunks = append(chunks, new([slotsPerChunk * graphRecordSize]byte))
		}
		off := int(j%slotsPerChunk) * graphRecordSize
		copy(chunks[j/slotsPerChunk][off:off+graphRecordSize], s.at(i))
		s.index[h] = j
	}
	s.chunks, s.used, s.free = chunks, used, nil
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
	if !ok || !recHasOps(b.rec(pos)) {
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
		}
	}
}

// detach drops every segment, putting nothing back: for a DAG that is closing.
func (s *nodeStore) detach() {
	for _, b := range s.segs {
		b.close()
	}
	s.segs, s.hidden, s.genOverride, s.segRecords = nil, nil, nil, 0
}
