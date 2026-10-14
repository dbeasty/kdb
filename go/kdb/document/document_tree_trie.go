package document

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"runtime"
	"slices"
	"sort"
	"sync"
	"unsafe"

	"github.com/limidus/kdb/go/kdb/codec"
)

// This file implements the incremental hashing engine behind DocumentTree.
// The previous algorithm (sort all entries, wire-encode the sorted array,
// SHA256 the whole thing) is inherently O(n) per commit - hashing itself
// touches every entry, so no amount of clever map handling around it can
// make a commit cheaper than O(n). See docs/benchmarks/phases-1-6-summary.md
// Phase 3 for that finding, and the "true O(delta) commit trees" gap it
// left open.
//
// This replaces the hash algorithm with a 16-ary trie over the codec.UUID's 32
// hex nibbles (128 bits), Merkle-style: a leaf hashes (uuid, contentHash),
// an internal node hashes its 16 children (a fixed all-zero sentinel for
// an absent child), and the tree hash is the root. This is canonical -
// the same set of (docID, contentHash) pairs always produces the same
// hash, independent of insertion order - and, critically, persistent:
// inserting or deleting one entry only touches the O(32) nodes on that
// entry's path, sharing every other subtree unchanged with the previous
// version. trieWith/trieWithout expose that incremental update; a full
// trieBuild from a flat map (used for wire-decoded trees, which don't
// carry a trie to build on) stays O(n).
//
// The nodes live in a pointer-free slab (document_tree_slab.go), which also explains how
// structure sharing and reclamation work there. The pointer trie that preceded it is kept, as
// the reference the tests hold this one to, in document_tree_trie_ref_test.go.
//
// Leaf compression. A leaf stands for the whole subtree beneath its position when that subtree
// holds exactly one entry, at whatever depth that becomes true, rather than only at trieDepth.
// Without it every entry owned a private spine of 32 internal nodes, which measured at ~4,900
// bytes of live heap per document regardless of the document's size (see
// docs/benchmarks/2026-09-08-base-tree-pinning.md).
//
// The hash is unaffected, and deliberately so. A subtree containing one entry has a determined
// hash - fold leafHash up through the single-child internal nodes it would have had - so a
// compressed leaf carries exactly the hash its 32-node spine would have produced. Every tree hash
// this package has ever emitted is therefore unchanged: the Kotlin implementation, the golden
// vectors, and every stored tree object stay valid.

const trieDepth = 32 // 128-bit codec.UUID, 4 bits (one hex nibble) per level

var trieZero [32]byte // all-zero sentinel for an absent child/subtree

func nibbleAt(uuidBytes []byte, depth int) int {
	b := uuidBytes[depth/2]
	if depth%2 == 0 {
		return int(b >> 4)
	}
	return int(b & 0x0f)
}

func keyNibble(k *[16]byte, depth int) int {
	b := k[depth>>1]
	if depth&1 == 0 {
		return int(b >> 4)
	}
	return int(b & 0x0f)
}

// uuidKey is id's bytes, big-endian: the trie's key, and the order ids sort in.
func uuidKey(id codec.UUID) (k [16]byte) {
	binary.BigEndian.PutUint64(k[:8], uint64(id.MSB))
	binary.BigEndian.PutUint64(k[8:], uint64(id.LSB))
	return k
}

func keyUUID(k *[16]byte) codec.UUID {
	return codec.UUID{MSB: int64(binary.BigEndian.Uint64(k[:8])), LSB: int64(binary.BigEndian.Uint64(k[8:]))}
}

// Both hashers build their preimage in a fixed-size array rather than a
// make()d slice. The buffer never escapes past sha256.Sum256, so it stays on
// the stack - which matters because internalHash runs once per trie level and
// the trie is trieDepth (32) levels deep, so a heap buffer here cost ~16KB of
// garbage per single-document put or delete.

func leafHash(uuidBytes []byte, contentHash codec.Hash) [32]byte {
	var buf [1 + 16 + 32]byte
	buf[0] = 0x00
	copy(buf[1:], uuidBytes)
	copy(buf[1+16:], contentHash.Bytes[:])
	return sha256.Sum256(buf[:])
}

// internalHashOfOnlyChild is the internal-node hash for the case that matters to
// compression: an internal node with one occupied slot. The preimage buffer
// starts zeroed and trieZero is all-zero, so the absent siblings need no
// writing at all.
func internalHashOfOnlyChild(nib int, childHash [32]byte) [32]byte {
	var buf [1 + 16*32]byte
	buf[0] = 0x01
	copy(buf[1+nib*32:], childHash[:])
	return sha256.Sum256(buf[:])
}

// foldLeaf is the hash of the subtree rooted at depth that holds exactly the
// one entry (uuidBytes, contentHash) - the spine that used to be built out of
// real nodes, evaluated instead. At depth 0 this is 32 rounds on top of the
// leaf, which is precisely what the uncompressed trie computed for a
// single-entry tree.
func foldLeaf(uuidBytes []byte, contentHash codec.Hash, depth int) [32]byte {
	h := leafHash(uuidBytes, contentHash)
	for d := trieDepth - 1; d >= depth; d-- {
		h = internalHashOfOnlyChild(nibbleAt(uuidBytes, d), h)
	}
	return h
}

// uuidAfter reports whether a sorts after b in the trie's order: unsigned big-endian bytes, which
// is also the order of their canonical (lowercase hex) string forms.
func uuidAfter(a, b codec.UUID) bool {
	if a.MSB != b.MSB {
		return uint64(a.MSB) > uint64(b.MSB)
	}
	return uint64(a.LSB) > uint64(b.LSB)
}

// --- Updates ---------------------------------------------------------------------------------

// trieEdit assembles the records one update creates before any of them is in the slab. The
// hashing - up to 33 SHA-256 rounds for a new leaf, one per new internal node - is most of an
// update's cost and needs no lock, because it only reads records of the tree being updated,
// which the caller holds. place then takes the slab lock just to copy the records in and count
// the references they add. Records refer to each other by refLocal|index until placed.
type trieEdit struct {
	v    trieView
	keep map[uint64]bool // translate: the segments a rebased tree may refer into
	// Shared across the trees of one rebase (rebaseTrees), so a record several of them reach is
	// translated, copied and placed once and then shared, as it was before.
	memo, placed map[tref]tref
	n            int
	recs         []editRec // grows only for a rebase (translate); an update needs at most 2*trieDepth+4
}

type editRec struct {
	hash    [32]byte
	child   [16]tref
	id      [16]byte
	content codec.Hash
	leaf    bool
}

// editPool recycles edits: a fresh one is ~10 KB that Go would zero on every update, and every
// record an edit uses is written whole before it is read, so a recycled one needs no clearing.
var editPool = sync.Pool{New: func() any { return new(trieEdit) }}

func getEdit(v trieView) *trieEdit {
	e := editPool.Get().(*trieEdit)
	e.v, e.n = v, 0
	return e
}

func putEdit(e *trieEdit) {
	e.v, e.keep, e.memo, e.placed = trieView{}, nil, nil, nil
	if len(e.recs) > 1024 { // a large rebase: do not keep its buffer
		e.recs = nil
	}
	editPool.Put(e)
}

func (e *trieEdit) local(r tref) *editRec { return &e.recs[r&refIndexMask] }

func (e *trieEdit) isLeaf(r tref) bool {
	if r&refLocal != 0 {
		return e.local(r).leaf
	}
	return r&refLeaf != 0
}

func (e *trieEdit) hash(r tref) *[32]byte {
	if r&refLocal != 0 {
		return &e.local(r).hash
	}
	return e.v.hash(r)
}

func (e *trieEdit) leafEntry(r tref) ([16]byte, codec.Hash) {
	if r&refLocal != 0 {
		l := e.local(r)
		return l.id, l.content
	}
	l := e.v.leaf(r)
	return l.id, l.content
}

func (e *trieEdit) children(r tref) [16]tref {
	if r&refLocal != 0 {
		return e.local(r).child
	}
	return e.v.children(r)
}

func (e *trieEdit) next() (tref, *editRec) {
	if e.n == len(e.recs) {
		e.recs = append(e.recs, make([]editRec, max(2*trieDepth+4, len(e.recs)))...)
	}
	r := refLocal | tref(e.n)
	e.n++
	return r, &e.recs[r&refIndexMask]
}

func (e *trieEdit) newLeaf(key *[16]byte, content codec.Hash, depth int) tref {
	r, rec := e.next()
	*rec = editRec{hash: foldLeaf(key[:], content, depth), id: *key, content: content, leaf: true}
	return r
}

func (e *trieEdit) newNode(children *[16]tref) tref {
	var buf [1 + 16*32]byte
	buf[0] = 0x01
	for i, c := range children {
		if c != 0 {
			copy(buf[1+i*32:], e.hash(c)[:])
		}
	}
	r, rec := e.next()
	*rec = editRec{hash: sha256.Sum256(buf[:]), child: *children}
	return r
}

// insert returns the subtree r at depth with (key, content) set, sharing every subtree off
// key's path. Returns r itself when the entry is already there with that content.
func (e *trieEdit) insert(r tref, key *[16]byte, content codec.Hash, depth int) tref {
	// An empty subtree becomes one leaf covering everything below here - the common case.
	if r == 0 {
		return e.newLeaf(key, content, depth)
	}
	if e.isLeaf(r) {
		id, c := e.leafEntry(r)
		if id == *key {
			if c == content {
				return r
			}
			return e.newLeaf(key, content, depth)
		}
		return e.split(r, key, content, depth)
	}
	if depth == trieDepth {
		return e.newLeaf(key, content, depth)
	}
	children := e.children(r)
	nib := keyNibble(key, depth)
	nc := e.insert(children[nib], key, content, depth+1)
	if nc == children[nib] {
		return r
	}
	children[nib] = nc
	return e.newNode(&children)
}

// split makes room beneath the compressed leaf r for a second entry. Only the levels the two
// keys genuinely share get real nodes, so what this creates is set by where the keys diverge -
// for random UUIDs, a handful of levels at n documents rather than 32 at any n.
func (e *trieEdit) split(r tref, key *[16]byte, content codec.Hash, depth int) tref {
	exID, exContent := e.leafEntry(r)
	d := depth
	for d < trieDepth && keyNibble(&exID, d) == keyNibble(key, d) {
		d++
	}
	if d == trieDepth {
		// Identical in all 128 bits but unequal as UUIDs is not reachable; treat it as a replace.
		return e.newLeaf(key, content, depth)
	}
	// The level they part on holds both, each compressed again beneath it.
	var children [16]tref
	children[keyNibble(&exID, d)] = e.newLeaf(&exID, exContent, d+1)
	children[keyNibble(key, d)] = e.newLeaf(key, content, d+1)
	node := e.newNode(&children)
	// Levels depth..d-1 are shared by both keys, so they stay ordinary single-child internal
	// nodes: more than one entry now lives below them.
	for k := d - 1; k >= depth; k-- {
		var c [16]tref
		c[keyNibble(key, k)] = node
		node = e.newNode(&c)
	}
	return node
}

// delete returns the subtree r at depth without key; r itself when key is absent.
func (e *trieEdit) delete(r tref, key *[16]byte, depth int) tref {
	if r == 0 {
		return 0
	}
	if e.isLeaf(r) {
		if id, _ := e.leafEntry(r); id == *key {
			return 0
		}
		return r // a different entry lives here; the key is absent
	}
	if depth == trieDepth {
		return r
	}
	children := e.children(r)
	nib := keyNibble(key, depth)
	nc := e.delete(children[nib], key, depth+1)
	if nc == children[nib] {
		return r
	}
	children[nib] = nc
	var only tref
	remaining := 0
	for _, c := range children {
		if c != 0 {
			remaining++
			only = c
		}
	}
	if remaining == 0 {
		return 0
	}
	// Re-compress on the way back up. One leaf left below means one entry left below, which is
	// exactly what a compressed leaf stands for. Skipping this would let the spines compression
	// removed grow back one deletion at a time.
	if remaining == 1 && e.isLeaf(only) {
		id, c := e.leafEntry(only)
		return e.newLeaf(&id, c, depth)
	}
	return e.newNode(&children)
}

// place copies the edit's records reachable from r into the slab and returns r's slab
// reference, counting each child reference the placed records hold. Records the edit made and
// then dropped (a leaf re-compressed one level up) are never placed. Under s.mu.
func (s *trieSlab) place(e *trieEdit, r tref) tref {
	if r&refLocal == 0 {
		return r
	}
	if p, ok := e.placed[r]; ok {
		return p
	}
	rec := e.local(r)
	if rec.leaf {
		i := s.allocLeaf()
		*s.leaves.at(i) = leafRec{hash: rec.hash, id: rec.id, content: rec.content}
		if e.placed != nil {
			e.placed[r] = refLeaf | tref(i)
		}
		return refLeaf | tref(i)
	}
	var children [16]tref
	for k, c := range rec.child {
		if c != 0 {
			p := s.place(e, c)
			s.retain(p)
			children[k] = p
		}
	}
	i := s.allocNode()
	*s.nodes.at(i) = nodeRec{hash: rec.hash, child: children}
	if e.placed != nil {
		e.placed[r] = tref(i)
	}
	return tref(i)
}

// commit places the edit's result and wraps it in a new handle. gen is the generation of the
// tree the edit derived from (see adoptAt).
func (s *trieSlab) commit(e *trieEdit, r tref, gen uint32) *trieTree {
	if r == 0 {
		return s.adoptAt(0, gen)
	}
	s.mu.Lock()
	s.drain(drainPerUpdate)
	r = s.place(e, r)
	s.retain(r)
	s.mu.Unlock()
	return s.adoptAt(r, gen)
}

// trieWith returns the tree h with (id, contentHash) set, sharing every subtree not on id's
// path with h. O(trieDepth). A nil h is the empty tree; the result then starts a new slab.
func trieWith(h *trieTree, id codec.UUID, contentHash codec.Hash) *trieTree {
	var s *trieSlab
	var root tref
	var gen uint32
	if h == nil {
		s = newTrieSlab()
	} else {
		h = h.rebased()
		s, root, gen = h.slab, h.root, h.gen
	}
	e := getEdit(s.view())
	key := uuidKey(id)
	nr := e.insert(root, &key, contentHash, 0)
	out := h
	if nr != root {
		out = s.commit(e, nr, gen)
		s.maybeRewrite(out)
	}
	putEdit(e)
	runtime.KeepAlive(h)
	return out
}

// trieWithAll is trieWith for a batch: items sorted by key, no key twice. Each node the batch
// touches is hashed once, where inserting the items one at a time hashes the nodes near the root
// once per item - which, building a large tree a page at a time, was most of the cost of
// building it.
func trieWithAll(h *trieTree, items []trieItem) *trieTree {
	var s *trieSlab
	var root tref
	var gen uint32
	if h == nil {
		s = newTrieSlab()
	} else {
		h = h.rebased()
		s, root, gen = h.slab, h.root, h.gen
	}
	e := getEdit(s.view())
	// A batch makes a record per new leaf and node - a few per item - which an edit's own buffer
	// (sized for one update, and dropped when large) would grow into by doubling, every batch.
	if bp, _ := batchRecs.Get().(*[]editRec); bp != nil {
		e.recs = *bp
	}
	if want := 4*len(items) + 2*trieDepth + 4; len(e.recs) < want {
		e.recs = make([]editRec, want)
	}
	nr := e.insertMany(root, items, 0)
	out := h
	if nr != root {
		out = s.commit(e, nr, gen)
		s.maybeRewrite(out)
	}
	recs := e.recs
	e.recs = nil
	putEdit(e)
	batchRecs.Put(&recs)
	runtime.KeepAlive(h)
	return out
}

// batchRecs keeps trieWithAll's record buffers between batches.
var batchRecs sync.Pool

// insertMany is insert for items sorted by key, no key twice: the subtree r at depth with all of
// them set, built as inserting them one by one would build it (the shape below a node depends only
// on the entries beneath it), with each new node made once.
func (e *trieEdit) insertMany(r tref, items []trieItem, depth int) tref {
	if len(items) == 1 {
		return e.insert(r, &items[0].key, items[0].hash, depth)
	}
	if r == 0 {
		return e.buildMany(items, depth)
	}
	if e.isLeaf(r) {
		// One existing entry below here: it joins the items, unless one of them replaces it.
		id, c := e.leafEntry(r)
		i := sort.Search(len(items), func(i int) bool { return bytes.Compare(items[i].key[:], id[:]) >= 0 })
		if i < len(items) && items[i].key == id {
			return e.buildMany(items, depth)
		}
		merged := make([]trieItem, 0, len(items)+1)
		merged = append(append(append(merged, items[:i]...), trieItem{key: id, hash: c}), items[i:]...)
		return e.buildMany(merged, depth)
	}
	children := e.children(r)
	changed := false
	forEachRun(items, depth, func(nib int, run []trieItem) {
		if nc := e.insertMany(children[nib], run, depth+1); nc != children[nib] {
			children[nib], changed = nc, true
		}
	})
	if !changed {
		return r
	}
	return e.newNode(&children)
}

// buildMany is the subtree at depth holding exactly items (sorted, distinct, at least one).
func (e *trieEdit) buildMany(items []trieItem, depth int) tref {
	if len(items) == 1 || depth == trieDepth {
		last := &items[len(items)-1]
		return e.newLeaf(&last.key, last.hash, depth)
	}
	var children [16]tref
	forEachRun(items, depth, func(nib int, run []trieItem) {
		children[nib] = e.buildMany(run, depth+1)
	})
	return e.newNode(&children)
}

// trieWithout returns the tree h without id (h itself if id is absent).
func trieWithout(h *trieTree, id codec.UUID) *trieTree {
	if h == nil {
		return nil
	}
	h = h.rebased()
	e := getEdit(h.slab.view())
	key := uuidKey(id)
	nr := e.delete(h.root, &key, 0)
	out := h
	if nr != h.root {
		out = h.slab.commit(e, nr, h.gen)
		h.slab.maybeRewrite(out)
	}
	putEdit(e)
	runtime.KeepAlive(h)
	return out
}

// --- Bulk build ------------------------------------------------------------------------------

// trieItem is one entry of a bulk build. Its layout is a packed tree entry's (16-byte id, then
// 32-byte content hash), so a packed image is used in place without copying.
type trieItem struct {
	key  [16]byte
	hash codec.Hash
}

var _ [PackedTreeEntrySize - unsafe.Sizeof(trieItem{})]struct{}
var _ [unsafe.Sizeof(trieItem{}) - PackedTreeEntrySize]struct{}

func packedItems(packed []byte) []trieItem {
	if len(packed) == 0 {
		return nil
	}
	return unsafe.Slice((*trieItem)(unsafe.Pointer(&packed[0])), len(packed)/PackedTreeEntrySize)
}

// trieBuild builds the trie for a whole set of entries at once. Opening a namespace builds its
// live tree this way (from the checkpoint's packed image, trieBuildItems), so it is sorted by key
// with each node built and hashed exactly once, and the sixteen top-level subtrees built on
// separate cores. The result is the same trie, node for node, as inserting the entries one at a
// time would give, and so the same hash.
func trieBuild(entries map[codec.UUID]codec.Hash) *trieTree {
	if len(entries) == 0 {
		return nil
	}
	items := make([]trieItem, 0, len(entries))
	for id, h := range entries {
		items = append(items, trieItem{key: uuidKey(id), hash: h})
	}
	slices.SortFunc(items, func(a, b trieItem) int { return bytes.Compare(a.key[:], b.key[:]) })
	return trieBuildItems(items)
}

// trieBuildItems builds a new slab's tree from items sorted by key.
func trieBuildItems(items []trieItem) *trieTree {
	if len(items) == 0 {
		return nil
	}
	s := newTrieSlab()
	b := trieBulk{s: s}
	var root tref
	// Fanning out pays only when there is enough work to split.
	if len(items) >= 4096 {
		var children [16]tref
		var wg sync.WaitGroup
		forEachRun(items, 0, func(nib int, run []trieItem) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				sub := trieBulk{s: s}
				sub.reserveLeaves(len(run))
				children[nib] = sub.build(run, 1)
				sub.finish()
			}()
		})
		wg.Wait()
		root = b.node(&children)
	} else {
		b.reserveLeaves(len(items))
		root = b.build(items, 0)
	}
	b.finish()
	s.mu.Lock()
	s.retain(root)
	s.mu.Unlock()
	return s.adopt(root)
}

// forEachRun calls f for each run of items sharing the nibble at depth, in nibble order.
func forEachRun(items []trieItem, depth int, f func(nib int, run []trieItem)) {
	start := 0
	for i := 1; i <= len(items); i++ {
		if i == len(items) || keyNibble(&items[i].key, depth) != keyNibble(&items[start].key, depth) {
			f(keyNibble(&items[start].key, depth), items[start:i])
			start = i
		}
	}
}

// trieBulk writes a bulk build's records straight into slots it reserves in blocks, so builders
// on separate goroutines share the slab lock once per block rather than once per record. The
// records are unpublished until the build returns, so they are written and counted without it.
type trieBulk struct {
	s                                    *trieSlab
	nodeNext, nodeEnd, leafNext, leafEnd uint32
}

// trieBulkBlock is how many node slots a builder reserves at a time. Leaves, one per item, are
// reserved exactly up front (reserveLeaves), so only node slots are left over, and few.
const trieBulkBlock = 256

func (b *trieBulk) reserveLeaves(n int) {
	b.s.mu.Lock()
	b.leafNext, _ = b.s.leaves.reserve(uint32(n))
	b.s.mu.Unlock()
	b.leafEnd = b.leafNext + uint32(n)
}

func (b *trieBulk) newLeaf() (tref, *leafRec) {
	if b.leafNext == b.leafEnd {
		b.s.mu.Lock()
		b.leafNext, _ = b.s.leaves.reserve(trieBulkBlock)
		b.s.mu.Unlock()
		b.leafEnd = b.leafNext + trieBulkBlock
	}
	i := b.leafNext
	b.leafNext++
	return refLeaf | tref(i), b.s.leaves.at(i)
}

func (b *trieBulk) newNode() (tref, *nodeRec) {
	if b.nodeNext == b.nodeEnd {
		b.s.mu.Lock()
		b.nodeNext, _ = b.s.nodes.reserve(trieBulkBlock)
		b.s.mu.Unlock()
		b.nodeEnd = b.nodeNext + trieBulkBlock
	}
	i := b.nodeNext
	b.nodeNext++
	return tref(i), b.s.nodes.at(i)
}

// finish returns the unused part of the last reserved blocks to the free lists.
func (b *trieBulk) finish() {
	b.s.mu.Lock()
	for i := b.nodeNext; i < b.nodeEnd; i++ {
		b.s.nodes.free = append(b.s.nodes.free, i)
	}
	for i := b.leafNext; i < b.leafEnd; i++ {
		b.s.leaves.free = append(b.s.leaves.free, i)
	}
	b.s.mu.Unlock()
	b.nodeNext, b.nodeEnd, b.leafNext, b.leafEnd = 0, 0, 0, 0
}

// build builds the subtree at depth holding items, which are sorted and all share the key's
// first depth nibbles. Mirrors what inserting them would produce: one item is a leaf that stands
// for the whole subtree; more than one is an internal node whose children are the runs of items
// sharing the next nibble - a run of all of them included, which is the single-child chain split
// builds.
func (b *trieBulk) build(items []trieItem, depth int) tref {
	if len(items) == 1 || depth == trieDepth {
		it := &items[len(items)-1] // at full depth equal keys collide; the last write wins, as with inserts
		r, l := b.newLeaf()
		*l = leafRec{hash: foldLeaf(it.key[:], it.hash, depth), id: it.key, content: it.hash}
		return r
	}
	var children [16]tref
	forEachRun(items, depth, func(nib int, run []trieItem) {
		children[nib] = b.build(run, depth+1)
	})
	return b.node(&children)
}

// node creates the internal node over children, counting a reference to each.
func (b *trieBulk) node(children *[16]tref) tref {
	var buf [1 + 16*32]byte
	buf[0] = 0x01
	for i, c := range children {
		switch {
		case c == 0:
			continue
		case c&refLeaf != 0:
			copy(buf[1+i*32:], b.s.leaves.at(uint32(c &^ refLeaf)).hash[:])
			*b.s.leaves.count(uint32(c &^ refLeaf))++
		default:
			copy(buf[1+i*32:], b.s.nodes.at(uint32(c)).hash[:])
			*b.s.nodes.count(uint32(c))++
		}
	}
	r, n := b.newNode()
	*n = nodeRec{hash: sha256.Sum256(buf[:]), child: *children}
	return r
}

// --- Reads -----------------------------------------------------------------------------------

// rootHash is the tree hash: the root record's hash, or zero for the empty tree.
func (h *trieTree) rootHash() codec.Hash {
	if h == nil {
		return codec.Hash{}
	}
	v := h.slab.view()
	out := codec.Hash{Bytes: *v.hash(h.root)}
	runtime.KeepAlive(h)
	return out
}

// get looks up id's content hash. O(trieDepth).
func (h *trieTree) get(id codec.UUID) (codec.Hash, bool) {
	if h == nil {
		return codec.Hash{}, false
	}
	key := uuidKey(id)
	v := h.slab.view()
	r := h.root
	var out codec.Hash
	found := false
	for depth := 0; r != 0; depth++ {
		if r&refLeaf != 0 {
			if l := v.leaf(r); l.id == key {
				out, found = l.content, true
			}
			break
		}
		if depth >= trieDepth {
			break
		}
		r = v.child(r, keyNibble(&key, depth))
	}
	runtime.KeepAlive(h)
	return out, found
}

// walkUntil visits leaves in ascending id order until visit returns false; reports whether the
// walk ran to completion.
func (h *trieTree) walkUntil(visit func(codec.UUID, codec.Hash) bool) bool {
	if h == nil {
		return true
	}
	v := h.slab.view()
	ok := v.walk(h.root, visit, nil)
	runtime.KeepAlive(h)
	return ok
}

// walk visits the leaves under r in order, counting the records entered when visited is non-nil.
func (v *trieView) walk(r tref, visit func(codec.UUID, codec.Hash) bool, visited *int) bool {
	if r == 0 {
		return true
	}
	if visited != nil {
		*visited++
	}
	if r&refLeaf != 0 {
		l := v.leaf(r)
		return visit(keyUUID(&l.id), l.content)
	}
	for _, c := range v.children(r) {
		if c != 0 && !v.walk(c, visit, visited) {
			return false
		}
	}
	return true
}

// walkAfter visits, in ascending id order, the leaves whose id sorts after after, until visit
// returns false. It descends only along after's path: every child before after's nibble holds
// ids at or below it and is skipped without being entered, every child after it is walked whole.
// So reaching the first id after costs O(trieDepth) records, not a walk of everything before it.
// visited, when non-nil, counts the records entered.
func (h *trieTree) walkAfter(after codec.UUID, visit func(codec.UUID, codec.Hash) bool, visited *int) {
	if h == nil {
		return
	}
	v := h.slab.view()
	key := uuidKey(after)
	v.walkAfter(h.root, &key, after, 0, visit, visited)
	runtime.KeepAlive(h)
}

func (v *trieView) walkAfter(r tref, key *[16]byte, after codec.UUID, depth int, visit func(codec.UUID, codec.Hash) bool, visited *int) bool {
	if r == 0 {
		return true
	}
	if visited != nil {
		*visited++
	}
	if r&refLeaf != 0 {
		l := v.leaf(r)
		if id := keyUUID(&l.id); uuidAfter(id, after) {
			return visit(id, l.content)
		}
		return true
	}
	if depth >= trieDepth {
		return true
	}
	children := v.children(r)
	nib := keyNibble(key, depth)
	if !v.walkAfter(children[nib], key, after, depth+1, visit, visited) {
		return false
	}
	for i := nib + 1; i < 16; i++ {
		if !v.walk(children[i], visit, visited) {
			return false
		}
	}
	return true
}

// entries materializes every (id, hash) pair into a flat map - O(n), used only where a full map
// is genuinely needed (wire/storage serialization, DAG diff, full scans), never on the per-write
// path.
func (h *trieTree) entries() map[codec.UUID]codec.Hash {
	out := make(map[codec.UUID]codec.Hash)
	h.walkUntil(func(id codec.UUID, c codec.Hash) bool {
		out[id] = c
		return true
	})
	return out
}
