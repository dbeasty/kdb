package document

import (
	"bytes"
	"crypto/sha256"
	"sort"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
)

// This is the pointer trie DocumentTree used before its nodes moved into a slab
// (document_tree_slab.go), kept unchanged apart from names as the reference the slab trie is
// tested against: same entries, same hashes, same shape. Its original commentary follows.
//
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
// ptrBuild from a flat map (used for wire-decoded trees, which don't
// carry a trie to build on) stays O(n).
//
// DocumentTree.Entries (the flat map[codec.UUID]codec.Hash) is untouched by this -
// it's still eagerly copied per commit, which Phase 3's own benchmarking
// showed costs ~81us at 2000 entries and isn't the bottleneck. Only
// TreeHash's computation changes.


// A ptrNode is either a leaf or an internal node (or neither, for a nil
// *ptrNode representing an empty subtree - callers use nil directly rather
// than allocating a ptrNode for that case).
//
// A leaf stands for the whole subtree beneath its position when that subtree
// holds exactly one entry, at whatever depth that becomes true, rather than
// only at trieDepth. This is the one thing keeping a namespace's memory
// sane: without it every entry owned a private spine of 32 internal nodes,
// each carrying a 16-pointer array, which measured at ~4,900 bytes of live
// heap per document *regardless of the document's size* - a 200,000-document
// namespace held a 932MB tree for documents of about 20 bytes each. See
// docs/benchmarks/2026-09-08-base-tree-pinning.md.
//
// The hash is unaffected, and deliberately so. A subtree containing one entry
// has a determined hash - fold leafHash up through the single-child internal
// nodes it would have had - so a compressed leaf can carry exactly the hash
// its 32-node spine would have produced. Every tree hash this package has
// ever emitted is therefore unchanged, which is what lets this be an internal
// representation change rather than an on-disk format break: the Kotlin
// implementation, the golden vectors, and every stored tree object stay valid
// without touching any of them.
type ptrNode struct {
	hash     [32]byte
	leaf     *ptrLeaf
	children *[16]*ptrNode
}

type ptrLeaf struct {
	uuid codec.UUID
	hash codec.Hash
}


// Both hashers build their preimage in a fixed-size array rather than a
// make()d slice. The buffer never escapes past sha256.Sum256, so it stays on
// the stack - which matters because ptrInternalHash runs once per trie level and
// the trie is trieDepth (32) levels deep, so a heap buffer here cost ~16KB of
// garbage per single-document put or delete.


func ptrInternalHash(children *[16]*ptrNode) [32]byte {
	var buf [1 + 16*32]byte
	buf[0] = 0x01
	off := 1
	for _, c := range children {
		if c == nil {
			copy(buf[off:], trieZero[:])
		} else {
			copy(buf[off:], c.hash[:])
		}
		off += 32
	}
	return sha256.Sum256(buf[:])
}

// internalHashOfOnlyChild is ptrInternalHash for the case that matters to
// compression: an internal node with one occupied slot. The preimage buffer
// starts zeroed and trieZero is all-zero, so the absent siblings need no
// writing at all.

// foldLeaf is the hash of the subtree rooted at depth that holds exactly the
// one entry (uuidBytes, contentHash) - the spine that used to be built out of
// real nodes, evaluated instead. At depth 0 this is 32 rounds on top of the
// leaf, which is precisely what the uncompressed trie computed for a
// single-entry tree.
//
// Costs the same SHA-256 work the old insert did on its way back up, so
// compression trades no CPU for the memory it saves; what it removes is the
// allocation of the nodes, not the hashing of them.

// ptrNewLeaf builds the compressed leaf standing for one entry at depth.
func ptrNewLeaf(uuidBytes []byte, id codec.UUID, contentHash codec.Hash, depth int) *ptrNode {
	return &ptrNode{
		hash: foldLeaf(uuidBytes, contentHash, depth),
		leaf: &ptrLeaf{uuid: id, hash: contentHash},
	}
}

func ptrNodeHash(n *ptrNode) [32]byte {
	if n == nil {
		return trieZero
	}
	return n.hash
}

// ptrInsert returns a new root with (id, contentHash) set, sharing every
// subtree not on id's path with the original. O(trieDepth) = O(1).
func ptrInsert(root *ptrNode, id codec.UUID, contentHash codec.Hash) *ptrNode {
	uuidBytes := id.Bytes()
	return ptrInsertAt(root, uuidBytes, id, contentHash, 0)
}

func ptrInsertAt(node *ptrNode, uuidBytes []byte, id codec.UUID, contentHash codec.Hash, depth int) *ptrNode {
	// An empty subtree becomes one leaf covering everything below here - the
	// common case, and the whole saving.
	if node == nil {
		return ptrNewLeaf(uuidBytes, id, contentHash, depth)
	}
	if node.leaf != nil {
		if node.leaf.uuid == id {
			return ptrNewLeaf(uuidBytes, id, contentHash, depth)
		}
		return ptrSplitLeafAt(node.leaf, uuidBytes, id, contentHash, depth)
	}
	if depth == trieDepth {
		return ptrNewLeaf(uuidBytes, id, contentHash, depth)
	}
	var children [16]*ptrNode
	if node.children != nil {
		children = *node.children
	}
	nib := nibbleAt(uuidBytes, depth)
	children[nib] = ptrInsertAt(children[nib], uuidBytes, id, contentHash, depth+1)
	return &ptrNode{hash: ptrInternalHash(&children), children: &children}
}

// ptrSplitLeafAt makes room beneath a compressed leaf for a second entry. Only
// the levels the two keys genuinely share get real nodes, so what this
// allocates is set by where the keys diverge rather than by trieDepth - for
// random UUIDs, a handful of levels at n documents rather than 32 at any n.
func ptrSplitLeafAt(existing *ptrLeaf, uuidBytes []byte, id codec.UUID, contentHash codec.Hash, depth int) *ptrNode {
	existingBytes := existing.uuid.Bytes()
	d := depth
	for d < trieDepth && nibbleAt(existingBytes, d) == nibbleAt(uuidBytes, d) {
		d++
	}
	if d == trieDepth {
		// Identical in all 128 bits but unequal as UUIDs is not reachable; treat
		// it as a replace rather than building a node that could never be read.
		return ptrNewLeaf(uuidBytes, id, contentHash, depth)
	}
	// The level they part on holds both, each compressed again beneath it.
	var children [16]*ptrNode
	children[nibbleAt(existingBytes, d)] = ptrNewLeaf(existingBytes, existing.uuid, existing.hash, d+1)
	children[nibbleAt(uuidBytes, d)] = ptrNewLeaf(uuidBytes, id, contentHash, d+1)
	node := &ptrNode{hash: ptrInternalHash(&children), children: &children}
	// Levels depth..d-1 are shared by both keys, so they stay ordinary
	// single-child internal nodes: more than one entry now lives below them,
	// and a single entry is the only thing a compressed leaf may stand for.
	for k := d - 1; k >= depth; k-- {
		var c [16]*ptrNode
		c[nibbleAt(uuidBytes, k)] = node
		node = &ptrNode{hash: ptrInternalHash(&c), children: &c}
	}
	return node
}

// ptrDelete returns a new root with id removed (no-op if absent),
// sharing every subtree not on id's path with the original.
func ptrDelete(root *ptrNode, id codec.UUID) *ptrNode {
	return ptrDeleteAt(root, id.Bytes(), id, 0)
}

func ptrDeleteAt(node *ptrNode, uuidBytes []byte, id codec.UUID, depth int) *ptrNode {
	if node == nil {
		return nil
	}
	if node.leaf != nil {
		if node.leaf.uuid == id {
			return nil
		}
		return node // a different entry lives here; the key is absent
	}
	if node.children == nil || depth == trieDepth {
		return node
	}
	children := *node.children
	nib := nibbleAt(uuidBytes, depth)
	children[nib] = ptrDeleteAt(children[nib], uuidBytes, id, depth+1)
	var only *ptrNode
	remaining := 0
	for _, c := range children {
		if c != nil {
			remaining++
			only = c
		}
	}
	if remaining == 0 {
		return nil
	}
	// Re-compress on the way back up. One leaf left below means one entry left
	// below, which is exactly what a compressed leaf stands for. Skipping this
	// would let the spines compression removed grow back one deletion at a
	// time, in a namespace that deletes and reinserts steadily.
	if remaining == 1 && only.leaf != nil {
		return ptrNewLeaf(only.leaf.uuid.Bytes(), only.leaf.uuid, only.leaf.hash, depth)
	}
	return &ptrNode{hash: ptrInternalHash(&children), children: &children}
}

// ptrBuild constructs a trie from scratch (O(n)); used when a
// DocumentTree has no trie to incrementally build on (e.g. decoded from
// the wire), or as the ground-truth path exercised by parity tests.
// ptrBuild builds the trie for a whole set of entries at once: sorted by key, each node built
// exactly once and hashed exactly once, and the sixteen top-level subtrees built on separate cores.
//
// It used to insert the entries one at a time, each insert copying and re-hashing the path it
// touched - and opening a namespace builds its live tree this way, so at a million documents
// that was 7.8 s of a 13 s open. The result is the same trie, node for node, and so the same
// hash: trieBulkMatchesInsert pins that.
func ptrBuild(entries map[codec.UUID]codec.Hash) *ptrNode {
	if len(entries) == 0 {
		return nil
	}
	items := make([]ptrItem, 0, len(entries))
	for id, h := range entries {
		items = append(items, ptrItem{key: id.Bytes(), id: id, hash: h})
	}
	sort.Slice(items, func(a, b int) bool { return bytes.Compare(items[a].key, items[b].key) < 0 })
	// Fanning out pays only when there is enough work to split.
	return ptrBuildRange(items, 0, len(items) >= 4096)
}

type ptrItem struct {
	key  []byte
	id   codec.UUID
	hash codec.Hash
}

// ptrBuildRange builds the subtree at depth holding items, which are sorted and all share the
// key's first depth nibbles. Mirrors what inserting them would produce: one item is a leaf that
// stands for the whole subtree (ptrNewLeaf); more than one is an internal node whose children
// are the runs of items sharing the next nibble - a run of all of them included, which is the
// single-child chain ptrSplitLeafAt builds.
func ptrBuildRange(items []ptrItem, depth int, parallel bool) *ptrNode {
	if len(items) == 1 || depth == trieDepth {
		it := items[len(items)-1] // at full depth equal keys collide; the last write wins, as with inserts
		return ptrNewLeaf(it.key, it.id, it.hash, depth)
	}
	var children [16]*ptrNode
	var runs [16][]ptrItem
	start := 0
	for i := 1; i <= len(items); i++ {
		if i == len(items) || nibbleAt(items[i].key, depth) != nibbleAt(items[start].key, depth) {
			runs[nibbleAt(items[start].key, depth)] = items[start:i]
			start = i
		}
	}
	if parallel {
		var wg sync.WaitGroup
		for nib := range runs {
			if len(runs[nib]) == 0 {
				continue
			}
			wg.Add(1)
			go func(nib int) {
				defer wg.Done()
				children[nib] = ptrBuildRange(runs[nib], depth+1, false)
			}(nib)
		}
		wg.Wait()
	} else {
		for nib := range runs {
			if len(runs[nib]) > 0 {
				children[nib] = ptrBuildRange(runs[nib], depth+1, false)
			}
		}
	}
	return &ptrNode{hash: ptrInternalHash(&children), children: &children}
}

// ptrGet looks up id's content hash. O(trieDepth) = O(1) - the read counterpart to
// ptrInsert/ptrDelete, letting DocumentTree.Contains/HashFor avoid needing a full flat map.
func ptrGet(root *ptrNode, id codec.UUID) (codec.Hash, bool) {
	if root == nil {
		return codec.Hash{}, false
	}
	uuidBytes := id.Bytes()
	n := root
	for depth := 0; depth < trieDepth; depth++ {
		if n == nil {
			return codec.Hash{}, false
		}
		if n.leaf != nil {
			if n.leaf.uuid == id {
				return n.leaf.hash, true
			}
			return codec.Hash{}, false
		}
		if n.children == nil {
			return codec.Hash{}, false
		}
		n = n.children[nibbleAt(uuidBytes, depth)]
	}
	if n != nil && n.leaf != nil && n.leaf.uuid == id {
		return n.leaf.hash, true
	}
	return codec.Hash{}, false
}

// ptrEntries materializes every (id, hash) pair into a flat map - O(n), used only where a full
// map is genuinely needed (wire/storage serialization, DAG diff, full scans), never on the
// per-write With/Without hot path.
func ptrEntries(root *ptrNode) map[codec.UUID]codec.Hash {
	out := make(map[codec.UUID]codec.Hash)
	ptrWalk(root, func(id codec.UUID, h codec.Hash) { out[id] = h })
	return out
}

func ptrWalk(n *ptrNode, visit func(codec.UUID, codec.Hash)) {
	ptrWalkUntil(n, func(id codec.UUID, h codec.Hash) bool {
		visit(id, h)
		return true
	})
}

// ptrWalkUntil visits leaves until visit returns false; reports whether the walk ran to
// completion. The early-stop variant exists for streaming scans (DocumentTree.Walk): a scan
// that has already collected everything it can return, or exhausted its row budget, stops
// walking instead of visiting the rest of the namespace to ignore it.
func ptrWalkUntil(n *ptrNode, visit func(codec.UUID, codec.Hash) bool) bool {
	if n == nil {
		return true
	}
	if n.leaf != nil {
		return visit(n.leaf.uuid, n.leaf.hash)
	}
	if n.children == nil {
		return true
	}
	for _, c := range n.children {
		if !ptrWalkUntil(c, visit) {
			return false
		}
	}
	return true
}

// ptrCount returns the number of leaves reachable from root - O(n), used only to back
// DocumentTree.Size when no cheaper count is already tracked (e.g. a tree built from a flat map
// via BuildDocumentTree, which already knows len(entries) directly).
func ptrCount(root *ptrNode) int {
	n := 0
	ptrWalk(root, func(codec.UUID, codec.Hash) { n++ })
	return n
}

func ptrTreeHash(root *ptrNode) codec.Hash {
	return codec.Hash{Bytes: ptrNodeHash(root)}
}

// uuidAfter reports whether a sorts after b in the trie's order: unsigned big-endian bytes, which
// is also the order of their canonical (lowercase hex) string forms.

// ptrWalkAfter visits, in ascending id order, the leaves under n whose id sorts after the one
// whose bytes are key, until visit returns false; reports whether the walk ran to completion.
//
// It descends only along key's path: every child before key's nibble holds ids at or below key
// and is skipped without being entered, every child after it is walked whole. So reaching the
// first id after key costs O(trieDepth) nodes, not a walk of everything before it - what lets a
// caller page through a tree by "the ids after the last one I saw" at the cost of the page.
// visited, when non-nil, counts the nodes entered (for tests asserting that cost).
func ptrWalkAfter(n *ptrNode, key []byte, after codec.UUID, depth int, visit func(codec.UUID, codec.Hash) bool, visited *int) bool {
	if n == nil {
		return true
	}
	if visited != nil {
		*visited++
	}
	if n.leaf != nil {
		if uuidAfter(n.leaf.uuid, after) {
			return visit(n.leaf.uuid, n.leaf.hash)
		}
		return true
	}
	if n.children == nil || depth >= trieDepth {
		return true
	}
	nib := nibbleAt(key, depth)
	if !ptrWalkAfter(n.children[nib], key, after, depth+1, visit, visited) {
		return false
	}
	for i := nib + 1; i < 16; i++ {
		if !ptrWalkCounted(n.children[i], visit, visited) {
			return false
		}
	}
	return true
}

// ptrWalkCounted is ptrWalkUntil, counting the nodes it enters when visited is non-nil.
func ptrWalkCounted(n *ptrNode, visit func(codec.UUID, codec.Hash) bool, visited *int) bool {
	if visited == nil {
		return ptrWalkUntil(n, visit)
	}
	if n == nil {
		return true
	}
	*visited++
	if n.leaf != nil {
		return visit(n.leaf.uuid, n.leaf.hash)
	}
	if n.children == nil {
		return true
	}
	for _, c := range n.children {
		if !ptrWalkCounted(c, visit, visited) {
			return false
		}
	}
	return true
}
