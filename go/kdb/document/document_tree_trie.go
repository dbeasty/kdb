package document

import (
	"crypto/sha256"

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
// DocumentTree.Entries (the flat map[codec.UUID]codec.Hash) is untouched by this -
// it's still eagerly copied per commit, which Phase 3's own benchmarking
// showed costs ~81us at 2000 entries and isn't the bottleneck. Only
// TreeHash's computation changes.

const trieDepth = 32 // 128-bit codec.UUID, 4 bits (one hex nibble) per level

var trieZero [32]byte // all-zero sentinel for an absent child/subtree

// A trieNode is either a leaf or an internal node (or neither, for a nil
// *trieNode representing an empty subtree - callers use nil directly rather
// than allocating a trieNode for that case).
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
type trieNode struct {
	hash     [32]byte
	leaf     *trieLeaf
	children *[16]*trieNode
}

type trieLeaf struct {
	uuid codec.UUID
	hash codec.Hash
}

func nibbleAt(uuidBytes []byte, depth int) int {
	b := uuidBytes[depth/2]
	if depth%2 == 0 {
		return int(b >> 4)
	}
	return int(b & 0x0f)
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

func internalHash(children *[16]*trieNode) [32]byte {
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

// internalHashOfOnlyChild is internalHash for the case that matters to
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
//
// Costs the same SHA-256 work the old insert did on its way back up, so
// compression trades no CPU for the memory it saves; what it removes is the
// allocation of the nodes, not the hashing of them.
func foldLeaf(uuidBytes []byte, contentHash codec.Hash, depth int) [32]byte {
	h := leafHash(uuidBytes, contentHash)
	for d := trieDepth - 1; d >= depth; d-- {
		h = internalHashOfOnlyChild(nibbleAt(uuidBytes, d), h)
	}
	return h
}

// newLeafNode builds the compressed leaf standing for one entry at depth.
func newLeafNode(uuidBytes []byte, id codec.UUID, contentHash codec.Hash, depth int) *trieNode {
	return &trieNode{
		hash: foldLeaf(uuidBytes, contentHash, depth),
		leaf: &trieLeaf{uuid: id, hash: contentHash},
	}
}

func nodeHash(n *trieNode) [32]byte {
	if n == nil {
		return trieZero
	}
	return n.hash
}

// trieInsert returns a new root with (id, contentHash) set, sharing every
// subtree not on id's path with the original. O(trieDepth) = O(1).
func trieInsert(root *trieNode, id codec.UUID, contentHash codec.Hash) *trieNode {
	uuidBytes := id.Bytes()
	return trieInsertAt(root, uuidBytes, id, contentHash, 0)
}

func trieInsertAt(node *trieNode, uuidBytes []byte, id codec.UUID, contentHash codec.Hash, depth int) *trieNode {
	// An empty subtree becomes one leaf covering everything below here - the
	// common case, and the whole saving.
	if node == nil {
		return newLeafNode(uuidBytes, id, contentHash, depth)
	}
	if node.leaf != nil {
		if node.leaf.uuid == id {
			return newLeafNode(uuidBytes, id, contentHash, depth)
		}
		return splitLeafAt(node.leaf, uuidBytes, id, contentHash, depth)
	}
	if depth == trieDepth {
		return newLeafNode(uuidBytes, id, contentHash, depth)
	}
	var children [16]*trieNode
	if node.children != nil {
		children = *node.children
	}
	nib := nibbleAt(uuidBytes, depth)
	children[nib] = trieInsertAt(children[nib], uuidBytes, id, contentHash, depth+1)
	return &trieNode{hash: internalHash(&children), children: &children}
}

// splitLeafAt makes room beneath a compressed leaf for a second entry. Only
// the levels the two keys genuinely share get real nodes, so what this
// allocates is set by where the keys diverge rather than by trieDepth - for
// random UUIDs, a handful of levels at n documents rather than 32 at any n.
func splitLeafAt(existing *trieLeaf, uuidBytes []byte, id codec.UUID, contentHash codec.Hash, depth int) *trieNode {
	existingBytes := existing.uuid.Bytes()
	d := depth
	for d < trieDepth && nibbleAt(existingBytes, d) == nibbleAt(uuidBytes, d) {
		d++
	}
	if d == trieDepth {
		// Identical in all 128 bits but unequal as UUIDs is not reachable; treat
		// it as a replace rather than building a node that could never be read.
		return newLeafNode(uuidBytes, id, contentHash, depth)
	}
	// The level they part on holds both, each compressed again beneath it.
	var children [16]*trieNode
	children[nibbleAt(existingBytes, d)] = newLeafNode(existingBytes, existing.uuid, existing.hash, d+1)
	children[nibbleAt(uuidBytes, d)] = newLeafNode(uuidBytes, id, contentHash, d+1)
	node := &trieNode{hash: internalHash(&children), children: &children}
	// Levels depth..d-1 are shared by both keys, so they stay ordinary
	// single-child internal nodes: more than one entry now lives below them,
	// and a single entry is the only thing a compressed leaf may stand for.
	for k := d - 1; k >= depth; k-- {
		var c [16]*trieNode
		c[nibbleAt(uuidBytes, k)] = node
		node = &trieNode{hash: internalHash(&c), children: &c}
	}
	return node
}

// trieDelete returns a new root with id removed (no-op if absent),
// sharing every subtree not on id's path with the original.
func trieDelete(root *trieNode, id codec.UUID) *trieNode {
	return trieDeleteAt(root, id.Bytes(), id, 0)
}

func trieDeleteAt(node *trieNode, uuidBytes []byte, id codec.UUID, depth int) *trieNode {
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
	children[nib] = trieDeleteAt(children[nib], uuidBytes, id, depth+1)
	var only *trieNode
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
		return newLeafNode(only.leaf.uuid.Bytes(), only.leaf.uuid, only.leaf.hash, depth)
	}
	return &trieNode{hash: internalHash(&children), children: &children}
}

// trieBuild constructs a trie from scratch (O(n)); used when a
// DocumentTree has no trie to incrementally build on (e.g. decoded from
// the wire), or as the ground-truth path exercised by parity tests.
func trieBuild(entries map[codec.UUID]codec.Hash) *trieNode {
	var root *trieNode
	for id, h := range entries {
		root = trieInsert(root, id, h)
	}
	return root
}

// trieGet looks up id's content hash. O(trieDepth) = O(1) - the read counterpart to
// trieInsert/trieDelete, letting DocumentTree.Contains/HashFor avoid needing a full flat map.
func trieGet(root *trieNode, id codec.UUID) (codec.Hash, bool) {
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

// trieEntries materializes every (id, hash) pair into a flat map - O(n), used only where a full
// map is genuinely needed (wire/storage serialization, DAG diff, full scans), never on the
// per-write With/Without hot path.
func trieEntries(root *trieNode) map[codec.UUID]codec.Hash {
	out := make(map[codec.UUID]codec.Hash)
	trieWalk(root, func(id codec.UUID, h codec.Hash) { out[id] = h })
	return out
}

func trieWalk(n *trieNode, visit func(codec.UUID, codec.Hash)) {
	trieWalkUntil(n, func(id codec.UUID, h codec.Hash) bool {
		visit(id, h)
		return true
	})
}

// trieWalkUntil visits leaves until visit returns false; reports whether the walk ran to
// completion. The early-stop variant exists for streaming scans (DocumentTree.Walk): a scan
// that has already collected everything it can return, or exhausted its row budget, stops
// walking instead of visiting the rest of the namespace to ignore it.
func trieWalkUntil(n *trieNode, visit func(codec.UUID, codec.Hash) bool) bool {
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
		if !trieWalkUntil(c, visit) {
			return false
		}
	}
	return true
}

// trieCount returns the number of leaves reachable from root - O(n), used only to back
// DocumentTree.Size when no cheaper count is already tracked (e.g. a tree built from a flat map
// via BuildDocumentTree, which already knows len(entries) directly).
func trieCount(root *trieNode) int {
	n := 0
	trieWalk(root, func(codec.UUID, codec.Hash) { n++ })
	return n
}

func trieTreeHash(root *trieNode) codec.Hash {
	return codec.Hash{Bytes: nodeHash(root)}
}
