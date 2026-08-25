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

type trieNode struct {
	hash [32]byte
	// Exactly one of these is set (or neither, for a nil *trieNode
	// representing an empty subtree - callers use nil directly rather
	// than allocating a trieNode for that case).
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

func leafHash(uuidBytes []byte, contentHash codec.Hash) [32]byte {
	buf := make([]byte, 0, 1+16+32)
	buf = append(buf, 0x00)
	buf = append(buf, uuidBytes...)
	buf = append(buf, contentHash.Bytes[:]...)
	return sha256.Sum256(buf)
}

func internalHash(children *[16]*trieNode) [32]byte {
	buf := make([]byte, 0, 1+16*32)
	buf = append(buf, 0x01)
	for _, c := range children {
		if c == nil {
			buf = append(buf, trieZero[:]...)
		} else {
			buf = append(buf, c.hash[:]...)
		}
	}
	return sha256.Sum256(buf)
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
	if depth == trieDepth {
		return &trieNode{hash: leafHash(uuidBytes, contentHash), leaf: &trieLeaf{uuid: id, hash: contentHash}}
	}
	var children [16]*trieNode
	if node != nil && node.children != nil {
		children = *node.children
	}
	nib := nibbleAt(uuidBytes, depth)
	children[nib] = trieInsertAt(children[nib], uuidBytes, id, contentHash, depth+1)
	return &trieNode{hash: internalHash(&children), children: &children}
}

// trieDelete returns a new root with id removed (no-op if absent),
// sharing every subtree not on id's path with the original.
func trieDelete(root *trieNode, id codec.UUID) *trieNode {
	return trieDeleteAt(root, id.Bytes(), 0)
}

func trieDeleteAt(node *trieNode, uuidBytes []byte, depth int) *trieNode {
	if node == nil {
		return nil
	}
	if depth == trieDepth {
		return nil
	}
	if node.children == nil {
		return node
	}
	children := *node.children
	nib := nibbleAt(uuidBytes, depth)
	children[nib] = trieDeleteAt(children[nib], uuidBytes, depth+1)
	allNil := true
	for _, c := range children {
		if c != nil {
			allNil = false
			break
		}
	}
	if allNil {
		return nil
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
	if n == nil {
		return
	}
	if n.leaf != nil {
		visit(n.leaf.uuid, n.leaf.hash)
		return
	}
	if n.children == nil {
		return
	}
	for _, c := range n.children {
		trieWalk(c, visit)
	}
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
