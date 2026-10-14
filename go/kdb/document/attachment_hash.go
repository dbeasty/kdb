package document

import (
	"crypto/sha256"
	"encoding/binary"
	"math/bits"

	"github.com/limidus/kdb/go/kdb/codec"
)

// Attachment content hashes (docs/kdb-attachments-plan.md, "Content identity").
//
// An attachment's identity is not a hash of the whole byte stream but the root of a Merkle tree
// over fixed-size chunks, so that
//   - the chunks of a large upload are hashed in parallel, across cores, as it streams in;
//   - a reader - a client downloading a range, a peer resuming a transfer - can check every chunk
//     on its own against the root (with an RFC 6962 inclusion proof), instead of only once the whole
//     blob has arrived.
//
// Scheme 1 (AttachmentHashSchemeChunked1), which every implementation must reproduce exactly:
//
//	chunk i   = bytes [i*AttachmentChunkBytes, min((i+1)*AttachmentChunkBytes, size))
//	leaf i    = SHA-256(0x00 || chunk i)
//	tree      = the RFC 6962 Merkle tree hash over the leaves: MTH of one leaf is the leaf; MTH of
//	            n > 1 leaves is SHA-256(0x01 || MTH(first k) || MTH(the rest)), k the largest power
//	            of two below n; a zero-byte attachment has one leaf, of the empty chunk
//	identity  = SHA-256(0x02 || be64(size) || tree)
//
// The size is folded into the identity so two streams with the same chunk hashes but different
// lengths (only possible for a trailing empty chunk, which scheme 1 never produces) cannot collide,
// and so a peer learns the length it must receive from the same 32 bytes it verifies against.

const (
	// AttachmentHashSchemeChunked1 is the only scheme so far: SHA-256 Merkle over 1 MiB chunks.
	AttachmentHashSchemeChunked1 byte = 1
	// AttachmentChunkBytes is scheme 1's chunk size. Part of the scheme: changing it is a new scheme.
	AttachmentChunkBytes = 1 << 20
)

// ChunkLeafHash is the leaf hash of one chunk.
func ChunkLeafHash(chunk []byte) codec.Hash {
	h := sha256.New()
	h.Write([]byte{0x00})
	h.Write(chunk)
	var out codec.Hash
	copy(out.Bytes[:], h.Sum(nil))
	return out
}

func merkleNode(left, right codec.Hash) codec.Hash {
	var b [65]byte
	b[0] = 0x01
	copy(b[1:], left.Bytes[:])
	copy(b[33:], right.Bytes[:])
	return codec.Hash{Bytes: sha256.Sum256(b[:])}
}

// AttachmentIdentity folds a tree hash and the attachment's size into its content hash.
func AttachmentIdentity(tree codec.Hash, size int64) codec.Hash {
	var b [41]byte
	b[0] = 0x02
	binary.BigEndian.PutUint64(b[1:], uint64(size))
	copy(b[9:], tree.Bytes[:])
	return codec.Hash{Bytes: sha256.Sum256(b[:])}
}

// MerkleBuilder computes the RFC 6962 tree hash of leaves added in order, holding O(log n) hashes:
// the roots of the perfect subtrees the leaves so far decompose into.
type MerkleBuilder struct {
	stack []codec.Hash // perfect subtree roots, largest (leftmost) first
	sizes []uint64     // their leaf counts, each a power of two, strictly decreasing
	n     uint64
}

// Add appends the next leaf.
func (m *MerkleBuilder) Add(leaf codec.Hash) {
	m.stack = append(m.stack, leaf)
	m.sizes = append(m.sizes, 1)
	m.n++
	// Two equal perfect subtrees side by side are one twice as large.
	for len(m.sizes) >= 2 && m.sizes[len(m.sizes)-1] == m.sizes[len(m.sizes)-2] {
		l, r := m.stack[len(m.stack)-2], m.stack[len(m.stack)-1]
		m.stack = append(m.stack[:len(m.stack)-2], merkleNode(l, r))
		s := m.sizes[len(m.sizes)-1] * 2
		m.sizes = append(m.sizes[:len(m.sizes)-2], s)
	}
}

// Leaves is how many leaves were added.
func (m *MerkleBuilder) Leaves() uint64 { return m.n }

// Root is the tree hash of the leaves added. RFC 6962 splits n leaves at the largest power of two
// below n, so the tree is the perfect subtrees combined right to left.
func (m *MerkleBuilder) Root() codec.Hash {
	if len(m.stack) == 0 {
		return ChunkLeafHash(nil)
	}
	h := m.stack[len(m.stack)-1]
	for i := len(m.stack) - 2; i >= 0; i-- {
		h = merkleNode(m.stack[i], h)
	}
	return h
}

// AttachmentContentHash is the scheme-1 content hash of data held in memory - for tests, and for
// small blobs. Streams use attachstore's parallel hasher, which produces the same value.
func AttachmentContentHash(data []byte) codec.Hash {
	var m MerkleBuilder
	for off := 0; off < len(data) || off == 0; off += AttachmentChunkBytes {
		end := min(off+AttachmentChunkBytes, len(data))
		m.Add(ChunkLeafHash(data[off:end]))
		if end == len(data) {
			break
		}
	}
	return AttachmentIdentity(m.Root(), int64(len(data)))
}

// MerkleTreeHash is the RFC 6962 tree hash of leaves, for a caller holding them all.
func MerkleTreeHash(leaves []codec.Hash) codec.Hash {
	switch len(leaves) {
	case 0:
		return ChunkLeafHash(nil)
	case 1:
		return leaves[0]
	}
	k := 1 << (bits.Len(uint(len(leaves)-1)) - 1)
	return merkleNode(MerkleTreeHash(leaves[:k]), MerkleTreeHash(leaves[k:]))
}

// MerkleInclusionProof is the RFC 6962 audit path for leaf index among leaves: the sibling hashes,
// bottom up, that VerifyMerkleInclusion combines with the leaf to reach the tree hash.
func MerkleInclusionProof(leaves []codec.Hash, index int) []codec.Hash {
	if len(leaves) <= 1 {
		return nil
	}
	k := 1 << (bits.Len(uint(len(leaves)-1)) - 1)
	if index < k {
		return append(MerkleInclusionProof(leaves[:k], index), MerkleTreeHash(leaves[k:]))
	}
	return append(MerkleInclusionProof(leaves[k:], index-k), MerkleTreeHash(leaves[:k]))
}

// VerifyMerkleInclusion checks that leaf is leaf index of a tree of n leaves whose hash is tree,
// given its audit path (RFC 6962 §2.1.1).
func VerifyMerkleInclusion(leaf codec.Hash, index, n int, proof []codec.Hash, tree codec.Hash) bool {
	if index < 0 || index >= n {
		return false
	}
	fn, sn := index, n-1
	r := leaf
	for _, p := range proof {
		if sn == 0 {
			return false
		}
		if fn&1 == 1 || fn == sn {
			r = merkleNode(p, r)
			for fn&1 == 0 && fn != 0 {
				fn >>= 1
				sn >>= 1
			}
		} else {
			r = merkleNode(r, p)
		}
		fn >>= 1
		sn >>= 1
	}
	return sn == 0 && r == tree
}

// MerkleTree holds every level of an RFC 6962 tree, so that each audit path costs O(log n) rather
// than the O(n) of MerkleInclusionProof. Built bottom up, a level's unpaired last node is carried
// up unchanged, which gives the same tree as RFC 6962's split at the largest power of two below n.
type MerkleTree struct {
	levels [][]codec.Hash // levels[0] the leaves, the last level the root alone
}

// NewMerkleTree builds the tree over leaves (none: the tree of one empty chunk).
func NewMerkleTree(leaves []codec.Hash) *MerkleTree {
	if len(leaves) == 0 {
		leaves = []codec.Hash{ChunkLeafHash(nil)}
	}
	t := &MerkleTree{levels: [][]codec.Hash{leaves}}
	for level := leaves; len(level) > 1; {
		next := make([]codec.Hash, (len(level)+1)/2)
		for i := range next {
			if 2*i+1 < len(level) {
				next[i] = merkleNode(level[2*i], level[2*i+1])
			} else {
				next[i] = level[2*i]
			}
		}
		t.levels = append(t.levels, next)
		level = next
	}
	return t
}

// Leaves is the tree's leaf count.
func (t *MerkleTree) Leaves() int { return len(t.levels[0]) }

// Root is the tree hash.
func (t *MerkleTree) Root() codec.Hash { return t.levels[len(t.levels)-1][0] }

// Proof is leaf index's audit path, as MerkleInclusionProof gives it.
func (t *MerkleTree) Proof(index int) []codec.Hash {
	var path []codec.Hash
	for _, level := range t.levels[:len(t.levels)-1] {
		if sib := index ^ 1; sib < len(level) {
			path = append(path, level[sib])
		}
		index >>= 1
	}
	return path
}

// AttachmentChunkCount is how many scheme-1 chunks, so leaves, an attachment of size bytes has: a
// zero-byte attachment has one, of the empty chunk.
func AttachmentChunkCount(size int64) int64 {
	if size <= 0 {
		return 1
	}
	return (size + AttachmentChunkBytes - 1) / AttachmentChunkBytes
}
