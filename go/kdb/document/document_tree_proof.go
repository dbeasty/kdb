package document

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"runtime"

	"github.com/limidus/kdb/go/kdb/codec"
)

// Merkle inclusion and absence proofs over the document trie: enough of the tree to show that a
// tree with a given hash maps a document id to a content hash - or holds no entry for it - without
// the rest of the tree. A node that holds only part of a namespace (a filtered projection) can
// fetch a document it does not hold and check it against a tree hash it already trusts.
//
// A proof is the path from the root towards the id: for every internal node on it, all 16 child
// hashes. The path ends at an empty slot (absent), or at a compressed leaf - the id's own
// (present) or another document's occupying the whole subtree the id would be in (absent). About
// ceil(log16 n) levels of 16 x 32 bytes: ~2.5KB at a million documents.

// TreeProof proves what a tree holds for DocID.
type TreeProof struct {
	DocID codec.UUID
	// Levels are the child hashes of each internal node on the path, root first.
	Levels [][16]codec.Hash
	// HasLeaf is set when the path ends at a compressed leaf, holding LeafID -> LeafHash.
	HasLeaf  bool
	LeafID   codec.UUID
	LeafHash codec.Hash
}

// Proof returns the proof of what t holds for id.
func (t DocumentTree) Proof(id codec.UUID) TreeProof {
	p := TreeProof{DocID: id}
	key := uuidKey(id)
	h := t.trieRootOrBuild()
	if h == nil {
		return p
	}
	defer runtime.KeepAlive(h)
	v := h.slab.view()
	r := h.root
	for depth := 0; r != 0; depth++ {
		if r&refLeaf != 0 {
			l := v.leaf(r)
			p.HasLeaf, p.LeafID, p.LeafHash = true, keyUUID(&l.id), l.content
			return p
		}
		children := v.children(r)
		var level [16]codec.Hash
		for i, c := range children {
			if c != 0 {
				level[i] = codec.Hash{Bytes: *v.hash(c)}
			}
		}
		p.Levels = append(p.Levels, level)
		if depth >= trieDepth {
			return p
		}
		r = children[keyNibble(&key, depth)]
	}
	return p
}

// ErrProofMismatch is a proof that does not lead to the tree hash it was checked against.
var ErrProofMismatch = errors.New("document tree proof: does not match the tree hash")

// VerifyTreeProof checks p against root, the hash of the tree it claims to be from, and reports
// what that tree holds for p.DocID: its content hash and true, or false when it holds nothing.
func VerifyTreeProof(root codec.Hash, p TreeProof) (codec.Hash, bool, error) {
	if len(p.Levels) > trieDepth {
		return codec.Hash{}, false, fmt.Errorf("document tree proof: %d levels, at most %d", len(p.Levels), trieDepth)
	}
	idBytes := p.DocID.Bytes()
	depth := len(p.Levels)
	var h [32]byte // an empty slot
	present := false
	var content codec.Hash
	if p.HasLeaf {
		leafBytes := p.LeafID.Bytes()
		// The leaf must sit on the id's path: it shares every nibble above where it hangs.
		for d := 0; d < depth; d++ {
			if nibbleAt(leafBytes, d) != nibbleAt(idBytes, d) {
				return codec.Hash{}, false, fmt.Errorf("document tree proof: leaf %s is not on the path to %s", p.LeafID, p.DocID)
			}
		}
		h = foldLeaf(leafBytes, p.LeafHash, depth)
		if p.LeafID == p.DocID {
			present, content = true, p.LeafHash
		}
	}
	for d := depth - 1; d >= 0; d-- {
		slot := nibbleAt(idBytes, d)
		if p.Levels[d][slot].Bytes != h {
			return codec.Hash{}, false, ErrProofMismatch
		}
		h = internalHashOfHashes(p.Levels[d])
	}
	if h != root.Bytes {
		return codec.Hash{}, false, ErrProofMismatch
	}
	return content, present, nil
}

func internalHashOfHashes(children [16]codec.Hash) [32]byte {
	var buf [1 + 16*32]byte
	buf[0] = 0x01
	for i, c := range children {
		copy(buf[1+i*32:], c.Bytes[:])
	}
	return sha256Sum(buf[:])
}

func sha256Sum(b []byte) [32]byte { return sha256.Sum256(b) }
