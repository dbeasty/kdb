package document

import (
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
)

// Merkle views of a DocumentTree, so two nodes can find the documents they hold differently by
// exchanging subtree hashes instead of whole trees - O(d log n) hashes for d differences
// (Merkle 1987; the anti-entropy of Dynamo, Cassandra and friends). The trie is already a Merkle
// tree; this exposes it.
//
// A position in the trie is a prefix of the document id's 32 hex nibbles ("" is the root). The
// hashes exposed are exactly the ones the tree hash is computed from, including for subtrees the
// trie stores compressed as a single leaf (their hash is folded, as the tree hash itself does), so
// two trees holding the same entries under a prefix report the same hash there whatever their
// internal shape.

// TreeNode is the subtree of a DocumentTree under Prefix.
type TreeNode struct {
	Prefix string
	// Hash is the subtree's hash; zero when the subtree is empty.
	Hash codec.Hash
	// Children are the 16 child subtrees' hashes (zero for an empty child). Zero when the subtree
	// is empty or Prefix is a full id.
	Children [16]codec.Hash
	// Entries lists the subtree's documents when it holds no more than the limit asked for;
	// nil when it holds more.
	Entries map[codec.UUID]codec.Hash
}

// ValidTreePrefix reports whether p is a nibble prefix: at most 32 lowercase hex digits.
func ValidTreePrefix(p string) bool {
	if len(p) > trieDepth {
		return false
	}
	for _, c := range p {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// Node describes t's subtree under prefix, listing its entries when it holds at most entryLimit.
func (t DocumentTree) Node(prefix string, entryLimit int) (TreeNode, error) {
	if !ValidTreePrefix(prefix) {
		return TreeNode{}, fmt.Errorf("document tree: %q is not a nibble prefix", prefix)
	}
	out := TreeNode{Prefix: prefix}
	depth := len(prefix)
	n := t.trieRootOrBuild()
	for d := 0; d < depth && n != nil; d++ {
		if n.leaf != nil {
			break
		}
		if n.children == nil {
			n = nil
			break
		}
		nib := int(hexNibble(prefix[d]))
		n = n.children[nib]
	}
	if n == nil {
		return out, nil
	}
	if n.leaf != nil {
		// A compressed leaf stands for everything below where it sits; it belongs to this
		// subtree only if its id carries the whole prefix.
		idBytes := n.leaf.uuid.Bytes()
		if !strings.HasPrefix(hex.EncodeToString(idBytes), prefix) {
			return out, nil
		}
		out.Hash = codec.Hash{Bytes: foldLeaf(idBytes, n.leaf.hash, depth)}
		if depth < trieDepth {
			out.Children[nibbleAt(idBytes, depth)] = codec.Hash{Bytes: foldLeaf(idBytes, n.leaf.hash, depth+1)}
		}
		if entryLimit >= 1 {
			out.Entries = map[codec.UUID]codec.Hash{n.leaf.uuid: n.leaf.hash}
		}
		return out, nil
	}
	out.Hash = codec.Hash{Bytes: n.hash}
	if n.children != nil {
		for i, c := range n.children {
			if c != nil {
				out.Children[i] = codec.Hash{Bytes: c.hash}
			}
		}
	}
	if entryLimit > 0 {
		entries := map[codec.UUID]codec.Hash{}
		small := trieWalkUntil(n, func(id codec.UUID, h codec.Hash) bool {
			entries[id] = h
			return len(entries) <= entryLimit
		})
		if small {
			out.Entries = entries
		}
	}
	return out, nil
}

func hexNibble(c byte) byte {
	if c <= '9' {
		return c - '0'
	}
	return c - 'a' + 10
}

// TreeDifference is one document two trees hold differently; a zero hash is absent on that side.
type TreeDifference struct {
	DocID  codec.UUID
	Local  codec.Hash
	Remote codec.Hash
}

// NodeSource answers TreeNode requests for a batch of prefixes, in order - a local tree, or a
// peer over the wire.
type NodeSource func(prefixes []string) ([]TreeNode, error)

// LocalNodes is a NodeSource over t.
func LocalNodes(t DocumentTree, entryLimit int) NodeSource {
	return func(prefixes []string) ([]TreeNode, error) {
		out := make([]TreeNode, len(prefixes))
		for i, p := range prefixes {
			n, err := t.Node(p, entryLimit)
			if err != nil {
				return nil, err
			}
			out[i] = n
		}
		return out, nil
	}
}

// DiffTrees finds every document local and remote hold differently, descending only into
// subtrees whose hashes differ: level by level, one batch request to each side per level, and at
// most 32 levels. Where both sides list a subtree's entries they are compared directly. The result
// is sorted by document id.
func DiffTrees(local, remote NodeSource) ([]TreeDifference, error) {
	var out []TreeDifference
	frontier := []string{""}
	for len(frontier) > 0 {
		ln, err := local(frontier)
		if err != nil {
			return nil, err
		}
		rn, err := remote(frontier)
		if err != nil {
			return nil, err
		}
		if len(ln) != len(frontier) || len(rn) != len(frontier) {
			return nil, fmt.Errorf("document tree diff: asked for %d nodes, got %d local and %d remote", len(frontier), len(ln), len(rn))
		}
		var next []string
		for i, prefix := range frontier {
			l, r := ln[i], rn[i]
			if l.Hash == r.Hash {
				continue
			}
			if (l.Entries != nil || l.Hash == (codec.Hash{})) && (r.Entries != nil || r.Hash == (codec.Hash{})) {
				out = append(out, diffEntries(l.Entries, r.Entries)...)
				continue
			}
			if len(prefix) == trieDepth {
				return nil, fmt.Errorf("document tree diff: subtree %s differs but lists no entries", prefix)
			}
			for nib := 0; nib < 16; nib++ {
				if l.Children[nib] != r.Children[nib] {
					next = append(next, prefix+string("0123456789abcdef"[nib]))
				}
			}
		}
		frontier = next
	}
	sort.Slice(out, func(i, j int) bool { return out[i].DocID.String() < out[j].DocID.String() })
	return out, nil
}

func diffEntries(l, r map[codec.UUID]codec.Hash) []TreeDifference {
	var out []TreeDifference
	for id, lh := range l {
		if rh := r[id]; rh != lh {
			out = append(out, TreeDifference{DocID: id, Local: lh, Remote: rh})
		}
	}
	for id, rh := range r {
		if _, ok := l[id]; !ok {
			out = append(out, TreeDifference{DocID: id, Remote: rh})
		}
	}
	return out
}
