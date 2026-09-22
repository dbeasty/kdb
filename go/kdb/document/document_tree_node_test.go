package document

import (
	"encoding/hex"
	"math/rand"
	"sort"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

func randomEntries(r *rand.Rand, n int) map[codec.UUID]codec.Hash {
	out := make(map[codec.UUID]codec.Hash, n)
	for len(out) < n {
		id := codec.UUID{MSB: r.Int63(), LSB: r.Int63()}
		var h codec.Hash
		r.Read(h.Bytes[:])
		out[id] = h
	}
	return out
}

func mustTree(t *testing.T, e map[codec.UUID]codec.Hash) DocumentTree {
	t.Helper()
	tree, err := BuildDocumentTree(e)
	if err != nil {
		t.Fatal(err)
	}
	return tree
}

func bruteDiff(l, r map[codec.UUID]codec.Hash) []TreeDifference {
	out := diffEntries(l, r)
	sort.Slice(out, func(i, j int) bool { return out[i].DocID.String() < out[j].DocID.String() })
	return out
}

// The root node's hash is the tree hash; a child's hash is the node it names.
func TestTreeNodeHashesAreTheTreesOwn(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for _, n := range []int{0, 1, 2, 17, 300} {
		tree := mustTree(t, randomEntries(r, n))
		root, err := tree.Node("", 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		if root.Hash != tree.TreeHash {
			t.Fatalf("n=%d: root node hash %s, tree hash %s", n, root.Hash.Hex(), tree.TreeHash.Hex())
		}
		if len(root.Entries) != n {
			t.Fatalf("n=%d: root lists %d entries", n, len(root.Entries))
		}
		for nib := 0; nib < 16; nib++ {
			child, err := tree.Node(string("0123456789abcdef"[nib]), 0)
			if err != nil {
				t.Fatal(err)
			}
			if child.Hash != root.Children[nib] {
				t.Fatalf("n=%d nibble %x: child node %s, parent says %s", n, nib, child.Hash.Hex(), root.Children[nib].Hex())
			}
		}
	}
}

// A subtree the trie stores as one compressed leaf reports, at every depth down to the full id,
// the same hash an uncompressed trie would - so shape never makes two equal subtrees look unequal.
func TestTreeNodeFoldsACompressedLeafAtEveryDepth(t *testing.T) {
	id := codec.UUID{MSB: 0x0123456789abcdef, LSB: 0x0fedcba987654321}
	var h codec.Hash
	h.Bytes[0] = 7
	tree := mustTree(t, map[codec.UUID]codec.Hash{id: h})
	full := hex.EncodeToString(id.Bytes())
	for depth := 0; depth <= 32; depth++ {
		n, err := tree.Node(full[:depth], 1)
		if err != nil {
			t.Fatal(err)
		}
		if want := (codec.Hash{Bytes: foldLeaf(id.Bytes(), h, depth)}); n.Hash != want || n.Entries[id] != h {
			t.Fatalf("depth %d: hash %s want %s, entries %v", depth, n.Hash.Hex(), want.Hex(), n.Entries)
		}
		if depth < 32 {
			other := full[:depth] + string("0123456789abcdef"[(hexNibble(full[depth])+1)%16])
			if o, _ := tree.Node(other, 1); o.Hash != (codec.Hash{}) || o.Entries != nil {
				t.Fatalf("depth %d: a sibling prefix %s should be empty, got %+v", depth, other, o)
			}
		}
	}
	// And the compressed leaf agrees with a tree where the same entry has a neighbour forcing
	// real nodes along most of its path.
	neighbour := codec.UUID{MSB: id.MSB, LSB: id.LSB ^ 1}
	wide := mustTree(t, map[codec.UUID]codec.Hash{id: h, neighbour: h})
	b, _ := wide.Node(full[:32], 4)
	c, _ := tree.Node(full[:32], 4)
	if b.Hash != c.Hash {
		t.Fatalf("the full-id subtree differs between shapes: %s vs %s", b.Hash.Hex(), c.Hash.Hex())
	}
}

func TestDiffTreesFindsExactlyTheDifferingDocuments(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	for trial := 0; trial < 40; trial++ {
		base := randomEntries(r, r.Intn(400))
		local := map[codec.UUID]codec.Hash{}
		remote := map[codec.UUID]codec.Hash{}
		for id, h := range base {
			local[id], remote[id] = h, h
		}
		for i := 0; i < r.Intn(12); i++ {
			switch r.Intn(3) {
			case 0: // changed on one side
				for id := range local {
					var h codec.Hash
					r.Read(h.Bytes[:])
					remote[id] = h
					break
				}
			case 1: // only local
				for id, h := range randomEntries(r, 1) {
					local[id] = h
				}
			case 2: // only remote
				for id, h := range randomEntries(r, 1) {
					remote[id] = h
				}
			}
		}
		lt, rt := mustTree(t, local), mustTree(t, remote)
		for _, limit := range []int{1, 8, 64} {
			calls := 0
			remoteSrc := LocalNodes(rt, limit)
			got, err := DiffTrees(LocalNodes(lt, limit), func(p []string) ([]TreeNode, error) { calls++; return remoteSrc(p) })
			if err != nil {
				t.Fatal(err)
			}
			want := bruteDiff(local, remote)
			if len(got) != len(want) {
				t.Fatalf("trial %d limit %d: got %d differences, want %d", trial, limit, len(got), len(want))
			}
			for i := range got {
				if got[i] != want[i] {
					t.Fatalf("trial %d limit %d: difference %d: got %+v want %+v", trial, limit, i, got[i], want[i])
				}
			}
			if calls > 33 {
				t.Fatalf("trial %d: %d round trips; the diff must be one per level", trial, calls)
			}
		}
	}
}

func TestDiffOfEqualTreesIsOneRoundTrip(t *testing.T) {
	e := randomEntries(rand.New(rand.NewSource(3)), 1000)
	tree := mustTree(t, e)
	calls := 0
	src := LocalNodes(tree, 16)
	got, err := DiffTrees(LocalNodes(tree, 16), func(p []string) ([]TreeNode, error) { calls++; return src(p) })
	if err != nil || len(got) != 0 || calls != 1 {
		t.Fatalf("equal trees: %d differences, %d round trips, %v", len(got), calls, err)
	}
}

func TestTreeNodeRejectsABadPrefix(t *testing.T) {
	tree := mustTree(t, nil)
	for _, p := range []string{"G", "ABC", "0123456789abcdef0123456789abcdef0"} {
		if _, err := tree.Node(p, 1); err == nil {
			t.Fatalf("prefix %q should be refused", p)
		}
	}
}
