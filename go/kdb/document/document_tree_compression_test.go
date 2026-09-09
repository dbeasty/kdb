package document

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

func idAndHash(t testing.TB, i int) (codec.UUID, codec.Hash) {
	t.Helper()
	id, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	h, err := codec.HashFromBytes(SHA256Digest([]byte(fmt.Sprintf("content-%d", i))))
	if err != nil {
		t.Fatal(err)
	}
	return id, h
}

// TestTreeMemoryPerDocumentStaysBounded is the regression test for the reason leaf compression
// exists. Every entry used to own a private 32-node spine, each node carrying a 16-pointer
// array, so a namespace cost ~4,900 bytes of live heap per document whatever the documents
// contained - 932MB at 200,000 documents of ~20 bytes each.
//
// Asserted as bytes per document rather than a total, so it fails on the defect (cost that
// scales with trie depth) rather than on the size of the machine's heap at the time.
func TestTreeMemoryPerDocumentStaysBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates tens of MB")
	}
	const n = 50000
	// Generous: the measured figure is ~160 bytes/doc, and the defect this guards against was
	// ~4,900. Anything under this ceiling means compression is working; the gap between them
	// is deliberate headroom for allocator and map behaviour, not slack in the claim.
	const maxBytesPerDoc = 600

	runtime.GC()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	tree := EmptyDocumentTree()
	for i := 0; i < n; i++ {
		id, h := idAndHash(t, i)
		var err error
		tree, err = tree.With(id, h)
		if err != nil {
			t.Fatal(err)
		}
	}

	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&after)

	perDoc := float64(after.HeapAlloc-before.HeapAlloc) / float64(n)
	t.Logf("%d documents: %.1f MB live, %.0f bytes/document",
		n, float64(after.HeapAlloc-before.HeapAlloc)/(1<<20), perDoc)
	if tree.Size() != n {
		t.Fatalf("tree holds %d entries, want %d", tree.Size(), n)
	}
	if perDoc > maxBytesPerDoc {
		t.Fatalf("%.0f bytes of live heap per document (ceiling %d); "+
			"entries are carrying a full-depth spine again - see trieNode's compression invariant",
			perDoc, maxBytesPerDoc)
	}
}

// TestCompressedLeafHashesMatchFullDepthSpine is the property that lets compression be an
// internal change rather than a format break: a compressed leaf must carry exactly the hash the
// 32-node spine it replaces would have produced.
//
// Verified against an independent fold rather than against the trie's own output, so it cannot
// pass by both sides sharing a mistake.
func TestCompressedLeafHashesMatchFullDepthSpine(t *testing.T) {
	id, h := idAndHash(t, 0)
	tree, err := EmptyDocumentTree().With(id, h)
	if err != nil {
		t.Fatal(err)
	}

	// Independent reconstruction: hash the leaf, then apply one 16-slot internal hash per
	// level, all siblings absent, exactly as the uncompressed trie did.
	want := leafHash(id.Bytes(), h)
	for d := trieDepth - 1; d >= 0; d-- {
		var children [16]*trieNode
		children[nibbleAt(id.Bytes(), d)] = &trieNode{hash: want}
		want = internalHash(&children)
	}
	if tree.TreeHash.Bytes != want {
		t.Fatalf("single-entry tree hash changed under compression:\n got %x\nwant %x",
			tree.TreeHash.Bytes, want)
	}
}

// trieNodeCount counts allocated nodes. Structural on purpose: a compressed leaf hashes
// *identically* to the spine it replaces - that equivalence is the whole premise - so no
// assertion about hashes can tell whether compression actually happened. Only counting nodes
// can.
func trieNodeCount(n *trieNode) int {
	if n == nil {
		return 0
	}
	c := 1
	if n.children != nil {
		for _, ch := range n.children {
			c += trieNodeCount(ch)
		}
	}
	return c
}

// TestDeleteRecompressesTheSpine covers the half of the invariant that is easy to leave out.
// Compression on insert alone would let the spines grow back one deletion at a time in a
// namespace that churns: a subtree collapsing to one entry has to become a leaf again, or the
// memory saving decays with every delete.
func TestDeleteRecompressesTheSpine(t *testing.T) {
	const n = 2000
	tree := EmptyDocumentTree()
	ids := make([]codec.UUID, 0, n)
	for i := 0; i < n; i++ {
		id, h := idAndHash(t, i)
		var err error
		if tree, err = tree.With(id, h); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}

	// Delete all but one. What is left must be indistinguishable from having inserted that one
	// entry into an empty tree - same hash, and therefore the same compressed shape.
	for _, id := range ids[1:] {
		var err error
		if tree, err = tree.Without(id); err != nil {
			t.Fatal(err)
		}
	}
	survivor := ids[0]
	survivorHash, ok := tree.HashFor(survivor)
	if !ok {
		t.Fatal("the entry that was never deleted is gone")
	}
	fresh, err := EmptyDocumentTree().With(survivor, survivorHash)
	if err != nil {
		t.Fatal(err)
	}
	if tree.TreeHash != fresh.TreeHash {
		t.Fatalf("a tree emptied down to one entry does not match that entry inserted fresh:\n"+
			" got %x\nwant %x", tree.TreeHash.Bytes, fresh.TreeHash.Bytes)
	}
	if tree.Size() != 1 {
		t.Fatalf("tree reports %d entries after deleting down to one", tree.Size())
	}
	// The hashes above would match even with the spine left in place, so the memory claim needs
	// its own assertion: one entry means one node.
	if got := trieNodeCount(tree.trieRoot); got != 1 {
		t.Fatalf("a tree holding one entry is made of %d nodes, want 1; "+
			"delete is not re-compressing, so the spines grow back as a namespace churns", got)
	}

	// And emptying it entirely must return the empty-tree hash, not a husk of internal nodes.
	if tree, err = tree.Without(survivor); err != nil {
		t.Fatal(err)
	}
	if tree.TreeHash != EmptyDocumentTree().TreeHash {
		t.Fatalf("fully emptied tree does not hash as empty: got %x", tree.TreeHash.Bytes)
	}
}
