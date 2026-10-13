package document

import (
	"math/rand"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

func randUUID(t *testing.T) codec.UUID {
	t.Helper()
	id, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func randHash(r *rand.Rand) codec.Hash {
	var b [32]byte
	r.Read(b[:])
	return codec.Hash{Bytes: b}
}

// TestTrieMatchesFullRebuild is the core parity check: an incrementally
// built tree (repeated With calls) must hash identically to one built
// from scratch via BuildDocumentTree with the same final entries,
// regardless of the order entries were inserted in.
func TestTrieMatchesFullRebuild(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	n := 500
	ids := make([]codec.UUID, n)
	hashes := make([]codec.Hash, n)
	entries := make(map[codec.UUID]codec.Hash, n)
	for i := 0; i < n; i++ {
		ids[i] = randUUID(t)
		hashes[i] = randHash(r)
		entries[ids[i]] = hashes[i]
	}

	incremental := EmptyDocumentTree()
	var err error
	for i := 0; i < n; i++ {
		incremental, err = incremental.With(ids[i], hashes[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	fromScratch, err := BuildDocumentTree(entries)
	if err != nil {
		t.Fatal(err)
	}

	if incremental.TreeHash.Hex() != fromScratch.TreeHash.Hex() {
		t.Fatalf("incremental hash %s != from-scratch hash %s", incremental.TreeHash.Hex(), fromScratch.TreeHash.Hex())
	}
}

// TestTrieInsertionOrderIndependent builds the same entry set via two
// different random insertion orders and requires identical hashes.
func TestTrieInsertionOrderIndependent(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	n := 300
	ids := make([]codec.UUID, n)
	hashes := make([]codec.Hash, n)
	for i := 0; i < n; i++ {
		ids[i] = randUUID(t)
		hashes[i] = randHash(r)
	}

	order1 := r.Perm(n)
	order2 := r.Perm(n)

	build := func(order []int) DocumentTree {
		tree := EmptyDocumentTree()
		for _, i := range order {
			var err error
			tree, err = tree.With(ids[i], hashes[i])
			if err != nil {
				t.Fatal(err)
			}
		}
		return tree
	}

	a := build(order1)
	b := build(order2)
	if a.TreeHash.Hex() != b.TreeHash.Hex() {
		t.Fatalf("insertion-order sensitivity: %s != %s", a.TreeHash.Hex(), b.TreeHash.Hex())
	}
}

// TestTrieUpdateReplacesNotDuplicates re-inserting an existing docID with
// a new content hash must change the tree hash and not affect Size.
func TestTrieUpdateReplacesNotDuplicates(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	id := randUUID(t)
	h1 := randHash(r)
	h2 := randHash(r)

	tree, err := EmptyDocumentTree().With(id, h1)
	if err != nil {
		t.Fatal(err)
	}
	if tree.Size() != 1 {
		t.Fatalf("size=%d, want 1", tree.Size())
	}
	firstHash := tree.TreeHash

	tree, err = tree.With(id, h2)
	if err != nil {
		t.Fatal(err)
	}
	if tree.Size() != 1 {
		t.Fatalf("size after update=%d, want 1 (not a duplicate insert)", tree.Size())
	}
	if tree.TreeHash.Hex() == firstHash.Hex() {
		t.Fatal("expected tree hash to change after updating a document's content hash")
	}
	got, ok := tree.HashFor(id)
	if !ok || got.Hex() != h2.Hex() {
		t.Fatalf("HashFor after update = %v, %v; want %v, true", got, ok, h2)
	}
}

// TestTrieDeleteThenReinsertMatchesDirect deletes then reinserts a subset
// and checks the result matches building that subset directly - proving
// delete's node-collapsing logic doesn't leave stale structure behind.
func TestTrieDeleteThenReinsertMatchesDirect(t *testing.T) {
	r := rand.New(rand.NewSource(4))
	n := 200
	ids := make([]codec.UUID, n)
	hashes := make([]codec.Hash, n)
	entries := make(map[codec.UUID]codec.Hash, n)
	for i := 0; i < n; i++ {
		ids[i] = randUUID(t)
		hashes[i] = randHash(r)
		entries[ids[i]] = hashes[i]
	}

	tree := EmptyDocumentTree()
	var err error
	for i := 0; i < n; i++ {
		tree, err = tree.With(ids[i], hashes[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	// Delete half.
	toDelete := ids[:n/2]
	for _, id := range toDelete {
		tree, err = tree.Without(id)
		if err != nil {
			t.Fatal(err)
		}
		delete(entries, id)
	}

	direct, err := BuildDocumentTree(entries)
	if err != nil {
		t.Fatal(err)
	}
	if tree.TreeHash.Hex() != direct.TreeHash.Hex() {
		t.Fatalf("after deletes: incremental %s != direct rebuild %s", tree.TreeHash.Hex(), direct.TreeHash.Hex())
	}
	if tree.Size() != len(entries) {
		t.Fatalf("size=%d, want %d", tree.Size(), len(entries))
	}

	// Delete everything and confirm collapse back to the canonical empty tree.
	for id := range entries {
		tree, err = tree.Without(id)
		if err != nil {
			t.Fatal(err)
		}
	}
	if tree.TreeHash.Hex() != EmptyDocumentTree().TreeHash.Hex() {
		t.Fatal("expected fully-deleted tree to collapse to the canonical empty tree hash")
	}
	if tree.Size() != 0 {
		t.Fatalf("size=%d after deleting everything, want 0", tree.Size())
	}
}

// TestTrieDeleteAbsentIsNoop deleting a docID never present must not
// change the tree hash.
func TestTrieDeleteAbsentIsNoop(t *testing.T) {
	r := rand.New(rand.NewSource(5))
	id := randUUID(t)
	tree, err := EmptyDocumentTree().With(id, randHash(r))
	if err != nil {
		t.Fatal(err)
	}
	before := tree.TreeHash
	absent := randUUID(t)
	after, err := tree.Without(absent)
	if err != nil {
		t.Fatal(err)
	}
	if after.TreeHash.Hex() != before.Hex() {
		t.Fatal("deleting an absent docID changed the tree hash")
	}
}

func BenchmarkIncrementalWith_vs_FullRebuild(b *testing.B) {
	r := rand.New(rand.NewSource(6))
	base := EmptyDocumentTree()
	entries := make(map[codec.UUID]codec.Hash, 5000)
	for i := 0; i < 5000; i++ {
		id, _ := codec.RandomUUID()
		h := randHash(r)
		entries[id] = h
		var err error
		base, err = base.With(id, h)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.Run("incremental_With_singleEntry", func(b *testing.B) {
		b.ReportAllocs()
		tree := base
		for i := 0; i < b.N; i++ {
			id, _ := codec.RandomUUID()
			var err error
			tree, err = tree.With(id, randHash(r))
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("fullRebuild_5000Entries", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := BuildDocumentTree(entries); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// trieBulkMatchesInsert: the bulk, parallel build must be the same trie, node for node, as
// inserting the entries one at a time - the hash every stored tree and the Kotlin port agree on.
func TestTrieBulkBuildMatchesInsert(t *testing.T) {
	for _, n := range []int{0, 1, 2, 3, 17, 255, 4095, 4096, 20000} {
		entries := make(map[codec.UUID]codec.Hash, n)
		for i := 0; i < n; i++ {
			id, _ := codec.RandomUUID()
			var h codec.Hash
			h.Bytes[0], h.Bytes[1], h.Bytes[2] = byte(i), byte(i>>8), byte(i>>16)
			entries[id] = h
		}
		// Keys sharing long prefixes exercise the single-child chains.
		if n > 3 {
			base, _ := codec.RandomUUID()
			b := base.Bytes()
			for k := 0; k < 3; k++ {
				c := append([]byte(nil), b...)
				c[15] ^= byte(k + 1)
				id, _ := codec.UUIDFromBytes(c)
				entries[id] = codec.Hash{}
			}
		}
		var inserted *trieTree
		var reference *ptrNode
		for id, h := range entries {
			inserted = trieWith(inserted, id, h)
			reference = ptrInsert(reference, id, h)
		}
		bulk := trieBuild(entries)
		if !slabMatchesPtr(inserted, reference) || !slabMatchesPtr(bulk, reference) {
			t.Fatalf("n=%d: bulk-built or inserted trie differs from the reference", n)
		}
		if inserted.rootHash() != bulk.rootHash() || bulk.rootHash() != ptrTreeHash(reference) {
			t.Fatalf("n=%d: tree hash differs", n)
		}
		tree, _ := BuildTrieDocumentTree(entries)
		full, _ := BuildDocumentTree(entries)
		if tree.TreeHash != full.TreeHash || tree.Size() != full.Size() || tree.Entries != nil {
			t.Fatalf("n=%d: trie-only tree disagrees with BuildDocumentTree", n)
		}
		for id, h := range entries {
			if got, ok := tree.HashFor(id); !ok || got != h {
				t.Fatalf("n=%d: trie-only tree lost %v", n, id)
			}
		}
	}
}

// A packed tree round-trips to the same tree, from either kind of source tree, and malformed
// packing is an error rather than a different tree.
func TestPackedTreeRoundTrip(t *testing.T) {
	for _, n := range []int{0, 1, 5, 5000} {
		entries := make(map[codec.UUID]codec.Hash, n)
		for i := 0; i < n; i++ {
			id, _ := codec.RandomUUID()
			var h codec.Hash
			h.Bytes[0] = byte(i)
			entries[id] = h
		}
		mapTree, _ := BuildDocumentTree(entries) // Entries-backed: unordered walk
		trieTree, _ := BuildTrieDocumentTree(entries)
		for name, src := range map[string]DocumentTree{"map": mapTree, "trie": trieTree} {
			packed := src.AppendPacked(nil)
			if len(packed) != n*PackedTreeEntrySize {
				t.Fatalf("%s n=%d: packed %d bytes", name, n, len(packed))
			}
			got, err := BuildTrieDocumentTreePacked(packed)
			if err != nil {
				t.Fatalf("%s n=%d: %v", name, n, err)
			}
			if got.TreeHash != src.TreeHash || got.Size() != n {
				t.Fatalf("%s n=%d: packed round trip changed the tree", name, n)
			}
		}
	}
	two, _ := BuildTrieDocumentTree(map[codec.UUID]codec.Hash{{MSB: 1}: {}, {MSB: 2}: {}})
	p := two.AppendPacked(nil)
	swapped := append(append([]byte(nil), p[PackedTreeEntrySize:]...), p[:PackedTreeEntrySize]...)
	if _, err := BuildTrieDocumentTreePacked(swapped); err == nil {
		t.Fatal("out-of-order packed entries accepted")
	}
	if _, err := BuildTrieDocumentTreePacked(p[:10]); err == nil {
		t.Fatal("a ragged packed tree accepted")
	}
}
