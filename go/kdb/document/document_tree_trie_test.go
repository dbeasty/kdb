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
		for i := 0; i < b.N; i++ {
			if _, err := BuildDocumentTree(entries); err != nil {
				b.Fatal(err)
			}
		}
	})
}
