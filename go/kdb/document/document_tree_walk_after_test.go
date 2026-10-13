package document

import (
	"bytes"
	"math/rand"
	"sort"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

func walkAfterEntries(r *rand.Rand, n int) map[codec.UUID]codec.Hash {
	out := make(map[codec.UUID]codec.Hash, n)
	for len(out) < n {
		id := codec.UUID{MSB: int64(r.Uint64()), LSB: int64(r.Uint64())}
		var h codec.Hash
		r.Read(h.Bytes[:])
		out[id] = h
	}
	return out
}

func sortedIDs(entries map[codec.UUID]codec.Hash) []codec.UUID {
	ids := make([]codec.UUID, 0, len(entries))
	for id := range entries {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return bytes.Compare(ids[i].Bytes(), ids[j].Bytes()) < 0 })
	return ids
}

// TestWalkAfterIsOrderedAndExact: WalkAfter visits exactly the ids after the cursor, ascending,
// for cursors in the tree, between its ids and beyond both ends - on a trie built in bulk, one
// grown by With, and one built from a flat map. String order and byte order agree.
func TestWalkAfterIsOrderedAndExact(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for _, n := range []int{0, 1, 2, 17, 300, 5000} {
		entries := walkAfterEntries(r, n)
		ids := sortedIDs(entries)
		for i := 1; i < len(ids); i++ {
			if ids[i-1].String() >= ids[i].String() {
				t.Fatalf("string order disagrees with byte order at %s, %s", ids[i-1], ids[i])
			}
		}
		bulk, _ := BuildTrieDocumentTree(entries)
		mapped, _ := BuildDocumentTree(entries)
		grown := EmptyDocumentTree()
		for id, h := range entries {
			grown, _ = grown.With(id, h)
		}
		cursors := []codec.UUID{{MSB: 0, LSB: 0}, {MSB: -1, LSB: -1}}
		for k := 0; k < 50 && len(ids) > 0; k++ {
			cursors = append(cursors, ids[r.Intn(len(ids))])
			cursors = append(cursors, codec.UUID{MSB: int64(r.Uint64()), LSB: int64(r.Uint64())})
		}
		for name, tree := range map[string]DocumentTree{"bulk": bulk, "mapped": mapped, "grown": grown} {
			var all []codec.UUID
			tree.WalkOrdered(func(id codec.UUID, _ codec.Hash) bool { all = append(all, id); return true })
			if len(all) != len(ids) {
				t.Fatalf("%s n=%d: WalkOrdered saw %d, want %d", name, n, len(all), len(ids))
			}
			for i := range ids {
				if all[i] != ids[i] {
					t.Fatalf("%s n=%d: WalkOrdered out of order at %d", name, n, i)
				}
			}
			for _, c := range cursors {
				want := sort.Search(len(ids), func(i int) bool { return bytes.Compare(ids[i].Bytes(), c.Bytes()) > 0 })
				var got []codec.UUID
				tree.WalkAfter(c, func(id codec.UUID, h codec.Hash) bool {
					if entries[id] != h {
						t.Fatalf("%s: wrong hash for %s", name, id)
					}
					got = append(got, id)
					return true
				})
				if len(got) != len(ids)-want {
					t.Fatalf("%s n=%d cursor %s: %d ids after, want %d", name, n, c, len(got), len(ids)-want)
				}
				for i := range got {
					if got[i] != ids[want+i] {
						t.Fatalf("%s n=%d cursor %s: wrong id at %d", name, n, c, i)
					}
				}
			}
		}
	}
}

// TestWalkAfterCostsThePage: reading a page of k ids after a cursor enters O(depth + k) nodes, the
// same at 2,000 documents as at 200,000 - not a walk of everything before the cursor.
func TestWalkAfterCostsThePage(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	const page = 100
	var visitedAt []int
	for _, n := range []int{2_000, 200_000} {
		entries := walkAfterEntries(r, n)
		ids := sortedIDs(entries)
		tree, _ := BuildTrieDocumentTree(entries)
		worst := 0
		for _, at := range []int{0, n / 3, n / 2, n - page - 1} {
			seen := 0
			visited := tree.WalkAfterCounting(ids[at], func(codec.UUID, codec.Hash) bool {
				seen++
				return seen < page
			})
			if seen != page {
				t.Fatalf("n=%d: page of %d, got %d", n, page, seen)
			}
			worst = max(worst, visited)
		}
		visitedAt = append(visitedAt, worst)
	}
	// A 16-ary trie over random ids is about log16(n) deep, and a page of 100 leaves sits under a
	// handful of internal nodes: a few hundred nodes either way.
	if visitedAt[1] > 2*visitedAt[0]+64 || visitedAt[1] > 1000 {
		t.Fatalf("a page entered %d nodes at 2k documents and %d at 200k: cost grows with the tree", visitedAt[0], visitedAt[1])
	}
}
