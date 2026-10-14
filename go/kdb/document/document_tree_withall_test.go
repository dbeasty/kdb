package document

import (
	"math/rand"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

// WithAll builds exactly the tree applying each entry with With would: same hash, size and
// entries, from any base, for batches that replace, collide on long prefixes, repeat a key, or
// arrive unsorted.
func TestWithAllMatchesWith(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	randID := func() codec.UUID { return codec.UUID{MSB: r.Int63(), LSB: r.Int63()} }
	// Ids that agree on their first prefix nibbles, so they share long spines.
	near := func(base codec.UUID, prefixBits uint) codec.UUID {
		mask := int64(-1) << (64 - prefixBits)
		return codec.UUID{MSB: base.MSB&mask | r.Int63()&^mask, LSB: r.Int63()}
	}
	for _, baseSize := range []int{0, 1, 2, 50, 3000} {
		base := EmptyDocumentTree()
		var baseIDs []codec.UUID
		for i := 0; i < baseSize; i++ {
			id := randID()
			baseIDs = append(baseIDs, id)
			var err error
			if base, err = base.With(id, randHash(r)); err != nil {
				t.Fatal(err)
			}
		}
		for _, batchSize := range []int{1, 2, 17, 600} {
			var batch []TreeEntry
			for i := 0; i < batchSize; i++ {
				var id codec.UUID
				switch {
				case len(baseIDs) > 0 && i%5 == 0:
					id = baseIDs[r.Intn(len(baseIDs))] // replace, new content
				case len(baseIDs) > 0 && i%7 == 0:
					id = baseIDs[r.Intn(len(baseIDs))]
					h, _ := base.HashFor(id)
					batch = append(batch, TreeEntry{DocID: id, ContentHash: h}) // replace, same content
					continue
				case i%3 == 0 && i > 0:
					id = near(batch[i-1].DocID, 60) // shares 15 nibbles with its neighbour
				case i%11 == 0 && i > 0:
					id = batch[r.Intn(i)].DocID // the same key twice: the later wins
				default:
					id = randID()
				}
				batch = append(batch, TreeEntry{DocID: id, ContentHash: randHash(r)})
			}
			want := base
			for _, e := range batch {
				var err error
				if want, err = want.With(e.DocID, e.ContentHash); err != nil {
					t.Fatal(err)
				}
			}
			got, err := base.WithAll(batch)
			if err != nil {
				t.Fatal(err)
			}
			if got.TreeHash != want.TreeHash || got.Size() != want.Size() {
				t.Fatalf("base %d batch %d: hash %s size %d, want %s size %d", baseSize, batchSize, got.TreeHash.Hex(), got.Size(), want.TreeHash.Hex(), want.Size())
			}
			wantEntries := want.MaterializedEntries()
			n := 0
			got.Walk(func(id codec.UUID, h codec.Hash) bool {
				n++
				if wantEntries[id] != h {
					t.Fatalf("base %d batch %d: entry %s differs", baseSize, batchSize, id)
				}
				return true
			})
			if n != len(wantEntries) {
				t.Fatalf("base %d batch %d: walked %d entries, want %d", baseSize, batchSize, n, len(wantEntries))
			}
			// The base is untouched: trees are persistent.
			if again, _ := base.WithAll(nil); again.TreeHash != base.TreeHash {
				t.Fatal("WithAll(nil) changed the tree")
			}
		}
	}
}
