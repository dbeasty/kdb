package document

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

// WithSpinePrev reports exactly what HashFor reported before the update, and leaves the tree
// With would.
func TestWithSpinePrevReportsTheReplacedVersion(t *testing.T) {
	tree := EmptyDocumentTree()
	ids := make([]codec.UUID, 300)
	for i := range ids {
		ids[i], _ = codec.RandomUUID()
	}
	content := func(i, v int) codec.Hash {
		return codec.Hash{Bytes: [32]byte{byte(i), byte(i >> 8), byte(v), 1}}
	}
	for round := 0; round < 3; round++ {
		for i, id := range ids {
			if round == 2 && i%3 == 0 {
				continue // some documents keep their version from round 1
			}
			h := content(i, round)
			if round == 2 && i%3 == 1 {
				h = content(i, 1) // rewritten with identical content
			}
			wantPrev, wantHad := tree.HashFor(id)
			viaWith, err := tree.With(id, h)
			if err != nil {
				t.Fatal(err)
			}
			next, prev, had, err := tree.WithSpinePrev(id, h, NewLeafSpine(id, h))
			if err != nil {
				t.Fatal(err)
			}
			if had != wantHad || prev != wantPrev {
				t.Fatalf("round %d doc %d: prev (%v, %v), HashFor said (%v, %v)", round, i, prev.Hex()[:8], had, wantPrev.Hex()[:8], wantHad)
			}
			if next.TreeHash != viaWith.TreeHash || next.Size() != viaWith.Size() {
				t.Fatalf("round %d doc %d: tree differs from With's", round, i)
			}
			tree = next
		}
	}
}
