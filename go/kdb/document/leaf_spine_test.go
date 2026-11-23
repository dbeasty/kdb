package document

import (
	"crypto/sha256"
	"math/rand/v2"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

// A tree built with spines folded in advance is the tree With builds: same root after every
// insert and update, including the ones that land deeper because a sibling split their leaf, and
// a spine for some other version is ignored rather than trusted.
func TestWithSpineBuildsTheTreeWithBuilds(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	plain, spined := EmptyDocumentTree(), EmptyDocumentTree()
	var ids []codec.UUID
	content := func() codec.Hash {
		var b [8]byte
		for i := range b {
			b[i] = byte(r.Uint32())
		}
		h, _ := codec.HashFromBytes(spineTestHash(b[:]))
		return h
	}
	for step := 0; step < 3000; step++ {
		var id codec.UUID
		if len(ids) > 0 && r.IntN(3) == 0 {
			id = ids[r.IntN(len(ids))] // an update
		} else {
			id = codec.UUID{MSB: int64(r.Uint64()), LSB: int64(r.Uint64())}
			if r.IntN(4) == 0 && len(ids) > 0 {
				// Shares a long prefix with an existing id, so the existing leaf moves deeper.
				id = codec.UUID{MSB: ids[len(ids)-1].MSB, LSB: int64(r.Uint64())}
			}
			ids = append(ids, id)
		}
		h := content()
		var err error
		if plain, err = plain.With(id, h); err != nil {
			t.Fatal(err)
		}
		spine := NewLeafSpine(id, h)
		if step%7 == 0 {
			spine = NewLeafSpine(id, content()) // stale: must not be used
		}
		if spined, err = spined.WithSpine(id, h, spine); err != nil {
			t.Fatal(err)
		}
		if plain.TreeHash != spined.TreeHash {
			t.Fatalf("step %d: root %s with a spine, %s without", step, spined.TreeHash.Hex(), plain.TreeHash.Hex())
		}
	}
}

func TestLeafSpineMatchesFoldLeaf(t *testing.T) {
	id := codec.UUID{MSB: 0x0123456789abcdef, LSB: -0x123456789abcdef}
	h, _ := codec.HashFromBytes(spineTestHash([]byte("v1")))
	sp := NewLeafSpine(id, h)
	key := uuidKey(id)
	for d := 0; d <= trieDepth; d++ {
		if sp.at[d] != foldLeaf(key[:], h, d) {
			t.Fatalf("depth %d differs", d)
		}
	}
}

func spineTestHash(b []byte) []byte {
	s := sha256.Sum256(b)
	return s[:]
}
