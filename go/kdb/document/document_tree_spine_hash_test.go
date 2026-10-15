package document

import (
	"crypto/sha256"
	"math/rand"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

// referenceOnlyChild is the single-child internal hash written out in full: 0x01 and sixteen
// 32-byte slots, only nib's occupied. Kotlin and TypeScript hash exactly this preimage.
func referenceOnlyChild(nib int, child [32]byte) [32]byte {
	var buf [1 + 16*32]byte
	buf[0] = 0x01
	copy(buf[1+nib*32:], child[:])
	return sha256.Sum256(buf[:])
}

// TestSpineHashMatchesTheFullPreimage: the spine hash is the hash of the full preimage, for every
// nibble and for arbitrary children, and so is a whole fold from any depth.
func TestSpineHashMatchesTheFullPreimage(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 2000; i++ {
		var c [32]byte
		r.Read(c[:])
		nib := i % 16
		if got, want := internalHashOfOnlyChild(nib, c), referenceOnlyChild(nib, c); got != want {
			t.Fatalf("nib %d: %x, want %x", nib, got, want)
		}
	}
	for i := 0; i < 200; i++ {
		var id [16]byte
		var content codec.Hash
		r.Read(id[:])
		r.Read(content.Bytes[:])
		depth := i % (trieDepth + 1)
		want := leafHash(id[:], content)
		for d := trieDepth - 1; d >= depth; d-- {
			want = referenceOnlyChild(nibbleAt(id[:], d), want)
		}
		if got := foldLeaf(id[:], content, depth); got != want {
			t.Fatalf("fold from depth %d: %x, want %x", depth, got, want)
		}
	}
}

// BenchmarkFoldLeaf: a new document's spine in a ~1M-document tree (compressed leaf at depth 5).
func BenchmarkFoldLeaf(b *testing.B) {
	r := rand.New(rand.NewSource(2))
	ids := make([][16]byte, 1024)
	for i := range ids {
		r.Read(ids[i][:])
	}
	var content codec.Hash
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		foldLeaf(ids[i%len(ids)][:], content, 5)
	}
}
