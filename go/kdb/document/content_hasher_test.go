package document

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

// TestContentHasherMatchesContentHash pins ContentHasher to the generic codec path byte for
// byte, across every LEB128 length boundary of the body and its JSON, and for text that is not
// valid JSON (a cold read hashes a patch exactly as stored, valid or not).
func TestContentHasherMatchesContentHash(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	var h ContentHasher
	lengths := []int{0, 1, 2, 100, 101, 102, 103, 104, 105, 126, 127, 128, 129, 16_000,
		16_359, 16_360, 16_361, 16_362, 16_383, 16_384, 16_385, 2_097_100, 2_097_151, 2_097_152}
	for _, n := range lengths {
		for trial := 0; trial < 2; trial++ {
			id := codec.UUID{MSB: rng.Int63() - rng.Int63(), LSB: rng.Int63() - rng.Int63()}
			var text string
			if trial == 0 {
				text = strings.Repeat("x", n)
			} else {
				b := make([]byte, n)
				rng.Read(b)
				text = string(b)
			}
			want, err := Document{ID: id, JSON: text}.ContentHash()
			if err != nil {
				t.Fatal(err)
			}
			if got := h.Hash(id, text); got != want {
				t.Fatalf("len %d trial %d: ContentHasher %s, ContentHash %s", n, trial, got.Hex(), want.Hex())
			}
		}
	}
	for _, text := range []string{`{}`, `{"a":1}`, `{"é":"日本語"}`, `not json`, `[1,2]`} {
		id := codec.UUID{MSB: -1, LSB: 1}
		want, _ := Document{ID: id, JSON: text}.ContentHash()
		if got := h.Hash(id, text); got != want {
			t.Fatalf("%q: hashes differ", text)
		}
	}
}

func TestContentHasherDoesNotAllocateOnceWarm(t *testing.T) {
	var h ContentHasher
	id := codec.UUID{MSB: 1, LSB: 2}
	text := `{"name":"warm","n":12345}`
	h.Hash(id, text)
	if n := testing.AllocsPerRun(100, func() { h.Hash(id, text) }); n != 0 {
		t.Fatalf("%v allocations per hash, want 0", n)
	}
}

func BenchmarkContentHash(b *testing.B) {
	id := codec.UUID{MSB: 1, LSB: 2}
	text := `{"name":"benchmark","tags":["a","b","c"],"n":12345,"nested":{"x":1.5,"y":"z"}}`
	b.Run("generic", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = Document{ID: id, JSON: text}.ContentHash()
		}
	})
	b.Run("hasher", func(b *testing.B) {
		b.ReportAllocs()
		var h ContentHasher
		for i := 0; i < b.N; i++ {
			h.Hash(id, text)
		}
	})
}
