package index

import (
	"bytes"
	"fmt"
	"math/rand"
	"slices"
	"testing"
)

// order stays the full sort of every interned key whether new keys arrive one at a time between
// reads (spliced in) or in batches (merged).
func TestKeyTableOrderMatchesAFullSort(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	kt := newKeyTable()
	for i := 0; i < 200; i++ {
		kt.intern(appendKeyEnc(nil, StringKey{Value: fmt.Sprintf("seed-%04d", rng.Intn(5000))}))
	}
	kt.order()
	for round := 0; round < 400; round++ {
		batch := 1
		if round%25 == 0 {
			batch = 200 // past the splice threshold: the merge path
		}
		for i := 0; i < batch; i++ {
			kt.intern(appendKeyEnc(nil, StringKey{Value: fmt.Sprintf("k-%06d", rng.Intn(1_000_000))}))
		}
		got := slices.Clone(kt.order())
		want := make([]uint32, kt.len())
		for i := range want {
			want[i] = uint32(i)
		}
		slices.SortFunc(want, func(a, b uint32) int { return bytes.Compare(kt.bytes(a), kt.bytes(b)) })
		if !slices.Equal(got, want) {
			t.Fatalf("round %d: order is not the sorted key table", round)
		}
	}
}

func BenchmarkKeyTableOrderAfterOneNewKey(b *testing.B) {
	kt := newKeyTable()
	for i := 0; i < 100_000; i++ {
		kt.intern(appendKeyEnc(nil, StringKey{Value: fmt.Sprintf("2026-10-%08d", i)}))
	}
	kt.order()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kt.intern(appendKeyEnc(nil, StringKey{Value: fmt.Sprintf("2026-11-%08d", i)}))
		kt.order()
	}
}
