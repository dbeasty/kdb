package peersync

import (
	"bytes"
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// referenceTopoSort is topoSort as it was written before it went index-based: Kahn's algorithm
// over maps, the ready set re-sorted by hash bytes before each pick.
func referenceTopoSort(set map[codec.Hash]document.Commit) []document.Commit {
	pending := map[codec.Hash]int{}
	children := map[codec.Hash][]codec.Hash{}
	for h, c := range set {
		n := 0
		for _, p := range c.ParentHashes {
			if _, ok := set[p]; ok {
				n++
				children[p] = append(children[p], h)
			}
		}
		pending[h] = n
	}
	var ready []codec.Hash
	for h, n := range pending {
		if n == 0 {
			ready = append(ready, h)
		}
	}
	var out []document.Commit
	for len(ready) > 0 {
		sort.Slice(ready, func(i, j int) bool { return bytes.Compare(ready[i].Bytes[:], ready[j].Bytes[:]) < 0 })
		h := ready[0]
		ready = ready[1:]
		out = append(out, set[h])
		for _, ch := range children[h] {
			pending[ch]--
			if pending[ch] == 0 {
				ready = append(ready, ch)
			}
		}
	}
	return out
}

// TestTopoSortMatchesReference holds the index-based topoSort, and commitsBetween's in-place use
// of it, to the map-based original on random DAGs: chains, fans, merges, parents named twice and
// parents outside the set.
func TestTopoSortMatchesReference(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	for round := 0; round < 300; round++ {
		n := r.Intn(200)
		var hs []codec.Hash
		set := map[codec.Hash]document.Commit{}
		for i := 0; i < n; i++ {
			var h codec.Hash
			r.Read(h.Bytes[:])
			c := document.Commit{Hash: h}
			for k := r.Intn(4); k > 0; k-- {
				switch {
				case len(hs) > 0 && r.Intn(5) > 0:
					c.ParentHashes = append(c.ParentHashes, hs[r.Intn(len(hs))])
				default:
					var outside codec.Hash
					r.Read(outside.Bytes[:])
					c.ParentHashes = append(c.ParentHashes, outside)
				}
			}
			if len(c.ParentHashes) > 0 && r.Intn(10) == 0 {
				c.ParentHashes = append(c.ParentHashes, c.ParentHashes[0])
			}
			hs = append(hs, h)
			set[h] = c
		}
		want := referenceTopoSort(set)
		if got := topoSort(set); !reflect.DeepEqual(hashesOf(got), hashesOf(want)) {
			t.Fatalf("round %d: topoSort order differs from the reference", round)
		}
		// commitsBetween's form: a list in discovery order, sorted in place.
		list := make([]document.Commit, 0, n)
		index := map[codec.Hash]int32{}
		for _, i := range r.Perm(n) {
			index[hs[i]] = int32(len(list))
			list = append(list, set[hs[i]])
		}
		if got := topoSortIndexed(list, index); !reflect.DeepEqual(hashesOf(got), hashesOf(want)) {
			t.Fatalf("round %d: in-place topoSortIndexed order differs from the reference", round)
		}
	}
}
