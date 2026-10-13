package document

import (
	"bytes"
	"math/rand"
	"runtime"
	"sort"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

func TestZZTraceResident(t *testing.T) {
	const budget = 2 << 20
	r := rand.New(rand.NewSource(17))
	n := 50000
	f := newTestTreeFile(t, budget)
	packed := make([]byte, n*PackedTreeEntrySize)
	r.Read(packed)
	items := packedItems(packed)
	sort.Slice(items, func(a, b int) bool { return bytes.Compare(items[a].key[:], items[b].key[:]) < 0 })
	tree, _ := OpenTreePacked(packed, f)
	ids := make([]codec.UUID, 2000)
	for i := range ids {
		ids[i] = keyUUID(&items[r.Intn(n)].key)
	}
	var ms runtime.MemStats
	for i := 0; i < 60000; i++ {
		var h codec.Hash
		r.Read(h.Bytes[:])
		tree, _ = tree.With(ids[i%len(ids)], h)
		if i%3000 == 0 {
			st := tree.trie.slab.stats()
			runtime.ReadMemStats(&ms)
			t.Logf("i=%d inuse=%d free=%d pending=%d MB=%.1f gen=%d numgc=%d", i, st.Nodes+st.Leaves, st.FreeNodes+st.FreeLeaves, st.Pending, float64(st.Bytes)/(1<<20), tree.trie.slab.gen.Load(), ms.NumGC)
		}
	}
	waitRewrites(tree)
}
