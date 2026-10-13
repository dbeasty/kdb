package document

import (
	"bytes"
	"math/rand"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
)

// slabMatchesPtr reports whether the slab tree h is, record for record, the reference pointer
// trie p: same shape, same hashes, same leaves.
func slabMatchesPtr(h *trieTree, p *ptrNode) bool {
	if h == nil {
		return p == nil
	}
	v := h.slab.view()
	ok := slabRecMatchesPtr(&v, h.root, p)
	runtime.KeepAlive(h)
	return ok
}

func slabRecMatchesPtr(v *trieView, r tref, p *ptrNode) bool {
	if r == 0 || p == nil {
		return r == 0 && p == nil
	}
	if r&refLeaf != 0 {
		l := v.leaf(r)
		return p.leaf != nil && l.hash == p.hash && keyUUID(&l.id) == p.leaf.uuid && l.content == p.leaf.hash
	}
	n := v.node(r)
	if p.leaf != nil || p.children == nil || n.hash != p.hash {
		return false
	}
	for i := range n.child {
		if !slabRecMatchesPtr(v, n.child[i], p.children[i]) {
			return false
		}
	}
	return true
}

// reachable adds every record reachable from h to seen.
func reachable(h *trieTree, seen map[tref]bool) {
	if h == nil {
		return
	}
	v := h.slab.view()
	var mark func(r tref)
	mark = func(r tref) {
		if r == 0 || seen[r] {
			return
		}
		seen[r] = true
		if r&refLeaf == 0 {
			for _, c := range v.node(r).child {
				mark(c)
			}
		}
	}
	mark(h.root)
	runtime.KeepAlive(h)
}

// testIDs returns n ids, a share of them clustered under long common prefixes so that splits and
// re-compression run deep, not only at the handful of levels random ids reach.
func testIDs(r *rand.Rand, n int) []codec.UUID {
	ids := make([]codec.UUID, 0, n)
	for len(ids) < n {
		var b [16]byte
		r.Read(b[:])
		ids = append(ids, keyUUID(&b))
		if r.Intn(4) == 0 {
			for k := 0; k < 3 && len(ids) < n; k++ {
				c := b
				c[8+r.Intn(8)] ^= byte(1 + r.Intn(255))
				ids = append(ids, keyUUID(&c))
			}
		}
	}
	return ids
}

func sortedModel(model map[codec.UUID]codec.Hash) []codec.UUID {
	ids := make([]codec.UUID, 0, len(model))
	for id := range model {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(a, b int) bool { return uuidAfter(ids[b], ids[a]) })
	return ids
}

func checkTreeAgainst(t *testing.T, tree DocumentTree, ref *ptrNode, model map[codec.UUID]codec.Hash, universe []codec.UUID, r *rand.Rand) {
	t.Helper()
	if !slabMatchesPtr(tree.trie, ref) {
		t.Fatal("slab trie differs from the reference trie")
	}
	if tree.TreeHash != ptrTreeHash(ref) || tree.Size() != len(model) {
		t.Fatalf("hash or size differs: size %d want %d", tree.Size(), len(model))
	}
	for _, id := range universe {
		got, ok := tree.HashFor(id)
		want, wok := model[id]
		if ok != wok || got != want {
			t.Fatalf("HashFor(%v) = %v,%v want %v,%v", id, got, ok, want, wok)
		}
	}
	order := sortedModel(model)
	i := 0
	tree.Walk(func(id codec.UUID, h codec.Hash) bool {
		if i >= len(order) || id != order[i] || h != model[id] {
			t.Fatalf("Walk entry %d is %v, want %v", i, id, order)
		}
		i++
		return true
	})
	if i != len(order) {
		t.Fatalf("Walk visited %d of %d", i, len(order))
	}
	after := universe[r.Intn(len(universe))]
	start := sort.Search(len(order), func(k int) bool { return uuidAfter(order[k], after) })
	var got []codec.UUID
	tree.WalkAfter(after, func(id codec.UUID, _ codec.Hash) bool {
		got = append(got, id)
		return len(got) < 37
	})
	want := order[start:min(len(order), start+37)]
	if len(got) != len(want) {
		t.Fatalf("WalkAfter returned %d, want %d", len(got), len(want))
	}
	for k := range got {
		if got[k] != want[k] {
			t.Fatalf("WalkAfter[%d] = %v want %v", k, got[k], want[k])
		}
	}
}

// TestSlabTrieMatchesReference drives random puts, re-puts and deletes through the slab trie and
// the reference pointer trie side by side, and requires identical shape, hashes and reads - for
// the current tree throughout, and for historical trees held across later churn, collections and
// slot reuse.
func TestSlabTrieMatchesReference(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	rounds := 12
	if testing.Short() {
		rounds = 3
	}
	for round := 0; round < rounds; round++ {
		universe := testIDs(r, 20+r.Intn(1500))
		tree := EmptyDocumentTree()
		var ref *ptrNode
		model := map[codec.UUID]codec.Hash{}
		type held struct {
			tree  DocumentTree
			ref   *ptrNode
			model map[codec.UUID]codec.Hash
		}
		var history []held
		steps := 4000
		for step := 0; step < steps; step++ {
			id := universe[r.Intn(len(universe))]
			var err error
			switch {
			case r.Intn(3) == 0:
				tree, err = tree.Without(id)
				ref = ptrDelete(ref, id)
				delete(model, id)
			case r.Intn(8) == 0 && model[id] != (codec.Hash{}):
				tree, err = tree.With(id, model[id]) // unchanged content
				ref = ptrInsert(ref, id, model[id])
			default:
				var h codec.Hash
				r.Read(h.Bytes[:])
				tree, err = tree.With(id, h)
				ref = ptrInsert(ref, id, h)
				model[id] = h
			}
			if err != nil {
				t.Fatal(err)
			}
			if tree.TreeHash != ptrTreeHash(ref) {
				t.Fatalf("round %d step %d: hash differs from the reference", round, step)
			}
			if step%97 == 0 {
				m := make(map[codec.UUID]codec.Hash, len(model))
				for k, v := range model {
					m[k] = v
				}
				history = append(history, held{tree, ref, m})
				if len(history) > 6 {
					drop := r.Intn(len(history))
					history = append(history[:drop], history[drop+1:]...)
				}
			}
			if step%613 == 0 {
				runtime.GC() // run cleanups, so later edits reuse freed slots under the held trees
				checkTreeAgainst(t, tree, ref, model, universe, r)
			}
		}
		checkTreeAgainst(t, tree, ref, model, universe, r)
		for _, h := range history {
			checkTreeAgainst(t, h.tree, h.ref, h.model, universe, r)
		}
		// Delete everything: the empty tree.
		for id := range model {
			var err error
			if tree, err = tree.Without(id); err != nil {
				t.Fatal(err)
			}
		}
		if tree.TreeHash != EmptyDocumentTree().TreeHash || tree.Size() != 0 || tree.trie != nil {
			t.Fatalf("emptied tree is not the empty tree")
		}
	}
}

// waitForCleanups collects until the slab's records in use reach want, or a deadline passes.
func waitForCleanups(s *trieSlab, want int) {
	for i := 0; i < 200; i++ {
		runtime.GC()
		time.Sleep(2 * time.Millisecond)
		s.mu.Lock()
		s.drain(-1)
		s.mu.Unlock()
		if st := s.stats(); st.Nodes+st.Leaves <= want {
			return
		}
	}
}

// TestSlabReclaimsDroppedTrees: once trees are dropped and their roots released, the slab holds
// exactly the records still reachable from the trees that remain - no leak through sharing, and
// no record freed while a held tree names it.
func TestSlabReclaimsDroppedTrees(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	universe := testIDs(r, 3000)
	entries := map[codec.UUID]codec.Hash{}
	for _, id := range universe[:2000] {
		var h codec.Hash
		r.Read(h.Bytes[:])
		entries[id] = h
	}
	tree, err := BuildTrieDocumentTree(entries)
	if err != nil {
		t.Fatal(err)
	}
	s := tree.trie.slab
	var keep []DocumentTree
	for i := 0; i < 30000; i++ {
		id := universe[r.Intn(len(universe))]
		if r.Intn(4) == 0 {
			tree, err = tree.Without(id)
		} else {
			var h codec.Hash
			r.Read(h.Bytes[:])
			tree, err = tree.With(id, h)
		}
		if err != nil {
			t.Fatal(err)
		}
		if i%5000 == 0 {
			keep = append(keep, tree)
		}
		if i%3000 == 0 {
			runtime.GC()
		}
	}
	keep = append(keep, tree)
	tree = DocumentTree{}
	seen := map[tref]bool{}
	for _, k := range keep {
		reachable(k.trie, seen)
	}
	waitForCleanups(s, len(seen))
	st := s.stats()
	if st.Nodes+st.Leaves != len(seen) {
		t.Fatalf("slab holds %d live records (%d nodes, %d leaves), %d are reachable from held trees",
			st.Nodes+st.Leaves, st.Nodes, st.Leaves, len(seen))
	}
	t.Logf("held %d trees: %d records live, %d free, %d KB of chunks", len(keep), len(seen), st.FreeNodes+st.FreeLeaves, st.Bytes>>10)

	// Drop all but the last: what remains is exactly the last tree.
	keep = []DocumentTree{keep[len(keep)-1]}
	seen = map[tref]bool{}
	reachable(keep[0].trie, seen)
	waitForCleanups(s, len(seen))
	if st := s.stats(); st.Nodes+st.Leaves != len(seen) {
		t.Fatalf("after dropping history: %d live records, %d reachable", st.Nodes+st.Leaves, len(seen))
	}
	runtime.KeepAlive(keep)
}

// TestSlabConcurrentReadersDuringWrites runs readers on published and on old trees while a writer
// keeps deriving new ones and the collector keeps releasing dropped ones, so freed slots are
// reused under the readers' feet if anything is wrong. Run under -race. Every read is checked
// against the tree hash the reader was handed: a Merkle proof for a point read, and a full
// rebuild of the tree from its walked entries.
func TestSlabConcurrentReadersDuringWrites(t *testing.T) {
	slabPoison.Store(true)
	defer slabPoison.Store(false)
	r := rand.New(rand.NewSource(3))
	universe := testIDs(r, 200)
	var published atomic.Pointer[DocumentTree]
	tree := EmptyDocumentTree()
	published.Store(&tree)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var reads, walks atomic.Int64

	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rr := rand.New(rand.NewSource(seed))
			var old []DocumentTree
			for {
				select {
				case <-stop:
					return
				default:
				}
				cur := *published.Load()
				if rr.Intn(10) == 0 {
					old = append(old, cur)
					if len(old) > 5 {
						old = old[1:]
					}
				}
				trees := append([]DocumentTree{cur}, old...)
				for _, tr := range trees {
					id := universe[rr.Intn(len(universe))]
					h, ok := tr.HashFor(id)
					got, present, err := VerifyTreeProof(tr.TreeHash, tr.Proof(id))
					if err != nil || present != ok || got != h {
						t.Errorf("point read of %v disagrees with the tree's hash: %v", id, err)
						return
					}
					reads.Add(1)
				}
				if rr.Intn(20) == 0 {
					tr := trees[rr.Intn(len(trees))]
					entries := tr.MaterializedEntries()
					if ptrTreeHash(ptrBuild(entries)) != tr.TreeHash || len(entries) != tr.Size() {
						t.Errorf("walk of a held tree does not rebuild to its hash")
						return
					}
					walks.Add(1)
				}
			}
		}(int64(g))
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(3 * time.Millisecond):
				runtime.GC()
			}
		}
	}()

	writes := 20000
	if testing.Short() {
		writes = 4000
	}
	cur := tree
	for i := 0; i < writes; i++ {
		id := universe[r.Intn(len(universe))]
		var err error
		if r.Intn(3) == 0 {
			cur, err = cur.Without(id)
		} else {
			var h codec.Hash
			r.Read(h.Bytes[:])
			cur, err = cur.With(id, h)
		}
		if err != nil {
			t.Fatal(err)
		}
		c := cur
		published.Store(&c)
	}
	close(stop)
	wg.Wait()
	reused := cur.trie.slab.stats().Reused
	t.Logf("%d writes, %d verified point reads, %d verified walks, %d slots reused", writes, reads.Load(), walks.Load(), reused)
	if reused == 0 {
		t.Fatal("no slot was reused during the run, so it tested nothing about reuse")
	}
}

// TestSlabRecordsArePointerFree: the reason for the slab. A pointer in a record type would make
// every chunk scanned by the collector again.
func TestSlabRecordsArePointerFree(t *testing.T) {
	var hasPointers func(reflect.Type) bool
	hasPointers = func(ty reflect.Type) bool {
		switch ty.Kind() {
		case reflect.Pointer, reflect.Map, reflect.Slice, reflect.String, reflect.Interface, reflect.Chan, reflect.Func, reflect.UnsafePointer:
			return true
		case reflect.Array:
			return hasPointers(ty.Elem())
		case reflect.Struct:
			for i := 0; i < ty.NumField(); i++ {
				if hasPointers(ty.Field(i).Type) {
					return true
				}
			}
		}
		return false
	}
	for _, ty := range []reflect.Type{reflect.TypeOf(nodeRec{}), reflect.TypeOf(leafRec{})} {
		if hasPointers(ty) {
			t.Fatalf("%v holds a pointer", ty)
		}
	}
	if n, l := reflect.TypeOf(nodeRec{}).Size(), reflect.TypeOf(leafRec{}).Size(); n != 96 || l != 80 {
		t.Fatalf("record sizes %d/%d, want 96/80", n, l)
	}
}

// TestSlabLocate: every slot maps to a distinct place inside its chunk, in order.
func TestSlabLocate(t *testing.T) {
	prevK, prevOff := 0, uint32(0)
	for i := uint32(1); i < slabGeomSlots+3*(1<<slabMaxShift); i++ {
		k, off := slabLocate(i)
		if int(off) >= slabChunkLen(k) {
			t.Fatalf("slot %d: offset %d beyond chunk %d of %d", i, off, k, slabChunkLen(k))
		}
		if !(k == prevK && off == prevOff+1) && !(k == prevK+1 && off == 0) {
			t.Fatalf("slot %d at (%d,%d) does not follow (%d,%d)", i, k, off, prevK, prevOff)
		}
		prevK, prevOff = k, off
	}
}

// TestSlabTreeMemoryPerDocument bounds what the live tree costs: bytes per document, and heap
// objects per document - the figure the collector's mark work scales with, about 2.8 per
// document for the pointer trie and a handful per tree for the slab.
func TestSlabTreeMemoryPerDocument(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates tens of MB")
	}
	const n = 200000
	const maxBytesPerDoc = 135 // measured ~121: 84-byte leaf + ~0.37 internal nodes of 100 bytes
	const maxObjectsPerDoc = 0.01
	r := rand.New(rand.NewSource(4))
	packed := make([]byte, n*PackedTreeEntrySize)
	r.Read(packed)
	items := packedItems(packed)
	sort.Slice(items, func(a, b int) bool { return bytes.Compare(items[a].key[:], items[b].key[:]) < 0 })

	heap0, obj0 := settledHeap()
	tree, err := BuildTrieDocumentTreePacked(packed)
	if err != nil {
		t.Fatal(err)
	}
	heap1, obj1 := settledHeap()
	perDoc := float64(int64(heap1)-int64(heap0)) / n
	objPerDoc := float64(int64(obj1)-int64(obj0)) / n
	st := tree.trie.slab.stats()
	t.Logf("%d documents: %.1f bytes/document, %.4f heap objects/document; %d nodes, %d leaves, %d free, %.1f MB of chunks",
		n, perDoc, objPerDoc, st.Nodes, st.Leaves, st.FreeNodes+st.FreeLeaves, float64(st.Bytes)/(1<<20))
	if perDoc > maxBytesPerDoc || objPerDoc > maxObjectsPerDoc {
		t.Fatalf("%.1f bytes and %.4f objects per document; ceilings %d and %.2f", perDoc, objPerDoc, maxBytesPerDoc, maxObjectsPerDoc)
	}

	// Churn: updates re-use what dropped versions free, so the slab does not grow with history.
	ids := make([]codec.UUID, 0, 1000)
	for i := 0; i < 1000; i++ {
		ids = append(ids, keyUUID(&items[r.Intn(n)].key))
	}
	// Long enough for many collection cycles: a slab whose bound drifted by a little each cycle
	// would show it here.
	for i := 0; i < 300000; i++ {
		var h codec.Hash
		r.Read(h.Bytes[:])
		if tree, err = tree.With(ids[i%len(ids)], h); err != nil {
			t.Fatal(err)
		}
	}
	heap2, _ := settledHeap()
	churned := float64(int64(heap2)-int64(heap0)) / n
	st = tree.trie.slab.stats()
	t.Logf("after 300,000 updates: %.1f bytes/document; %d nodes, %d leaves, %d free nodes, %d free leaves, %.1f MB of chunks",
		churned, st.Nodes, st.Leaves, st.FreeNodes, st.FreeLeaves, float64(st.Bytes)/(1<<20))
	// Bounded by slabNudgeFloor's rule: about 1.5 times the live records, plus a chunk and what
	// the cleanups have not yet released.
	if churned > maxBytesPerDoc*1.8 {
		t.Fatalf("%.1f bytes per document after updates: dropped versions are not being reused", churned)
	}
	runtime.KeepAlive(tree)
	runtime.KeepAlive(packed)
}

// BenchmarkTrieUpdateChain compares the slab trie with the reference pointer trie (the code it
// replaced) in one process: a chain of updates to existing documents of a large tree, each
// version derived from the last and only the last kept, as the live tree is. KDB_TREE_BENCH_N
// sets the size (default 1,000,000).
func BenchmarkTrieUpdateChain(b *testing.B) {
	n := 1_000_000
	if v, err := strconv.Atoi(os.Getenv("KDB_TREE_BENCH_N")); err == nil && v > 0 {
		n = v
	}
	r := rand.New(rand.NewSource(9))
	entries := make(map[codec.UUID]codec.Hash, n)
	ids := make([]codec.UUID, 0, n)
	for len(ids) < n {
		var k [16]byte
		r.Read(k[:])
		id := keyUUID(&k)
		var h codec.Hash
		r.Read(h.Bytes[:])
		entries[id] = h
		ids = append(ids, id)
	}
	b.Run("ptr", func(b *testing.B) {
		root := ptrBuild(entries)
		runtime.GC()
		b.ReportAllocs()
		b.ResetTimer()
		var h codec.Hash
		for i := 0; i < b.N; i++ {
			h.Bytes[0], h.Bytes[1], h.Bytes[2] = byte(i), byte(i>>8), byte(i>>16)
			root = ptrInsert(root, ids[(i*7919)%n], h)
		}
		runtime.KeepAlive(root)
	})
	b.Run("slab", func(b *testing.B) {
		tree, _ := BuildTrieDocumentTree(entries)
		runtime.GC()
		b.ReportAllocs()
		b.ResetTimer()
		var h codec.Hash
		for i := 0; i < b.N; i++ {
			h.Bytes[0], h.Bytes[1], h.Bytes[2] = byte(i), byte(i>>8), byte(i>>16)
			tree, _ = tree.With(ids[(i*7919)%n], h)
		}
		runtime.KeepAlive(tree)
	})
	b.Run("slab-trie", func(b *testing.B) { // trieWith alone, without DocumentTree.With's lookup
		h0 := trieBuild(entries)
		runtime.GC()
		b.ReportAllocs()
		b.ResetTimer()
		var h codec.Hash
		for i := 0; i < b.N; i++ {
			h.Bytes[0], h.Bytes[1], h.Bytes[2] = byte(i), byte(i>>8), byte(i>>16)
			h0 = trieWith(h0, ids[(i*7919)%n], h)
		}
		runtime.KeepAlive(h0)
	})
}

// TestSlabConcurrentWritersFromOneBase: several goroutines derive their own chains from one
// shared base tree at once - concurrent transactions on a namespace - so their records are
// placed into one slab concurrently, while the collector releases what each drops. Each chain
// must still match the reference trie built from the same operations.
func TestSlabConcurrentWritersFromOneBase(t *testing.T) {
	slabPoison.Store(true)
	defer slabPoison.Store(false)
	r := rand.New(rand.NewSource(5))
	universe := testIDs(r, 600)
	entries := map[codec.UUID]codec.Hash{}
	for _, id := range universe[:400] {
		var h codec.Hash
		r.Read(h.Bytes[:])
		entries[id] = h
	}
	base, err := BuildTrieDocumentTree(entries)
	if err != nil {
		t.Fatal(err)
	}
	baseRef := ptrBuild(entries)
	steps := 3000
	if testing.Short() {
		steps = 500
	}
	var wg sync.WaitGroup
	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rr := rand.New(rand.NewSource(seed))
			tree, ref := base, baseRef
			for i := 0; i < steps; i++ {
				id := universe[rr.Intn(len(universe))]
				var err error
				if rr.Intn(3) == 0 {
					tree, err = tree.Without(id)
					ref = ptrDelete(ref, id)
				} else {
					var h codec.Hash
					rr.Read(h.Bytes[:])
					tree, err = tree.With(id, h)
					ref = ptrInsert(ref, id, h)
				}
				if err != nil || tree.TreeHash != ptrTreeHash(ref) {
					t.Errorf("writer %d step %d: hash differs from the reference (%v)", seed, i, err)
					return
				}
				if i%200 == 0 {
					runtime.GC()
					if !slabMatchesPtr(tree.trie, ref) {
						t.Errorf("writer %d step %d: shape differs from the reference", seed, i)
						return
					}
				}
			}
		}(int64(g))
	}
	wg.Wait()
	if !slabMatchesPtr(base.trie, baseRef) {
		t.Fatal("the shared base changed under its writers")
	}
}
