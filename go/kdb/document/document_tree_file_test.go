package document

import (
	"bytes"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
)

func packedOf(entries map[codec.UUID]codec.Hash) []byte {
	items := make([]trieItem, 0, len(entries))
	for id, h := range entries {
		items = append(items, trieItem{key: uuidKey(id), hash: h})
	}
	sort.Slice(items, func(a, b int) bool { return bytes.Compare(items[a].key[:], items[b].key[:]) < 0 })
	out := make([]byte, 0, len(items)*PackedTreeEntrySize)
	for _, it := range items {
		out = append(out, it.key[:]...)
		out = append(out, it.hash.Bytes[:]...)
	}
	return out
}

func newTestTreeFile(t *testing.T, resident int64) *TreeFile {
	t.Helper()
	f, err := NewTreeFile(TreeFileOptions{Dir: filepath.Join(t.TempDir(), "tree"), ResidentBytes: resident})
	if err != nil {
		t.Skipf("tree files unsupported: %v", err)
	}
	return f
}

func segmentFiles(t *testing.T, f *TreeFile) []uint64 { return listTreeSegments(f.opts.Dir) }

// waitRewrites waits for a background rewrite of the tree's slab to finish, so a test does not
// return (and remove its directory) under one.
func waitRewrites(tree DocumentTree) {
	if tree.trie == nil {
		return
	}
	for i := 0; i < 1000 && tree.trie.slab.rewriting.Load(); i++ {
		time.Sleep(2 * time.Millisecond)
	}
}

// isFileBacked reports whether the tree's root is in a mapped segment.
func isFileBacked(tree DocumentTree) bool { return tree.trie != nil && tree.trie.root&refFile != 0 }

// TestTreeFileOpenMatchesHeap: a tree opened from a packed image through a tree file is the heap
// tree, record for record against the reference trie; a second open of the same image maps the
// segment the first wrote instead of building one; another image builds a new one.
func TestTreeFileOpenMatchesHeap(t *testing.T) {
	r := rand.New(rand.NewSource(11))
	f := newTestTreeFile(t, 1<<20)
	for _, n := range []int{1, 2, 17, 4095, 4096, 30000} {
		universe := testIDs(r, n)
		model := randomEntriesFrom(r, universe)
		packed := packedOf(model)
		tree, err := OpenTreePacked(packed, f)
		if err != nil {
			t.Fatal(err)
		}
		if !isFileBacked(tree) {
			t.Fatalf("n=%d: tree is not in a segment", n)
		}
		heap, _ := BuildTrieDocumentTreePacked(packed)
		ref := ptrBuild(model)
		if tree.TreeHash != heap.TreeHash || tree.Size() != len(model) {
			t.Fatalf("n=%d: hash or size differs from the heap build", n)
		}
		checkTreeAgainst(t, tree, ref, model, universe, r)

		before := segmentFiles(t, f)
		again, err := OpenTreePacked(packed, f)
		if err != nil || again.TreeHash != tree.TreeHash || !isFileBacked(again) {
			t.Fatalf("n=%d: reopen: %v", n, err)
		}
		if after := segmentFiles(t, f); len(after) != len(before) || after[len(after)-1] != before[len(before)-1] {
			t.Fatalf("n=%d: reopening the same image wrote a segment: %v -> %v", n, before, after)
		}
		checkTreeAgainst(t, again, ref, model, universe, r)
	}
}

// TestTreeFileUpdatesMatchReference: updates to a file-backed tree with a tiny resident budget,
// so that it is rewritten and rebased many times, against the reference trie - the current tree
// throughout, and historical trees held across later rewrites, rebases and retired segments.
func TestTreeFileUpdatesMatchReference(t *testing.T) {
	r := rand.New(rand.NewSource(12))
	f := newTestTreeFile(t, 32<<10)
	universe := testIDs(r, 3000)
	model := randomEntriesFrom(r, universe[:2000])
	tree, err := OpenTreePacked(packedOf(model), f)
	if err != nil {
		t.Fatal(err)
	}
	ref := ptrBuild(model)
	type held struct {
		tree  DocumentTree
		ref   *ptrNode
		model map[codec.UUID]codec.Hash
	}
	var history []held
	gen0 := tree.trie.slab.gen.Load()
	steps := 20000
	if testing.Short() {
		steps = 4000
	}
	for step := 0; step < steps; step++ {
		id := universe[r.Intn(len(universe))]
		if r.Intn(3) == 0 {
			tree, err = tree.Without(id)
			ref = ptrDelete(ref, id)
			delete(model, id)
		} else {
			var h codec.Hash
			r.Read(h.Bytes[:])
			tree, err = tree.With(id, h)
			ref = ptrInsert(ref, id, h)
			model[id] = h
		}
		if err != nil || tree.TreeHash != ptrTreeHash(ref) {
			t.Fatalf("step %d: hash differs from the reference (%v)", step, err)
		}
		if step%500 == 0 {
			m := make(map[codec.UUID]codec.Hash, len(model))
			for k, v := range model {
				m[k] = v
			}
			history = append(history, held{tree, ref, m})
			if len(history) > 5 {
				history = history[1:]
			}
			runtime.GC()
		}
		if step%2500 == 0 {
			checkTreeAgainst(t, tree, ref, model, universe, r)
		}
	}
	// Let the last rewrite land, then check everything held.
	waitRewrites(tree)
	checkTreeAgainst(t, tree, ref, model, universe, r)
	for _, h := range history {
		checkTreeAgainst(t, h.tree, h.ref, h.model, universe, r)
	}
	gens := tree.trie.slab.gen.Load() - gen0
	if gens < 3 {
		t.Fatalf("only %d rewrites in %d updates with a 32 KB budget", gens, steps)
	}
	// Dropping history lets superseded segments go: mapped and on disk.
	history = nil
	s := tree.trie.slab
	for i := 0; i < 100; i++ {
		runtime.GC()
		time.Sleep(5 * time.Millisecond)
		s.mu.Lock()
		s.drain(-1)
		s.mu.Unlock()
		if len(*s.segs.p.Load()) <= f.opts.MaxSegments+1 {
			break
		}
	}
	// The live tree spans at most MaxSegments (a longer chain is compacted), plus the newest.
	if n := len(*s.segs.p.Load()); n > f.opts.MaxSegments+1 {
		t.Fatalf("%d segments still mapped after %d writes with only the live tree held", n, gens)
	}
	if files := segmentFiles(t, f); len(files) > f.opts.MaxSegments+2 {
		t.Fatalf("%d segment files on disk: %v", len(files), files)
	}
	t.Logf("%d updates, %d rewrites, %d segments mapped at the end", steps, gens, len(*s.segs.p.Load()))
	runtime.KeepAlive(tree)
}

// TestTreeFileConcurrentReaders: readers on published and old trees, checked by Merkle proof
// against the hash they were handed, while a writer churns a tree with a tiny budget - so
// segments are written, trees rebased, and superseded segments unmapped and deleted under the
// readers. Run under -race; an unmap under a reader would crash it.
func TestTreeFileConcurrentReaders(t *testing.T) {
	slabPoison.Store(true)
	defer slabPoison.Store(false)
	r := rand.New(rand.NewSource(13))
	f := newTestTreeFile(t, 16<<10)
	universe := testIDs(r, 800)
	tree, err := OpenTreePacked(packedOf(randomEntriesFrom(r, universe[:500])), f)
	if err != nil {
		t.Fatal(err)
	}
	var published atomic.Pointer[DocumentTree]
	published.Store(&tree)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var reads atomic.Int64
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
				if rr.Intn(8) == 0 {
					old = append(old, cur)
					if len(old) > 4 {
						old = old[1:]
					}
				}
				for _, tr := range append([]DocumentTree{cur}, old...) {
					id := universe[rr.Intn(len(universe))]
					h, ok := tr.HashFor(id)
					got, present, err := VerifyTreeProof(tr.TreeHash, tr.Proof(id))
					if err != nil || present != ok || got != h {
						t.Errorf("point read disagrees with the tree's hash: %v", err)
						return
					}
					reads.Add(1)
				}
				if rr.Intn(30) == 0 {
					tr := old[rr.Intn(len(old))]
					if e := tr.MaterializedEntries(); ptrTreeHash(ptrBuild(e)) != tr.TreeHash {
						t.Errorf("walk of a held tree does not rebuild to its hash")
						return
					}
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
	writes := 15000
	if testing.Short() {
		writes = 3000
	}
	cur := tree
	gen0 := cur.trie.slab.gen.Load()
	for i := 0; i < writes; i++ {
		id := universe[r.Intn(len(universe))]
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
	waitRewrites(cur)
	gens := cur.trie.slab.gen.Load() - gen0
	t.Logf("%d writes, %d rewrites, %d verified reads", writes, gens, reads.Load())
	if gens < 2 {
		t.Fatal("too few rewrites to have tested anything")
	}
}

// TestTreeFileDamageFallsBack: a damaged, truncated or mistagged segment is never used; the open
// builds a fresh one (or the heap tree) and the result is still exactly the tree.
func TestTreeFileDamageFallsBack(t *testing.T) {
	r := rand.New(rand.NewSource(14))
	universe := testIDs(r, 5000)
	model := randomEntriesFrom(r, universe)
	packed := packedOf(model)
	ref := ptrBuild(model)
	damage := map[string]func(path string){
		"flipped byte": func(p string) {
			b, _ := os.ReadFile(p)
			b[len(b)/2] ^= 0x40
			os.WriteFile(p, b, 0o644)
		},
		"truncated": func(p string) {
			fi, _ := os.Stat(p)
			os.Truncate(p, fi.Size()-100)
		},
		"wrong source": func(p string) {
			b, _ := os.ReadFile(p + treeSrcSuffix)
			b[0] ^= 1
			os.WriteFile(p+treeSrcSuffix, b, 0o644)
		},
	}
	for name, hurt := range damage {
		t.Run(name, func(t *testing.T) {
			f := newTestTreeFile(t, 1<<20)
			first, err := OpenTreePacked(packed, f)
			if err != nil {
				t.Fatal(err)
			}
			seqs := segmentFiles(t, f)
			path := f.segPath(seqs[len(seqs)-1])
			first = DocumentTree{}
			runtime.GC()
			f.Close() // as a process exit would: nothing deletes the file
			hurt(path)
			f2, err := NewTreeFile(f.opts)
			if err != nil {
				t.Fatal(err)
			}
			tree, err := OpenTreePacked(packed, f2)
			if err != nil {
				t.Fatal(err)
			}
			checkTreeAgainst(t, tree, ref, model, universe, r)
			if got := segmentFiles(t, f2); got[len(got)-1] == seqs[len(seqs)-1] {
				t.Fatalf("the damaged segment %v is still the newest: %v", seqs, got)
			}
			runtime.KeepAlive(first)
		})
	}
}

// TestTreeFileCheckpointSegment: SaveTreeSegment tags a segment holding exactly the checkpointed
// tree, so the next open of that checkpoint maps it - including after updates, which it writes
// out first.
func TestTreeFileCheckpointSegment(t *testing.T) {
	r := rand.New(rand.NewSource(15))
	f := newTestTreeFile(t, 1<<20)
	universe := testIDs(r, 4000)
	model := randomEntriesFrom(r, universe[:3000])
	tree, err := OpenTreePacked(packedOf(model), f)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range universe[2500:] {
		var h codec.Hash
		r.Read(h.Bytes[:])
		if tree, err = tree.With(id, h); err != nil {
			t.Fatal(err)
		}
		model[id] = h
	}
	packed := tree.AppendPacked(nil)
	if err := SaveTreeSegment(tree, packed); err != nil {
		t.Fatal(err)
	}
	f.Close()
	f2, _ := NewTreeFile(f.opts)
	before := segmentFiles(t, f2)
	again, err := OpenTreePacked(packed, f2)
	if err != nil || again.TreeHash != tree.TreeHash || !isFileBacked(again) {
		t.Fatalf("reopen: %v", err)
	}
	if after := segmentFiles(t, f2); after[len(after)-1] != before[len(before)-1] {
		t.Fatalf("the checkpoint's segment was not used: %v -> %v", before, after)
	}
	checkTreeAgainst(t, again, ptrBuild(model), model, universe, r)
}

// TestTreeFileBindHeapTree: binding a heap tree larger than the budget moves it into a segment.
func TestTreeFileBindHeapTree(t *testing.T) {
	r := rand.New(rand.NewSource(16))
	universe := testIDs(r, 20000)
	model := randomEntriesFrom(r, universe)
	heap, _ := BuildTrieDocumentTree(model)
	f := newTestTreeFile(t, 64<<10)
	bound := BindTreeFile(heap, f)
	if !isFileBacked(bound) || bound.TreeHash != heap.TreeHash || bound.Size() != heap.Size() {
		t.Fatal("a large heap tree was not moved into a segment")
	}
	checkTreeAgainst(t, bound, ptrBuild(model), model, universe, r)
	empty := BindTreeFile(EmptyDocumentTree(), f)
	id, h := universe[0], codec.Hash{Bytes: [32]byte{1}}
	one, err := empty.With(id, h)
	if err != nil || one.trie == nil || one.trie.slab.file.Load() != f {
		t.Fatal("a tree derived from a bound empty tree is not bound")
	}
	if zero, _ := one.Without(id); zero.Size() != 0 || zero.TreeHash != EmptyDocumentTree().TreeHash || zero.trie == nil {
		t.Fatal("emptying a bound tree lost its binding or its emptiness")
	}
}

// TestTreeFileResidentMemory: what a file-backed tree keeps in the heap does not grow with its
// documents - after open, and across updates beyond what the budget allows.
func TestTreeFileResidentMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("writes tens of MB")
	}
	const budget = 2 << 20
	r := rand.New(rand.NewSource(17))
	for _, n := range []int{50_000, 400_000} {
		f := newTestTreeFile(t, budget)
		packed := make([]byte, n*PackedTreeEntrySize)
		r.Read(packed)
		items := packedItems(packed)
		sort.Slice(items, func(a, b int) bool { return bytes.Compare(items[a].key[:], items[b].key[:]) < 0 })
		heap0, _ := settledHeap()
		tree, err := OpenTreePacked(packed, f)
		if err != nil {
			t.Fatal(err)
		}
		heap1, _ := settledHeap()
		ids := make([]codec.UUID, 2000)
		for i := range ids {
			ids[i] = keyUUID(&items[r.Intn(n)].key)
		}
		for i := 0; i < 60000; i++ {
			var h codec.Hash
			r.Read(h.Bytes[:])
			if tree, err = tree.With(ids[i%len(ids)], h); err != nil {
				t.Fatal(err)
			}
		}
		waitRewrites(tree)
		heap2, _ := settledHeap()
		open, churned := int64(heap1)-int64(heap0), int64(heap2)-int64(heap0)
		st := tree.trie.slab.stats()
		t.Logf("n=%d: slab %+v; segments %d", n, st, len(*tree.trie.slab.segs.p.Load()))
		t.Logf("n=%d: heap after open %.2f MB, after 60k updates %.2f MB (budget %.0f MB), slab %.2f MB, %d rewrites",
			n, float64(open)/(1<<20), float64(churned)/(1<<20), float64(budget)/(1<<20),
			float64(tree.SlabBytes())/(1<<20), tree.trie.slab.gen.Load())
		if open > 1<<20 {
			t.Fatalf("n=%d: %d bytes of heap after open; a file-backed tree should hold almost none", n, open)
		}
		if churned > 4*budget {
			t.Fatalf("n=%d: %d bytes of heap after updates; resident budget %d", n, churned, budget)
		}
		runtime.KeepAlive(packed)
		runtime.KeepAlive(tree)
	}
}

type held struct {
	tree DocumentTree
	ref  *ptrNode
}

func rebaseHistory(history []held) {
	ts := make([]DocumentTree, len(history))
	for i, h := range history {
		ts[i] = h.tree
	}
	for i, t := range RebaseTrees(ts) {
		history[i].tree = t
	}
}

// TestTreeFileRebasedHistoryReleasesSegments: trees held but never updated - the engine's
// history - keep every segment they refer into mapped. Rebasing them when a segment lands
// (SetOnWrite, RebaseTrees) lets the segments compactions superseded go, while each held tree
// stays exactly what it was - and, the trees' copies being shared, costs no more heap than not
// rebasing (the control, which keeps the superseded segments mapped instead).
func TestTreeFileRebasedHistoryReleasesSegments(t *testing.T) {
	var controlInUse int
	for _, rebase := range []bool{false, true} {
		r := rand.New(rand.NewSource(18))
		f, err := NewTreeFile(TreeFileOptions{Dir: filepath.Join(t.TempDir(), "tree"), ResidentBytes: 8 << 10, MaxSegments: 2})
		if err != nil {
			t.Skip(err)
		}
		var wrote atomic.Int64
		f.SetOnWrite(func() { wrote.Add(1) })
		universe := testIDs(r, 3000)
		model := randomEntriesFrom(r, universe[:2000])
		tree, err := OpenTreePacked(packedOf(model), f)
		if err != nil {
			t.Fatal(err)
		}
		ref := ptrBuild(model)
		var history []held
		seen := int64(0)
		for step := 0; step < 15000; step++ {
			id := universe[r.Intn(len(universe))]
			var h codec.Hash
			r.Read(h.Bytes[:])
			if tree, err = tree.With(id, h); err != nil {
				t.Fatal(err)
			}
			ref = ptrInsert(ref, id, h)
			if step%100 == 0 {
				history = append(history, held{tree, ref})
				if len(history) > 80 {
					// Copied, not resliced: a resliced history keeps the dropped tree alive in
					// its backing array, pinning what it refers to.
					history = append([]held(nil), history[1:]...)
				}
			}
			if rebase && wrote.Load() != seen {
				seen = wrote.Load()
				rebaseHistory(history)
			}
		}
		waitRewrites(tree)
		if rebase { // as the engine's hook does: its live tree, and everything it holds
			tree = tree.Rebased()
			rebaseHistory(history)
		}
		s := tree.trie.slab
		// Superseded segments go once the old trees' cleanups have run: wait for the count to settle.
		for deadline, last, stable := time.Now().Add(5*time.Second), -1, 0; time.Now().Before(deadline) && stable < 20; {
			runtime.GC()
			time.Sleep(5 * time.Millisecond)
			s.mu.Lock()
			s.drain(-1)
			n := len(*s.segs.p.Load())
			s.mu.Unlock()
			if n == last {
				stable++
			} else {
				last, stable = n, 0
			}
		}
		mapped := len(*s.segs.p.Load())
		{
			seen := map[tref]bool{}
			reachable(tree.trie, seen)
			for _, h := range history {
				reachable(h.tree.trie, seen)
			}
			resident := 0
			for r := range seen {
				if r&refFile == 0 {
					resident++
				}
			}
			st := s.stats()
			t.Logf("  resident in use %d, reachable from held trees %d, pending %d", st.Nodes+st.Leaves, resident, st.Pending)
			if st.Nodes+st.Leaves != resident {
				t.Fatalf("%d records in use, %d reachable", st.Nodes+st.Leaves, resident)
			}
			if !rebase {
				controlInUse = resident
			} else if resident > controlInUse*12/10 {
				t.Fatalf("rebasing the history holds %d heap records, against %d without", resident, controlInUse)
			}
		}

		t.Logf("rebase=%v: %d writes, %d segments mapped, %d files, %d history trees held", rebase, wrote.Load(), mapped, len(segmentFiles(t, f)), len(history))
		for _, h := range history {
			if !slabMatchesPtr(h.tree.trie, h.ref) || h.tree.TreeHash != ptrTreeHash(h.ref) {
				t.Fatal("a held tree changed")
			}
		}
		if rebase && mapped > f.opts.MaxSegments+1 {
			t.Fatalf("%d segments mapped with history rebased; the newest chain is at most %d", mapped, f.opts.MaxSegments)
		}

		runtime.KeepAlive(tree)
	}
}

// TestTreeFileStampSkipsFullChecksum: reopening a checkpoint whose segments still carry the
// stamps its sidecar recorded maps them without reading them through for their checksum; a
// segment whose stamp changed is checksummed in full. The tree is the same either way.
func TestTreeFileStampSkipsFullChecksum(t *testing.T) {
	r := rand.New(rand.NewSource(19))
	f := newTestTreeFile(t, 1<<20)
	universe := testIDs(r, 20000)
	model := randomEntriesFrom(r, universe)
	packed := packedOf(model)
	if _, err := OpenTreePacked(packed, f); err != nil {
		t.Fatal(err)
	}
	f.Close()
	reopen := func() DocumentTree {
		f2, err := NewTreeFile(f.opts)
		if err != nil {
			t.Fatal(err)
		}
		tree, err := OpenTreePacked(packed, f2)
		if err != nil {
			t.Fatal(err)
		}
		if _, mapped := tree.Paged(); !mapped {
			t.Fatal("reopen did not map the segment")
		}
		checkTreeAgainst(t, tree, ptrBuild(model), model, universe[:200], r)
		f2.Close()
		return tree
	}
	before := treeFullVerifies.Load()
	reopen()
	if n := treeFullVerifies.Load() - before; n != 0 {
		t.Fatalf("a reopen with matching stamps checksummed %d segments in full", n)
	}
	seqs := segmentFiles(t, f)
	later := time.Now().Add(time.Hour)
	if err := os.Chtimes(f.segPath(seqs[len(seqs)-1]), later, later); err != nil {
		t.Fatal(err)
	}
	before = treeFullVerifies.Load()
	reopen()
	if n := treeFullVerifies.Load() - before; n != 1 {
		t.Fatalf("a segment whose stamp changed was checksummed %d times, want 1", n)
	}
}

// Dropping the mapped segments' pages loses nothing: readers walking the tree while pages are
// dropped, again and again, see every entry, and the tree's hash and contents are unchanged after.
func TestDropMappedPagesKeepsTheTreeReadable(t *testing.T) {
	r := rand.New(rand.NewSource(29))
	f := newTestTreeFile(t, 16<<10)
	universe := testIDs(r, 3000)
	entries := randomEntriesFrom(r, universe)
	tree, err := OpenTreePacked(packedOf(entries), f)
	if err != nil {
		t.Fatal(err)
	}
	if _, mapped := tree.Paged(); !mapped {
		t.Skip("the tree is not mapped from a segment; nothing to drop")
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan string, 8)
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				n := 0
				tree.Walk(func(id codec.UUID, h codec.Hash) bool {
					if entries[id] != h {
						errs <- "a walk read an entry that differs after its pages were dropped"
						return false
					}
					n++
					return true
				})
				if n != len(entries) {
					errs <- "a walk missed entries after pages were dropped"
					return
				}
			}
		}()
	}
	for i := 0; i < 50; i++ {
		tree.DropMappedPages()
	}
	close(stop)
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	again, err := BuildTrieDocumentTreePacked(packedOf(entries))
	if err != nil {
		t.Fatal(err)
	}
	if tree.TreeHash != again.TreeHash || tree.Size() != len(entries) {
		t.Fatal("the tree changed across dropping its pages")
	}
	DocumentTree{}.DropMappedPages() // an unbound tree: a no-op
}
