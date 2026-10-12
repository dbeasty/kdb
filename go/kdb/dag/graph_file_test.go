package dag

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage/mmapfile"
)

func skipWithoutMmap(t *testing.T) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "probe")
	os.WriteFile(p, []byte{1}, 0o644)
	m, err := mmapfile.Open(p)
	if errors.Is(err, mmapfile.ErrUnsupported) {
		t.Skip("no mmap on this platform")
	}
	if m != nil {
		m.Close()
	}
}

// graphView is everything observable about a DAG that the graph file must not change.
type graphView struct {
	commits  map[codec.Hash]document.Commit
	gens     map[codec.Hash]uint32
	count    int
	ancestry map[[2]codec.Hash]bool
	byTx     map[codec.UUID]codec.Hash
	walk     []string
	horizon  int
}

func viewOf(t *testing.T, b *dagBuilder, hashes []codec.Hash) graphView {
	t.Helper()
	d := b.dag
	v := graphView{
		commits:  map[codec.Hash]document.Commit{},
		gens:     map[codec.Hash]uint32{},
		ancestry: map[[2]codec.Hash]bool{},
		byTx:     map[codec.UUID]codec.Hash{},
		count:    d.CommitCount(),
		horizon:  len(d.Horizon()),
	}
	if got, want := sortedHashes(d.Horizon()), naiveHorizon(d); !reflect.DeepEqual(got, want) {
		t.Fatalf("Horizon %v disagrees with a walk of every commit %v", got, want)
	}
	for _, h := range hashes {
		c, ok := d.GetCommit(h)
		if !ok {
			t.Fatalf("%s missing", b.name(h))
		}
		v.commits[h] = c
		d.mu.Lock()
		d.ensureGenerationsLocked()
		v.gens[h] = d.nodes.generation(h)
		d.mu.Unlock()
		if got, ok := d.GetCommitByTransactionID(c.TransactionID); ok {
			v.byTx[c.TransactionID] = got.Hash
		}
		for _, o := range hashes {
			v.ancestry[[2]codec.Hash{h, o}] = d.IsAncestor(h, o)
		}
	}
	head, _ := d.Head()
	for _, e := range d.Walk(head, nil, 1<<30) {
		if f, ok := e.(FullEntry); ok {
			v.walk = append(v.walk, b.name(f.Commit.Hash))
		}
	}
	return v
}

func assertSameView(t *testing.T, before, after graphView) {
	t.Helper()
	if before.count != after.count {
		t.Errorf("CommitCount %d -> %d", before.count, after.count)
	}
	for h, c := range before.commits {
		if !reflect.DeepEqual(c, after.commits[h]) {
			t.Errorf("commit %s changed:\n before %+v\n after  %+v", h.Hex()[:8], c, after.commits[h])
		}
		if before.gens[h] != after.gens[h] {
			t.Errorf("generation of %s %d -> %d", h.Hex()[:8], before.gens[h], after.gens[h])
		}
	}
	if !reflect.DeepEqual(before.ancestry, after.ancestry) {
		t.Error("IsAncestor answers changed")
	}
	if !reflect.DeepEqual(before.byTx, after.byTx) {
		t.Errorf("transaction lookups changed: %v -> %v", before.byTx, after.byTx)
	}
	if len(before.walk) < 2 {
		t.Fatalf("walk saw %d commits; the comparison below would prove nothing", len(before.walk))
	}
	if !reflect.DeepEqual(before.walk, after.walk) {
		t.Errorf("Walk changed:\n before %v\n after  %v", before.walk, after.walk)
	}
	if before.horizon != after.horizon {
		t.Errorf("Horizon %d -> %d", before.horizon, after.horizon)
	}
}

func graphDag(t *testing.T) (*dagBuilder, []codec.Hash, string) {
	t.Helper()
	skipWithoutMmap(t)
	b := newDagBuilder(t)
	b.dag.SetGraphSettings(GraphSettings{FileEnabled: true, RebuildCommits: 1 << 30})
	path := filepath.Join(t.TempDir(), "graph")
	b.dag.ConfigureGraphFile(path, false)
	t.Cleanup(b.dag.CloseGraphFile)
	hashes := append([]codec.Hash{b.root}, richGraph(b)...)
	_, _, _, m := b.diamond()
	hashes = append(hashes, m)
	return b, hashes, path
}

func allHashes(b *dagBuilder) []codec.Hash {
	var out []codec.Hash
	b.dag.mu.RLock()
	b.dag.nodes.forEach(func(h codec.Hash) { out = append(out, h) })
	b.dag.mu.RUnlock()
	return out
}

// The file changes where commits live and nothing else.
func TestGraphFileIsInvisibleToReaders(t *testing.T) {
	b, _, _ := graphDag(t)
	hashes := allHashes(b)
	before := viewOf(t, b, hashes)

	if err := b.dag.RebuildGraphFile(); err != nil {
		t.Fatal(err)
	}
	if !b.dag.GraphFileActive() {
		t.Fatal("file not active after rebuild")
	}
	st := b.dag.GraphFileStats()
	if st.FileCommits != len(hashes) || st.ResidentCommits != 0 || st.Segments != 1 {
		t.Fatalf("stats %+v; want %d commits in one segment and none resident", st, len(hashes))
	}
	if n := len(b.dag.txIndex); n != 0 {
		t.Fatalf("txIndex still holds %d entries the file answers", n)
	}
	assertSameView(t, before, viewOf(t, b, hashes))
}

// Commits appended after the file stay resident, the next rebuild folds them in, and the result
// is still the same graph.
func TestGraphFileRebuildMergesResidentTier(t *testing.T) {
	b, _, _ := graphDag(t)
	if err := b.dag.RebuildGraphFile(); err != nil {
		t.Fatal(err)
	}
	head, _ := b.dag.Head()
	x := b.commit("x", head)
	y := b.commit("y", x)
	if st := b.dag.GraphFileStats(); st.ResidentCommits != 2 {
		t.Fatalf("resident %d after two appends, want 2", st.ResidentCommits)
	}
	hashes := allHashes(b)
	before := viewOf(t, b, hashes)
	if err := b.dag.RebuildGraphFile(); err != nil {
		t.Fatal(err)
	}
	if st := b.dag.GraphFileStats(); st.ResidentCommits != 0 || st.Segments != 1 {
		t.Fatalf("after a full rewrite: %+v", st)
	}
	assertSameView(t, before, viewOf(t, b, hashes))
	if !b.dag.IsAncestor(b.root, y) {
		t.Fatal("root is not an ancestor of y")
	}
}

// A commit removed after the file was written must stay removed - in this process, and in the
// next one that attaches the same file to a DAG that does not hold it.
func TestGraphFileNeverResurrectsDeletedCommits(t *testing.T) {
	b, _, path := graphDag(t)
	head, _ := b.dag.Head()
	tip := b.commit("tip", head)
	victim := b.commit("victim", tip)
	if err := b.dag.RebuildGraphFile(); err != nil {
		t.Fatal(err)
	}
	if _, err := b.dag.StubCommit(victim, "archive://x"); err != nil {
		t.Fatal(err)
	}
	if b.dag.HasCommit(victim) {
		t.Fatal("stubbed commit still visible through the file")
	}
	countAfterStub := b.dag.CommitCount()

	// A fresh DAG that never held victim, attaching the same file.
	fresh := newDagBuilder(t)
	fresh.dag.SetGraphSettings(GraphSettings{FileEnabled: true, RebuildCommits: 1 << 30})
	fresh.dag.ConfigureGraphFile(path, false)
	t.Cleanup(fresh.dag.CloseGraphFile)
	for _, h := range allHashes(b) {
		c, _ := b.dag.GetCommit(h)
		if len(c.ParentHashes) == 0 {
			continue // genesis differs per DAG
		}
		_ = fresh.dag.PutCommit(c, false)
	}
	before := fresh.dag.CommitCount()
	if err := fresh.dag.OpenGraphFile(); err != nil {
		t.Fatal(err)
	}
	if fresh.dag.HasCommit(victim) {
		t.Fatal("file resurrected a commit the DAG does not hold")
	}
	if got := fresh.dag.CommitCount(); got != before {
		t.Fatalf("attach changed CommitCount %d -> %d", before, got)
	}
	_ = countAfterStub
}

func TestGraphFileCorruptIsRemovedAndHarmless(t *testing.T) {
	b, _, path := graphDag(t)
	if err := b.dag.RebuildGraphFile(); err != nil {
		t.Fatal(err)
	}
	seg := onlySegment(t, path)
	raw, err := os.ReadFile(seg)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 0xFF
	if err := os.WriteFile(seg, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	fresh := newDagBuilder(t)
	fresh.dag.SetGraphSettings(GraphSettings{FileEnabled: true})
	fresh.dag.ConfigureGraphFile(path, false)
	head, _ := fresh.dag.Head()
	fresh.commit("a", head)
	before := fresh.dag.CommitCount()
	if err := fresh.dag.OpenGraphFile(); !errors.Is(err, errGraphFile) {
		t.Fatalf("OpenGraphFile on a corrupt file = %v, want errGraphFile", err)
	}
	if fresh.dag.GraphFileActive() {
		t.Fatal("corrupt file attached")
	}
	if _, err := os.Stat(seg); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("corrupt segment left in place")
	}
	if fresh.dag.CommitCount() != before {
		t.Fatal("failed attach changed the DAG")
	}
}

func TestGraphFileRejectsOtherNamespace(t *testing.T) {
	b, _, path := graphDag(t)
	if err := b.dag.RebuildGraphFile(); err != nil {
		t.Fatal(err)
	}
	if _, err := openGraphBase(onlySegment(t, path), "other"); !errors.Is(err, errGraphFile) {
		t.Fatalf("open for another namespace = %v", err)
	}
}

func TestGraphFileRebuildThreshold(t *testing.T) {
	skipWithoutMmap(t)
	b := newDagBuilder(t)
	b.dag.SetGraphSettings(GraphSettings{FileEnabled: true, RebuildCommits: 5})
	b.dag.ConfigureGraphFile(filepath.Join(t.TempDir(), "g"), false)
	t.Cleanup(b.dag.CloseGraphFile)
	head := b.root
	for i := 0; i < 3; i++ {
		head = b.commit("c", head)
	}
	if did, err := b.dag.MaybeRebuildGraphFile(); err != nil || did {
		t.Fatalf("rebuilt below threshold: %v %v", did, err)
	}
	for i := 0; i < 3; i++ {
		head = b.commit("c", head)
	}
	if did, err := b.dag.MaybeRebuildGraphFile(); err != nil || !did {
		t.Fatalf("did not rebuild above threshold: %v %v", did, err)
	}
}

// Ops and messages survive the move, and ops eviction/hydration still works on file commits.
func TestGraphFileKeepsMessagesAndOctopusParents(t *testing.T) {
	skipWithoutMmap(t)
	d, _ := NewInMemoryCommitDag("ns")
	d.SetGraphSettings(GraphSettings{FileEnabled: true})
	d.ConfigureGraphFile(filepath.Join(t.TempDir(), "g"), false)
	t.Cleanup(d.CloseGraphFile)
	var parents []codec.Hash
	for i := 0; i < 4; i++ {
		tx, _ := codec.RandomUUID()
		c, err := document.BuildCommit(nil, "ns", tx, codec.Timestamp{EpochMillis: int64(i)}, tx, nil, codec.Hash{}, nil, "root")
		if err != nil {
			t.Fatal(err)
		}
		if err := d.PutCommit(c, false); err != nil {
			t.Fatal(err)
		}
		parents = append(parents, c.Hash)
	}
	schema := codec.Hash{}
	schema.Bytes[3] = 7
	tx, _ := codec.RandomUUID()
	oct, err := document.BuildCommit(parents, "ns", tx, codec.Timestamp{EpochMillis: 99, MicroRemainder: 5}, tx, nil, codec.Hash{}, &schema, "octopus merge")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.PutCommit(oct, true); err != nil {
		t.Fatal(err)
	}
	if err := d.RebuildGraphFile(); err != nil {
		t.Fatal(err)
	}
	got, ok := d.GetCommit(oct.Hash)
	if !ok || !reflect.DeepEqual(got, oct) {
		t.Fatalf("octopus commit did not round trip:\n got %+v\nwant %+v", got, oct)
	}
	if again, _ := document.ComputeCommitHash(got); again != oct.Hash {
		t.Fatal("commit read from the file no longer hashes to itself")
	}
}

// Readers never see a torn graph while a rebuild swaps the file underneath them.
func TestGraphFileConcurrentRebuildAndReads(t *testing.T) {
	mappedBefore := mmapfile.MappedBytes()
	b, _, _ := graphDag(t)
	head, _ := b.dag.Head()
	for i := 0; i < 50; i++ {
		head = b.commit("c", head)
	}
	readHead := head
	hashes := allHashes(b)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for _, h := range hashes {
					if _, ok := b.dag.GetCommit(h); !ok {
						t.Errorf("commit vanished during rebuild")
						return
					}
				}
				b.dag.Walk(readHead, nil, 1<<30)
			}
		}()
	}
	for i := 0; i < 10; i++ {
		head = b.commit("w", head)
		if err := b.dag.RebuildGraphFile(); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
	b.dag.CloseGraphFile()
	if n := mmapfile.MappedBytes() - mappedBefore; n != 0 {
		t.Fatalf("%d bytes still mapped after close", n)
	}
}

func onlySegment(t *testing.T, dir string) string {
	t.Helper()
	seqs, err := listGraphSegments(dir)
	if err != nil || len(seqs) != 1 {
		t.Fatalf("want exactly one segment in %s, have %v (%v)", dir, seqs, err)
	}
	return filepath.Join(dir, graphSegmentName(seqs[0]))
}

// segmentedDag is a DAG with one full segment of ~100 commits and the given policy.
func segmentedDag(t *testing.T, rebuild, maxSegs int) (*dagBuilder, string, codec.Hash) {
	t.Helper()
	skipWithoutMmap(t)
	b := newDagBuilder(t)
	b.dag.SetGraphSettings(GraphSettings{FileEnabled: true, RebuildCommits: rebuild, MaxSegments: maxSegs})
	dir := filepath.Join(t.TempDir(), "graph")
	b.dag.ConfigureGraphFile(dir, false)
	t.Cleanup(b.dag.CloseGraphFile)
	head := b.root
	for i := 0; i < 100; i++ {
		head = b.commit("base", head)
	}
	if err := b.dag.CompactGraphFile(); err != nil {
		t.Fatal(err)
	}
	return b, dir, head
}

// Below the rewrite triggers, a write flushes only the new commits as a segment of their own.
func TestGraphFlushWritesOnlyTheResidentTier(t *testing.T) {
	b, dir, head := segmentedDag(t, 5, 8)
	before := b.dag.GraphFileStats()
	for i := 0; i < 6; i++ {
		head = b.commit("new", head)
	}
	hashes := allHashes(b)
	view := viewOf(t, b, hashes)
	did, err := b.dag.MaybeRebuildGraphFile()
	if err != nil || !did {
		t.Fatalf("no flush: %v %v", did, err)
	}
	st := b.dag.GraphFileStats()
	if st.Segments != 2 || st.ResidentCommits != 0 || st.FileCommits != before.FileCommits+6 {
		t.Fatalf("after flush: %+v (before %+v)", st, before)
	}
	seqs, _ := listGraphSegments(dir)
	newest, err := openGraphBase(filepath.Join(dir, graphSegmentName(seqs[len(seqs)-1])), "ns")
	if err != nil {
		t.Fatal(err)
	}
	defer newest.close()
	if newest.full || newest.count != 6 {
		t.Fatalf("flush segment full=%v count=%d, want a 6-record partial", newest.full, newest.count)
	}
	assertSameView(t, view, viewOf(t, b, hashes))
}

// Past MaxSegments, the next write is a full rewrite: one segment, older files gone.
func TestGraphTooManySegmentsTriggersFullRewrite(t *testing.T) {
	b, dir, head := segmentedDag(t, 2, 3)
	for round := 0; round < 4; round++ {
		head = b.commit("r", head)
		head = b.commit("r", head)
		if _, err := b.dag.MaybeRebuildGraphFile(); err != nil {
			t.Fatal(err)
		}
	}
	hashes := allHashes(b)
	view := viewOf(t, b, hashes)
	st := b.dag.GraphFileStats()
	if st.Segments > 3 {
		t.Fatalf("%d segments with MaxSegments 3", st.Segments)
	}
	seqs, _ := listGraphSegments(dir)
	if len(seqs) != st.Segments {
		t.Fatalf("%d files on disk for %d attached segments - a full rewrite left its inputs behind", len(seqs), st.Segments)
	}
	assertSameView(t, view, viewOf(t, b, hashes))
}

// Deleting more than the tombstone ratio of what the segments hold triggers a rewrite that drops
// the dead records, even with nothing new to write.
func TestGraphTombstonesTriggerFullRewrite(t *testing.T) {
	b, _, _ := segmentedDag(t, 1<<30, 8)
	head, _ := b.dag.Head()
	// Squash a window of history: every squashed commit becomes a hidden record.
	var window []codec.Hash
	for _, e := range b.dag.Walk(head, nil, 60) {
		if f, ok := e.(FullEntry); ok {
			window = append(window, f.Commit.Hash)
		}
	}
	// Archive the oldest 40 commits, which no branch names.
	victims := window[len(window)-40:]
	for _, h := range victims {
		if _, err := b.dag.StubCommit(h, "archive://x"); err != nil {
			t.Fatal(err)
		}
	}
	st := b.dag.GraphFileStats()
	if st.HiddenRecords != 40 {
		t.Fatalf("hidden %d after stubbing 40", st.HiddenRecords)
	}
	did, err := b.dag.MaybeRebuildGraphFile()
	if err != nil || !did {
		t.Fatalf("tombstone ratio did not trigger a rewrite: %v %v", did, err)
	}
	if st := b.dag.GraphFileStats(); st.HiddenRecords != 0 || st.Segments != 1 {
		t.Fatalf("after the tombstone rewrite: %+v", st)
	}
	for _, h := range victims {
		if b.dag.HasCommit(h) {
			t.Fatal("a stubbed commit came back")
		}
	}
}

// A crash between a full rewrite's rename and its deletes leaves the full segment beside the ones
// it replaced. The next open uses only the full one and removes the rest; a read-only open uses
// only the full one and removes nothing.
func TestGraphOpenHonoursSupersedingFullSegment(t *testing.T) {
	b, dir, head := segmentedDag(t, 1, 8)
	head = b.commit("x", head)
	if _, err := b.dag.MaybeRebuildGraphFile(); err != nil {
		t.Fatal(err)
	}
	// Snapshot the pre-rewrite files, rewrite, then put the old files back as a crash would.
	saved := map[string][]byte{}
	seqs, _ := listGraphSegments(dir)
	for _, s := range seqs {
		p := filepath.Join(dir, graphSegmentName(s))
		saved[p], _ = os.ReadFile(p)
	}
	if err := b.dag.CompactGraphFile(); err != nil {
		t.Fatal(err)
	}
	for p, raw := range saved {
		os.WriteFile(p, raw, 0o644)
	}
	hashes := allHashes(b)

	open := func(ro bool) *dagBuilder {
		f := newDagBuilder(t)
		f.dag.SetGraphSettings(GraphSettings{FileEnabled: true})
		f.dag.ConfigureGraphFile(dir, ro)
		t.Cleanup(f.dag.CloseGraphFile)
		for _, h := range hashes {
			if c, ok := b.dag.GetCommit(h); ok && len(c.ParentHashes) > 0 {
				f.dag.PutCommit(c, false)
			}
		}
		if err := f.dag.OpenGraphFile(); err != nil {
			t.Fatal(err)
		}
		if st := f.dag.GraphFileStats(); st.Segments != 1 {
			t.Fatalf("read-only=%v: %d segments attached, want only the full one", ro, st.Segments)
		}
		return f
	}
	open(true)
	if left, _ := listGraphSegments(dir); len(left) != len(saved)+1 {
		t.Fatalf("read-only open removed files: %d left", len(left))
	}
	open(false)
	if left, _ := listGraphSegments(dir); len(left) != 1 {
		t.Fatalf("writable open left %d segment files, want 1", len(left))
	}
}

// A commit deleted and then put back is resident while a segment still has its (hidden) record.
// A flush must not write it - two segments would share it - but a full rewrite must.
func TestGraphReaddedCommitWaitsForFullRewrite(t *testing.T) {
	b, _, head := segmentedDag(t, 1, 8)
	c, _ := b.dag.GetCommit(head)
	parent := c.ParentHashes[0]
	if err := b.dag.SetHead(mainBranch, parent); err != nil {
		t.Fatal(err)
	}
	if _, err := b.dag.StubCommit(head, "archive://x"); err != nil {
		t.Fatal(err)
	}
	b.dag.mu.Lock()
	delete(b.dag.stubs, head)
	b.dag.storeCommitLocked(c)
	b.dag.mu.Unlock()
	if !b.dag.HasCommit(head) {
		t.Fatal("re-added commit not visible")
	}
	b.commit("y", parent) // something genuinely new, so a flush has work
	if _, err := b.dag.MaybeRebuildGraphFile(); err != nil {
		t.Fatal(err)
	}
	if st := b.dag.GraphFileStats(); st.ResidentCommits != 1 {
		t.Fatalf("after flush %+v: the re-added commit should be the only one left resident", st)
	}
	count := b.dag.CommitCount()
	if err := b.dag.CompactGraphFile(); err != nil {
		t.Fatal(err)
	}
	if st := b.dag.GraphFileStats(); st.ResidentCommits != 0 || st.HiddenRecords != 0 {
		t.Fatalf("after full rewrite: %+v", st)
	}
	if got, ok := b.dag.GetCommit(head); !ok || got.Hash != head || b.dag.CommitCount() != count {
		t.Fatal("re-added commit lost in the full rewrite")
	}
}

// find must answer exactly what a linear scan answers, for present and absent hashes alike - the
// interpolation is an optimization and may never change a result.
func TestGraphFindMatchesLinearScan(t *testing.T) {
	skipWithoutMmap(t)
	d, _ := NewInMemoryCommitDag("ns")
	d.SetGraphSettings(GraphSettings{FileEnabled: true})
	dir := filepath.Join(t.TempDir(), "g")
	d.ConfigureGraphFile(dir, false)
	t.Cleanup(d.CloseGraphFile)
	parent, _ := d.Head()
	for i := 0; i < 5000; i++ {
		tx, _ := codec.RandomUUID()
		c, _ := document.BuildCommit([]codec.Hash{parent}, "ns", tx, codec.Timestamp{EpochMillis: int64(i)}, tx, nil, codec.Hash{}, nil, "")
		if err := d.PutCommit(c, true); err != nil {
			t.Fatal(err)
		}
		parent = c.Hash
	}
	if err := d.CompactGraphFile(); err != nil {
		t.Fatal(err)
	}
	b := d.nodes.segs[0]
	for pos := 0; pos < b.count; pos++ {
		if got, ok := b.find(b.hashAt(pos)); !ok || got != pos {
			t.Fatalf("find(record %d) = %d, %v", pos, got, ok)
		}
	}
	for i := 0; i < 5000; i++ {
		var h codec.Hash
		h.Bytes[0], h.Bytes[1], h.Bytes[5], h.Bytes[31] = byte(i), byte(i>>8), byte(i*7), byte(i*13)
		_, ok := b.find(h)
		want := false
		for pos := 0; pos < b.count; pos++ {
			if b.hashAt(pos) == h {
				want = true
			}
		}
		if ok != want {
			t.Fatalf("find(%x) = %v, linear scan says %v", h.Bytes[:4], ok, want)
		}
	}
}

// naiveHorizon is Horizon by walking every commit - what the candidate-only walk must agree with.
func naiveHorizon(d *InMemoryCommitDag) []codec.Hash {
	d.mu.RLock()
	defer d.mu.RUnlock()
	seen := map[codec.Hash]struct{}{}
	for h := range d.shallow {
		seen[h] = struct{}{}
	}
	d.nodes.forEach(func(h codec.Hash) {
		d.nodes.forEachParent(h, func(p codec.Hash) bool {
			_, st := d.stubs[p]
			if d.nodes.has(p) || st {
				return true
			}
			seen[h] = struct{}{}
			return false
		})
	})
	out := make([]codec.Hash, 0, len(seen))
	for h := range seen {
		out = append(out, h)
	}
	return sortedHashes(out)
}

func sortedHashes(hs []codec.Hash) []codec.Hash {
	out := append([]codec.Hash(nil), hs...)
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].Bytes[:], out[j].Bytes[:]) < 0 })
	return out
}

// Horizon after commits are removed from beneath segment commits - stubbed (not on the horizon)
// and dropped outright (on it) - must match the walk of every commit.
func TestGraphHorizonMatchesFullWalk(t *testing.T) {
	b, _, head := segmentedDag(t, 1, 8)
	c, _ := b.dag.GetCommit(head)
	mid := c.ParentHashes[0]
	cm, _ := b.dag.GetCommit(mid)
	below := cm.ParentHashes[0]
	b.dag.mu.Lock()
	b.dag.stubs[below] = document.CommitStub{OriginalHash: below}
	b.dag.nodes.delete(below)
	b.dag.mu.Unlock()
	viewOf(t, b, []codec.Hash{head})
	b.dag.mu.Lock()
	delete(b.dag.stubs, below) // now missing outright: mid is on the horizon
	b.dag.mu.Unlock()
	if got := sortedHashes(b.dag.Horizon()); !reflect.DeepEqual(got, naiveHorizon(b.dag)) || len(got) == 0 {
		t.Fatalf("Horizon %v after an unstubbed removal; want %v", got, naiveHorizon(b.dag))
	}
}
