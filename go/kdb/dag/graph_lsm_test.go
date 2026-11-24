package dag

import (
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// Tests for the commit graph as an LSM that flushes by size (M1 in
// docs/kdb-basic-rw-performance-plan.md): the bound on the resident tier, the streaming flush,
// size-tiered merges and the file order that keeps a crash from leaving two segments that share
// a commit.

// lsmDag is a DAG with the graph file on, the given flush threshold and segment cap.
func lsmDag(t *testing.T, rebuild, maxSegs int) (*InMemoryCommitDag, string) {
	t.Helper()
	skipWithoutMmap(t)
	d, _ := NewInMemoryCommitDag("ns")
	d.SetGraphSettings(GraphSettings{FileEnabled: true, RebuildCommits: rebuild, MaxSegments: maxSegs})
	dir := filepath.Join(t.TempDir(), "graph")
	d.ConfigureGraphFile(dir, false)
	t.Cleanup(d.CloseGraphFile)
	return d, dir
}

// chain appends n commits on top of parent, every 97th a merge of a side branch, and returns the
// new head and every hash added.
func chain(t testing.TB, d *InMemoryCommitDag, parent codec.Hash, n int, seed int64) (codec.Hash, []codec.Hash) {
	t.Helper()
	author := codec.UUID{MSB: 1, LSB: seed % 3} // a server's commits come from a handful of nodes
	var out []codec.Hash
	var side codec.Hash
	for i := 0; i < n; i++ {
		tx, _ := codec.RandomUUID()
		parents := []codec.Hash{parent}
		if i%97 == 96 && side != (codec.Hash{}) {
			parents = append(parents, side)
		}
		msg := ""
		if i%31 == 0 {
			msg = "m" + strconv.Itoa(i)
		}
		c, err := document.BuildCommit(parents, "ns", tx, codec.Timestamp{EpochMillis: seed*1e9 + int64(i)}, author, nil, codec.Hash{}, nil, msg)
		if err != nil {
			t.Fatal(err)
		}
		if err := d.PutCommit(c, true); err != nil {
			t.Fatal(err)
		}
		if i%97 == 40 {
			side = c.Hash
		}
		parent = c.Hash
		out = append(out, c.Hash)
	}
	return parent, out
}

// waitGraphIdle waits for the background flusher and compactor to have nothing left to do.
func waitGraphIdle(t testing.TB, d *InMemoryCommitDag) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		d.mu.RLock()
		busy := d.graphFlushing || d.graphMerging
		d.mu.RUnlock()
		if !busy {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("graph writers still busy after 60s")
}

// assertDisjointOnDisk checks the invariant every reader relies on: no two segments on disk under
// their final names share a commit, unless a higher one is full (and so supersedes the other).
func assertDisjointOnDisk(t *testing.T, dir string) {
	t.Helper()
	seqs, err := listGraphSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[codec.Hash]uint64{}
	var bases []*graphBase
	for _, s := range seqs {
		b, err := openGraphBase(filepath.Join(dir, graphSegmentName(s)), "ns")
		if err != nil {
			t.Fatalf("segment %d on disk unusable: %v", s, err)
		}
		bases = append(bases, b)
	}
	defer func() {
		for _, b := range bases {
			b.close()
		}
	}()
	start := 0
	for i, b := range bases {
		if b.full {
			start = i
		}
	}
	for _, b := range bases[start:] {
		for pos := 0; pos < b.count; pos++ {
			h := b.hashAt(pos)
			if other, dup := seen[h]; dup {
				t.Fatalf("segments %d and %d on disk both hold %s", other, b.seq, h.Hex()[:12])
			}
			seen[h] = b.seq
		}
	}
}

// The pin for M1: with size-triggered flushes on, the resident tier stays near its threshold
// however many commits arrive - it used to hold every commit since the last maintenance pass.
func TestGraphResidentTierStaysBounded(t *testing.T) {
	const threshold = 500
	n := 20_000
	if !testing.Short() {
		n = 100_000
	}
	d, dir := lsmDag(t, threshold, 0)
	d.StartGraphAutoFlush()
	head, _ := d.Head()
	maxResident := 0
	for done := 0; done < n; done += 100 {
		head, _ = chain(t, d, head, 100, int64(done))
		if r := d.GraphFileStats().ResidentCommits; r > maxResident {
			maxResident = r
		}
		// Appending in memory outruns a flush's fsync many times over; a server's commit rate
		// does not. Letting each flush finish before the next batch is that rate, so the bound
		// below is exact rather than a race with the disk.
		waitGraphIdle(t, d)
	}
	waitGraphIdle(t, d)
	st := d.GraphFileStats()
	t.Logf("after %d commits: max resident %d, now %+v", n, maxResident, st)
	if st.ResidentCommits >= threshold {
		t.Fatalf("resident tier holds %d commits once idle, want < %d", st.ResidentCommits, threshold)
	}
	// The threshold plus one batch: a flush starts at the threshold and the batch in flight
	// finishes first.
	if maxResident > threshold+100 {
		t.Fatalf("resident tier reached %d commits with a threshold of %d", maxResident, threshold)
	}
	if st.FileCommits+st.ResidentCommits != n+1 {
		t.Fatalf("%d commits in segments + %d resident, want %d", st.FileCommits, st.ResidentCommits, n+1)
	}
	// Size-tiered: segments stay few, and each record is rewritten a logarithmic number of times.
	if st.Segments > 3*graphMergeMin {
		t.Fatalf("%d segments after %d commits", st.Segments, n)
	}
	if amp := float64(st.RecordsWritten) / float64(n); amp > 6 {
		t.Fatalf("write amplification %.1f records written per commit", amp)
	}
	assertDisjointOnDisk(t, dir)
}

// A flush streams the resident tier from where it is: it allocates a small fraction of what the
// tier holds (the old flush copied every record first - the 2x peak).
func TestGraphFlushDoesNotCopyTheResidentTier(t *testing.T) {
	n := 20_000
	d, _ := lsmDag(t, 1<<30, 0)
	head, _ := d.Head()
	chain(t, d, head, n, 1)
	if _, err := d.flushResident(true); err != nil { // the first, full segment
		t.Fatal(err)
	}
	head, _ = d.Head()
	chain(t, d, head, n, 2)
	tier := d.GraphResidentBytes()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	if did, err := d.flushResident(true); err != nil || !did {
		t.Fatalf("flush: %v %v", did, err)
	}
	runtime.ReadMemStats(&after)
	alloc := int64(after.TotalAlloc - before.TotalAlloc)
	perCommit := float64(alloc) / float64(n)
	t.Logf("flush of %d commits allocated %d bytes (%.0f B/commit) against a %d-byte resident tier", n, alloc, perCommit, tier)
	// 40 B of entry, 4 of position, 20 of transaction key, 4 of free slot, and the writer's
	// 256 KiB buffer. A copy of the tier would be over 256 B a commit.
	if perCommit > 120 {
		t.Fatalf("flush allocated %.0f B per commit; it is copying the resident tier", perCommit)
	}
	if st := d.GraphFileStats(); st.ResidentCommits != 0 {
		t.Fatalf("after flush %+v", st)
	}
}

// Writes that land on frozen slots - a re-put, a generation change, a delete - while a flush
// streams them go to fresh slots or wait, and the graph comes out the same.
func TestGraphFlushWithConcurrentWrites(t *testing.T) {
	b := newDagBuilder(t)
	skipWithoutMmap(t)
	b.dag.SetGraphSettings(GraphSettings{FileEnabled: true, RebuildCommits: 50})
	dir := filepath.Join(t.TempDir(), "g")
	b.dag.ConfigureGraphFile(dir, false)
	t.Cleanup(b.dag.CloseGraphFile)
	b.dag.StartGraphAutoFlush()
	head := b.root
	var all []codec.Hash
	for i := 0; i < 3000; i++ {
		head = b.commit("c", head)
		all = append(all, head)
		if i%7 == 0 {
			// Re-put the commit with its operations: a rewrite of a resident record.
			c, _ := b.dag.GetCommit(all[len(all)/2])
			b.dag.mu.Lock()
			b.dag.replaceCommitLocked(c)
			b.dag.mu.Unlock()
		}
		if i%13 == 0 {
			b.dag.InvalidateGenerations() // the next ancestry query rewrites generations
			b.dag.IsAncestor(b.root, head)
		}
	}
	waitGraphIdle(t, b.dag)
	// viewOf compares every pair, so a sample: the oldest, some in between, and the newest.
	var sample []codec.Hash
	for i := 0; i < len(all); i += len(all) / 40 {
		sample = append(sample, all[i])
	}
	sample = append(sample, b.root, head)
	before := viewOf(t, b, sample)
	if err := b.dag.CompactGraphFile(); err != nil {
		t.Fatal(err)
	}
	assertSameView(t, before, viewOf(t, b, sample))
	for _, h := range all {
		if !b.dag.HasCommit(h) {
			t.Fatalf("%s lost", h.Hex()[:12])
		}
	}
}

// Every write kind - flush, merge, tombstone rewrite, major compaction - changes where commits live
// and nothing else, across a random sequence of them with deletions and re-adds in between.
func TestGraphLSMStepsAreInvisibleToReaders(t *testing.T) {
	skipWithoutMmap(t)
	rng := rand.New(rand.NewSource(7))
	b := newDagBuilder(t)
	b.dag.SetGraphSettings(GraphSettings{FileEnabled: true, RebuildCommits: 20, MaxSegments: 6})
	dir := filepath.Join(t.TempDir(), "g")
	b.dag.ConfigureGraphFile(dir, false)
	t.Cleanup(b.dag.CloseGraphFile)
	head := b.root
	for step := 0; step < 60; step++ {
		for i := rng.Intn(40); i > 0; i-- {
			head = b.commit("c", head)
		}
		if rng.Intn(4) == 0 {
			_, _, _, m := b.diamond()
			_ = m
			head, _ = b.dag.Head()
		}
		if rng.Intn(5) == 0 {
			// Archive an old commit no branch names: a hidden record, if it is in a segment.
			var window []codec.Hash
			for _, e := range b.dag.Walk(head, nil, 200) {
				if f, ok := e.(FullEntry); ok {
					window = append(window, f.Commit.Hash)
				}
			}
			if len(window) > 50 {
				for _, h := range window[len(window)-10:] {
					b.dag.StubCommit(h, "archive://x")
				}
			}
		}
		hashes := allHashes(b)
		if len(hashes) > 60 {
			rng.Shuffle(len(hashes), func(i, j int) { hashes[i], hashes[j] = hashes[j], hashes[i] })
			hashes = hashes[:60]
		}
		before := viewOf(t, b, hashes)
		switch rng.Intn(5) {
		case 0:
			if err := b.dag.CompactGraphFile(); err != nil {
				t.Fatal(err)
			}
		case 1:
			if _, err := b.dag.flushResident(true); err != nil {
				t.Fatal(err)
			}
		default:
			if _, err := b.dag.MaybeRebuildGraphFile(); err != nil {
				t.Fatal(err)
			}
		}
		assertSameView(t, before, viewOf(t, b, hashes))
		assertDisjointOnDisk(t, dir)
		if seqs, _ := listGraphSegments(dir); len(seqs) != b.dag.GraphFileStats().Segments {
			t.Fatalf("step %d: %d segment files for %d attached", step, len(seqs), b.dag.GraphFileStats().Segments)
		}
	}
}

// Four flushes of a size merge into one; segments of very different sizes do not.
func TestGraphMergesSizeTiered(t *testing.T) {
	d, dir := lsmDag(t, 10, 64)
	head, _ := d.Head()
	head, _ = chain(t, d, head, 1000, 1)
	if err := d.CompactGraphFile(); err != nil { // one big segment
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		head, _ = chain(t, d, head, 10, int64(10+i))
		if _, err := d.MaybeRebuildGraphFile(); err != nil {
			t.Fatal(err)
		}
	}
	if st := d.GraphFileStats(); st.Segments != 4 {
		t.Fatalf("after three small flushes: %+v, want 4 segments", st)
	}
	head, _ = chain(t, d, head, 10, 20)
	if _, err := d.MaybeRebuildGraphFile(); err != nil {
		t.Fatal(err)
	}
	st := d.GraphFileStats()
	if st.Segments != 2 {
		t.Fatalf("after the fourth small flush: %+v, want the big one and the four merged", st)
	}
	d.mu.RLock()
	counts := []int{d.nodes.segs[0].count, d.nodes.segs[1].count}
	d.mu.RUnlock()
	if counts[0] != 40 || counts[1] != 1001 {
		t.Fatalf("segment sizes %v, want [40 1001]", counts)
	}
	assertDisjointOnDisk(t, dir)
}

// A merge whose inputs the checkpoint on disk names cannot delete them, so its output waits under
// .pending - never sharing a commit with them under a final name - until the next checkpoint names
// the output; then the inputs go and the output takes its name.
func TestGraphMergeWaitsForTheCheckpoint(t *testing.T) {
	d, dir := lsmDag(t, 10, 64)
	head, _ := d.Head()
	for i := 0; i < 4; i++ {
		head, _ = chain(t, d, head, 10, int64(i))
		if _, err := d.flushResident(true); err != nil {
			t.Fatal(err)
		}
	}
	// A checkpoint names the four segments now attached.
	_, refs, _, err := d.CheckpointSnapshotWithGraph(nil)
	if err != nil || len(refs) != 4 {
		t.Fatalf("refs %v err %v", refs, err)
	}
	d.SetGraphCheckpointRefs(refs)
	if did, err := d.mergeOnce(); err != nil || !did {
		t.Fatalf("merge: %v %v", did, err)
	}
	st := d.GraphFileStats()
	if st.Segments != 1 || st.Pending != 1 {
		t.Fatalf("after merging pinned inputs: %+v, want one attached segment, pending", st)
	}
	if seqs, _ := listGraphSegments(dir); len(seqs) != 4 {
		t.Fatalf("final-named segments %v, want the four inputs only", seqs)
	}
	assertDisjointOnDisk(t, dir)
	// The next checkpoint names the merge output.
	_, refs, _, err = d.CheckpointSnapshotWithGraph(nil)
	if err != nil || len(refs) != 1 {
		t.Fatalf("refs %v err %v", refs, err)
	}
	d.SetGraphCheckpointRefs(refs)
	if st := d.GraphFileStats(); st.Pending != 0 {
		t.Fatalf("still pending after the checkpoint: %+v", st)
	}
	seqs, _ := listGraphSegments(dir)
	if len(seqs) != 1 || seqs[0] != refs[0].Seq {
		t.Fatalf("on disk %v, want only %d", seqs, refs[0].Seq)
	}
	if leftovers := graphLeftovers(dir); len(leftovers) != 0 {
		t.Fatalf("leftover files %v", leftovers)
	}
}

func graphLeftovers(dir string) []string {
	entries, _ := os.ReadDir(dir)
	var out []string
	for _, e := range entries {
		if _, ok := graphLeftoverSeq(e.Name()); ok {
			out = append(out, e.Name())
		}
	}
	return out
}

// The crash windows of a merge with pinned inputs, as the next open sees them.
func TestGraphMergeCrashWindowsOpenCleanly(t *testing.T) {
	setup := func(t *testing.T) (*InMemoryCommitDag, string, []codec.Hash, []GraphSegmentRef, []codec.Hash) {
		d, dir := lsmDag(t, 10, 64)
		head, _ := d.Head()
		var hashes []codec.Hash
		for i := 0; i < 4; i++ {
			var hs []codec.Hash
			head, hs = chain(t, d, head, 10, int64(i))
			hashes = append(hashes, hs...)
			if _, err := d.flushResident(true); err != nil {
				t.Fatal(err)
			}
		}
		_, refs, hidden, _ := d.CheckpointSnapshotWithGraph(nil)
		d.SetGraphCheckpointRefs(refs)
		if did, err := d.mergeOnce(); err != nil || !did {
			t.Fatalf("merge: %v %v", did, err)
		}
		return d, dir, hashes, refs, hidden
	}
	reopen := func(t *testing.T, dir string, d *InMemoryCommitDag, hashes []codec.Hash, viaCheckpoint bool, refs []GraphSegmentRef, hidden []codec.Hash) *InMemoryCommitDag {
		f, _ := NewInMemoryCommitDag("ns")
		f.SetGraphSettings(GraphSettings{FileEnabled: true})
		f.ConfigureGraphFile(dir, false)
		t.Cleanup(f.CloseGraphFile)
		if viaCheckpoint {
			if err := f.AttachCheckpointGraph(refs, hidden); err != nil {
				t.Fatalf("checkpoint attach: %v", err)
			}
		} else {
			// The replay path: every commit resident first, then whatever segments are on disk.
			for _, h := range hashes {
				c, _ := d.GetCommit(h)
				f.PutCommit(c, false)
			}
			if err := f.OpenGraphFile(); err != nil {
				t.Fatal(err)
			}
		}
		for _, h := range hashes {
			if !f.HasCommit(h) {
				t.Fatalf("reopened DAG lost %s", h.Hex()[:12])
			}
		}
		assertDisjointOnDisk(t, dir)
		if l := graphLeftovers(dir); len(l) != 0 {
			t.Fatalf("leftovers after a writable open: %v", l)
		}
		return f
	}

	// Crash before the next checkpoint: the checkpoint on disk names the four inputs, which are
	// still there; the pending output is garbage.
	t.Run("old checkpoint", func(t *testing.T) {
		d, dir, hashes, refs, hidden := setup(t)
		f := reopen(t, dir, d, hashes, true, refs, hidden)
		if st := f.GraphFileStats(); st.Segments != 4 {
			t.Fatalf("%+v, want the four the checkpoint names", st)
		}
	})
	// Crash after a checkpoint naming the output was written, before SetGraphCheckpointRefs: the
	// output is found under .pending, the inputs removed, the output renamed.
	t.Run("new checkpoint, not yet renamed", func(t *testing.T) {
		d, dir, hashes, _, _ := setup(t)
		_, refs, hidden, err := d.CheckpointSnapshotWithGraph(nil)
		if err != nil || len(refs) != 1 {
			t.Fatalf("refs %v %v", refs, err)
		}
		f := reopen(t, dir, d, hashes, true, refs, hidden)
		if st := f.GraphFileStats(); st.Segments != 1 || st.Pending != 0 {
			t.Fatalf("%+v, want the merge output, renamed", st)
		}
	})
	// The replay path (checkpoint unusable): only final-named segments count, and they are
	// disjoint.
	t.Run("replay", func(t *testing.T) {
		d, dir, hashes, _, _ := setup(t)
		f := reopen(t, dir, d, hashes, false, nil, nil)
		if st := f.GraphFileStats(); st.Segments != 4 {
			t.Fatalf("%+v, want the four inputs", st)
		}
	})
}

// A checkpoint pins what it names from the capture on: a merge that finishes between the capture
// and the write must not delete a segment the checkpoint is about to name.
func TestGraphCheckpointCapturePinsSegments(t *testing.T) {
	d, dir := lsmDag(t, 10, 64)
	head, _ := d.Head()
	for i := 0; i < 4; i++ {
		head, _ = chain(t, d, head, 10, int64(i))
		if _, err := d.flushResident(true); err != nil {
			t.Fatal(err)
		}
	}
	_, refs, _, _ := d.CheckpointSnapshotWithGraph(nil) // captured; not yet written
	if did, err := d.mergeOnce(); err != nil || !did {
		t.Fatalf("merge: %v %v", did, err)
	}
	for _, r := range refs {
		if _, err := os.Stat(filepath.Join(dir, graphSegmentName(r.Seq))); err != nil {
			t.Fatalf("segment %d the checkpoint names was deleted: %v", r.Seq, err)
		}
	}
	d.SetGraphCheckpointRefs(refs) // the checkpoint is written
	if st := d.GraphFileStats(); st.Pending != 1 {
		t.Fatalf("%+v: the merge output must wait for a checkpoint that names it", st)
	}
	assertDisjointOnDisk(t, dir)
}

// The background writers stop when the DAG closes, mid-write if need be, and leave nothing behind
// a next open would misread.
func TestGraphBackgroundWritersStopAtClose(t *testing.T) {
	d, dir := lsmDag(t, 200, 0)
	d.StartGraphAutoFlush()
	head, _ := d.Head()
	var hashes []codec.Hash
	for i := 0; i < 20; i++ {
		var hs []codec.Hash
		head, hs = chain(t, d, head, 200, int64(i))
		hashes = append(hashes, hs...)
	}
	d.StopGraphWork()
	if d.GraphFileStats().ResidentCommits > 0 {
		if did, _ := d.MaybeRebuildGraphFile(); did {
			t.Fatal("a stopped DAG still writes segments")
		}
	}
	all := append([]codec.Hash{}, hashes...)
	commits := map[codec.Hash]document.Commit{}
	for _, h := range all {
		c, _ := d.GetCommit(h)
		commits[h] = c
	}
	d.CloseGraphFile()
	assertDisjointOnDisk(t, dir)
	f, _ := NewInMemoryCommitDag("ns")
	f.SetGraphSettings(GraphSettings{FileEnabled: true})
	f.ConfigureGraphFile(dir, false)
	t.Cleanup(f.CloseGraphFile)
	for _, h := range all {
		f.PutCommit(commits[h], false)
	}
	if err := f.OpenGraphFile(); err != nil {
		t.Fatal(err)
	}
	for _, h := range all {
		if !f.HasCommit(h) {
			t.Fatal("commit lost across close and reopen")
		}
	}
}

func TestGraphBloomFalsePositives(t *testing.T) {
	const n = 100_000
	f := newGraphBloom(n)
	rng := rand.New(rand.NewSource(3))
	for i := 0; i < n; i++ {
		f.add(rng.Uint64())
	}
	rng = rand.New(rand.NewSource(3))
	for i := 0; i < n; i++ {
		if !f.has(rng.Uint64()) {
			t.Fatal("false negative")
		}
	}
	fp := 0
	for i := 0; i < n; i++ {
		if f.has(rng.Uint64()) {
			fp++
		}
	}
	if rate := float64(fp) / n; rate > 0.02 {
		t.Fatalf("false positive rate %.3f", rate)
	}
}

// A segment's bloom filter never says "absent" for a commit or transaction it holds.
func TestGraphSegmentBloomHasEveryRecord(t *testing.T) {
	d, _ := lsmDag(t, 1<<30, 0)
	head, _ := d.Head()
	_, hashes := chain(t, d, head, 3000, 1)
	if err := d.CompactGraphFile(); err != nil {
		t.Fatal(err)
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	b := d.nodes.segs[0]
	for _, h := range hashes {
		if !b.mayHold(&h) {
			t.Fatal("bloom filter misses a commit the segment holds")
		}
		c, _ := d.nodes.commit(h)
		if !b.mayHoldTx(c.TransactionID) {
			t.Fatal("bloom filter misses a transaction the segment holds")
		}
	}
}

// The long run: a million commits or more through size-triggered flushes and merges, sampling the
// heap. The resident tier stays bounded, and the heap does not double at a flush.
// KDB_GRAPH_LONGRUN sets the commit count (default 1,000,000).
func TestGraphResidentTierLongRun(t *testing.T) {
	if testing.Short() {
		t.Skip("long run")
	}
	n := 1_000_000
	if v, _ := strconv.Atoi(os.Getenv("KDB_GRAPH_LONGRUN")); v > 0 {
		n = v
	}
	d, dir := lsmDag(t, DefaultGraphRebuildCommits, 0)
	d.StartGraphAutoFlush()
	head, _ := d.Head()
	var mu sync.Mutex
	var peakHeap, peakResident uint64
	var samples []string
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		var ms runtime.MemStats
		for {
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
			}
			runtime.ReadMemStats(&ms)
			res := uint64(d.GraphResidentBytes())
			mu.Lock()
			peakHeap = max(peakHeap, ms.HeapInuse)
			peakResident = max(peakResident, res)
			mu.Unlock()
		}
	}()
	start := time.Now()
	step := n / 10
	for done := 0; done < n; done += 1000 {
		head, _ = chain(t, d, head, 1000, int64(done))
		// A real writer waits for its commits to be acknowledged; this loop does not, and on a
		// machine with one or two CPUs (a CI runner running other packages' tests) it would
		// starve the flusher and measure the overshoot of a writer no server has. The burst
		// case is TestGraphResidentTierConvergesUnderBurst.
		time.Sleep(time.Millisecond)
		if (done+1000)%step == 0 {
			st := d.GraphFileStats()
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			samples = append(samples, strconv.Itoa(done+1000)+": resident "+strconv.Itoa(st.ResidentCommits)+
				" commits, graph heap "+strconv.FormatInt(d.GraphResidentBytes()>>20, 10)+" MiB, heap in use "+
				strconv.FormatUint(ms.HeapInuse>>20, 10)+" MiB, "+strconv.Itoa(st.Segments)+" segments")
		}
	}
	waitGraphIdle(t, d)
	close(stop)
	<-sampled
	st := d.GraphFileStats()
	t.Logf("%d commits in %v; written %d records (%.2f per commit); %d segments, %d MiB on disk",
		n, time.Since(start).Round(time.Millisecond), st.RecordsWritten, float64(st.RecordsWritten)/float64(n), st.Segments, st.FileBytes>>20)
	t.Logf("peak graph heap %d MiB, peak heap in use %d MiB\n%s", peakResident>>20, peakHeap>>20, strings.Join(samples, "\n"))
	// ~400 B a resident commit, plus bloom filters at ~2.5 B a commit: a bounded tier is a few tens
	// of MiB at the default threshold; the unbounded one was 400 MB at a million.
	if limit := uint64(64<<20) + uint64(n)*4; peakResident > limit {
		t.Fatalf("graph heap peaked at %d MiB", peakResident>>20)
	}
	assertDisjointOnDisk(t, dir)
}

// Unthrottled, appends outrun every flush, and each flush takes everything resident when it
// starts - so the tier still converges instead of growing with the commit count.
func TestGraphResidentTierConvergesUnderBurst(t *testing.T) {
	const threshold, n = 500, 40_000
	d, _ := lsmDag(t, threshold, 0)
	d.StartGraphAutoFlush()
	head, _ := d.Head()
	flushedDuring := false
	for done := 0; done < n; done += 100 {
		head, _ = chain(t, d, head, 100, int64(done))
		if d.GraphFileStats().FileCommits > 0 {
			flushedDuring = true
		}
	}
	waitGraphIdle(t, d)
	st := d.GraphFileStats()
	if !flushedDuring {
		t.Fatal("nothing was flushed while commits arrived")
	}
	if st.ResidentCommits >= threshold {
		t.Fatalf("resident tier holds %d commits once idle, want < %d (%+v)", st.ResidentCommits, threshold, st)
	}
}

// How long a flush of a full tier keeps writers waiting for the DAG's lock - measured, not pinned
// (it is wall time). Before the snapshot was taken in batches and the attach rebuilt only what
// stays resident, a 50,000-commit flush held the lock ~8 ms and then ~14 ms; batched, 1.2 ms,
// since the flusher won the lock back between batches; yielding between them, ~0.25 ms.
func TestGraphFlushWriterStall(t *testing.T) {
	if testing.Short() {
		t.Skip("timing")
	}
	d, _ := lsmDag(t, 1<<30, 0)
	head, _ := d.Head()
	head, _ = chain(t, d, head, 50_000, 1)
	if _, err := d.flushResident(true); err != nil {
		t.Fatal(err)
	}
	chain(t, d, head, 50_000, 2)
	stop := make(chan struct{})
	worst := make(chan time.Duration)
	go func() {
		var w time.Duration
		for {
			select {
			case <-stop:
				worst <- w
				return
			default:
			}
			s := time.Now()
			d.mu.Lock()
			d.mu.Unlock()
			w = max(w, time.Since(s))
		}
	}()
	s := time.Now()
	if did, err := d.flushResident(true); err != nil || !did {
		t.Fatalf("flush: %v %v", did, err)
	}
	took := time.Since(s)
	close(stop)
	t.Logf("flush of 50,000 commits took %v; the longest a writer waited for the lock: %v", took, <-worst)
}

// buildChain builds n commits on parent without storing them.
func buildChain(t *testing.T, parent codec.Hash, n int, seed int64) []document.Commit {
	t.Helper()
	var out []document.Commit
	for i := 0; i < n; i++ {
		tx, _ := codec.RandomUUID()
		var parents []codec.Hash
		if parent != (codec.Hash{}) {
			parents = []codec.Hash{parent}
		}
		c, err := document.BuildCommit(parents, "ns", tx, codec.Timestamp{EpochMillis: seed*1e6 + int64(i)}, codec.UUID{MSB: 1}, nil, codec.Hash{}, nil, "")
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, c)
		parent = c.Hash
	}
	return out
}

// History that arrives below a commit already flushed to a segment - a deepen under a snapshot root
// - must still leave every parent's generation below its child's, or the pruned ancestry walks
// answer wrongly. Recomputing only the resident tier missed the segment commits (found when
// size-triggered flushes put a snapshot root on disk before the deepen).
func TestGraphHistoryArrivingBelowASegment(t *testing.T) {
	d, _ := lsmDag(t, 1<<30, 0)
	history := buildChain(t, codec.Hash{}, 20, 1)
	root := buildChain(t, history[len(history)-1].Hash, 1, 2)[0]
	if err := d.PutShallowCommit(root); err != nil {
		t.Fatal(err)
	}
	tip, above := chain(t, d, root.Hash, 30, 3)
	if err := d.CompactGraphFile(); err != nil { // the root and what is above it, on disk
		t.Fatal(err)
	}
	for _, c := range history {
		if err := d.PutCommit(c, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Unshallow(root.Hash); err != nil {
		t.Fatal(err)
	}
	if !d.IsAncestor(history[0].Hash, root.Hash) || !d.IsAncestor(history[0].Hash, tip) || !d.IsAncestor(history[5].Hash, above[10]) {
		t.Fatal("history below a flushed root is not its ancestor")
	}
	d.mu.Lock()
	d.ensureGenerationsLocked()
	d.nodes.forEach(func(h codec.Hash) {
		g := d.nodes.generation(h)
		d.nodes.forEachParent(h, func(p codec.Hash) bool {
			if d.nodes.has(p) && d.nodes.generation(p) >= g {
				t.Errorf("parent %s generation %d >= child %s generation %d", p.Hex()[:8], d.nodes.generation(p), h.Hex()[:8], g)
			}
			return true
		})
	})
	d.mu.Unlock()
}

// The same with no shallow mark at all: a parent stored after its child was flushed.
func TestGraphParentStoredAfterItsChildWasFlushed(t *testing.T) {
	d, _ := lsmDag(t, 1<<30, 0)
	commits := buildChain(t, codec.Hash{}, 10, 1)
	for _, c := range commits[5:] {
		if err := d.PutCommit(c, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.CompactGraphFile(); err != nil {
		t.Fatal(err)
	}
	for _, c := range commits[:5] {
		if err := d.PutCommit(c, false); err != nil {
			t.Fatal(err)
		}
	}
	if !d.IsAncestor(commits[0].Hash, commits[9].Hash) {
		t.Fatal("a parent stored after its flushed child is not its ancestor")
	}
}

// A commit in a segment that becomes unlogged again (its log write failed after a flush took it)
// is hidden by a checkpoint that names the segment, so the next open does not bring it back with
// no operations to read.
func TestGraphCheckpointHidesUnloggedSegmentCommits(t *testing.T) {
	d, _ := lsmDag(t, 1<<30, 0)
	head, _ := d.Head()
	_, hashes := chain(t, d, head, 10, 1)
	if err := d.CompactGraphFile(); err != nil {
		t.Fatal(err)
	}
	victim := hashes[len(hashes)-1] // the tip: nothing logged is built on it
	d.MarkUnlogged(victim)
	// main names the tip; move it back so the checkpoint is allowed at all.
	if err := d.SetHead(mainBranch, hashes[len(hashes)-2]); err != nil {
		t.Fatal(err)
	}
	_, refs, hidden, err := d.CheckpointSnapshotWithGraph(nil)
	if err != nil || len(refs) == 0 {
		t.Fatalf("refs %v err %v", refs, err)
	}
	found := false
	for _, h := range hidden {
		found = found || h == victim
	}
	if !found {
		t.Fatal("the checkpoint names the segment holding an unlogged commit without hiding it")
	}
}
