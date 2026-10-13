package deltaindex

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/delta"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
	"github.com/limidus/kdb/go/kdb/storage/mmapfile"
)

const testNS = "test/deltaindex"

// testLog writes commits into delta segments through the real writer, one AppendBatch per
// segment (the in-memory shim copies a segment on every append, so one append per commit is
// quadratic - fine here, minutes on a slow CI runner at these sizes).
type testLog struct {
	t      testing.TB
	shim   storage.PlatformIOShim
	cfg    storage.StorageEngineConfig
	seq    int64
	parent []codec.Hash
	// where records the frame each commit was written at.
	where map[codec.Hash]Location
	order []codec.Hash
}

func newTestLog(t testing.TB) *testLog {
	shim := storio.NewInMemoryPlatformIO()
	return &testLog{t: t, shim: shim, cfg: storage.StorageEngineConfig{CompressionCodec: storage.CompressionNone, IOShim: shim},
		where: map[codec.Hash]Location{}}
}

// segment appends one sealed segment of n commits at the next sequence.
func (l *testLog) segment(n int) {
	l.t.Helper()
	id, _ := codec.RandomUUID()
	w := delta.NewDefaultWriter(testNS, id, l.seq, l.shim, l.cfg)
	recs := make([]storage.DeltaRecord, 0, n)
	commits := make([]document.Commit, 0, n)
	for i := 0; i < n; i++ {
		doc, _ := codec.RandomUUID()
		c := document.Commit{ParentHashes: l.parent, NamespaceID: testNS, Timestamp: codec.Timestamp{EpochMillis: int64(len(l.order) + i)},
			Operations:       []document.Op{document.WriteOp{DocID: doc, Patch: fmt.Sprintf(`{"i":%d}`, len(l.order)+i)}},
			DocumentTreeHash: document.EmptyDocumentTree().TreeHash}
		h, err := document.ComputeCommitHash(c)
		if err != nil {
			l.t.Fatal(err)
		}
		c.Hash = h
		payload, err := c.ToPayloadBytes()
		if err != nil {
			l.t.Fatal(err)
		}
		recs = append(recs, storage.DeltaRecord{CommitHash: h, NamespaceID: testNS, CommitPayload: payload})
		commits = append(commits, c)
		l.parent = []codec.Hash{h}
	}
	offsets, err := w.AppendBatch(recs)
	if err != nil {
		l.t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		l.t.Fatal(err)
	}
	if _, err := w.Seal(); err != nil {
		l.t.Fatal(err)
	}
	for i, c := range commits {
		l.where[c.Hash] = Location{Segment: storage.DeltaSegmentRef{SequenceNumber: l.seq}, FrameOffset: offsets[i]}
		l.order = append(l.order, c.Hash)
	}
	l.seq++
}

func (l *testLog) reader() storage.DeltaSegmentReader {
	return &countingReader{DefaultReader: delta.NewDefaultReader(testNS, l.shim, l.cfg)}
}

// countingReader counts segment scans, so tests can tell a sidecar hit from a rebuild.
type countingReader struct {
	*delta.DefaultReader
	scans atomic.Int64
}

func (r *countingReader) StreamCommits(seg storage.DeltaSegmentRef, fn func(document.Commit, int64) error) error {
	r.scans.Add(1)
	return r.DefaultReader.StreamCommits(seg, fn)
}

func (r *countingReader) StreamCommitsPastDamage(seg storage.DeltaSegmentRef, fn func(document.Commit, int64) error) error {
	r.scans.Add(1)
	return r.DefaultReader.StreamCommitsPastDamage(seg, fn)
}

// inEverySidecarMode runs a sidecar test against mapped sidecars, where this platform maps files,
// and against file-backed sidecars (positioned reads), the path a platform without mmap takes -
// so both are tested on every platform that can, and Windows tests the one it uses.
func inEverySidecarMode(t *testing.T, test func(*testing.T)) {
	if mmapfile.Supported {
		t.Run("mapped", func(t *testing.T) { test(t) })
	}
	t.Run("file", func(t *testing.T) {
		mapSidecars = false
		defer func() { mapSidecars = true }()
		test(t)
	})
}

// offHeap reports whether exactly n segments are in sidecars, of the kind this mode and platform
// use: mapped when the index maps and the platform can, file-backed otherwise. Logs a mismatch.
func offHeap(t *testing.T, st Stats, n int) bool {
	t.Helper()
	want := Stats{FileSegs: n}
	if mapSidecars && mmapfile.Supported {
		want = Stats{MappedSegs: n}
	}
	if st.MappedSegs != want.MappedSegs || st.FileSegs != want.FileSegs {
		t.Logf("sidecars: %d mapped, %d file-backed; want %d mapped, %d file-backed", st.MappedSegs, st.FileSegs, want.MappedSegs, want.FileSegs)
		return false
	}
	return true
}

func commitKeys(c document.Commit, emit func(codec.Hash)) { emit(c.Hash) }

func newIndex(r storage.DeltaSegmentReader, dir string) *Index {
	x := New(r, Options{Kind: "commits", Keys: commitKeys})
	if dir != "" {
		x.SetDir(dir, false)
	}
	return x
}

// requireAll asserts every commit written so far is found at the frame it was written to.
func requireAll(t *testing.T, x *Index, l *testLog) {
	t.Helper()
	for i, h := range l.order {
		loc, ok, err := x.Lookup(h)
		if err != nil || !ok {
			t.Fatalf("commit %d not found: ok=%v err=%v", i, ok, err)
		}
		want := l.where[h]
		if loc.Segment.SequenceNumber != want.Segment.SequenceNumber || loc.FrameOffset != want.FrameOffset {
			t.Fatalf("commit %d found at (%d,%d), written at (%d,%d)", i,
				loc.Segment.SequenceNumber, loc.FrameOffset, want.Segment.SequenceNumber, want.FrameOffset)
		}
	}
}

func sidecars(t *testing.T, dir string) []string {
	t.Helper()
	m, _ := filepath.Glob(filepath.Join(dir, "*"+SidecarSuffix))
	return m
}

// Every key in a multi-segment log is found, sealed segments are mapped sidecars, only the newest
// is on the heap, and an absent key is reported absent.
func TestEveryKeyFoundAcrossSegments(t *testing.T) {
	inEverySidecarMode(t, testEveryKeyFoundAcrossSegments)
}

func testEveryKeyFoundAcrossSegments(t *testing.T) {
	l := newTestLog(t)
	for i := 0; i < 4; i++ {
		l.segment(500)
	}
	dir := t.TempDir()
	x := newIndex(l.reader(), dir)
	defer x.Close()
	requireAll(t, x, l)
	var absent codec.Hash
	absent.Bytes[0] = 0xEE
	if _, ok, err := x.Lookup(absent); ok || err != nil {
		t.Fatalf("absent key: ok=%v err=%v", ok, err)
	}
	st := x.Stats()
	if st.Segments != 4 || !offHeap(t, st, 3) {
		t.Fatalf("stats %+v: want 4 segments, 3 mapped", st)
	}
	if st.HeapBytes != 500*recordSize {
		t.Fatalf("heap holds %d bytes, want the newest segment's %d", st.HeapBytes, 500*recordSize)
	}
	if n := len(sidecars(t, dir)); n != 3 {
		t.Fatalf("%d sidecars on disk, want 3", n)
	}
}

// A new index over the same directory - a reopen - reads the sidecars rather than the segments.
func TestReopenUsesSidecars(t *testing.T) { inEverySidecarMode(t, testReopenUsesSidecars) }

func testReopenUsesSidecars(t *testing.T) {
	l := newTestLog(t)
	for i := 0; i < 3; i++ {
		l.segment(300)
	}
	dir := t.TempDir()
	first := newIndex(l.reader(), dir)
	requireAll(t, first, l)
	first.Close()

	r := l.reader().(*countingReader)
	again := newIndex(r, dir)
	defer again.Close()
	requireAll(t, again, l)
	if n := r.scans.Load(); n != 1 {
		t.Fatalf("reopen scanned %d segments; want 1 (the newest, which has no sidecar)", n)
	}
}

// Segments added after the index was built are found on a later miss, and the segment that was
// newest - on the heap - moves to a sidecar once it is sealed, without being scanned again.
func TestCatchesUpWhenSegmentsAreAdded(t *testing.T) {
	inEverySidecarMode(t, testCatchesUpWhenSegmentsAreAdded)
}

func testCatchesUpWhenSegmentsAreAdded(t *testing.T) {
	l := newTestLog(t)
	l.segment(200)
	l.segment(200)
	dir := t.TempDir()
	r := l.reader().(*countingReader)
	x := newIndex(r, dir)
	defer x.Close()
	requireAll(t, x, l)
	scans := r.scans.Load()
	l.segment(200)
	l.segment(200)
	requireAll(t, x, l)
	if got := r.scans.Load() - scans; got != 2 {
		t.Fatalf("catching up scanned %d segments; want 2 (the new ones)", got)
	}
	if st := x.Stats(); st.Segments != 4 || !offHeap(t, st, 3) {
		t.Fatalf("stats %+v: want 4 segments, 3 mapped", st)
	}
	// Nothing new: a miss scans nothing.
	if changed, err := x.Refresh(); changed || err != nil {
		t.Fatalf("refresh with an unchanged log: changed=%v err=%v", changed, err)
	}
}

// Truncation deletes segments from the front of the log: their keys stop resolving, the rest
// still do, and their sidecars are removed.
func TestTruncatedSegmentsAndTheirSidecarsGo(t *testing.T) {
	inEverySidecarMode(t, testTruncatedSegmentsAndTheirSidecarsGo)
}

func testTruncatedSegmentsAndTheirSidecarsGo(t *testing.T) {
	l := newTestLog(t)
	for i := 0; i < 4; i++ {
		l.segment(200)
	}
	dir := t.TempDir()
	x := newIndex(l.reader(), dir)
	defer x.Close()
	requireAll(t, x, l)
	gone := l.order[:400]
	names, err := l.shim.ListSegments(testNS)
	if err != nil {
		t.Fatal(err)
	}
	deleted := 0
	for _, name := range names {
		if seq, ok := storio.ParseDeltaSequencedFileName(filepath.Base(name)); ok && seq < 2 {
			if err := l.shim.DeleteSegment(name); err != nil {
				t.Fatal(err)
			}
			deleted++
		}
	}
	if deleted != 2 {
		t.Fatalf("deleted %d segments, want 2", deleted)
	}
	for _, h := range gone {
		delete(l.where, h)
	}
	l.order = l.order[400:]
	var absent codec.Hash
	absent.Bytes[0] = 0xAB
	if _, ok, err := x.Lookup(absent); ok || err != nil { // a miss refreshes
		t.Fatalf("absent key: ok=%v err=%v", ok, err)
	}
	if _, ok, _ := x.Lookup(gone[0]); ok {
		t.Fatal("a key from a truncated segment still resolves")
	}
	requireAll(t, x, l)
	if n := len(sidecars(t, dir)); n != 1 {
		t.Fatalf("%d sidecars left, want 1 (segment 2; segment 3 is the newest)", n)
	}
}

// A damaged, truncated, stale or foreign sidecar is never trusted: the index rebuilds it from
// the segment, and every key is still found.
func TestBadSidecarsFallBackToTheSegment(t *testing.T) {
	inEverySidecarMode(t, testBadSidecarsFallBackToTheSegment)
}

func testBadSidecarsFallBackToTheSegment(t *testing.T) {
	l := newTestLog(t)
	for i := 0; i < 5; i++ {
		l.segment(300)
	}
	dir := t.TempDir()
	first := newIndex(l.reader(), dir)
	requireAll(t, first, l)
	first.Close()
	path := func(seq int64) string { return filepath.Join(dir, fmt.Sprintf("%020d.commits%s", seq, SidecarSuffix)) }
	// 0: a flipped bit in a record. 1: cut short. 2: another segment's sidecar under its name.
	// 3: removed.
	b, err := os.ReadFile(path(0))
	if err != nil {
		t.Fatal(err)
	}
	b[headerSize+5] ^= 0x40
	if err := os.WriteFile(path(0), b, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path(1), headerSize+recordSize*10); err != nil {
		t.Fatal(err)
	}
	other, err := os.ReadFile(path(3))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path(2), other, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path(3)); err != nil {
		t.Fatal(err)
	}
	r := l.reader().(*countingReader)
	again := newIndex(r, dir)
	defer again.Close()
	requireAll(t, again, l)
	if n := r.scans.Load(); n != 5 {
		t.Fatalf("scanned %d segments; want 5 (four bad sidecars and the newest)", n)
	}
	// Rebuilt: a third index needs only the newest segment.
	again.Close()
	r3 := l.reader().(*countingReader)
	third := newIndex(r3, dir)
	defer third.Close()
	requireAll(t, third, l)
	if n := r3.scans.Load(); n != 1 {
		t.Fatalf("after the rebuild a reopen scanned %d segments; want 1", n)
	}
}

// A segment rewritten under the same sequence (a repair that truncates a damaged tail) changes
// its size and last commit, so its old sidecar no longer matches it and is rebuilt.
func TestSidecarOfAChangedSegmentIsRebuilt(t *testing.T) {
	inEverySidecarMode(t, testSidecarOfAChangedSegmentIsRebuilt)
}

func testSidecarOfAChangedSegmentIsRebuilt(t *testing.T) {
	l := newTestLog(t)
	l.segment(100)
	l.segment(100)
	l.segment(100)
	dir := t.TempDir()
	x := newIndex(l.reader(), dir)
	requireAll(t, x, l)
	x.Close()
	// Rewrite segment 0 with different commits.
	names, _ := l.shim.ListSegments(testNS)
	for _, name := range names {
		if seq, ok := storio.ParseDeltaSequencedFileName(filepath.Base(name)); ok && seq == 0 {
			if err := l.shim.DeleteSegment(name); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, h := range l.order[:100] {
		delete(l.where, h)
	}
	l.order = l.order[100:]
	next := l.seq
	l.seq = 0
	l.segment(50)
	l.seq = next
	r := l.reader().(*countingReader)
	again := newIndex(r, dir)
	defer again.Close()
	requireAll(t, again, l)
	if n := r.scans.Load(); n != 2 {
		t.Fatalf("scanned %d segments; want 2 (the rewritten one and the newest)", n)
	}
}

// Within a segment and across segments, the newest frame holding a key is the one returned.
func TestNewestFrameWins(t *testing.T) { inEverySidecarMode(t, testNewestFrameWins) }

func testNewestFrameWins(t *testing.T) {
	l := newTestLog(t)
	l.segment(50)
	l.segment(50)
	l.segment(10)
	// Every commit's key is "the same document version" for this index: one key for everything.
	var one codec.Hash
	one.Bytes[0] = 7
	x := New(l.reader(), Options{Kind: "versions", Keys: func(_ document.Commit, emit func(codec.Hash)) { emit(one) }})
	x.SetDir(t.TempDir(), false)
	defer x.Close()
	loc, ok, err := x.Lookup(one)
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	last := l.where[l.order[len(l.order)-1]]
	if loc.Segment.SequenceNumber != 2 || loc.FrameOffset != last.FrameOffset {
		t.Fatalf("found at (%d,%d); want the newest frame (2,%d)", loc.Segment.SequenceNumber, loc.FrameOffset, last.FrameOffset)
	}
	if st := x.Stats(); st.Keys != 3 {
		t.Fatalf("index holds %d records; want one per segment", st.Keys)
	}
}

// A read-only index uses sidecars it finds and never writes one.
func TestReadOnlyWritesNoSidecars(t *testing.T) {
	l := newTestLog(t)
	l.segment(100)
	l.segment(100)
	dir := t.TempDir()
	x := New(l.reader(), Options{Kind: "commits", Keys: commitKeys})
	x.SetDir(dir, true)
	defer x.Close()
	requireAll(t, x, l)
	if n := len(sidecars(t, dir)); n != 0 {
		t.Fatalf("read-only index wrote %d sidecars", n)
	}
	if st := x.Stats(); st.OffHeapSegs() != 0 || st.Segments != 2 {
		t.Fatalf("stats %+v", st)
	}
}

// Without a directory every segment stays on the heap, still pointer-free: ~recordSize bytes a key.
func TestNoDirKeepsCompactHeapArrays(t *testing.T) {
	l := newTestLog(t)
	l.segment(300)
	l.segment(300)
	x := newIndex(l.reader(), "")
	defer x.Close()
	requireAll(t, x, l)
	if st := x.Stats(); st.OffHeapSegs() != 0 || st.HeapBytes != 600*recordSize {
		t.Fatalf("stats %+v", st)
	}
}

// The bound: with sidecars, what the index keeps on the heap is the newest segment's records plus
// a small struct per segment - the same for a log of two segments and of eight. The map this
// replaced held ~112 bytes per version in the whole log.
func TestRetainedHeapDoesNotGrowWithHistory(t *testing.T) {
	inEverySidecarMode(t, testRetainedHeapDoesNotGrowWithHistory)
}

func testRetainedHeapDoesNotGrowWithHistory(t *testing.T) {
	const perSegment = 4000
	measure := func(segments int) (int64, Stats) {
		l := newTestLog(t)
		for i := 0; i < segments; i++ {
			l.segment(perSegment)
		}
		dir := t.TempDir()
		r := l.reader()
		runtime.GC()
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		x := newIndex(r, dir)
		if _, err := x.Refresh(); err != nil {
			t.Fatal(err)
		}
		runtime.GC()
		runtime.GC()
		runtime.ReadMemStats(&after)
		st := x.Stats()
		x.Close()
		runtime.KeepAlive(r)
		return int64(after.HeapAlloc) - int64(before.HeapAlloc), st
	}
	small, smallSt := measure(2)
	large, largeSt := measure(8)
	t.Logf("2 segments (%d keys): %d B heap, %+v; 8 segments (%d keys): %d B heap, %+v",
		smallSt.Keys, small, smallSt, largeSt.Keys, large, largeSt)
	if smallSt.HeapBytes != largeSt.HeapBytes {
		t.Fatalf("record bytes on the heap grew with history: %d -> %d", smallSt.HeapBytes, largeSt.HeapBytes)
	}
	// Six more segments add 24,000 keys; at the old ~112 B/key that would be ~2.6 MB. Allow a
	// generous fixed slack for GC noise, but far less than one byte per added key would need.
	if grow := large - small; grow > 256<<10 {
		t.Fatalf("retained heap grew by %d bytes for 6 more segments (%d more keys)", grow, largeSt.Keys-smallSt.Keys)
	}
}

// Concurrent lookups while the log grows and the index catches up (run under -race).
func TestConcurrentLookupsWhileTheLogGrows(t *testing.T) {
	inEverySidecarMode(t, testConcurrentLookupsWhileTheLogGrows)
}

func testConcurrentLookupsWhileTheLogGrows(t *testing.T) {
	l := newTestLog(t)
	l.segment(200)
	l.segment(200)
	x := newIndex(l.reader(), t.TempDir())
	defer x.Close()
	known := append([]codec.Hash(nil), l.order...)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	stop := make(chan struct{})
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := g; ; i = (i + 7) % len(known) {
				select {
				case <-stop:
					return
				default:
				}
				if _, ok, err := x.Lookup(known[i]); err != nil || !ok {
					errs <- fmt.Errorf("lookup %d: ok=%v err=%v", i, ok, err)
					return
				}
			}
		}(g)
	}
	for i := 0; i < 4; i++ {
		l.segment(100) // the writer and the refreshes race the readers
		if _, err := x.Refresh(); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	requireAll(t, x, l)
}

// Prune drops deleted segments and their sidecars - every kind's - without scanning anything.
func TestPruneDropsDeletedSegmentsWithoutScanning(t *testing.T) {
	inEverySidecarMode(t, testPruneDropsDeletedSegmentsWithoutScanning)
}

func testPruneDropsDeletedSegmentsWithoutScanning(t *testing.T) {
	l := newTestLog(t)
	for i := 0; i < 4; i++ {
		l.segment(100)
	}
	dir := t.TempDir()
	r := l.reader().(*countingReader)
	x := newIndex(r, dir)
	defer x.Close()
	requireAll(t, x, l)
	other := New(r, Options{Kind: "versions", Keys: commitKeys})
	other.SetDir(dir, false)
	defer other.Close()
	if _, err := other.Refresh(); err != nil {
		t.Fatal(err)
	}
	if n := len(sidecars(t, dir)); n != 6 {
		t.Fatalf("%d sidecars, want 6 (3 sealed segments, 2 kinds)", n)
	}
	names, _ := l.shim.ListSegments(testNS)
	for _, name := range names {
		if seq, ok := storio.ParseDeltaSequencedFileName(filepath.Base(name)); ok && seq < 2 {
			if err := l.shim.DeleteSegment(name); err != nil {
				t.Fatal(err)
			}
		}
	}
	scans := r.scans.Load()
	// Both indexes, as the engine does after truncation: on Windows the other index's open
	// sidecars can only go once it has closed them.
	if err := x.Prune(); err != nil {
		t.Fatal(err)
	}
	if err := other.Prune(); err != nil {
		t.Fatal(err)
	}
	if r.scans.Load() != scans {
		t.Fatal("prune scanned a segment")
	}
	if n := len(sidecars(t, dir)); n != 2 {
		t.Fatalf("%d sidecars after pruning, want 2 (segment 2, both kinds)", n)
	}
	if st := x.Stats(); st.Segments != 2 {
		t.Fatalf("index still holds %d segments", st.Segments)
	}
	for _, h := range l.order[200:] {
		if _, ok, err := x.Lookup(h); !ok || err != nil {
			t.Fatalf("a retained commit no longer resolves: ok=%v err=%v", ok, err)
		}
	}
}
