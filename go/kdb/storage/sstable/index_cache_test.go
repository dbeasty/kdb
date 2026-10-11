package sstable

import (
	"crypto/sha256"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// countingIO counts ReadFromSegment calls and the bytes they return. It embeds the in-memory
// shim so every optional interface that shim implements is promoted unchanged - the count is
// of what the store asks the IO layer for, not of a different code path.
type countingIO struct {
	*storio.InMemoryPlatformIO
	reads     atomic.Int64
	readBytes atomic.Int64
}

func (c *countingIO) ReadFromSegment(name string, offset int64, length int) ([]byte, error) {
	b, err := c.InMemoryPlatformIO.ReadFromSegment(name, offset, length)
	c.reads.Add(1)
	c.readBytes.Add(int64(len(b)))
	return b, err
}

// writeSizedTable writes one table of n keys with valueBytes of incompressible value each, so
// the segment on disk is about n*valueBytes plus the footer index.
func writeSizedTable(tb testing.TB, shim storage.PlatformIOShim, ns string, n, valueBytes int, seed int64) ([]codec.Hash, Handle) {
	tb.Helper()
	rng := rand.New(rand.NewSource(seed))
	w := NewDefaultWriter(shim, ns, 0)
	keys := make([]codec.Hash, n)
	for i := 0; i < n; i++ {
		v := make([]byte, valueBytes)
		rng.Read(v)
		sum := sha256.Sum256(v)
		var k codec.Hash
		copy(k.Bytes[:], sum[:])
		keys[i] = k
		w.Put(k, v)
	}
	h, err := w.Finish()
	if err != nil {
		tb.Fatal(err)
	}
	return keys, h
}

func segmentBytes(tb testing.TB, shim storage.PlatformIOShim, name string) int64 {
	tb.Helper()
	b, err := shim.ReadFromSegment(name, 0, 1<<30)
	if err != nil {
		tb.Fatal(err)
	}
	return int64(len(b))
}

// TestLookupReadsDoNotScaleWithTableSize is the regression guard for the crash-recovery stall
// found under issue #43: every Lookup built a fresh reader, and every fresh reader learned the
// segment's size by reading the whole file in 8 KB chunks and then re-read and re-parsed the
// footer. Against a 1.3 MB table that was ~160 reads per probe, and replay probes once per tree
// object per commit.
//
// A table is immutable once written, so a probe should cost at most the block it returns; the
// size and the index are learned once per table.
func TestLookupReadsDoNotScaleWithTableSize(t *testing.T) {
	io := &countingIO{InMemoryPlatformIO: storio.NewInMemoryPlatformIO()}
	ns := "app/docs"
	keys, h := writeSizedTable(t, io, ns, 2000, 512, 1)
	if size := segmentBytes(t, io, h.SegmentName); size < 1<<20 {
		t.Fatalf("test table is %d bytes; it needs to be about 1 MB to show the defect", size)
	}
	store := NewLsmBlobStore(io, ns, nil)
	store.AddTable(h)

	io.reads.Store(0)
	if v := store.Get(keys[0]); v == nil {
		t.Fatal("first key not found")
	}
	first := io.reads.Load()
	// Size (0 reads when the IO layer knows it), trailer, footer, block.
	if first > 4 {
		t.Fatalf("the first lookup against a table made %d reads; it should need at most 4 "+
			"(trailer, footer, block, and a size probe only where the IO layer cannot say)", first)
	}

	io.reads.Store(0)
	const probes = 200
	for i := 1; i <= probes; i++ {
		if v := store.Get(keys[i]); v == nil {
			t.Fatalf("key %d not found", i)
		}
	}
	if got := io.reads.Load(); got > probes {
		t.Fatalf("%d hits made %d reads; a warm hit should read only its own block", probes, got)
	}

	io.reads.Store(0)
	var absent codec.Hash
	absent.Bytes[0] = 0xAB
	for i := 0; i < probes; i++ {
		if v := store.Get(absent); v != nil {
			t.Fatal("absent key found")
		}
	}
	if got := io.reads.Load(); got != 0 {
		t.Fatalf("%d misses made %d reads; a warm miss should read nothing", probes, got)
	}

	// Replay's existence probe needs no block at all.
	io.reads.Store(0)
	for i := 0; i < probes; i++ {
		if !store.Contains(keys[i]) {
			t.Fatalf("Contains missed key %d", i)
		}
	}
	if store.Contains(absent) {
		t.Fatal("Contains found an absent key")
	}
	if got := io.reads.Load(); got != 0 {
		t.Fatalf("%d existence probes made %d reads; they should read nothing", probes, got)
	}
}

// TestContainsHonoursTombstones: a newer tombstone hides an older value from Contains, as it
// does from Lookup.
func TestContainsHonoursTombstones(t *testing.T) {
	io := storio.NewInMemoryPlatformIO()
	ns := "app/docs"
	store := NewLsmBlobStore(io, ns, nil)
	store.AddTable(writeTable(t, io, ns, 0, map[string]string{"a": "1", "b": "2"}, nil))
	store.AddTable(writeTable(t, io, ns, 0, nil, []string{"a"}))
	if store.Contains(keyFor([]byte("a"))) {
		t.Fatal("a tombstoned key is reported present")
	}
	if !store.Contains(keyFor([]byte("b"))) {
		t.Fatal("a live key is reported absent")
	}
}

// TestIndexCacheFollowsCompaction makes sure a cached index cannot outlive its table: after a
// compaction swaps the inputs for their merged output, every key still resolves, and the
// inputs' indexes are no longer held.
func TestIndexCacheFollowsCompaction(t *testing.T) {
	io := &countingIO{InMemoryPlatformIO: storio.NewInMemoryPlatformIO()}
	ns := "app/docs"
	store := NewLsmBlobStore(io, ns, nil)
	var all []codec.Hash
	for i := 0; i < 4; i++ {
		keys, h := writeSizedTable(t, io, ns, 50, 64, int64(i+10))
		store.AddTable(h)
		all = append(all, keys...)
	}
	for _, k := range all {
		if store.Get(k) == nil {
			t.Fatal("key missing before compaction")
		}
	}
	if n := store.cachedIndexCount(); n != 4 {
		t.Fatalf("expected 4 cached indexes before compaction, got %d", n)
	}
	res, err := store.Compact(2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Merged != 4 {
		t.Fatalf("compaction merged %d tables, want 4", res.Merged)
	}
	if n := store.cachedIndexCount(); n > 1 {
		t.Fatalf("%d indexes cached after compaction left one table; the replaced ones leaked", n)
	}
	for _, k := range all {
		if store.Get(k) == nil {
			t.Fatal("key missing after compaction")
		}
	}
	if n := store.cachedIndexCount(); n != 1 {
		t.Fatalf("expected exactly the merged table's index cached, got %d", n)
	}
}

// TestIndexCacheIsBounded checks the budget: a cache too small for every index keeps answering
// correctly and holds no more than its budget (plus the one index being used).
func TestIndexCacheIsBounded(t *testing.T) {
	io := storio.NewInMemoryPlatformIO()
	ns := "app/docs"
	store := NewLsmBlobStore(io, ns, nil)
	var all []codec.Hash
	for i := 0; i < 8; i++ {
		keys, h := writeSizedTable(t, io, ns, 100, 32, int64(i+100))
		store.AddTable(h)
		all = append(all, keys...)
	}
	// Room for roughly two tables' indexes.
	store.SetIndexCacheBudget(2 * 100 * indexEntryBytes)
	for round := 0; round < 3; round++ {
		for _, k := range all {
			if store.Get(k) == nil {
				t.Fatal("key missing under a small index budget")
			}
		}
	}
	if used, budget := store.indexCacheUsage(); used > budget {
		t.Fatalf("index cache holds %d bytes over a %d byte budget", used, budget)
	}
}

// TestIndexCacheConcurrentLookupAndCompaction runs lookups, table additions and compactions at
// once, for the race detector, and then checks nothing was lost: every key ever added resolves,
// and the cache holds no index for a table the store no longer has.
//
// Lookups during the run are not asserted here; TestLookupNeverMissesUnderConcurrentCompaction
// asserts them, now that a lookup pins the tables it reads (lookup_race_test.go).
func TestIndexCacheConcurrentLookupAndCompaction(t *testing.T) {
	io := storio.NewInMemoryPlatformIO()
	ns := "app/docs"
	store := NewLsmBlobStore(io, ns, nil)
	var mu sync.Mutex
	var all []codec.Hash
	for i := 0; i < 3; i++ {
		keys, h := writeSizedTable(t, io, ns, 40, 32, int64(i+200))
		store.AddTable(h)
		all = append(all, keys...)
	}
	stop := make(chan struct{})
	var readers sync.WaitGroup
	for g := 0; g < 4; g++ {
		readers.Add(1)
		go func(g int) {
			defer readers.Done()
			rng := rand.New(rand.NewSource(int64(g)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				mu.Lock()
				k := all[rng.Intn(len(all))]
				mu.Unlock()
				store.Get(k)
			}
		}(g)
	}
	var writers sync.WaitGroup
	writers.Add(2)
	go func() {
		defer writers.Done()
		for i := 0; i < 20; i++ {
			keys, h := writeSizedTable(t, io, ns, 40, 32, int64(i+300))
			store.AddTable(h)
			mu.Lock()
			all = append(all, keys...)
			mu.Unlock()
		}
	}()
	go func() {
		defer writers.Done()
		for i := 0; i < 10; i++ {
			if _, err := store.Compact(2, nil); err != nil {
				t.Error(err)
			}
		}
	}()
	writers.Wait()
	close(stop)
	readers.Wait()

	for i, k := range all {
		if store.Get(k) == nil {
			t.Fatalf("key %d of %d is gone after concurrent flushes and compactions", i, len(all))
		}
	}
	registered := map[string]bool{}
	for _, h := range store.Tables() {
		registered[h.SegmentName] = true
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for name := range store.indexes.byName {
		if !registered[name] {
			t.Fatalf("index for %s is cached but the table is no longer registered", name)
		}
	}
}

// TestCompactKeepsTablesAddedDuringTheMerge pins the swap: a table registered after Compact
// chose its inputs must still be registered afterwards. Compact used to replace the whole list
// with its output, so such a table disappeared until the next open rediscovered it.
func TestCompactKeepsTablesAddedDuringTheMerge(t *testing.T) {
	io := storio.NewInMemoryPlatformIO()
	ns := "app/docs"
	store := NewLsmBlobStore(io, ns, nil)
	for i := 0; i < 3; i++ {
		_, h := writeSizedTable(t, io, ns, 10, 32, int64(i+400))
		store.AddTable(h)
	}
	late, lateHandle := writeSizedTable(t, io, ns, 10, 32, 499)
	// Register the late table from inside the keep predicate, which Compact calls after it has
	// fixed its inputs and before it swaps - exactly where a concurrent flush can land.
	added := false
	keep := func(codec.Hash) bool {
		if !added {
			added = true
			store.AddTable(lateHandle)
		}
		return true
	}
	if _, err := store.Compact(2, keep); err != nil {
		t.Fatal(err)
	}
	for _, k := range late {
		if store.Get(k) == nil {
			t.Fatal("a table flushed during the compaction was dropped from the store")
		}
	}
}

func benchmarkLookup(b *testing.B, shim storage.PlatformIOShim) {
	ns := "app/docs"
	keys, h := writeSizedTable(b, shim, ns, 2000, 512, 1)
	store := NewLsmBlobStore(shim, ns, nil)
	store.AddTable(h)
	b.ReportMetric(float64(segmentBytes(b, shim, h.SegmentName))/(1<<20), "table-MB")
	var absent codec.Hash
	absent.Bytes[0] = 0xAB
	b.Run("hit", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if store.Get(keys[i%len(keys)]) == nil {
				b.Fatal("missing")
			}
		}
	})
	b.Run("miss", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if store.Get(absent) != nil {
				b.Fatal("found")
			}
		}
	})
}

// BenchmarkLsmLookup1MB probes an LsmBlobStore holding one ~1 MB table, in memory and on disk.
func BenchmarkLsmLookup1MB(b *testing.B) {
	b.Run("memory", func(b *testing.B) { benchmarkLookup(b, storio.NewInMemoryPlatformIO()) })
	b.Run("file", func(b *testing.B) {
		root := b.TempDir()
		store, err := storio.NewOSByteStore(storio.PlatformIOConfig{RootDirectory: &root})
		if err != nil {
			b.Fatal(err)
		}
		defer store.Close()
		benchmarkLookup(b, storio.NewFileBackedPlatformIO(storio.DefaultPlatformIOConfig(), store))
	})
}

func (s *LsmBlobStore) cachedIndexCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.indexes.recency.Len()
}

func (s *LsmBlobStore) indexCacheUsage() (used, budget int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.indexes.used, s.indexes.budget
}

// TestSegmentSizeFallsBackToReading covers a shim that cannot report a size: lookups still
// work, by reading, and the index cache still means the read happens once per table.
func TestSegmentSizeFallsBackToReading(t *testing.T) {
	inner := storio.NewInMemoryPlatformIO()
	io := &noSizeIO{PlatformIOShim: inner}
	ns := "app/docs"
	keys, h := writeSizedTable(t, io, ns, 200, 512, 7)
	store := NewLsmBlobStore(io, ns, nil)
	store.AddTable(h)
	if store.Get(keys[0]) == nil {
		t.Fatal("first key not found")
	}
	io.reads.Store(0)
	for _, k := range keys[1:] {
		if store.Get(k) == nil {
			t.Fatal("key not found")
		}
	}
	if got, want := io.reads.Load(), int64(len(keys)-1); got != want {
		t.Fatalf("%d warm hits made %d reads, want one each", want, got)
	}
}

// noSizeIO hides every optional interface of the shim it wraps, SegmentSizer included.
type noSizeIO struct {
	storage.PlatformIOShim
	reads atomic.Int64
}

func (n *noSizeIO) ReadFromSegment(name string, offset int64, length int) ([]byte, error) {
	n.reads.Add(1)
	return n.PlatformIOShim.ReadFromSegment(name, offset, length)
}

// TestFileBackedSegmentSize checks the file-backed shim reports a segment's size both while
// this process holds it open for writing and after a fresh store reopens the directory.
func TestFileBackedSegmentSize(t *testing.T) {
	root := t.TempDir()
	open := func() (*storio.OSByteStore, *storio.FileBackedPlatformIO) {
		st, err := storio.NewOSByteStore(storio.PlatformIOConfig{RootDirectory: &root})
		if err != nil {
			t.Fatal(err)
		}
		return st, storio.NewFileBackedPlatformIO(storio.DefaultPlatformIOConfig(), st)
	}
	st, shim := open()
	_, h := writeSizedTable(t, shim, "app/docs", 50, 100, 3)
	want := segmentBytes(t, shim, h.SegmentName)
	got, err := shim.SegmentSize(h.SegmentName)
	if err != nil || got != want {
		t.Fatalf("size while open: got %d (%v), want %d", got, err, want)
	}
	st.Close()
	st2, shim2 := open()
	defer st2.Close()
	got, err = shim2.SegmentSize(h.SegmentName)
	if err != nil || got != want {
		t.Fatalf("size after reopen: got %d (%v), want %d", got, err, want)
	}
	if _, err := shim2.SegmentSize("ns/app/docs/sstable/L0/missing.sst"); err == nil {
		t.Fatal("a missing segment reported a size")
	}
}
