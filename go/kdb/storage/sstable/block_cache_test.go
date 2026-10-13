package sstable

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

func val(n int, b byte) []byte { return bytes.Repeat([]byte{b}, n) }

func TestBlockCacheHitAndMiss(t *testing.T) {
	c := NewBlockCache(1 << 20)
	if _, ok := c.Get("seg", 0); ok {
		t.Fatal("empty cache reported a hit")
	}
	c.Put("seg", 0, []byte("a"))
	c.Put("seg", 16, []byte("b"))
	c.Put("other", 0, []byte("c"))
	for _, tc := range []struct {
		seg  string
		off  int64
		want string
	}{{"seg", 0, "a"}, {"seg", 16, "b"}, {"other", 0, "c"}} {
		got, ok := c.Get(tc.seg, tc.off)
		if !ok || string(got) != tc.want {
			t.Fatalf("Get(%s, %d) = %q, %v; want %q", tc.seg, tc.off, got, ok, tc.want)
		}
	}
	if _, ok := c.Get("seg", 8); ok {
		t.Fatal("a different offset in the same segment hit")
	}
	st := c.Stats()
	if st.Hits != 3 || st.Misses != 2 || st.Entries != 3 {
		t.Fatalf("stats %+v, want 3 hits, 2 misses, 3 entries", st)
	}
	if want := int64(3 * (1 + blockEntryOverhead)); st.UsedBytes != want {
		t.Fatalf("used %d bytes, want %d", st.UsedBytes, want)
	}
}

// TestBlockCacheEvictsByBytesInLRUOrder pins both halves of "LRU bounded by bytes": eviction
// starts when the charged bytes (not the entry count) pass the capacity, and the entry that goes
// is the least recently *used*, not the least recently inserted - the old cache was FIFO.
func TestBlockCacheEvictsByBytesInLRUOrder(t *testing.T) {
	const size = 1000
	cost := int64(size + blockEntryOverhead)
	c := NewBlockCache(3 * cost)
	c.Put("s", 0, val(size, 'a'))
	c.Put("s", 1, val(size, 'b'))
	c.Put("s", 2, val(size, 'c'))
	if st := c.Stats(); st.Entries != 3 || st.Evictions != 0 {
		t.Fatalf("three blocks that fit exactly: %+v", st)
	}
	// Touch the oldest, so the middle one becomes least recently used.
	if _, ok := c.Get("s", 0); !ok {
		t.Fatal("block 0 missing")
	}
	c.Put("s", 3, val(size, 'd'))
	if _, ok := c.Get("s", 1); ok {
		t.Fatal("block 1 was least recently used and should have been evicted")
	}
	for _, off := range []int64{0, 2, 3} {
		if _, ok := c.Get("s", off); !ok {
			t.Fatalf("block %d evicted; only block 1 should have gone", off)
		}
	}
	if used := c.UsedBytes(); used != 3*cost {
		t.Fatalf("used %d after eviction, want %d", used, 3*cost)
	}

	// A large block pushes out as many small ones as it needs to, oldest-used first.
	c.Put("s", 4, val(2*size, 'e')) // costs ~2 small blocks
	if used := c.UsedBytes(); used > 3*cost {
		t.Fatalf("used %d over capacity %d", used, 3*cost)
	}
	if _, ok := c.Get("s", 4); !ok {
		t.Fatal("the block just inserted was evicted")
	}
	// Recency was 0, 2, 3 (Get order above), so 0 and 2 go and 3 stays.
	if _, ok := c.Get("s", 3); !ok {
		t.Fatal("most recently used small block evicted")
	}
	if _, ok := c.Get("s", 0); ok {
		t.Fatal("least recently used block survived")
	}
}

func TestBlockCacheRefusesBlocksLargerThanCapacity(t *testing.T) {
	c := NewBlockCache(1000)
	c.Put("s", 0, val(100, 'a'))
	c.Put("s", 1, val(5000, 'b'))
	if _, ok := c.Get("s", 1); ok {
		t.Fatal("a block larger than the whole cache was admitted")
	}
	if _, ok := c.Get("s", 0); !ok {
		t.Fatal("admitting nothing still flushed the cache")
	}
	zero := NewBlockCache(0)
	if zero.Put("s", 0, val(1, 'x')); zero.UsedBytes() != 0 {
		t.Fatal("a zero-capacity cache held something")
	}
}

func TestBlockCacheSetCapacityShrinks(t *testing.T) {
	c := NewBlockCache(1 << 20)
	for i := 0; i < 10; i++ {
		c.Put("s", int64(i), val(1000, byte(i)))
	}
	c.SetCapacity(2 * (1000 + blockEntryOverhead))
	st := c.Stats()
	if st.Entries != 2 {
		t.Fatalf("%d entries after shrinking to two blocks' worth", st.Entries)
	}
	for _, off := range []int64{8, 9} {
		if _, ok := c.Get("s", off); !ok {
			t.Fatalf("most recent block %d was not the one kept", off)
		}
	}
}

func TestBlockCacheRemoveSegment(t *testing.T) {
	c := NewBlockCache(1 << 20)
	for i := 0; i < 5; i++ {
		c.Put("dead", int64(i), val(10, 'd'))
		c.Put("live", int64(i), val(10, 'l'))
	}
	c.RemoveSegment("dead")
	st := c.Stats()
	if st.Entries != 5 || st.UsedBytes != 5*(10+blockEntryOverhead) {
		t.Fatalf("after removing one segment: %+v", st)
	}
	for i := 0; i < 5; i++ {
		if _, ok := c.Get("dead", int64(i)); ok {
			t.Fatal("removed segment still served")
		}
		if _, ok := c.Get("live", int64(i)); !ok {
			t.Fatal("other segment lost an entry")
		}
	}
}

// TestNilBlockCacheIsANoOp: NewLsmBlobStore takes a nil cache (most tests pass one), and every
// method must then behave as "never cached".
func TestNilBlockCacheIsANoOp(t *testing.T) {
	var c *BlockCache
	c.Put("s", 0, []byte("x"))
	if _, ok := c.Get("s", 0); ok {
		t.Fatal("nil cache hit")
	}
	c.RemoveSegment("s")
	c.SetCapacity(10)
	if c.UsedBytes() != 0 || c.Stats() != (BlockCacheStats{}) {
		t.Fatal("nil cache reports contents")
	}
}

// TestLookupServesRepeatsFromTheBlockCache is the wiring: the cache used to be built and never
// read, so every hit went to disk. Now the second lookup of a key reads nothing.
func TestLookupServesRepeatsFromTheBlockCache(t *testing.T) {
	io := &countingIO{InMemoryPlatformIO: storio.NewInMemoryPlatformIO()}
	ns := "app/docs"
	keys, h := writeSizedTable(t, io, ns, 50, 256, 11)
	cache := NewBlockCache(1 << 20)
	store := NewLsmBlobStore(io, ns, cache)
	store.AddTable(h)

	first := make([][]byte, len(keys))
	for i, k := range keys {
		first[i] = store.Get(k)
		if first[i] == nil {
			t.Fatalf("key %d missing", i)
		}
	}
	io.reads.Store(0)
	for i, k := range keys {
		got := store.Get(k)
		if !bytes.Equal(got, first[i]) {
			t.Fatalf("key %d changed between reads", i)
		}
	}
	if n := io.reads.Load(); n != 0 {
		t.Fatalf("%d warm lookups made %d reads, want none", len(keys), n)
	}
	if st := cache.Stats(); st.Entries != len(keys) || st.Hits < int64(len(keys)) {
		t.Fatalf("cache stats %+v", st)
	}
}

// TestBlockCacheNeverServesAReplacedTable: compaction replaces tables, and a cached block of a
// replaced table must neither answer a lookup nor keep holding budget.
//
// Two ways it could go wrong are pinned. A key compaction dropped must read as absent, not as
// the block cached from its old table. And a newer tombstone must shadow a value cached from an
// older table - the cache is consulted only after the index says which table owns the key.
func TestBlockCacheNeverServesAReplacedTable(t *testing.T) {
	shim := testShim(t)
	ns := "app/docs"
	cache := NewBlockCache(1 << 20)
	store := NewLsmBlobStore(shim, ns, cache)
	old := map[string]string{}
	for i := 0; i < 4; i++ {
		k := fmt.Sprintf("k%d", i)
		old[k] = fmt.Sprintf("v%d", i)
		store.AddTable(writeTable(t, shim, ns, 0, map[string]string{k: old[k], "doomed": "x"}, nil))
	}
	for k, want := range old {
		if got := store.Get(keyFor([]byte(k))); string(got) != want {
			t.Fatalf("%s = %q before compaction", k, got)
		}
	}
	if got := store.Get(keyFor([]byte("doomed"))); string(got) != "x" {
		t.Fatalf("doomed = %q before compaction", got)
	}
	inputs := map[string]bool{}
	for _, h := range store.Tables() {
		inputs[h.SegmentName] = true
	}
	if cache.Stats().Entries == 0 {
		t.Fatal("nothing was cached; the test would prove nothing")
	}

	doomed := keyFor([]byte("doomed"))
	res, err := store.Compact(2, func(k codec.Hash) bool { return k != doomed })
	if err != nil {
		t.Fatal(err)
	}
	if res.Merged != 4 {
		t.Fatalf("merged %d, want 4", res.Merged)
	}
	if got := store.Get(doomed); got != nil {
		t.Fatalf("a key compaction dropped still reads %q - served from a replaced table's cached block", got)
	}
	cache.mu.Lock()
	for e := cache.head; e != nil; e = e.next {
		if inputs[e.key.segment] {
			cache.mu.Unlock()
			t.Fatalf("block of replaced table %s still cached", e.key.segment)
		}
	}
	cache.mu.Unlock()
	for k, want := range old {
		if got := store.Get(keyFor([]byte(k))); string(got) != want {
			t.Fatalf("%s = %q after compaction, want %q", k, got, want)
		}
	}

	// A newer tombstone shadows the cached value under it.
	store.AddTable(writeTable(t, shim, ns, 0, nil, []string{"k0"}))
	if v, deleted, found := store.Lookup(keyFor([]byte("k0"))); v != nil || !deleted || !found {
		t.Fatalf("tombstoned k0: value %q deleted %v found %v", v, deleted, found)
	}
}

// TestBlockCacheConcurrentLookupsAndCompaction runs lookups against flushes and compactions with
// a cache small enough to evict constantly. Under -race it checks the cache's own locking and the
// register-then-insert rule; afterwards every key must still read back its own value, and no
// block of an unregistered table may be left in the cache.
func TestBlockCacheConcurrentLookupsAndCompaction(t *testing.T) {
	io := storio.NewInMemoryPlatformIO()
	ns := "app/docs"
	cache := NewBlockCache(16 * (64 + blockEntryOverhead))
	store := NewLsmBlobStore(io, ns, cache)
	var mu sync.Mutex
	want := map[codec.Hash][]byte{}
	record := func(keys []codec.Hash) {
		for _, k := range keys {
			v := store.Get(k)
			mu.Lock()
			want[k] = append([]byte(nil), v...)
			mu.Unlock()
		}
	}
	var all []codec.Hash
	for i := 0; i < 3; i++ {
		keys, h := writeSizedTable(t, io, ns, 40, 64, int64(i+500))
		store.AddTable(h)
		record(keys)
		all = append(all, keys...)
	}
	stop := make(chan struct{})
	var readers sync.WaitGroup
	errs := make(chan error, 8)
	for g := 0; g < 8; g++ {
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
				w := want[k]
				mu.Unlock()
				if got := store.Get(k); !bytes.Equal(got, w) {
					errs <- fmt.Errorf("key read back %d bytes, want %d", len(got), len(w))
					return
				}
			}
		}(g)
	}
	var writers sync.WaitGroup
	writers.Add(2)
	go func() {
		defer writers.Done()
		for i := 0; i < 20; i++ {
			keys, h := writeSizedTable(t, io, ns, 40, 64, int64(i+600))
			store.AddTable(h)
			record(keys)
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
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	for k, w := range want {
		if got := store.Get(k); !bytes.Equal(got, w) {
			t.Fatal("a key changed value after concurrent flushes and compactions")
		}
	}
	registered := map[string]bool{}
	for _, h := range store.Tables() {
		registered[h.SegmentName] = true
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	for e := cache.head; e != nil; e = e.next {
		if !registered[e.key.segment] {
			t.Fatalf("block of unregistered table %s is cached", e.key.segment)
		}
	}
	if cache.used > cache.capacity {
		t.Fatalf("cache over capacity: %d > %d", cache.used, cache.capacity)
	}
}

// BenchmarkLsmLookupBlockCache is BenchmarkLsmLookup1MB's file case with the store built the way
// the engine builds it - with a block cache large enough for the table. Before the cache was
// wired in, the cache made no difference and every hit read, CRC-checked and decompressed its
// block.
func BenchmarkLsmLookupBlockCache(b *testing.B) {
	root := b.TempDir()
	osStore, err := storio.NewOSByteStore(storio.PlatformIOConfig{RootDirectory: &root})
	if err != nil {
		b.Fatal(err)
	}
	defer osStore.Close()
	shim := storio.NewFileBackedPlatformIO(storio.DefaultPlatformIOConfig(), osStore)
	ns := "app/docs"
	keys, h := writeSizedTable(b, shim, ns, 2000, 512, 1)
	store := NewLsmBlobStore(shim, ns, NewBlockCache(32<<20))
	store.AddTable(h)
	for _, k := range keys {
		store.Get(k)
	}
	b.Run("hit", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if store.Get(keys[i%len(keys)]) == nil {
				b.Fatal("missing")
			}
		}
	})
	b.Run("hit-parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				if store.Get(keys[i%len(keys)]) == nil {
					b.Fatal("missing")
				}
				i++
			}
		})
	})
}
