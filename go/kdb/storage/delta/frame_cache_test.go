package delta

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/bufpool"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// framePayloadFor is the deterministic content the cache tests store under k, so any read can
// check it got the bytes of the frame it asked for and not some other frame's.
func framePayloadFor(k frameKey, size int) []byte {
	b := make([]byte, size)
	rng := rand.New(rand.NewSource(int64(k.gen)*1_000_003 + k.off))
	rng.Read(b)
	return b
}

func cacheGet(t testing.TB, c *FrameCache, k frameKey) ([]byte, bool) {
	t.Helper()
	bp, ok := c.get(k)
	if !ok {
		return nil, false
	}
	out := append([]byte(nil), *bp...)
	bufpool.Put(bp)
	return out, true
}

// checkShardInvariants walks every shard: the LRU ring and the index agree, every page is either
// free or owned by exactly one frame, and the frame's page chain is exactly as long as its length.
func checkShardInvariants(t *testing.T, c *FrameCache) {
	t.Helper()
	for si := range c.shards {
		s := &c.shards[si]
		s.mu.Lock()
		owner := make(map[uint32]bool)
		for _, p := range s.freePages {
			if int(p) >= s.capacity || owner[p] {
				t.Fatalf("shard %d: free page %d out of range or listed twice", si, p)
			}
			owner[p] = true
		}
		n, used := 0, 0
		for ei := s.entries[0].next; ei != 0; ei = s.entries[ei].next {
			e := s.entries[ei]
			if got, ok := s.index[e.key]; !ok || got != ei {
				t.Fatalf("shard %d: LRU entry %d not indexed", si, ei)
			}
			pages := 0
			for p := e.first; p != noPage; p = s.pageNext[p] {
				if int(p) >= s.capacity || owner[p] {
					t.Fatalf("shard %d: page %d out of range or shared", si, p)
				}
				owner[p] = true
				pages++
			}
			if want := (int(e.length) + framePageSize - 1) / framePageSize; pages != want {
				t.Fatalf("shard %d: frame of %d bytes holds %d pages", si, e.length, pages)
			}
			n++
			used += pages
		}
		if n != len(s.index) || used != s.usedPages || len(owner) != s.capacity {
			t.Fatalf("shard %d: %d in ring, %d indexed; %d pages used, %d counted; %d of %d pages accounted",
				si, n, len(s.index), used, s.usedPages, len(owner), s.capacity)
		}
		s.mu.Unlock()
	}
}

func TestFrameCacheHitAndMiss(t *testing.T) {
	c := NewFrameCache(4 << 20)
	k := frameKey{gen: 1, off: 20}
	if _, ok := cacheGet(t, c, k); ok {
		t.Fatal("hit on an empty cache")
	}
	for _, size := range []int{0, 1, framePageSize - 1, framePageSize, framePageSize + 1, 50_000} {
		k := frameKey{gen: 2, off: int64(size)}
		want := framePayloadFor(k, size)
		c.put(k, want)
		got, ok := cacheGet(t, c, k)
		if !ok || !bytes.Equal(got, want) {
			t.Fatalf("size %d: hit=%v, bytes equal=%v", size, ok, bytes.Equal(got, want))
		}
	}
	// Same offset, another generation: a different segment incarnation, so a miss.
	if _, ok := cacheGet(t, c, frameKey{gen: 3, off: 50_000}); ok {
		t.Fatal("hit across generations")
	}
	st := c.Stats()
	if st.Hits != 6 || st.Misses != 2 || st.Inserts != 6 || st.Frames != 6 {
		t.Fatalf("stats %+v", st)
	}
	checkShardInvariants(t, c)
	var nilCache *FrameCache
	nilCache.put(k, []byte("x"))
	if _, ok := nilCache.get(k); ok || nilCache.Bytes() != 0 {
		t.Fatal("a nil cache held something")
	}
}

// TestFrameCacheEvictsByBytes: the cache never holds more than its budget, however many frames
// pass through it, and what it evicts is what was used least recently.
func TestFrameCacheEvictsByBytes(t *testing.T) {
	const budget = 256 << 10
	c := NewFrameCache(budget)
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 5000; i++ {
		k := frameKey{gen: uint64(1 + i%7), off: int64(i)}
		c.put(k, framePayloadFor(k, 1+rng.Intn(6000)))
		if b := c.Bytes(); b > budget {
			t.Fatalf("after %d puts the cache holds %d bytes, budget %d", i+1, b, budget)
		}
	}
	st := c.Stats()
	if st.Evictions == 0 || st.Frames == 0 {
		t.Fatalf("stats %+v", st)
	}
	checkShardInvariants(t, c)
	// Whatever is still cached is intact.
	for i := 0; i < 5000; i++ {
		k := frameKey{gen: uint64(1 + i%7), off: int64(i)}
		if got, ok := cacheGet(t, c, k); ok && !bytes.Equal(got, framePayloadFor(k, len(got))) {
			t.Fatalf("frame %v corrupted", k)
		}
	}
}

func TestFrameCacheLRUOrder(t *testing.T) {
	c := NewFrameCache(frameCacheShards * 64 << 10)
	// Keys that all land in one shard, so its LRU is the only one in play.
	var keys []frameKey
	target := c.shard(frameKey{gen: 9, off: 0})
	for off := int64(0); len(keys) < 400; off++ {
		if k := (frameKey{gen: 9, off: off}); c.shard(k) == target {
			keys = append(keys, k)
		}
	}
	const size = 1000 // 4 pages
	fill := func(ks []frameKey) {
		for _, k := range ks {
			c.put(k, framePayloadFor(k, size))
		}
	}
	// Fill to just under capacity, touch the oldest, then push more in.
	capacityFrames := int(target.budgetBytes / (4*framePageCost + frameEntryOverhead))
	fill(keys[:capacityFrames-5])
	if c.Stats().Evictions != 0 {
		t.Fatalf("evicted before the shard was full: %+v", c.Stats())
	}
	if _, ok := cacheGet(t, c, keys[0]); !ok {
		t.Fatal("oldest frame missing")
	}
	fill(keys[capacityFrames-5 : capacityFrames+20])
	if _, ok := cacheGet(t, c, keys[0]); !ok {
		t.Fatal("the most recently read frame was evicted")
	}
	for _, k := range keys[1:20] {
		if _, ok := cacheGet(t, c, k); ok {
			t.Fatalf("frame %v survived while newer frames were evicted", k)
		}
	}
	if _, ok := cacheGet(t, c, keys[capacityFrames+19]); !ok {
		t.Fatal("newest frame missing")
	}
	checkShardInvariants(t, c)
}

// TestFrameCacheSkipsOversizedFrames: one frame larger than a quarter of its shard is not cached,
// so a single huge commit cannot flush everything else.
func TestFrameCacheSkipsOversizedFrames(t *testing.T) {
	c := NewFrameCache(frameCacheShards * 64 << 10)
	small := frameKey{gen: 1, off: 1}
	c.put(small, framePayloadFor(small, 100))
	big := frameKey{gen: 1, off: 2}
	c.put(big, framePayloadFor(big, 20<<10))
	if _, ok := cacheGet(t, c, big); ok {
		t.Fatal("oversized frame cached")
	}
	if _, ok := cacheGet(t, c, small); !ok {
		t.Fatal("small frame lost")
	}
}

// TestFrameCacheSetBudget: shrinking evicts least recently used frames and gives arena back, moving
// surviving frames off released pages intact; growing again lets the cache fill.
func TestFrameCacheSetBudget(t *testing.T) {
	c := NewFrameCache(8 << 20)
	var keys []frameKey
	for i := 0; i < 2000; i++ {
		k := frameKey{gen: 5, off: int64(i)}
		keys = append(keys, k)
		c.put(k, framePayloadFor(k, 300+i%3000))
	}
	full := c.Bytes()
	c.SetBudget(1 << 20)
	if b := c.Bytes(); b > 1<<20 || b >= full {
		t.Fatalf("after shrinking to 1 MiB the cache holds %d bytes (was %d)", b, full)
	}
	checkShardInvariants(t, c)
	kept := 0
	for i, k := range keys {
		got, ok := cacheGet(t, c, k)
		if !ok {
			continue
		}
		kept++
		if !bytes.Equal(got, framePayloadFor(k, 300+i%3000)) {
			t.Fatalf("frame %d corrupted by the shrink", i)
		}
	}
	if kept == 0 {
		t.Fatal("shrink evicted everything")
	}
	c.SetBudget(0)
	if c.Bytes() != 0 || c.Stats().Frames != 0 {
		t.Fatalf("zero budget holds %d bytes, %d frames", c.Bytes(), c.Stats().Frames)
	}
	c.SetBudget(4 << 20)
	for _, k := range keys[:100] {
		c.put(k, framePayloadFor(k, 500))
	}
	if c.Stats().Frames != 100 {
		t.Fatalf("regrown cache holds %d frames", c.Stats().Frames)
	}
	checkShardInvariants(t, c)
}

// TestFrameCacheConcurrent hammers one cache from many goroutines - puts, gets and budget changes
// - and checks every hit returns the bytes of the frame asked for. Run under -race.
func TestFrameCacheConcurrent(t *testing.T) {
	c := NewFrameCache(512 << 10)
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(g)))
			for i := 0; i < 4000; i++ {
				k := frameKey{gen: uint64(1 + rng.Intn(3)), off: int64(rng.Intn(400))}
				size := int(k.off)*13 + 1
				if bp, ok := c.get(k); ok {
					if !bytes.Equal(*bp, framePayloadFor(k, size)) {
						errs <- fmt.Errorf("frame %v: wrong bytes", k)
						bufpool.Put(bp)
						return
					}
					bufpool.Put(bp)
				} else {
					c.put(k, framePayloadFor(k, size))
				}
				if g == 0 && i%500 == 0 {
					c.SetBudget(int64(256+rng.Intn(512)) << 10)
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	checkShardInvariants(t, c)
}

// cachedReader writes commits to one segment through a cache-enabled reader's shim and returns
// the reader and each commit's frame offset.
func cachedReader(t *testing.T, shim storage.PlatformIOShim, seq int64, commits []document.Commit) (*DefaultReader, storage.DeltaSegmentRef, []int64) {
	t.Helper()
	cfg := newConfig(shim, storage.CompressionZSTD)
	w := NewDefaultWriter(testNS, codec.UUID{MSB: seq + 1}, seq, shim, cfg)
	var offs []int64
	for _, c := range commits {
		off, err := w.Append(record(t, c))
		if err != nil {
			t.Fatal(err)
		}
		offs = append(offs, off)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	r := NewDefaultReader(testNS, shim, cfg)
	r.SetFrameCache(NewFrameCache(1 << 20))
	return r, storage.DeltaSegmentRef{NamespaceID: testNS, SequenceNumber: seq}, offs
}

// TestFrameCacheServesRepeatReads: the second read of a frame comes from the cache, reads no
// segment bytes, and decodes the same commit.
func TestFrameCacheServesRepeatReads(t *testing.T) {
	for _, shim := range []storage.PlatformIOShim{storio.NewInMemoryPlatformIO(), newTestShim(t)} {
		commits := buildChain(t, 5)
		counting := &readCountingShim{PlatformIOShim: shim}
		r, ref, offs := cachedReader(t, counting, 0, commits)
		for pass := 0; pass < 2; pass++ {
			before := counting.reads
			for i, off := range offs {
				c, err := r.ReadCommitAt(ref, off)
				if err != nil || c.Hash != commits[i].Hash {
					t.Fatalf("pass %d commit %d: %v", pass, i, err)
				}
			}
			if pass == 1 && counting.reads != before {
				t.Fatalf("cached pass read %d times", counting.reads-before)
			}
		}
		if st := r.FrameCache().Stats(); st.Hits != 5 || st.Inserts != 5 {
			t.Fatalf("stats %+v", st)
		}
	}
}

// readCountingShim counts segment reads, forwarding the optional interfaces the reader uses.
type readCountingShim struct {
	storage.PlatformIOShim
	reads int
}

func (s *readCountingShim) ReadFromSegment(name string, off int64, n int) ([]byte, error) {
	s.reads++
	return s.PlatformIOShim.ReadFromSegment(name, off, n)
}

func (s *readCountingShim) SegmentGeneration(name string) (uint64, bool) {
	return s.PlatformIOShim.(storage.SegmentGenerationer).SegmentGeneration(name)
}

// TestFrameCacheNoStaleFrameAfterSegmentDeleted: a deleted segment's frames are never served -
// neither while the segment is gone (truncation, retention) nor once another file carries its
// name with different frames at the same offsets (repair rewriting it, a namespace recreated).
func TestFrameCacheNoStaleFrameAfterSegmentDeleted(t *testing.T) {
	for name, shim := range map[string]storage.PlatformIOShim{"memory": storio.NewInMemoryPlatformIO(), "file": newTestShim(t)} {
		t.Run(name, func(t *testing.T) {
			old := buildChain(t, 3)
			r, ref, offs := cachedReader(t, shim, 4, old)
			for i, off := range offs {
				if c, err := r.ReadCommitAt(ref, off); err != nil || c.Hash != old[i].Hash {
					t.Fatalf("commit %d: %v", i, err)
				}
			}
			if r.FrameCache().Stats().Frames != 3 {
				t.Fatal("frames not cached")
			}
			// Truncated away: reads now fail, as they do without a cache.
			if err := shim.DeleteSegment(segName(4)); err != nil {
				t.Fatal(err)
			}
			if _, err := r.ReadCommitAt(ref, offs[0]); err == nil {
				t.Fatal("deleted segment's frame served from the cache")
			}
			if err := r.ReadDocumentAt(ref, offs[0], codec.UUID{}, func([]byte) bool { return true }); err == nil {
				t.Fatal("deleted segment's frame served to ReadDocumentAt")
			}
			// Same name, new frames of the same sizes at the same offsets.
			fresh := buildChain(t, 3)
			w := NewDefaultWriter(testNS, codec.UUID{MSB: 77}, 4, shim, newConfig(shim, storage.CompressionZSTD))
			for i, c := range fresh {
				off, err := w.Append(record(t, c))
				if err != nil || off != offs[i] {
					t.Fatalf("rewrite: off %d want %d, %v", off, offs[i], err)
				}
			}
			if err := w.Flush(); err != nil {
				t.Fatal(err)
			}
			for i, off := range offs {
				c, err := r.ReadCommitAt(ref, off)
				if err != nil || c.Hash != fresh[i].Hash {
					t.Fatalf("commit %d after rewrite: got %v (stale=%v), err %v", i, c.Hash.Hex(), c.Hash == old[i].Hash, err)
				}
			}
		})
	}
}

// TestFrameCacheNewGenerationAfterRelease: a namespace whose handles were released (it closed)
// comes back under new generations, since another process may have changed it meanwhile.
func TestFrameCacheNewGenerationAfterRelease(t *testing.T) {
	root := t.TempDir()
	store, err := storio.NewOSByteStore(storio.PlatformIOConfig{RootDirectory: &root})
	if err != nil {
		t.Fatal(err)
	}
	shim := storio.NewFileBackedPlatformIO(storio.PlatformIOConfig{RootDirectory: &root}, store)
	if _, err := shim.AppendToSegment(segName(1), []byte("x")); err != nil {
		t.Fatal(err)
	}
	g1, _ := shim.SegmentGeneration(segName(1))
	if g, _ := shim.SegmentGeneration(segName(1)); g != g1 {
		t.Fatal("generation changed with nothing deleted")
	}
	if _, err := shim.AppendToSegment(segName(1), []byte("y")); err != nil {
		t.Fatal(err)
	}
	if g, _ := shim.SegmentGeneration(segName(1)); g != g1 {
		t.Fatal("an append changed the generation")
	}
	if err := shim.ReleaseHandles(storio.SegmentNameBuilder.NamespacePrefix(testNS)); err != nil {
		t.Fatal(err)
	}
	if g, _ := shim.SegmentGeneration(segName(1)); g == g1 {
		t.Fatal("generation survived ReleaseHandles")
	}
}

// TestFrameCacheDamagedFrameDetected: a frame whose body no longer matches its CRC is reported as
// corrupt by both single-frame reads, and nothing is cached for it, so it keeps being reported.
func TestFrameCacheDamagedFrameDetected(t *testing.T) {
	shim := newTestShim(t)
	commits := buildChain(t, 3)
	r, ref, offs := cachedReader(t, shim, 2, commits)
	raw := readSegment(t, shim, 2)
	raw[offs[1]+PageFrameHeaderSize+3] ^= 0x40
	rewriteSegmentBytes(t, shim, 2, raw)
	for pass := 0; pass < 2; pass++ {
		_, err := r.ReadCommitAt(ref, offs[1])
		var corrupt *CorruptFrameError
		if !errors.As(err, &corrupt) || corrupt.Offset != int(offs[1]) {
			t.Fatalf("pass %d: ReadCommitAt of a damaged frame: %v", pass, err)
		}
		err = r.ReadDocumentAt(ref, offs[1], codec.UUID{}, func([]byte) bool { return true })
		if !errors.As(err, &corrupt) {
			t.Fatalf("pass %d: ReadDocumentAt of a damaged frame: %v", pass, err)
		}
	}
	if st := r.FrameCache().Stats(); st.Frames != 0 {
		t.Fatalf("damaged frame cached: %+v", st)
	}
	// Its intact neighbours still read, and are cached.
	for _, i := range []int{0, 2} {
		if c, err := r.ReadCommitAt(ref, offs[i]); err != nil || c.Hash != commits[i].Hash {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	if st := r.FrameCache().Stats(); st.Frames != 2 {
		t.Fatalf("stats %+v", st)
	}
}

// TestReadDocumentAtMatchesReadCommitAt: for every document of many random multi-op commits -
// several writes of one document, deletes, other op kinds - ReadDocumentAt yields exactly the
// writes ReadCommitAt's full decode holds, with and without the cache, compressed or not.
func TestReadDocumentAtMatchesReadCommitAt(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	ids := make([]codec.UUID, 8)
	for i := range ids {
		ids[i] = codec.UUID{MSB: rng.Int63(), LSB: rng.Int63()}
	}
	for _, comp := range []storage.CompressionCodec{storage.CompressionNone, storage.CompressionZSTD} {
		shim := storio.NewInMemoryPlatformIO()
		cfg := newConfig(shim, comp)
		w := NewDefaultWriter(testNS, codec.UUID{MSB: 1}, 0, shim, cfg)
		var offs []int64
		for n := 0; n < 200; n++ {
			var ops []document.Op
			for j := 0; j < 1+rng.Intn(30); j++ {
				id := ids[rng.Intn(len(ids))]
				switch rng.Intn(5) {
				case 0:
					ops = append(ops, document.DeleteOp{DocID: id})
				case 1:
					ops = append(ops, document.SchemaMigrationOp{MigrationID: id, MigrationPayload: "m"})
				default:
					ops = append(ops, document.WriteOp{DocID: id, Patch: fmt.Sprintf(`{"n":%d,"j":%d}`, n, j)})
				}
			}
			c := document.Commit{NamespaceID: testNS, Operations: ops, DocumentTreeHash: document.EmptyDocumentTree().TreeHash}
			payload, err := c.ToPayloadBytes()
			if err != nil {
				t.Fatal(err)
			}
			off, err := w.Append(storage.DeltaRecord{NamespaceID: testNS, CommitPayload: payload})
			if err != nil {
				t.Fatal(err)
			}
			offs = append(offs, off)
		}
		ref := storage.DeltaSegmentRef{NamespaceID: testNS, SequenceNumber: 0}
		for _, cache := range []*FrameCache{nil, NewFrameCache(1 << 20)} {
			r := NewDefaultReader(testNS, shim, cfg)
			r.SetFrameCache(cache)
			for pass := 0; pass < 2; pass++ {
				for _, off := range offs {
					full, err := r.ReadCommitAt(ref, off)
					if err != nil {
						t.Fatal(err)
					}
					for _, id := range ids {
						var want, got []string
						for _, op := range full.Operations {
							if wop, ok := op.(document.WriteOp); ok && wop.DocID == id {
								want = append(want, wop.Patch)
							}
						}
						if err := r.ReadDocumentAt(ref, off, id, func(p []byte) bool {
							got = append(got, string(p))
							return true
						}); err != nil {
							t.Fatal(err)
						}
						if fmt.Sprintf("%q", got) != fmt.Sprintf("%q", want) {
							t.Fatalf("codec %d frame %d doc %v: ReadDocumentAt %q, ReadCommitAt %q", comp, off, id, got, want)
						}
					}
				}
			}
			if cache != nil && cache.Stats().Hits == 0 {
				t.Fatal("cache never hit")
			}
		}
	}
}
