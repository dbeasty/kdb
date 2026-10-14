package engine

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// shardHash returns a distinct hash that lands in shard 0, so a test can drive one shard's
// budget, recency and compaction exactly.
func shardHash(n int) codec.Hash {
	var h codec.Hash
	h.Bytes[0] = 0
	binary.BigEndian.PutUint64(h.Bytes[8:], uint64(n)+1)
	return h
}

func bodyFor(n, size int) document.Document {
	var id codec.UUID
	id.MSB, id.LSB = int64(n), int64(n)*7+1
	prefix := fmt.Sprintf(`{"n":%d,"p":"`, n)
	pad := size - len(prefix) - 2
	if pad < 0 {
		pad = 0
	}
	return document.Document{ID: id, JSON: prefix + strings.Repeat(string(rune('a'+n%26)), pad) + `"}`}
}

// checkShard verifies every structural invariant of one shard: the recency ring is a
// consistent cycle holding exactly the unpinned entries, every counter equals the sum it
// caches, and every indexed record points at a live record.
func checkShard(t *testing.T, sh *docHashShard) {
	t.Helper()
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.index.n != sh.usedSlots {
		t.Fatalf("index has %d entries, %d slots used", sh.index.n, sh.usedSlots)
	}
	var bytes, live int64
	unpinned := 0
	chunkLive := make([]int64, len(sh.chunks))
	for i := range sh.slots {
		r := sh.slots[i]
		if r.chunk == slotFree {
			continue
		}
		if got, ok := sh.index.find(sh.slots, &r.hash); !ok || got != uint32(i) {
			t.Fatalf("slot %d (%s) is not found through the index (%d, %v)", i, r.hash.Hex(), got, ok)
		}
		bytes += int64(r.n) + docHashEntryOverhead
		live += int64(r.n)
		if r.n > 0 {
			if off := r.off &^ slotReferenced; sh.chunks[r.chunk] == nil || int(off+r.n) > len(sh.chunks[r.chunk]) {
				t.Fatalf("slot %d addresses %d+%d of chunk %d (len %d)", i, off, r.n, r.chunk, len(sh.chunks[r.chunk]))
			}
			chunkLive[r.chunk] += int64(r.n)
		}
		if r.pins == 0 {
			unpinned++
			if r.next == noSlot {
				t.Fatalf("unpinned slot %d is not on the recency ring", i)
			}
		} else if r.next != noSlot {
			t.Fatalf("pinned slot %d is on the recency ring", i)
		}
	}
	if bytes != sh.bytes || live != sh.arenaLive {
		t.Fatalf("charge %d/%d live %d/%d (counted/cached)", bytes, sh.bytes, live, sh.arenaLive)
	}
	var capSum, usedSum int64
	for c, b := range sh.chunks {
		if chunkLive[c] != sh.chunkLive[c] {
			t.Fatalf("chunk %d: live %d counted, %d cached", c, chunkLive[c], sh.chunkLive[c])
		}
		if b == nil {
			continue
		}
		if chunkLive[c] == 0 && c != sh.cur {
			t.Fatalf("chunk %d holds no live body and is not current, but was not released", c)
		}
		capSum += int64(cap(b))
		usedSum += int64(len(b))
	}
	if capSum != sh.arenaCap || usedSum != sh.arenaUsed {
		t.Fatalf("arena cap %d/%d used %d/%d (counted/cached)", capSum, sh.arenaCap, usedSum, sh.arenaUsed)
	}
	if unpinned != sh.lruLen {
		t.Fatalf("%d unpinned entries, ring length %d", unpinned, sh.lruLen)
	}
	if sh.head == noSlot {
		if unpinned != 0 {
			t.Fatal("ring empty with unpinned entries")
		}
		return
	}
	seen := 0
	for i, n := sh.head, 0; ; n++ {
		r := sh.slots[i]
		if sh.slots[r.next].prev != i {
			t.Fatalf("ring broken at slot %d", i)
		}
		seen++
		i = r.next
		if i == sh.head {
			break
		}
		if n > sh.lruLen {
			t.Fatal("ring longer than lruLen")
		}
	}
	if seen != sh.lruLen {
		t.Fatalf("walked %d ring entries, lruLen %d", seen, sh.lruLen)
	}
}

func lruOrder(sh *docHashShard) []codec.Hash {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	var out []codec.Hash
	if sh.head == noSlot {
		return nil
	}
	for i := sh.head; ; {
		out = append(out, sh.slots[i].hash)
		i = sh.slots[i].next
		if i == sh.head {
			return out
		}
	}
}

// Put, Get, Pin, Unpin, eviction and compaction against a model: a version that Get returns is
// byte-for-byte the version that was stored under that hash, whatever compactions happened in
// between, and a pinned version is never lost.
func TestDocArenaModel(t *testing.T) {
	for _, size := range []int{40, 900, bodyArenaLargeBody + 1000} {
		t.Run(fmt.Sprintf("body%d", size), func(t *testing.T) {
			s := newShardedDocByHashStore(0)
			sh := s.shards[0]
			// Enough for a few dozen versions: eviction and compaction run constantly.
			sh.budget = 40 * (int64(size) + docHashEntryOverhead)
			rng := rand.New(rand.NewSource(int64(size)))
			pins := map[int]int{}
			// mustHave: resident while pinned, so it can never be evicted until unpinned.
			mustHave := map[int]bool{}
			const keys = 400
			for step := 0; step < 20000; step++ {
				k := rng.Intn(keys)
				h := shardHash(k)
				switch op := rng.Intn(10); {
				case op < 4:
					s.Put(h, bodyFor(k, size))
					if pins[k] > 0 {
						mustHave[k] = true
					}
				case op < 7:
					if d, ok := s.Get(h); ok {
						if want := bodyFor(k, size); d != want {
							t.Fatalf("step %d: Get(%d) = %d bytes, not the stored body", step, k, len(d.JSON))
						}
					} else if mustHave[k] {
						t.Fatalf("step %d: pinned version %d was evicted", step, k)
					}
				case op < 8 || pins[k] == 0:
					if s.Contains(h) {
						mustHave[k] = true
					}
					s.Pin(h)
					pins[k]++
				default:
					s.Unpin(h)
					pins[k]--
					if pins[k] == 0 {
						delete(pins, k)
						delete(mustHave, k)
					}
				}
				if step%997 == 0 {
					checkShard(t, sh)
				}
			}
			checkShard(t, sh)
			if sh.compacted == 0 && size < bodyArenaLargeBody {
				t.Fatal("the model never compacted: it is not exercising relocation")
			}
			for k := range mustHave {
				if d, ok := s.Get(shardHash(k)); !ok || d != bodyFor(k, size) {
					t.Fatalf("pinned version %d lost or corrupted", k)
				}
			}
		})
	}
}

// The least recently used unpinned version is the one evicted; a Get promotes; a pinned version
// is off the ring and comes back at the front when its last pin is released.
func TestDocArenaLRUOrder(t *testing.T) {
	s := newShardedDocByHashStore(0)
	sh := s.shards[0]
	const size = 100
	per := int64(size) + docHashEntryOverhead
	sh.budget = 4 * per
	for i := 0; i < 4; i++ {
		s.Put(shardHash(i), bodyFor(i, size))
	}
	if got, want := lruOrder(sh), []codec.Hash{shardHash(3), shardHash(2), shardHash(1), shardHash(0)}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("order after puts: %v", got)
	}
	// A read marks 0 referenced rather than moving it (second-chance eviction): the ring order is
	// unchanged, and the evictor passes over 0 once instead.
	s.Get(shardHash(0))
	s.Pin(shardHash(2)) // off the ring
	if got, want := lruOrder(sh), []codec.Hash{shardHash(3), shardHash(1), shardHash(0)}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("order after get+pin: %v", got)
	}
	if i, _ := sh.index.find(sh.slots, ptrHash(shardHash(0))); sh.slots[i].off&slotReferenced == 0 {
		t.Fatal("a read did not mark the version referenced")
	}
	s.Put(shardHash(4), bodyFor(4, size)) // over budget by one: 0 gets its second chance, 1 goes
	if _, ok := s.Get(shardHash(1)); ok {
		t.Fatal("least recently used version survived eviction")
	}
	for _, k := range []int{0, 2, 3, 4} {
		if _, ok := s.Get(shardHash(k)); !ok {
			t.Fatalf("version %d evicted out of order", k)
		}
	}
	s.Unpin(shardHash(2))
	if got := lruOrder(sh); got[0] != shardHash(2) {
		t.Fatalf("released version did not re-enter at the front: %v", got)
	}
	checkShard(t, sh)
}

// A reader that was handed a body keeps exactly those bytes, whatever the store does with the
// chunk afterwards: eviction, chunk release, and compactions that relocate every live body.
func TestDocArenaReaderHoldsBodyAcrossCompaction(t *testing.T) {
	s := newShardedDocByHashStore(0)
	sh := s.shards[0]
	const size = 200
	sh.budget = 50 * (int64(size) + docHashEntryOverhead)
	held := map[int]string{}
	for i := 0; i < 50; i++ {
		s.Put(shardHash(i), bodyFor(i, size))
		d, _ := s.Get(shardHash(i))
		held[i] = d.JSON
	}
	// Pin a few so they are relocated rather than dropped.
	for i := 0; i < 50; i += 5 {
		s.Pin(shardHash(i))
	}
	before := sh.compacted
	for i := 50; i < 5000; i++ {
		s.Put(shardHash(i), bodyFor(i, size))
		if i%100 == 0 {
			runtime.GC() // a dropped chunk that nothing referenced would be reclaimed here
		}
	}
	if sh.compacted == before {
		t.Fatal("no compaction ran")
	}
	for i, body := range held {
		if body != bodyFor(i, size).JSON {
			t.Fatalf("a held body changed under its reader after compaction: %d", i)
		}
	}
	for i := 0; i < 50; i += 5 {
		if d, ok := s.Get(shardHash(i)); !ok || d != bodyFor(i, size) {
			t.Fatalf("pinned version %d lost or corrupted by compaction", i)
		}
	}
	checkShard(t, sh)
}

// Readers and writers on every shard at once, with eviction and compaction running underneath.
// Run under -race: a reader reading a body while a writer appends to the same chunk, or while
// a compaction relocates it, must not be a race.
func TestDocArenaConcurrentReadersAndWriters(t *testing.T) {
	s := newShardedDocByHashStore(2 << 20)
	const keys = 4000
	var wg sync.WaitGroup
	errs := make(chan string, 16)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w)))
			for i := 0; i < 20000; i++ {
				k := rng.Intn(keys)
				h := hashN(k)
				switch rng.Intn(6) {
				case 0, 1:
					s.Put(h, bodyFor(k, 50+k%300))
				case 2:
					s.Pin(h)
					s.Put(h, bodyFor(k, 50+k%300))
					s.Unpin(h)
				default:
					if d, ok := s.Get(h); ok {
						// Read every byte, after the lock is released, as a caller would.
						if d != bodyFor(k, 50+k%300) {
							select {
							case errs <- fmt.Sprintf("Get(%d) returned the wrong body", k):
							default:
							}
							return
						}
					}
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	for _, sh := range s.shards {
		checkShard(t, sh)
	}
}

// What the store reports occupying matches what it actually adds to the heap.
func TestDocArenaAccountingMatchesHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates tens of MB")
	}
	if raceEnabled {
		t.Skip("the race detector changes allocation sizes")
	}
	for _, tc := range []struct {
		name   string
		n      int
		size   int
		pinned bool
	}{
		{"pinned-small", 200_000, 120, true},
		{"pinned-1KiB", 40_000, 1024, true},
		{"unpinned-small", 200_000, 120, false},
		{"placeholders", 200_000, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			docs := make([]document.Document, tc.n)
			for i := range docs {
				docs[i] = bodyFor(i, tc.size)
			}
			before := heapNow()
			s := newShardedDocByHashStore(0)
			for i := range docs {
				var h codec.Hash
				binary.BigEndian.PutUint64(h.Bytes[:], uint64(i)*0x9E3779B97F4A7C15)
				if tc.pinned {
					s.Pin(h)
				}
				if tc.size > 0 {
					s.Put(h, docs[i])
				}
			}
			heap := heapNow() - before
			runtime.KeepAlive(docs)
			reported := s.ResidentBytes()
			ratio := float64(reported) / float64(heap)
			t.Logf("%d versions of %d B: heap %.1f MB, reported %.1f MB (%.2fx), %.0f B/version",
				tc.n, tc.size, float64(heap)/1e6, float64(reported)/1e6, ratio, float64(heap)/float64(tc.n))
			if ratio < 0.8 || ratio > 1.25 {
				t.Fatalf("ResidentBytes is %.2fx the heap it occupies", ratio)
			}
		})
	}
}

func ptrHash(h codec.Hash) *codec.Hash { return &h }
