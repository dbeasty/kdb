package engine

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

func hashN(n int) codec.Hash {
	var h codec.Hash
	binary.BigEndian.PutUint64(h.Bytes[8:], uint64(n))
	h.Bytes[0] = byte(n) // spread across shards
	return h
}

// Pinned entries (the live tree) must not be on the recency list: with them there, every Put into
// a shard whose live data exceeded its budget walked all of it, so a bulk load slowed linearly
// with namespace size.
func TestPinnedVersionsDoNotMakeEvictionScanThem(t *testing.T) {
	s := newShardedDocByHashStore(1 << 20) // far below what is pinned below
	doc := document.Document{JSON: `{"pad":"` + string(make([]byte, 100)) + `"}`}

	const live = 60000
	for i := 0; i < live; i++ {
		h := hashN(i)
		s.Pin(h) // the engine pins a version before it stores it
		s.Put(h, doc)
	}
	if s.ResidentBytes() <= 1<<20 {
		t.Fatal("test needs the pinned set to exceed the budget")
	}

	// The structural guarantee: nothing pinned is on a recency list, so eviction has nothing to skip.
	for i, sh := range s.shards {
		if n := sh.lruLen; n != 0 {
			t.Fatalf("shard %d has %d entries on its recency list, all of them pinned", i, n)
		}
	}

	start := time.Now()
	for i := live; i < live+20000; i++ {
		s.Put(hashN(i), doc) // unpinned history: evicted immediately, since nothing else is evictable
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("20k puts beside %d pinned versions took %s: eviction is scanning the pinned set", live, d)
	}
	for i := 0; i < live; i += 997 {
		if _, ok := s.Get(hashN(i)); !ok {
			t.Fatalf("pinned version %d was evicted", i)
		}
	}

	// Unpinning makes a version evictable again, and eviction then brings the store to budget.
	for i := 0; i < live; i++ {
		s.Unpin(hashN(i))
	}
	if got := s.LiveBytes(); got > 5<<20 {
		t.Fatalf("after unpinning everything the store still holds %d live bytes", got)
	}
	// The footprint follows the live bytes down: dead bodies are compacted away and the slot
	// array and index are rebuilt smaller. It stays above them by the arena's unfilled tail
	// and up to bodyArenaDeadFraction of dead bodies per shard, which at 64 shards of 64 KiB
	// each is the same order as the live bytes themselves.
	if got := s.ResidentBytes(); got > 10<<20 {
		t.Fatalf("after unpinning everything the store still occupies %d bytes", got)
	}
}
