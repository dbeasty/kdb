package sstable

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// segmentExists reports whether name is still on disk.
func segmentExists(t *testing.T, io *storio.InMemoryPlatformIO, ns, name string) bool {
	t.Helper()
	names, err := io.ListSegments(ns)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range names {
		if n == name {
			return true
		}
	}
	return false
}

// compactingStore builds a store of three tables and returns it with one key from the oldest
// table, its value, and the handles a compaction will consume.
func compactingStore(t *testing.T) (*storio.InMemoryPlatformIO, *LsmBlobStore, codec.Hash, []Handle) {
	t.Helper()
	io := storio.NewInMemoryPlatformIO()
	ns := "app/docs"
	store := NewLsmBlobStore(io, ns, nil)
	var first []codec.Hash
	for i := 0; i < 3; i++ {
		keys, h := writeSizedTable(t, io, ns, 20, 32, int64(i+700))
		store.AddTable(h)
		if i == 0 {
			first = keys
		}
	}
	return io, store, first[0], store.Tables()
}

// TestLookupDuringCompactionStillFindsTheKey is the deterministic reproducer for the
// compaction/lookup race recorded by #123: a lookup takes the table list, a compaction then
// swaps its inputs out and deletes them, and the lookup goes on to read tables that are gone.
// Lookup treats the read error as "this table cannot answer", so a key that exists - in the
// merged table the lookup never saw - came back as not found.
func TestLookupDuringCompactionStillFindsTheKey(t *testing.T) {
	for _, probe := range []string{"Lookup", "Contains"} {
		t.Run(probe, func(t *testing.T) {
			io, store, key, inputs := compactingStore(t)
			want, _, _ := store.Lookup(key) // also caches every input's index
			if want == nil {
				t.Fatal("key missing before any compaction")
			}
			fired := false
			store.afterSearchOrder = func() {
				if fired {
					return
				}
				fired = true
				res, err := store.Compact(2, nil)
				if err != nil {
					t.Fatal(err)
				}
				if res.Merged != len(inputs) {
					t.Fatalf("compaction merged %d tables, want %d", res.Merged, len(inputs))
				}
				// The lookup in flight still holds the inputs: none may be gone yet.
				for _, in := range inputs {
					if !segmentExists(t, io, "app/docs", in.SegmentName) {
						t.Errorf("compaction deleted %s while a lookup still held it", in.SegmentName)
					}
				}
			}
			switch probe {
			case "Lookup":
				got, deleted, found := store.Lookup(key)
				if !found || deleted || !bytes.Equal(got, want) {
					t.Fatalf("lookup racing a compaction: found=%v deleted=%v, value matches=%v",
						found, deleted, bytes.Equal(got, want))
				}
			case "Contains":
				// Drop the cached indexes so Contains has to read the input files, as a probe of a
				// table whose index was evicted does.
				store.mu.Lock()
				store.indexes = newTableIndexCache(DefaultIndexCacheBytes)
				store.mu.Unlock()
				if !store.Contains(key) {
					t.Fatal("Contains racing a compaction reported a present key as absent")
				}
			}
			if !fired {
				t.Fatal("the hook never ran")
			}
			// Once the last reader let go, the inputs must be deleted - deferral, not a leak.
			for _, in := range inputs {
				if segmentExists(t, io, "app/docs", in.SegmentName) {
					t.Fatalf("input %s outlived the last reader holding it", in.SegmentName)
				}
			}
			store.afterSearchOrder = nil
			if got := store.Get(key); !bytes.Equal(got, want) {
				t.Fatal("key missing from the merged table")
			}
		})
	}
}

// TestCompactDeletesUnheldInputsAtOnce pins the common case: with no reader in flight, a
// compaction deletes its inputs before it returns, exactly as it did before deferral existed.
func TestCompactDeletesUnheldInputsAtOnce(t *testing.T) {
	io, store, _, inputs := compactingStore(t)
	res, err := store.Compact(2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Removed != len(inputs) || res.Deferred != 0 {
		t.Fatalf("removed %d, deferred %d; want %d and 0", res.Removed, res.Deferred, len(inputs))
	}
	for _, in := range inputs {
		if segmentExists(t, io, "app/docs", in.SegmentName) {
			t.Fatalf("input %s was not deleted", in.SegmentName)
		}
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.readers) != 0 || len(store.doomed) != 0 {
		t.Fatalf("bookkeeping left behind: %d readers, %d doomed", len(store.readers), len(store.doomed))
	}
}

// TestLookupNeverMissesUnderConcurrentCompaction is the stress form: lookups of keys known to be
// present, while flushes add tables and compactions merge and delete them over and over. Every
// probe must find its key. Before tables were pinned, a run of this missed within a few
// compactions.
func TestLookupNeverMissesUnderConcurrentCompaction(t *testing.T) {
	io := storio.NewInMemoryPlatformIO()
	ns := "app/docs"
	store := NewLsmBlobStore(io, ns, nil)
	var mu sync.Mutex
	var present []codec.Hash
	for i := 0; i < 4; i++ {
		keys, h := writeSizedTable(t, io, ns, 30, 32, int64(i+800))
		store.AddTable(h)
		present = append(present, keys...)
	}
	stop := make(chan struct{})
	var misses, probes atomic.Int64
	var readers sync.WaitGroup
	for g := 0; g < 6; g++ {
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
				k := present[rng.Intn(len(present))]
				mu.Unlock()
				probes.Add(1)
				if g%2 == 0 {
					if v, _, found := store.Lookup(k); !found || v == nil {
						misses.Add(1)
					}
				} else {
					if g%4 == 1 {
						// Contains answers from cached indexes when it can; dropping them makes it
						// read the files, which is where the race bites.
						store.SetIndexCacheBudget(1)
					}
					if !store.Contains(k) {
						misses.Add(1)
					}
				}
			}
		}(g)
	}
	var writers sync.WaitGroup
	writers.Add(2)
	go func() {
		defer writers.Done()
		for i := 0; i < 40; i++ {
			keys, h := writeSizedTable(t, io, ns, 30, 32, int64(i+900))
			store.AddTable(h)
			mu.Lock()
			present = append(present, keys...)
			mu.Unlock()
		}
	}()
	go func() {
		defer writers.Done()
		for i := 0; i < 60; i++ {
			if _, err := store.Compact(2, nil); err != nil {
				t.Error(err)
			}
		}
	}()
	writers.Wait()
	close(stop)
	readers.Wait()

	if n := misses.Load(); n > 0 {
		t.Fatalf("%d of %d probes of present keys missed while compactions ran", n, probes.Load())
	}
	// No reader is left, so nothing may still be waiting to be deleted: every file on disk is a
	// registered table.
	registered := map[string]bool{}
	for _, h := range store.Tables() {
		registered[h.SegmentName] = true
	}
	names, err := io.ListSegments(ns)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range names {
		if !registered[n] {
			t.Fatalf("segment %s is on disk but not registered: a deferred deletion never ran", n)
		}
	}
	t.Logf("%d probes, 0 misses", probes.Load())
}
