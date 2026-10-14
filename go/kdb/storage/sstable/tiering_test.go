package sstable

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

func sizedRuns(bytes ...int64) []mergeRun {
	runs := make([]mergeRun, len(bytes))
	for i, b := range bytes {
		runs[i].bytes = b
	}
	return runs
}

func TestPickMerge(t *testing.T) {
	for _, tc := range []struct {
		name   string
		runs   []int64 // newest first
		sized  bool
		lo, hi int
		full   bool
	}{
		{"below the trigger", []int64{1, 1, 1}, true, 0, 0, false},
		{"equal flushes merge whole", []int64{1, 1, 1, 1}, true, 0, 4, true},
		{"newer outgrow the oldest", []int64{1, 1, 2, 4}, true, 0, 4, true},
		{"four new flushes beside a big run", []int64{1, 1, 1, 1, 8}, true, 0, 4, false},
		{"three are not a window", []int64{1, 1, 1, 8}, true, 0, 0, false},
		{"tiers like a counter", []int64{1, 1, 1, 1, 4, 16}, true, 0, 5, false},
		{"a window that does not start at the newest", []int64{1, 13, 3, 3, 3, 3, 60}, true, 1, 6, false},
		{"dissimilar runs under the cap wait", []int64{1, 10, 100, 1000}, true, 0, 0, false},
		{"over the cap, the cheapest window", []int64{1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000, 10000000000, 100000000000, 1000000000000}, true, 0, 2, false},
		{"unsized merges whole", []int64{1, 10, 100, 1000}, false, 0, 4, true},
	} {
		lo, hi, full := pickMerge(sizedRuns(tc.runs...), tc.sized, DefaultCompactionTrigger)
		if lo != tc.lo || hi != tc.hi || full != tc.full {
			t.Errorf("%s: pickMerge(%v) = [%d:%d] %v; want [%d:%d] %v", tc.name, tc.runs, lo, hi, full, tc.lo, tc.hi, tc.full)
		}
	}
}

// simulateGrowth replays pickMerge over a store fed equal flushes, one merge per flush as
// maintenance runs one, and reports the bytes merges wrote per byte flushed and the most runs the
// store held.
func simulateGrowth(flushes int) (amplification float64, peakRuns int) {
	var runs []int64 // newest first
	var written int64
	for f := 0; f < flushes; f++ {
		runs = append([]int64{1}, runs...)
		lo, hi, _ := pickMerge(sizedRuns(runs...), true, DefaultCompactionTrigger)
		if hi > 0 {
			var sum int64
			for _, b := range runs[lo:hi] {
				sum += b
			}
			written += sum
			next := append(append([]int64(nil), runs[:lo]...), sum)
			runs = append(next, runs[hi:]...)
		}
		peakRuns = max(peakRuns, len(runs))
	}
	return float64(written) / float64(flushes), peakRuns
}

// Tiering's two promises, on the selection alone: bytes written grow like log(N), not N, and the
// runs a lookup searches stay bounded.
func TestTieringWriteAmplificationIsLogarithmic(t *testing.T) {
	for _, tc := range []struct {
		flushes int
		most    float64
	}{{100, 4}, {1000, 6}, {10000, 9}} {
		amp, peak := simulateGrowth(tc.flushes)
		t.Logf("%d flushes: merges wrote %.1fx the data, at most %d runs", tc.flushes, amp, peak)
		if amp > tc.most {
			t.Errorf("%d flushes: merges wrote %.1fx the data, want at most %.0fx", tc.flushes, amp, tc.most)
		}
		if peak > mergeMaxRuns(DefaultCompactionTrigger)+1 {
			t.Errorf("%d flushes: %d runs at once, over the cap", tc.flushes, peak)
		}
	}
}

// partialAppendCounter counts the bytes merges write: every merge writes its tables under partial
// names first, so those appends are exactly a merge's output.
type partialAppendCounter struct {
	*storio.InMemoryPlatformIO
	written int64
}

func (c *partialAppendCounter) AppendToSegment(name string, b []byte) (int64, error) {
	if strings.Contains(name, "/sstable-partial/") {
		c.written += int64(len(b))
	}
	return c.InMemoryPlatformIO.AppendToSegment(name, b)
}

// growStore flushes tables of equal size into a store, merging after each as maintenance would,
// and reports the bytes the merges wrote and the bytes the flushes did.
func growStore(t *testing.T, flushes int) (merged, flushed int64, store *LsmBlobStore) {
	t.Helper()
	shim := &partialAppendCounter{InMemoryPlatformIO: testShim(t)}
	ns := "app/docs"
	store = NewLsmBlobStore(shim, ns, NewBlockCache(1<<20))
	for i := 0; i < flushes; i++ {
		h := bigTable(t, shim, ns, 50, 200, int64(i), nil)
		n, err := shim.SegmentSize(h.SegmentName)
		if err != nil {
			t.Fatal(err)
		}
		flushed += n
		store.AddTable(h)
		if _, err := store.Compact(DefaultCompactionTrigger, nil); err != nil {
			t.Fatal(err)
		}
		if runs := store.RunCount(); runs > mergeMaxRuns(DefaultCompactionTrigger) {
			t.Fatalf("after flush %d the store holds %d runs, over the cap", i, runs)
		}
	}
	for i := 0; i < flushes; i++ {
		for _, j := range []int{0, 49} {
			if got := store.Get(keyFor([]byte(fmt.Sprintf("key-%d-%06d", i, j)))); len(got) != 200 {
				t.Fatalf("flush %d key %d reads %d bytes", i, j, len(got))
			}
		}
	}
	return shim.written, flushed, store
}

// The point of tiering: a store that keeps growing writes about log(N) times its data in merges,
// not about N/6 times.
func TestTieredMergesWriteLessThanFullMerges(t *testing.T) {
	const flushes = 96
	tiered, flushed, _ := growStore(t, flushes)
	tieredMerges = false
	full, _, _ := growStore(t, flushes)
	tieredMerges = true
	t.Logf("%d flushes of %d KB: merges wrote %.1fx the data tiered, %.1fx with full merges",
		flushes, flushed/flushes>>10, float64(tiered)/float64(flushed), float64(full)/float64(flushed))
	if tiered*4 > full {
		t.Fatalf("tiered merges wrote %d bytes, full merges %d: not even a quarter less", tiered, full)
	}
	if float64(tiered) > 8*float64(flushed) {
		t.Fatalf("tiered merges wrote %.1fx the data, more than log2(%d)", float64(tiered)/float64(flushed), flushes)
	}
}

// A window's merge keeps its tombstones and takes the window's place in the search order: a
// delete in a new flush still hides the value an older, unmerged run holds, and a value written
// after the delete still wins over it.
func TestWindowMergeKeepsItsPlaceInTheSearchOrder(t *testing.T) {
	shim := testShim(t)
	ns := "app/docs"
	store := NewLsmBlobStore(shim, ns, NewBlockCache(1<<20))
	// One large run holding "gone" and "back".
	for i := 0; i < DefaultCompactionTrigger; i++ {
		puts := map[string]string{}
		if i == 0 {
			puts["gone"], puts["back"] = "old", "old"
		}
		store.AddTable(writeTable(t, shim, ns, 0, puts, nil))
		store.AddTable(bigTable(t, shim, ns, 200, 200, int64(i), nil))
	}
	if res, err := store.Compact(DefaultCompactionTrigger, nil); err != nil || !res.Full {
		t.Fatalf("building the large run: %+v, %v", res, err)
	}
	// Four small flushes, oldest first: filler, "x", a delete of both, then "back" written again.
	// Each carries filler values so their sizes are alike (the newest a little the largest), which
	// is what makes them a window rather than three runs waiting beside one large one.
	flush := func(fill int, puts map[string]string, deletes []string) {
		h := padTable(t, shim, ns, fill, puts, deletes)
		store.AddTable(h)
	}
	flush(20, nil, nil)
	flush(20, map[string]string{"x": "1"}, nil)
	flush(20, nil, []string{"gone", "back"})
	flush(30, map[string]string{"back": "new"}, nil)
	res, err := store.Compact(DefaultCompactionTrigger, nil)
	if err != nil || res.Full || res.Merged != 4 {
		t.Fatalf("window merge: %+v, %v", res, err)
	}
	if _, deleted, found := store.Lookup(keyFor([]byte("gone"))); !found || !deleted {
		t.Fatal("the window's tombstone stopped hiding the large run's value")
	}
	if got := store.Get(keyFor([]byte("back"))); string(got) != "new" {
		t.Fatalf("back reads %q, want the value written after the delete", got)
	}
	if got := store.Get(keyFor([]byte("x"))); string(got) != "1" {
		t.Fatalf("x reads %q", got)
	}
	// A full merge later drops the tombstone and what it hid.
	for i := 0; i < 2*DefaultCompactionTrigger && !res.Full; i++ {
		store.AddTable(bigTable(t, shim, ns, 400, 200, int64(100+i), nil))
		if res, err = store.Compact(DefaultCompactionTrigger, nil); err != nil {
			t.Fatal(err)
		}
	}
	if !res.Full {
		t.Fatal("no full merge after the store's newer runs outgrew its oldest")
	}
	if _, _, found := store.Lookup(keyFor([]byte("gone"))); found {
		t.Fatal("a full merge kept the tombstone or the value it hid")
	}
	if got := store.Get(keyFor([]byte("back"))); string(got) != "new" {
		t.Fatalf("back reads %q after the full merge", got)
	}
}

// A bounded merge (close) still runs the cheap merge in a store too large to merge whole.
func TestBoundedMergeTakesTheWindowNotTheStore(t *testing.T) {
	shim := testShim(t)
	ns := "app/docs"
	store := NewLsmBlobStore(shim, ns, NewBlockCache(1<<20))
	for i := 0; i < DefaultCompactionTrigger; i++ {
		store.AddTable(bigTable(t, shim, ns, 500, 200, int64(i), nil))
	}
	if _, err := store.Compact(DefaultCompactionTrigger, nil); err != nil {
		t.Fatal(err)
	}
	total, _ := store.TableBytes()
	for i := 0; i < DefaultCompactionTrigger; i++ {
		store.AddTable(bigTable(t, shim, ns, 10, 200, int64(50+i), nil))
	}
	bytes, due := store.PendingMerge(DefaultCompactionTrigger)
	if !due || bytes <= 0 || bytes >= total/4 {
		t.Fatalf("PendingMerge = %d, %v; want a small window of a %d-byte store", bytes, due, total)
	}
	if res, err := store.CompactContextUpTo(t.Context(), DefaultCompactionTrigger, nil, bytes-1); err != nil || res.Postponed != bytes || res.Merged != 0 {
		t.Fatalf("a bound below the window: %+v, %v", res, err)
	}
	res, err := store.CompactContextUpTo(t.Context(), DefaultCompactionTrigger, nil, total/4)
	if err != nil || res.Merged != DefaultCompactionTrigger || res.Full || res.Postponed != 0 {
		t.Fatalf("a bound above the window, below the store: %+v, %v", res, err)
	}
}

// Merged runs reopen in the order they were written - their names sort by time - so a store that
// tiered before a restart searches its runs in the same order after it.
func TestRunsReopenInTheOrderTheyWereWritten(t *testing.T) {
	_, _, store := growStore(t, 24)
	before := store.searchOrderForTest()
	reopened := NewLsmBlobStore(store.io, "app/docs", NewBlockCache(1<<20))
	if err := reopened.DiscoverTables(); err != nil {
		t.Fatal(err)
	}
	after := reopened.searchOrderForTest()
	if len(after) != len(before) {
		t.Fatalf("%d tables after reopening, %d before", len(after), len(before))
	}
	pos := map[string]int{}
	for i, h := range after {
		pos[h.SegmentName] = i
	}
	var prevRun string
	prev := -1
	for _, h := range before {
		if r := runOf(h.SegmentName); r == prevRun {
			continue
		} else {
			prevRun = r
		}
		if pos[h.SegmentName] < prev {
			t.Fatalf("run %s is searched before a run that was newer than it", runOf(h.SegmentName))
		}
		prev = pos[h.SegmentName]
	}
}

// Ages never tie, however coarse the clock: Windows' reads the same time for consecutive flushes,
// and two tables of one age sort by their random UUIDs.
func TestAgesAreStrictlyIncreasing(t *testing.T) {
	prev := nextAge()
	for i := 0; i < 10000; i++ {
		age := nextAge()
		if age <= prev {
			t.Fatalf("age %s after %s", age, prev)
		}
		prev = age
	}
}

// A table found on disk with an age ahead of the clock - written before the clock stepped back -
// still sorts before every table written after it.
func TestAgesFollowTheNewestTableFound(t *testing.T) {
	ahead := time.Now().Add(time.Hour).UnixNano()
	noteAge(fmt.Sprintf("ns/app/docs/sstable/L0/%016x-00000000-0000-0000-0000-000000000000", ahead))
	age, err := strconv.ParseUint(nextAge(), 16, 64)
	if err != nil || int64(age) <= ahead {
		t.Fatalf("age %d, %v after a table aged %d", age, err, ahead)
	}
	// Kotlin's names, and this package's before ages, carry none; they leave ages alone.
	before := lastAge.Load()
	noteAge("ns/app/docs/sstable/L0/00000000-0000-0000-0000-000000000000")
	if lastAge.Load() != before {
		t.Fatal("an ageless name moved the ages")
	}
}

func (s *LsmBlobStore) searchOrderForTest() []Handle {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Handle(nil), s.searchOrderLocked()...)
}

// padTable is writeTable with fill extra random 200-byte values, so small tables can be made of
// alike size.
func padTable(t *testing.T, shim storage.PlatformIOShim, ns string, fill int, puts map[string]string, deletes []string) Handle {
	t.Helper()
	rng := rand.New(rand.NewSource(int64(fill*7919 + len(puts)*31 + len(deletes))))
	w := NewDefaultWriter(shim, ns, 0)
	for k, v := range puts {
		w.Put(keyFor([]byte(k)), []byte(v))
	}
	for _, k := range deletes {
		w.Delete(keyFor([]byte(k)))
	}
	for i := 0; i < fill; i++ {
		v := make([]byte, 200)
		rng.Read(v)
		var k [16]byte
		rng.Read(k[:])
		w.Put(keyFor(append([]byte("pad-"), k[:]...)), v)
	}
	h, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// TestMeasureMergeWritesOnDisk is a measurement, not a correctness test: a file-backed store fed
// KDB_MERGE_FLUSHES flushes of ~1 MB (default 200), merged after each as maintenance would, tiered
// and with full merges. Skipped unless KDB_MERGE_MEASURE=1.
//
//	KDB_MERGE_MEASURE=1 go test ./kdb/storage/sstable -run TestMeasureMergeWritesOnDisk -v
func TestMeasureMergeWritesOnDisk(t *testing.T) {
	if os.Getenv("KDB_MERGE_MEASURE") != "1" {
		t.Skip("set KDB_MERGE_MEASURE=1 to run")
	}
	flushes := 200
	if n, err := strconv.Atoi(os.Getenv("KDB_MERGE_FLUSHES")); err == nil && n > 0 {
		flushes = n
	}
	for _, tiered := range []bool{true, false} {
		tieredMerges = tiered
		shim, _ := fileShim(t)
		ns := "app/docs"
		store := NewLsmBlobStore(shim, ns, NewBlockCache(1<<20))
		var merged, flushed int64
		var mergeTime time.Duration
		maxRuns := 0
		for i := 0; i < flushes; i++ {
			h := bigTable(t, shim, ns, 5000, 200, int64(i), nil)
			n, _ := shim.SegmentSize(h.SegmentName)
			flushed += n
			store.AddTable(h)
			start := time.Now()
			res, err := store.Compact(DefaultCompactionTrigger, nil)
			if err != nil {
				t.Fatal(err)
			}
			mergeTime += time.Since(start)
			for _, name := range res.Outputs {
				n, _ := shim.SegmentSize(name)
				merged += n
			}
			maxRuns = max(maxRuns, store.RunCount())
		}
		t.Logf("tiered=%v: %d flushes, %d MB flushed; merges wrote %d MB (%.1fx) in %v; at most %d runs, %d tables at the end",
			tiered, flushes, flushed>>20, merged>>20, float64(merged)/float64(flushed), mergeTime.Round(time.Millisecond), maxRuns, len(store.Tables()))
	}
	tieredMerges = true
}
