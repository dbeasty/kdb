package document

import (
	"bytes"
	"crypto/rand"
	"fmt"
	mrand "math/rand"
	"os"
	"runtime"
	"runtime/metrics"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
)

// TestTreeScale measures the live tree alone at the sizes named in KDB_TREE_SCALE (comma-separated
// document counts, e.g. "100000,1000000,5000000"): open (bulk build from a packed checkpoint
// image), retained heap and heap objects per document, the time of a full GC with the tree live,
// point reads, updates, deletes, keyset pages, and the heap once a run of updates has been
// absorbed. It uses only the public DocumentTree API so the same file measures any implementation.
//
//	KDB_TREE_SCALE=100000,1000000 go test ./kdb/document -run TestTreeScale -v -count=1
func TestTreeScale(t *testing.T) {
	spec := os.Getenv("KDB_TREE_SCALE")
	if spec == "" {
		t.Skip("set KDB_TREE_SCALE to document counts")
	}
	for _, f := range strings.Split(spec, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil || n <= 0 {
			t.Fatalf("bad KDB_TREE_SCALE entry %q", f)
		}
		measureTreeScale(t, n)
	}
}

func heapObjects() uint64 {
	s := []metrics.Sample{{Name: "/gc/heap/objects:objects"}}
	metrics.Read(s)
	return s[0].Value.Uint64()
}

func settledHeap() (alloc, objects uint64) {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc, heapObjects()
}

// gcTime is the median wall time of a forced, blocking full collection.
func gcTime() time.Duration {
	var ds []time.Duration
	for i := 0; i < 7; i++ {
		t0 := time.Now()
		runtime.GC()
		ds = append(ds, time.Since(t0))
	}
	sort.Slice(ds, func(a, b int) bool { return ds[a] < ds[b] })
	return ds[len(ds)/2]
}

func measureTreeScale(t *testing.T, n int) {
	packed := make([]byte, n*PackedTreeEntrySize)
	if _, err := rand.Read(packed); err != nil {
		t.Fatal(err)
	}
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	sort.Slice(idx, func(a, b int) bool {
		return bytes.Compare(packed[idx[a]*PackedTreeEntrySize:idx[a]*PackedTreeEntrySize+16],
			packed[idx[b]*PackedTreeEntrySize:idx[b]*PackedTreeEntrySize+16]) < 0
	})
	sorted := make([]byte, 0, len(packed))
	for _, i := range idx {
		sorted = append(sorted, packed[i*PackedTreeEntrySize:(i+1)*PackedTreeEntrySize]...)
	}
	packed, idx = nil, nil
	r := mrand.New(mrand.NewSource(int64(n)))
	const sampleN = 100000
	sample := make([]codec.UUID, sampleN)
	for i := range sample {
		j := r.Intn(n)
		id, err := codec.UUIDFromBytes(sorted[j*PackedTreeEntrySize : j*PackedTreeEntrySize+16])
		if err != nil {
			t.Fatal(err)
		}
		sample[i] = id
	}

	gcEmpty := gcTime()
	heap0, obj0 := settledHeap()
	t0 := time.Now()
	var tree DocumentTree
	var err error
	if os.Getenv("KDB_TREE_SCALE_FILE") == "1" { // paged: built to a segment, mapped
		f, ferr := NewTreeFile(TreeFileOptions{Dir: t.TempDir()})
		if ferr != nil {
			t.Skip(ferr)
		}
		tree, err = OpenTreePacked(sorted, f)
	} else {
		tree, err = BuildTrieDocumentTreePacked(sorted)
	}
	if err != nil {
		t.Fatal(err)
	}
	openTime := time.Since(t0)
	// The packed image was live at heap0 and is dropped here: add its bytes (one object) back.
	sortedBytes := uint64(cap(sorted))
	sorted = nil
	heap1, obj1 := settledHeap()
	heap1, obj1 = heap1+sortedBytes, obj1+1
	gcLive := gcTime()

	var ms0, ms1 runtime.MemStats
	// Point reads.
	const reads = 1_000_000
	runtime.ReadMemStats(&ms0)
	t0 = time.Now()
	for i := 0; i < reads; i++ {
		if _, ok := tree.HashFor(sample[i%sampleN]); !ok {
			t.Fatal("sampled id missing")
		}
	}
	getNs := float64(time.Since(t0).Nanoseconds()) / reads
	runtime.ReadMemStats(&ms1)
	getAllocs := float64(ms1.Mallocs-ms0.Mallocs) / reads

	// Updates: a chain, each version derived from the last and only the last kept, as the live
	// tree is.
	const updates = 100_000
	var h codec.Hash
	runtime.ReadMemStats(&ms0)
	t0 = time.Now()
	cur := tree
	for i := 0; i < updates; i++ {
		h.Bytes[0], h.Bytes[1], h.Bytes[2] = byte(i), byte(i>>8), byte(i>>16)
		if cur, err = cur.With(sample[i%sampleN], h); err != nil {
			t.Fatal(err)
		}
	}
	withNs := float64(time.Since(t0).Nanoseconds()) / updates
	runtime.ReadMemStats(&ms1)
	withAllocs := float64(ms1.Mallocs-ms0.Mallocs) / updates
	withBytes := float64(ms1.TotalAlloc-ms0.TotalAlloc) / updates

	// Deletes and re-inserts of existing documents (a delete chain, then the same ids back).
	const dels = 20_000
	t0 = time.Now()
	del := cur
	for i := 0; i < dels; i++ {
		if del, err = del.Without(sample[i]); err != nil {
			t.Fatal(err)
		}
	}
	withoutNs := float64(time.Since(t0).Nanoseconds()) / dels
	if os.Getenv("KDB_TREE_SCALE_DEBUG") != "" {
		t0 = time.Now()
		d2 := tree
		for i := 0; i < dels; i++ {
			if d2, err = d2.Without(sample[i]); err != nil {
				t.Fatal(err)
			}
		}
		t.Logf("without on the opened tree: %.0fns", float64(time.Since(t0).Nanoseconds())/dels)
		runtime.KeepAlive(d2)
		d2 = DocumentTree{}
		runtime.GC()
		time.Sleep(200 * time.Millisecond)
		t0 = time.Now()
		d3 := cur
		for i := 0; i < dels; i++ {
			if d3, err = d3.Without(sample[i]); err != nil {
				t.Fatal(err)
			}
		}
		t.Logf("without on the updated tree after a settle: %.0fns", float64(time.Since(t0).Nanoseconds())/dels)
		runtime.KeepAlive(d3)
	}
	del = DocumentTree{}

	// Keyset pages of 100 from random positions.
	const pages, pageLen = 2000, 100
	t0 = time.Now()
	for i := 0; i < pages; i++ {
		got := 0
		cur.WalkAfter(sample[(i*7919)%sampleN], func(codec.UUID, codec.Hash) bool {
			got++
			return got < pageLen
		})
	}
	pageUs := float64(time.Since(t0).Nanoseconds()) / pages / 1e3

	t0 = time.Now()
	walked := 0
	cur.WalkOrdered(func(codec.UUID, codec.Hash) bool { walked++; return true })
	walkTime := time.Since(t0)
	if walked != n {
		t.Fatalf("walked %d of %d", walked, n)
	}

	tree = DocumentTree{}
	heap2, obj2 := settledHeap()
	heap2, obj2 = heap2+sortedBytes, obj2+1
	gcAfter := gcTime()
	runtime.KeepAlive(cur)

	mb := func(b uint64) float64 { return float64(b) / (1 << 20) }
	t.Logf("n=%d open=%v retained=%.1fMB (%.1f B/doc) objects=%d (%.2f/doc) gc(empty)=%v gc(live)=%v",
		n, openTime.Round(time.Millisecond), mb(heap1-heap0), float64(heap1-heap0)/float64(n),
		obj1-obj0, float64(obj1-obj0)/float64(n), gcEmpty.Round(10*time.Microsecond), gcLive.Round(10*time.Microsecond))
	t.Logf("n=%d get=%.0fns (%.2f allocs) with=%.0fns (%.1f allocs, %.0f B) without=%.0fns page100=%.1fus walk=%v",
		n, getNs, getAllocs, withNs, withAllocs, withBytes, withoutNs, pageUs, walkTime.Round(time.Millisecond))
	t.Logf("n=%d after %d updates: retained=%.1fMB (%.1f B/doc) objects=%d gc=%v",
		n, updates, mb(heap2-heap0), float64(heap2-heap0)/float64(n), obj2-obj0, gcAfter.Round(10*time.Microsecond))
	fmt.Fprintf(os.Stderr, "TREESCALE n=%d open_ms=%.0f bpd=%.1f objpd=%.2f gc_empty_ms=%.2f gc_live_ms=%.2f get_ns=%.0f with_ns=%.0f with_allocs=%.1f with_B=%.0f without_ns=%.0f page_us=%.1f walk_ms=%.0f bpd_after=%.1f gc_after_ms=%.2f\n",
		n, openTime.Seconds()*1e3, float64(heap1-heap0)/float64(n), float64(obj1-obj0)/float64(n),
		gcEmpty.Seconds()*1e3, gcLive.Seconds()*1e3, getNs, withNs, withAllocs, withBytes, withoutNs, pageUs,
		walkTime.Seconds()*1e3, float64(heap2-heap0)/float64(n), gcAfter.Seconds()*1e3)
}
