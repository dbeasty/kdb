package stores_test

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/index"
	"github.com/limidus/kdb/go/kdb/index/stores"
)

// TestIndexMemoryProfile measures what each index kind retains as a namespace takes writes:
// retained heap after GC, bytes per document and per write, heap objects (the GC's pointer
// cost), scannable heap bytes, and how long a full GC takes with the index resident.
//
// It builds a commit DAG of the same shape twice - once with no index (the control) and once
// feeding one index - so what the DAG itself holds is subtracted out. Every commit carries
// writesPerCommit document writes, and every commit is followed by a head read, the way a
// served namespace interleaves them.
//
// Sizes are large, so it runs only with KDB_INDEX_MEMPROFILE=1. KDB_INDEX_MEMPROFILE_SCALE
// scales the document and write counts (default 1).
func TestIndexMemoryProfile(t *testing.T) {
	if os.Getenv("KDB_INDEX_MEMPROFILE") != "1" {
		t.Skip("set KDB_INDEX_MEMPROFILE=1 to run the index memory profile")
	}
	scale := 1.0
	if s := os.Getenv("KDB_INDEX_MEMPROFILE_SCALE"); s != "" {
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			t.Fatal(err)
		}
		scale = f
	}
	sz := func(n int) int { return int(float64(n) * scale) }
	cases := []memCase{
		{kind: index.IndexTypeHash, docs: sz(100_000), writes: 0},
		{kind: index.IndexTypeHash, docs: sz(100_000), writes: sz(250_000)},
		{kind: index.IndexTypeHash, docs: sz(100_000), writes: sz(1_000_000)},
		{kind: index.IndexTypeBTree, docs: sz(100_000), writes: 0},
		{kind: index.IndexTypeBTree, docs: sz(100_000), writes: sz(250_000)},
		{kind: index.IndexTypeBTree, docs: sz(100_000), writes: sz(1_000_000)},
		{kind: index.IndexTypeFullText, docs: sz(5_000), writes: 0},
		{kind: index.IndexTypeFullText, docs: sz(5_000), writes: sz(25_000)},
		{kind: index.IndexTypeFullText, docs: sz(5_000), writes: sz(100_000)},
		{kind: index.IndexTypeVector, docs: sz(2_000), writes: 0},
		{kind: index.IndexTypeVector, docs: sz(2_000), writes: sz(10_000)},
		{kind: index.IndexTypeVector, docs: sz(2_000), writes: sz(40_000)},
	}
	if only := os.Getenv("KDB_INDEX_MEMPROFILE_KIND"); only != "" {
		var kept []memCase
		for _, c := range cases {
			if strings.EqualFold(c.kind.String(), only) {
				kept = append(kept, c)
			}
		}
		cases = kept
	}
	t.Logf("| index | docs | writes | retained MB | B/doc | B/write | heap objects | scannable MB | full GC ms | build s |")
	t.Logf("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
	for _, c := range cases {
		r := runMemCase(t, c)
		perWrite := "-"
		if c.writes > 0 {
			perWrite = fmt.Sprintf("%.0f", float64(r.retained)/float64(c.docs+c.writes))
		}
		t.Logf("| %s | %d | %d | %.1f | %.0f | %s | %d | %.1f | %.2f | %.1f |",
			c.kind, c.docs, c.writes, float64(r.retained)/1e6, float64(r.retained)/float64(c.docs), perWrite,
			r.objects, float64(r.scannable)/1e6, float64(r.gc)/1e6, r.build.Seconds())
	}
}

type memCase struct {
	kind   index.IndexType
	docs   int
	writes int
}

type memResult struct {
	retained  int64
	objects   int64
	scannable int64
	gc        time.Duration
	build     time.Duration
}

const writesPerCommit = 10

func runMemCase(t *testing.T, c memCase) memResult {
	t.Helper()
	// Control: the same DAG with no index.
	ctrlHeap, ctrlObjs, ctrlScan, keepCtrl := buildMemWorkload(t, c, false)
	runtime.KeepAlive(keepCtrl)
	keepCtrl = nil
	start := time.Now()
	heap, objs, scan, keep := buildMemWorkload(t, c, true)
	build := time.Since(start)
	gc := minGC()
	runtime.KeepAlive(keep)
	return memResult{
		retained: heap - ctrlHeap, objects: objs - ctrlObjs, scannable: scan - ctrlScan,
		gc: gc, build: build,
	}
}

// minGC is the fastest of three forced collections: the mark cost with the heap as it is.
func minGC() time.Duration {
	best := time.Duration(1 << 62)
	for i := 0; i < 3; i++ {
		s := time.Now()
		runtime.GC()
		if d := time.Since(s); d < best {
			best = d
		}
	}
	return best
}

func settledHeap() (heap, objects, scannable int64) {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	sample := []metrics.Sample{{Name: "/gc/scan/heap:bytes"}}
	metrics.Read(sample)
	var scan int64
	if sample[0].Value.Kind() == metrics.KindUint64 {
		scan = int64(sample[0].Value.Uint64())
	}
	return int64(ms.HeapAlloc), int64(ms.HeapObjects), scan
}

type memKeep struct {
	d     *dag.InMemoryCommitDag
	store index.Store
}

var memWords = func() []string {
	out := make([]string, 2000)
	for i := range out {
		out[i] = fmt.Sprintf("w%04dx", i)
	}
	return out
}()

func memDoc(kind index.IndexType, rng *rand.Rand, docs int) string {
	switch kind {
	case index.IndexTypeHash:
		return fmt.Sprintf(`{"k":"v%d"}`, rng.Intn(docs/10+1))
	case index.IndexTypeBTree:
		return fmt.Sprintf(`{"k":%d}`, rng.Intn(docs))
	case index.IndexTypeFullText:
		var b strings.Builder
		b.WriteString(`{"title":"`)
		for i := 0; i < 20; i++ {
			if i > 0 {
				b.WriteByte(' ')
			}
			b.WriteString(memWords[rng.Intn(len(memWords))])
		}
		b.WriteString(`"}`)
		return b.String()
	default:
		var b strings.Builder
		b.WriteString(`{"embedding":[`)
		for i := 0; i < memVecDims; i++ {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(strconv.FormatFloat(rng.Float64()*2-1, 'f', 4, 64))
		}
		b.WriteString(`]}`)
		return b.String()
	}
}

const memVecDims = 32

func memDesc(kind index.IndexType) index.Descriptor {
	id, _ := codec.UUIDFromString("00000000-0000-4000-8000-0000000009" + fmt.Sprintf("%02d", int(kind)%100))
	desc := index.Descriptor{IndexID: id, NamespaceID: "ns/mem", Type: kind}
	switch kind {
	case index.IndexTypeFullText:
		desc.FieldName, desc.Fields = "title", []string{"title"}
	case index.IndexTypeVector:
		desc.FieldName, desc.Fields = "embedding", []string{"embedding"}
		desc.Options = map[string]string{index.OptionDimensions: strconv.Itoa(memVecDims)}
	default:
		desc.FieldName, desc.Fields = "k", []string{"k"}
	}
	return desc
}

// buildMemWorkload inserts c.docs documents and then makes c.writes rewrites of random
// documents, writesPerCommit per commit, reading at head after every commit. With withIndex
// false it builds only the DAG. It returns the settled heap, object count and scannable bytes.
func buildMemWorkload(t *testing.T, c memCase, withIndex bool) (int64, int64, int64, *memKeep) {
	t.Helper()
	runtime.GC()
	d, err := dag.NewInMemoryCommitDag("ns/mem")
	if err != nil {
		t.Fatal(err)
	}
	keep := &memKeep{d: d}
	var ds index.DocumentStore
	if withIndex {
		s, err := stores.NewFactory(d, memOpts).Create(memDesc(c.kind))
		if err != nil {
			t.Fatal(err)
		}
		ds = s.(index.DocumentStore)
		keep.store = s
	}
	rng := rand.New(rand.NewSource(42))
	ids := make([]codec.UUID, c.docs)
	for i := range ids {
		ids[i] = codec.UUID{MSB: rng.Int63(), LSB: rng.Int63()}
	}
	head, _ := d.Head()
	author := codec.UUID{MSB: 1, LSB: 2}
	commit := func() codec.Hash {
		tx := document.Transaction{
			ID: codec.UUID{MSB: rng.Int63(), LSB: rng.Int63()}, BaseVersion: head,
			Timestamp: codec.TimestampNow(), AuthorNodeID: author,
		}
		cm, err := d.AppendCommit(tx, head, document.EmptyDocumentTree(), nil, "")
		if err != nil {
			t.Fatal(err)
		}
		head = cm.Hash
		return head
	}
	probe := memDoc(c.kind, rand.New(rand.NewSource(7)), c.docs)
	read := func() {
		if ds == nil {
			return
		}
		switch c.kind {
		case index.IndexTypeHash:
			_, _ = ds.Lookup(index.StringKey{Value: "v1"}, nil)
		case index.IndexTypeBTree:
			_, _ = ds.Range(index.Int64Key{Value: 10}, index.Int64Key{Value: 20}, nil, 0, true)
		case index.IndexTypeFullText:
			_, _ = ds.Search(memWords[1]+" "+memWords[2], nil, 10)
		default:
			_ = probe
			q := make([]float32, memVecDims)
			q[0] = 1
			_, _ = ds.NearestNeighbours(q, 10, nil)
		}
	}
	total := c.docs + c.writes
	for i := 0; i < total; i += writesPerCommit {
		h := commit()
		for j := i; j < i+writesPerCommit && j < total; j++ {
			doc := ids[j%c.docs]
			if j >= c.docs {
				doc = ids[rng.Intn(c.docs)]
			}
			text := memDoc(c.kind, rng, c.docs)
			if ds != nil {
				if err := ds.PutDocument(doc, h, text); err != nil {
					t.Fatal(err)
				}
			}
		}
		// Reads interleave with the writes: every 10 commits for the keyed kinds, every 100 for
		// the scored ones. The baseline sorts every key per range and rebuilds a whole view per
		// search, so reading after every commit would make its runs take hours.
		if n := i / writesPerCommit; ((c.kind == index.IndexTypeHash || c.kind == index.IndexTypeBTree) && n%10 == 0) || n%100 == 0 {
			read()
		}
	}
	read()
	heap, objs, scan := settledHeap()
	return heap, objs, scan, keep
}

// memOpts forces exact vector search: on main the HNSW graph panics on the second put after a
// search (fixed on this branch), so an A/B over this workload has to keep the graph unbuilt.
var memOpts = func() stores.Options {
	var o stores.Options
	o.Vector.ExactThreshold = 1 << 30
	return o
}()
