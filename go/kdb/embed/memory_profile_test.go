package embed

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/mmapfile"
)

// TestMemoryProfile measures what a namespace costs in memory across its ordinary life - startup,
// point reads, writes, the checkpoint written at close - one phase per process, so a phase's peak
// is its own and not the build's. It is a measurement, not a correctness test, and is skipped
// unless KDB_MEMPROFILE_PHASE is set. docs/kdb-mmap-plan.md has the numbers it produced.
//
//	export KDB_MEMPROFILE_DIR=/tmp/kdbmem KDB_MEMPROFILE_GRAPH=1   # GRAPH=0 for the heap-only graph
//	for p in build open reads writes checkpoint; do
//	  KDB_MEMPROFILE_PHASE=$p go test ./kdb/embed -run TestMemoryProfile -v -timeout 60m
//	done
//
// KDB_MEMPROFILE_DOCS and KDB_MEMPROFILE_COMMITS size the namespace (default 100,000 documents,
// 500,000 commits: every document inserted once, then updated at random). KDB_MEMPROFILE_BATCH=n
// builds it n writes to a commit (putJSONBatch) - the same documents and writes in a fraction of
// the commits, for namespaces too large to build one write at a time (the 10M-document run);
// COMMITS then counts writes, and the namespace holds about COMMITS/n commits.
func TestMemoryProfile(t *testing.T) {
	phase := os.Getenv("KDB_MEMPROFILE_PHASE")
	if phase == "" {
		t.Skip("set KDB_MEMPROFILE_PHASE to run")
	}
	dir := os.Getenv("KDB_MEMPROFILE_DIR")
	if dir == "" {
		t.Fatal("KDB_MEMPROFILE_DIR is required")
	}
	docs := envOr("KDB_MEMPROFILE_DOCS", 100_000)
	commits := envOr("KDB_MEMPROFILE_COMMITS", 500_000)
	const ns = "mem/profile"
	opts := FileRuntimeOptions{}
	opts.Storage.Durability = storage.DurabilityAsync // memory, not fsync, is under test
	if os.Getenv("KDB_MEMPROFILE_GRAPH") == "1" {
		opts.Storage.Graph = dag.GraphSettings{FileEnabled: true}
	}
	// KDB_MEMPROFILE_TREE=1 pages the live tree to segment files (StorageOptions.TreeFile), with
	// KDB_MEMPROFILE_TREE_RESIDENT bytes of resident overlay (default when unset).
	opts.Storage.TreeFile = os.Getenv("KDB_MEMPROFILE_TREE") == "1"
	opts.Storage.TreeResidentBytes = int64(envOr("KDB_MEMPROFILE_TREE_RESIDENT", 0))
	// KDB_MEMPROFILE_BOUNDED=1 lets live versions be evicted (StorageOptions.BoundedLiveBodies).
	opts.Storage.BoundedLiveBodies = os.Getenv("KDB_MEMPROFILE_BOUNDED") == "1"
	if m := os.Getenv("KDB_MEMPROFILE_MEMORY"); m != "" {
		opts.Storage.Graph.Memory = dag.GraphMemory(m) // "go" or "kdb"; ignored by builds without it
	}
	docID := func(i int) string { return fmt.Sprintf("00000000-0000-4000-8000-%012d", i) }
	body := func(i, rev int) string {
		b, _ := json.Marshal(map[string]any{"id": docID(i), "rev": rev, "name": fmt.Sprintf("user-%d", i), "score": rev % 997})
		return string(b)
	}
	put := func(rt *EmbeddedKdbRuntime, i, rev int) {
		if _, err := PutJSONDocument(rt, ns, body(i, rev)); err != nil {
			t.Fatal(err)
		}
	}
	batch := envOr("KDB_MEMPROFILE_BATCH", 1)
	open := func() *EmbeddedKdbRuntime {
		rt, err := OpenFileRuntimeWithOptions(dir, "mem", ns, schema.None(), opts)
		if err != nil {
			t.Fatal(err)
		}
		return rt
	}

	switch phase {
	case "build":
		os.RemoveAll(dir)
		rt := open()
		start := time.Now()
		rng := rand.New(rand.NewSource(1))
		pending := make([]string, 0, batch)
		flush := func() {
			if len(pending) == 0 {
				return
			}
			if err := putJSONBatch(rt, ns, pending); err != nil {
				t.Fatal(err)
			}
			pending = pending[:0]
		}
		for c := 0; c < commits; c++ {
			i, rev := c, 0
			if c >= docs {
				i, rev = rng.Intn(docs), c
			}
			if batch <= 1 {
				put(rt, i, rev)
			} else if pending = append(pending, body(i, rev)); len(pending) == batch {
				flush()
			}
			// Maintenance as a running server's loop would: every 50,000 commits, or for a batched
			// build every 500,000 writes, so tables are compacted as they accumulate rather than all
			// at close.
			if every := 50_000 * min(batch, 10); c%every == every-1 {
				flush()
				m := time.Now()
				rt.Maintain() // what a running server's maintenance loop does
				if d := time.Since(m); d > time.Second {
					t.Logf("  maintenance at %d writes took %v", c+1, d.Round(time.Millisecond))
				}
			}
			if c > 0 && c%1_000_000 == 0 {
				t.Logf("  %d writes in %v", c, time.Since(start).Round(time.Second))
			}
		}
		flush()
		t.Logf("  %d writes in %v; maintenance and close follow", commits, time.Since(start).Round(time.Second))
		m := time.Now()
		rt.Maintain()
		c := time.Now()
		rt.Close()
		t.Logf("  final maintenance %v, close %v", c.Sub(m).Round(time.Millisecond), time.Since(c).Round(time.Millisecond))
		t.Logf("built %d documents, %d writes (%d to a commit) in %v", docs, commits, batch, time.Since(start).Round(time.Second))

	case "open":
		peak := sampleHeap()
		start := time.Now()
		rt := open()
		took := time.Since(start)
		openPeak := peak.stop()
		report(t, "open", fmt.Sprintf("took=%v peakHeapDuringOpen=%s", took.Round(time.Millisecond), mb(openPeak)))
		if dir := os.Getenv("KDB_MEMPROFILE_PPROF"); dir != "" { // what the live heap after open is made of
			runtime.GC()
			f, err := os.Create(filepath.Join(dir, "heap-after-open.pb.gz"))
			if err != nil {
				t.Fatal(err)
			}
			pprof.Lookup("heap").WriteTo(f, 0)
			f.Close()
			cpu, err := os.Create(filepath.Join(dir, "open-cpu.pb.gz"))
			if err != nil {
				t.Fatal(err)
			}
			rt.Close()
			pprof.StartCPUProfile(cpu) // a second open, profiled, to say where the time goes
			rt = open()
			pprof.StopCPUProfile()
			cpu.Close()
		}
		rt.Close()

	case "reads":
		rt := open()
		baseline := liveHeapBytes()
		head := headTree(t, rt)
		rng := rand.New(rand.NewSource(2))
		const reads = 200_000
		peak := sampleHeap()
		start := time.Now()
		for i := 0; i < reads; i++ {
			id, _ := codec.UUIDFromString(docID(rng.Intn(docs)))
			if d, err := rt.Storage.GetDocument(ns, id, head); err != nil || d == nil {
				t.Fatalf("read %d: %v, found=%v", i, err, d != nil)
			}
		}
		took := time.Since(start)
		report(t, "reads", fmt.Sprintf("%d point reads in %v, heap after open %s, peakHeapDuringReads=%s", reads, took.Round(time.Millisecond), mb(baseline), mb(peak.stop())))
		rt.Close()

	case "fullread":
		// Every live document read once, so every live body is resident - the state a server
		// reaches after serving reads for a while - and then what that costs the collector.
		rt := open()
		afterOpen := liveHeapBytes()
		openGC := forcedGCCost(5)
		head := headTree(t, rt)
		var cpuBefore, cpuAfter [2]float64
		cpuBefore = gcCPU()
		start := time.Now()
		for i := 0; i < docs; i++ {
			id, _ := codec.UUIDFromString(docID(i))
			if d, err := rt.Storage.GetDocument(ns, id, head); err != nil || d == nil {
				t.Fatalf("read %d: %v, found=%v", i, err, d != nil)
			}
		}
		took := time.Since(start)
		cpuAfter = gcCPU()
		afterRead := liveHeapBytes()
		readGC := forcedGCCost(5)
		report(t, "fullread", fmt.Sprintf("%d reads in %v; heap after open %s, after full read %s (+%.0f B/doc); "+
			"forced GC after open %v (mark CPU %v), after full read %v (mark CPU %v); GC CPU during the pass %.0f ms of %.0f ms",
			docs, took.Round(time.Millisecond), mb(afterOpen), mb(afterRead), float64(afterRead-afterOpen)/float64(docs),
			openGC.wall, openGC.markCPU, readGC.wall, readGC.markCPU,
			(cpuAfter[0]-cpuBefore[0])*1e3, (cpuAfter[1]-cpuBefore[1])*1e3))
		logMemoryBreakdown(t, rt)
		if dir := os.Getenv("KDB_MEMPROFILE_PPROF"); dir != "" { // what the full-read heap is made of
			f, err := os.Create(filepath.Join(dir, "heap-after-fullread.pb.gz"))
			if err != nil {
				t.Fatal(err)
			}
			pprof.Lookup("heap").WriteTo(f, 0)
			f.Close()
		}
		rt.Close()

	case "writes":
		rt := open()
		baseline := liveHeapBytes()
		rng := rand.New(rand.NewSource(3))
		const writes = 20_000
		peak := sampleHeap()
		start := time.Now()
		for i := 0; i < writes; i++ {
			put(rt, rng.Intn(docs), commits+i)
		}
		took := time.Since(start)
		report(t, "writes", fmt.Sprintf("%d updates in %v, heap after open %s, peakHeapDuringWrites=%s", writes, took.Round(time.Millisecond), mb(baseline), mb(peak.stop())))
		rt.Close()

	case "checkpoint":
		rt := open()
		pprofDir := os.Getenv("KDB_MEMPROFILE_PPROF") // allocation profiles bracketing the close
		writeAllocs := func(name string) {
			if pprofDir == "" {
				return
			}
			f, err := os.Create(filepath.Join(pprofDir, name))
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			pprof.Lookup("allocs").WriteTo(f, 0)
		}
		writeAllocs("before.pb.gz")
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		peak := sampleHeap()
		start := time.Now()
		rt.Close() // seals the log and writes the checkpoint
		took := time.Since(start)
		writeAllocs("after.pb.gz")
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		report(t, "checkpoint", fmt.Sprintf("close+checkpoint took %v, allocated %s, peakHeapDuringClose=%s",
			took.Round(time.Millisecond), mb(int64(after.TotalAlloc-before.TotalAlloc)), mb(peak.stop())))

	default:
		t.Fatalf("unknown phase %q", phase)
	}
}

// gcCost is what a forced, otherwise idle collection costs: its median wall time and the mark
// CPU it burned (dedicated + idle + assist workers), per collection.
type gcCost struct{ wall, markCPU time.Duration }

func forcedGCCost(n int) gcCost {
	walls := make([]time.Duration, n)
	marks := []metrics.Sample{
		{Name: "/cpu/classes/gc/mark/dedicated:cpu-seconds"},
		{Name: "/cpu/classes/gc/mark/idle:cpu-seconds"},
		{Name: "/cpu/classes/gc/mark/assist:cpu-seconds"},
	}
	sum := func() float64 {
		metrics.Read(marks)
		var s float64
		for _, m := range marks {
			s += m.Value.Float64()
		}
		return s
	}
	runtime.GC()
	before := sum()
	for i := range walls {
		start := time.Now()
		runtime.GC()
		walls[i] = time.Since(start)
	}
	after := sum()
	for i := 1; i < len(walls); i++ { // insertion sort; n is tiny
		for j := i; j > 0 && walls[j] < walls[j-1]; j-- {
			walls[j], walls[j-1] = walls[j-1], walls[j]
		}
	}
	return gcCost{
		wall:    walls[len(walls)/2].Round(10 * time.Microsecond),
		markCPU: time.Duration((after - before) / float64(n) * 1e9).Round(10 * time.Microsecond),
	}
}

// gcCPU reads the process's cumulative GC CPU and total CPU, in seconds. The runtime only
// refreshes these at GC boundaries, which is precise enough over a pass with many collections.
func gcCPU() [2]float64 {
	s := []metrics.Sample{{Name: "/cpu/classes/gc/total:cpu-seconds"}, {Name: "/cpu/classes/total:cpu-seconds"}}
	metrics.Read(s)
	return [2]float64{s[0].Value.Float64(), s[1].Value.Float64()}
}

// headTree is the document tree hash of the head commit - what Storage.GetDocument reads at. The
// reads phase used to pass the head *commit* hash, which names no tree, so every read returned
// "not found" without an error and the phase measured nothing.
func headTree(t *testing.T, rt *EmbeddedKdbRuntime) codec.Hash {
	t.Helper()
	_, c, ok, err := rt.DAG.HeadCommit()
	if err != nil || !ok {
		t.Fatalf("head commit: ok=%v err=%v", ok, err)
	}
	return c.DocumentTreeHash
}

func envOr(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

func mb(b int64) string { return fmt.Sprintf("%.1fMB", float64(b)/1e6) }

func liveHeapBytes() int64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.HeapAlloc)
}

// report logs live heap after a GC, current RSS, and the process's peak RSS.
func report(t *testing.T, phase, detail string) {
	t.Helper()
	live := liveHeapBytes()
	t.Logf("PHASE %-10s liveHeap=%s rss=%s peakRSS=%s mapped=%s | %s", phase, mb(live), mb(currentRSS()), mb(peakRSS()), mb(mmapfile.MappedBytes()), detail)
}

func currentRSS() int64 {
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil {
		return -1
	}
	kb, _ := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	return kb * 1024
}

// heapSampler tracks the largest live-object heap seen while it runs.
type heapSampler struct {
	max  atomic.Int64
	done chan struct{}
}

func sampleHeap() *heapSampler {
	s := &heapSampler{done: make(chan struct{})}
	go func() {
		sample := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
		tick := time.NewTicker(2 * time.Millisecond)
		defer tick.Stop()
		for {
			metrics.Read(sample)
			if v := int64(sample[0].Value.Uint64()); v > s.max.Load() {
				s.max.Store(v)
			}
			select {
			case <-s.done:
				return
			case <-tick.C:
			}
		}
	}()
	return s
}

func (s *heapSampler) stop() int64 {
	close(s.done)
	time.Sleep(5 * time.Millisecond)
	return s.max.Load()
}
