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
// 500,000 commits: every document inserted once, then updated at random).
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
	if m := os.Getenv("KDB_MEMPROFILE_MEMORY"); m != "" {
		opts.Storage.Graph.Memory = dag.GraphMemory(m) // "go" or "kdb"; ignored by builds without it
	}
	docID := func(i int) string { return fmt.Sprintf("00000000-0000-4000-8000-%012d", i) }
	put := func(rt *EmbeddedKdbRuntime, i, rev int) {
		b, _ := json.Marshal(map[string]any{"id": docID(i), "rev": rev, "name": fmt.Sprintf("user-%d", i), "score": rev % 997})
		if _, err := PutJSONDocument(rt, ns, string(b)); err != nil {
			t.Fatal(err)
		}
	}
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
		for c := 0; c < commits; c++ {
			if c < docs {
				put(rt, c, 0)
			} else {
				put(rt, rng.Intn(docs), c)
			}
			if c%50_000 == 49_999 {
				rt.Maintain() // what a running server's maintenance loop does
			}
		}
		rt.Maintain()
		rt.Close()
		t.Logf("built %d documents, %d commits in %v", docs, commits, time.Since(start).Round(time.Second))

	case "open":
		peak := sampleHeap()
		start := time.Now()
		rt := open()
		took := time.Since(start)
		openPeak := peak.stop()
		report(t, "open", fmt.Sprintf("took=%v peakHeapDuringOpen=%s", took.Round(time.Millisecond), mb(openPeak)))
		rt.Close()

	case "reads":
		rt := open()
		baseline := liveHeapBytes()
		head, err := rt.DAG.Head()
		if err != nil {
			t.Fatal(err)
		}
		rng := rand.New(rand.NewSource(2))
		const reads = 200_000
		peak := sampleHeap()
		start := time.Now()
		for i := 0; i < reads; i++ {
			id, _ := codec.UUIDFromString(docID(rng.Intn(docs)))
			if _, err := rt.Storage.GetDocument(ns, id, head); err != nil {
				t.Fatal(err)
			}
		}
		took := time.Since(start)
		report(t, "reads", fmt.Sprintf("%d point reads in %v, heap after open %s, peakHeapDuringReads=%s", reads, took.Round(time.Millisecond), mb(baseline), mb(peak.stop())))
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
	t.Logf("PHASE %-10s liveHeap=%s rss=%s peakRSS=%s | %s", phase, mb(live), mb(currentRSS()), mb(peakRSS()), detail)
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
