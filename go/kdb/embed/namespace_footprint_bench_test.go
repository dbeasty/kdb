package embed

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"testing"

	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/schema"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// BenchmarkNamespaceFootprint is the many-small-namespaces case zolik reported against v0.8.0
// (docs/kdb-issues-from-zolik-0.8.md in zolik): one namespace per match, thousands open at once,
// a few dozen commits each. v0.8.0 gave every namespace a 1 MiB commit-graph chunk on its first
// commit, ~1.5 MB of heap per namespace against ~0.5 MB on v0.7.1; PR #295 (v0.8.1) starts that
// chunk at 4 KiB. This holds the line on it.
//
// Each iteration opens KDB_BENCH_NAMESPACES namespaces (default 5,000) on one file host, writes
// KDB_BENCH_COMMITS commits to each (default 20: 4 documents, 5 versions each) and keeps them all
// open, then reports live heap per namespace after a GC, once per graph memory mode: the default
// (graph.memory=kdb with the mapped graph file), kdb without the file, and the Go map
// (graph.memory=go, which needs the file off) - the opt-out zolik shipped with.
//
//	go test ./embed -run '^$' -bench NamespaceFootprint -benchtime 1x
func BenchmarkNamespaceFootprint(b *testing.B) {
	n := benchEnvInt("KDB_BENCH_NAMESPACES", 5000)
	commits := benchEnvInt("KDB_BENCH_COMMITS", 20)
	for _, mode := range []struct {
		name   string
		file   bool
		memory dag.GraphMemory
	}{
		{"graph=kdb+file", true, dag.GraphMemoryKDB},
		{"graph=kdb", false, dag.GraphMemoryKDB},
		{"graph=go", false, dag.GraphMemoryGo},
	} {
		b.Run(mode.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				opts := FileRuntimeOptionsFromEnv()
				opts.Storage.Graph.FileEnabled = mode.file
				opts.Storage.Graph.Memory = mode.memory
				// Heap is the figure, not durability: F_FULLFSYNC per commit made 5,000 namespaces
				// take over an hour a mode on macOS.
				opts.Storage.SyncMode = storio.SyncModeFast
				live := namespaceFootprint(b, opts, n, commits)
				b.ReportMetric(float64(live)/float64(n), "heap-B/namespace")
			}
			b.ReportMetric(0, "ns/op") // one iteration is minutes of setup, not the figure
		})
	}
}

// namespaceFootprint opens n namespaces with commits commits each on a fresh host and returns the
// live heap they hold, all still open, after a GC.
func namespaceFootprint(b *testing.B, opts FileRuntimeOptions, n, commits int) int64 {
	b.Helper()
	b.StopTimer()
	dir := b.TempDir()
	host, err := OpenFileHost(dir, opts)
	if err != nil {
		b.Fatal(err)
	}
	defer host.Close()
	base := liveHeap()
	b.StartTimer()
	const docs = 4
	for i := 0; i < n; i++ {
		ns := fmt.Sprintf("zolik/match-%05d", i)
		rt, err := host.Namespace(CatalogFromNamespace(ns), ns, schema.None())
		if err != nil {
			b.Fatalf("open %s: %v", ns, err)
		}
		for c := 0; c < commits; c++ {
			doc := fmt.Sprintf(`{"id":"seat-%d","match":%d,"move":%d,"score":%d}`, c%docs, i, c, c*7)
			if _, err := PutJSONDocument(rt, ns, doc); err != nil {
				b.Fatalf("commit %d to %s: %v", c, ns, err)
			}
		}
	}
	b.StopTimer()
	live := liveHeap() - base
	b.StartTimer()
	return live
}

func liveHeap() int64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.HeapAlloc)
}

func benchEnvInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}
