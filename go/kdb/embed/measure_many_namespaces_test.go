package embed

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/schema"
)

// TestMeasureManyNamespaces (Phase 16.6, G6): what one small namespace costs on a file host - to
// open, in memory, in file descriptors and on disk - at increasing counts, and what reopening one
// after it was closed costs. KDB_MEASURE=1 to run; KDB_MEASURE_NS=1000,10000 to choose the counts.
func TestMeasureManyNamespaces(t *testing.T) {
	if os.Getenv("KDB_MEASURE") == "" {
		t.Skip("measurement: KDB_MEASURE=1")
	}
	counts := []int{1000, 5000}
	if v := os.Getenv("KDB_MEASURE_NS"); v != "" {
		counts = nil
		for _, f := range strings.Split(v, ",") {
			n, _ := strconv.Atoi(f)
			counts = append(counts, n)
		}
	}
	for _, n := range counts {
		measureNamespaces(t, n)
	}
}

func measureNamespaces(t *testing.T, n int) {
	if os.Getenv("KDB_MEASURE_HEAPPROF") != "" {
		runtime.MemProfileRate = 64
	}
	dir := t.TempDir()
	host, err := OpenFileHost(dir, FileRuntimeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	runtime.GC()
	base := heapInuse()
	baseRSS, baseFD := rssKB(), fdCount()
	start := time.Now()
	var slowest time.Duration
	for i := 0; i < n; i++ {
		ns := fmt.Sprintf("app/u/%d", i)
		t0 := time.Now()
		rt, err := host.Namespace(CatalogFromNamespace(ns), ns, schema.None())
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		if _, err := PutJSONDocument(rt, ns, fmt.Sprintf(`{"id":"profile","user":%d,"name":"player %d"}`, i, i)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if d := time.Since(t0); d > slowest {
			slowest = d
		}
	}
	elapsed := time.Since(start)
	runtime.GC()
	heap := heapInuse() - base
	rss, fds := rssKB()-baseRSS, fdCount()-baseFD
	disk, onDisk := diskKB(dir), duKB(dir)
	t.Logf("G6 %6d namespaces open: open+write %6.2f ms/ns (slowest %v), heap %6.1f KB/ns, rss %6.1f KB/ns, fds %5.2f/ns, disk %6.1f KB/ns logical, %6.1f KB/ns allocated",
		n, float64(elapsed.Microseconds())/1000/float64(n), slowest.Round(time.Millisecond),
		float64(heap)/1024/float64(n), float64(rss)/float64(n), float64(fds)/float64(n), float64(disk)/float64(n), float64(onDisk)/float64(n))

	// Close them all, then reopen a sample cold: the cost idle close trades for.
	closeStart := time.Now()
	for i := 0; i < n; i++ {
		if err := host.CloseNamespace(fmt.Sprintf("app/u/%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	closeEach := time.Since(closeStart) / time.Duration(n)
	runtime.GC()
	if path := os.Getenv("KDB_MEASURE_HEAPPROF"); path != "" {
		if f, err := os.Create(path); err == nil {
			_ = pprof.WriteHeapProfile(f)
			f.Close()
		}
	}
	after := heapInuse() - base
	goroutinesAfterClose := runtime.NumGoroutine()
	sample := min(n, 200)
	reopenStart := time.Now()
	for i := 0; i < sample; i++ {
		ns := fmt.Sprintf("app/u/%d", i*(n/sample))
		if _, err := host.Namespace(CatalogFromNamespace(ns), ns, schema.None()); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("G6 %6d namespaces closed: close %v/ns, heap left %6.1f KB/ns, fds %d, goroutines %d; cold reopen %6.2f ms/ns",
		n, closeEach.Round(time.Microsecond), float64(after)/1024/float64(n), fdCount()-baseFD, goroutinesAfterClose,
		float64(time.Since(reopenStart).Microseconds())/1000/float64(sample))
}

func heapInuse() int64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.HeapInuse)
}

func rssKB() int64 {
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil {
		return 0
	}
	v, _ := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	return v
}

func fdCount() int {
	// macOS's /dev/fd lists only a few descriptors; lsof sees them all.
	if out, err := exec.Command("lsof", "-n", "-P", "-p", strconv.Itoa(os.Getpid())).Output(); err == nil {
		return strings.Count(string(out), "\n") - 1
	}
	for _, d := range []string{"/proc/self/fd", "/dev/fd"} {
		if entries, err := os.ReadDir(d); err == nil {
			return len(entries)
		}
	}
	return 0
}

func diskKB(dir string) int64 {
	var total int64
	_ = filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total / 1024
}

// duKB is what the filesystem allocated under dir: blocks and directories, not only bytes.
func duKB(dir string) int64 {
	out, err := exec.Command("du", "-sk", dir).Output()
	if err != nil {
		return 0
	}
	v, _ := strconv.ParseInt(strings.Fields(string(out))[0], 10, 64)
	return v
}
