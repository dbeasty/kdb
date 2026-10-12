package server

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/index/stores"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/sql"
)

// The write cost of indexes on a disk-backed runtime: what a commit pays for keeping N indexes
// current, and what CREATE INDEX pays on an empty namespace. Snapshotting every index is not a
// write-path cost - it used to be, every 64 commits under the write gate, which made a commit's
// cost grow with (number of indexes x their total size).
//
// The measuring test only runs with KDB_INDEX_PERF=1 (timings mean nothing on a shared CI box);
// it prints docs/s and per-commit latency percentiles with and without indexes.

const writeCostIndexCount = 27

func writeCostFields() []string {
	out := make([]string, writeCostIndexCount)
	for i := range out {
		out[i] = fmt.Sprintf("f%02d", i)
	}
	return out
}

func writeCostDoc(i int) string {
	var b strings.Builder
	b.WriteString(`{"_c":"tasks"`)
	for j, f := range writeCostFields() {
		fmt.Fprintf(&b, `,"%s":"v%d-%d"`, f, j, i%(50+j))
	}
	b.WriteString("}")
	return b.String()
}

// openDiskIndexedRuntime opens a file-backed runtime (durability sync, the default) with its
// index registry wired up.
func openDiskIndexedRuntime(t testing.TB, dir string) (*KdbServerRuntime, *RegistryIndexProvider) {
	t.Helper()
	rt, err := embed.OpenFileRuntime(dir, "app", "app/data", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	srv := NewKdbServerRuntime(rt)
	p, err := srv.OpenIndexes(stores.Options{})
	if err != nil {
		t.Fatal(err)
	}
	return srv, p
}

func createWriteCostIndexes(t testing.TB, p *RegistryIndexProvider) time.Duration {
	t.Helper()
	start := time.Now()
	for i, f := range writeCostFields() {
		using := "HASH"
		if i%3 == 0 {
			using = "BTREE"
		}
		if err := p.CreateIndex(sql.StmtCreateIndex{
			Name: "by_" + f, Table: "docs", Fields: []sql.IndexField{{Path: f}}, Using: using,
		}, sql.QueryContext{NamespaceID: "app/data"}); err != nil {
			t.Fatalf("CREATE INDEX %s: %v", f, err)
		}
	}
	return time.Since(start)
}

type writeCostResult struct {
	docsPerSec float64
	p50, p99   time.Duration
}

func runSequentialWrites(t testing.TB, srv *KdbServerRuntime, n int) writeCostResult {
	t.Helper()
	lat := make([]time.Duration, 0, n)
	start := time.Now()
	for i := 0; i < n; i++ {
		id, err := codec.RandomUUID()
		if err != nil {
			t.Fatal(err)
		}
		s := time.Now()
		if _, err := srv.Upsert("app/data", id, writeCostDoc(i), auth.Principal{}); err != nil {
			t.Fatal(err)
		}
		lat = append(lat, time.Since(s))
	}
	total := time.Since(start)
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	return writeCostResult{
		docsPerSec: float64(n) / total.Seconds(),
		p50:        lat[len(lat)/2],
		p99:        lat[len(lat)*99/100],
	}
}

func runConcurrentWrites(t testing.TB, srv *KdbServerRuntime, n int) time.Duration {
	t.Helper()
	var wg sync.WaitGroup
	errs := make(chan error, n)
	start := time.Now()
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id, err := codec.RandomUUID()
			if err != nil {
				errs <- err
				return
			}
			for {
				_, err := srv.Upsert("app/data", id, writeCostDoc(i), auth.Principal{})
				if err != nil && strings.Contains(err.Error(), "busy") {
					// Backpressure, as a client sees it: wait and retry.
					time.Sleep(20 * time.Millisecond)
					continue
				}
				if err != nil {
					errs <- err
				}
				return
			}
		}(i)
	}
	wg.Wait()
	d := time.Since(start)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	return d
}

func TestIndexWriteCostOnDisk(t *testing.T) {
	if os.Getenv("KDB_INDEX_PERF") == "" {
		t.Skip("set KDB_INDEX_PERF=1 to measure index write cost on disk")
	}
	const sequential = 2000
	const concurrent = 150

	for _, withIndexes := range perfOrder() {
		srv, p := openDiskIndexedRuntime(t, t.TempDir())
		label := "no indexes"
		if withIndexes {
			d := createWriteCostIndexes(t, p)
			t.Logf("CREATE INDEX x%d on an empty namespace: %s total, %s each",
				writeCostIndexCount, d.Round(time.Millisecond), (d / writeCostIndexCount).Round(time.Microsecond))
			label = fmt.Sprintf("%d indexes", writeCostIndexCount)
		}
		r := runSequentialWrites(t, srv, sequential)
		t.Logf("%-11s sequential %d single-doc commits: %.0f docs/s, p50 %s, p99 %s",
			label, sequential, r.docsPerSec, r.p50.Round(time.Microsecond), r.p99.Round(time.Microsecond))
		d := runConcurrentWrites(t, srv, concurrent)
		t.Logf("%-11s %d concurrent single-doc commits: %s", label, concurrent, d.Round(time.Millisecond))
		srv.Release()
	}
}

func perfOrder() []bool {
	if os.Getenv("KDB_INDEX_PERF_REVERSE") != "" {
		return []bool{true, false, true, false}
	}
	return []bool{false, true}
}
