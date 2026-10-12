// kdb-diskbench measures the file-backed engine at --durability sync (default sync-mode full).
// Every earlier benchmark was in memory; this one exercises the real disk path end to end:
// bulk write throughput, point reads, a filtered SELECT, reopen time and resident memory.
//
// It runs in two phases that must be separate processes, so "after restart" really is a fresh
// heap and RSS:
//
//	kdb-diskbench load   -dir D -n 100000      bulk load, then measure on the hot instance
//	kdb-diskbench reopen -dir D -n 100000      reopen the same directory, then measure
//
// scripts/bench-disk.sh runs both for 10k, 100k and 1M documents and prints one JSON object per
// phase. Results: docs/benchmarks/disk-mode.md.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

const (
	catalog = "bench"
	ns      = "bench/disk"
)

type report map[string]any

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: kdb-diskbench load|reopen -dir D -n N [-batch 1000] [-reads 5000] [-single 300]")
		os.Exit(2)
	}
	mode := os.Args[1]
	fs := flag.NewFlagSet(mode, flag.ExitOnError)
	dir := fs.String("dir", "", "data directory (created if missing; use a path on the disk under test)")
	n := fs.Int("n", 100000, "number of documents")
	batch := fs.Int("batch", 1000, "documents per commit during the bulk load")
	reads := fs.Int("reads", 5000, "point reads for the latency percentiles")
	single := fs.Int("single", 300, "single-document commits to time after the bulk load (load phase only)")
	_ = fs.Parse(os.Args[2:])
	if *dir == "" {
		fmt.Fprintln(os.Stderr, "-dir is required")
		os.Exit(2)
	}
	out := report{"phase": mode, "docs": *n, "goos": runtime.GOOS, "goarch": runtime.GOARCH}

	opts := embed.FileRuntimeOptions{Storage: embed.StorageOptions{
		Durability: storage.DurabilitySync,
		SyncMode:   storio.SyncModeFull,
	}}

	start := time.Now()
	rt, err := embed.OpenFileRuntimeWithOptions(*dir, catalog, ns, schema.None(), opts)
	must(err)
	out["open_ms"] = ms(time.Since(start))
	srv := server.NewKdbServerRuntime(rt)
	out["rss_after_open_mb"] = rssMB()

	switch mode {
	case "load":
		bulkLoad(srv, *n, *batch, out)
		out["rss_after_load_mb"] = rssMB()
		singleCommits(srv, *single, out)
	case "reopen":
		// Nothing to write: measuring what a restart costs and what the reopened instance holds.
	default:
		fmt.Fprintln(os.Stderr, "unknown phase", mode)
		os.Exit(2)
	}

	measureReads(srv, *n, *reads, out)
	out["rss_after_reads_mb"] = rssMB()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	out["go_heap_inuse_mb"] = int(mem.HeapInuse >> 20)
	out["go_sys_mb"] = int(mem.Sys >> 20)

	start = time.Now()
	rt.Close()
	out["close_ms"] = float64Round(time.Since(start))
	out["disk_mb"] = diskMB(*dir)

	b, _ := json.Marshal(out)
	fmt.Println(string(b))
}

func docID(i int) codec.UUID { return codec.UUID{MSB: int64(i) + 1, LSB: 0x4000000000000001} }

func body(i int) string {
	// ~200 bytes, the size loadtest uses; "n" is unique, "bucket" selects ~1% of the documents.
	return fmt.Sprintf(`{"n":%d,"bucket":%d,"name":"doc-%d","pad":"%s"}`, i, i%100, i, strings.Repeat("x", 120))
}

func bulkLoad(srv *server.KdbServerRuntime, n, batch int, out report) {
	start := time.Now()
	commits := 0
	for lo := 0; lo < n; lo += batch {
		hi := lo + batch
		if hi > n {
			hi = n
		}
		head, err := srv.Runtime.DAG.Head()
		must(err)
		txID, _ := codec.RandomUUID()
		ops := make([]document.Op, 0, hi-lo)
		for i := lo; i < hi; i++ {
			ops = append(ops, document.WriteOp{DocID: docID(i), Patch: body(i)})
		}
		tx := document.Transaction{ID: txID, BaseVersion: head, Operations: ops, Timestamp: codec.TimestampNow()}
		_, err = srv.Commit(ns, tx, "bench", auth.Principal{})
		must(err)
		commits++
		if commits%100 == 0 {
			fmt.Fprintf(os.Stderr, "  loaded %d/%d (%.0f docs/s so far)\n", hi, n, float64(hi)/time.Since(start).Seconds())
		}
	}
	el := time.Since(start)
	out["bulk_batch"] = batch
	out["bulk_commits"] = commits
	out["bulk_seconds"] = round2(el.Seconds())
	out["bulk_docs_per_sec"] = int(float64(n) / el.Seconds())
}

// singleCommits times one-document Upserts: the durability cost of a lone writer, where group
// commit has nobody to share an fsync with.
func singleCommits(srv *server.KdbServerRuntime, count int, out report) {
	if count <= 0 {
		return
	}
	lat := make([]time.Duration, 0, count)
	for i := 0; i < count; i++ {
		id := codec.UUID{MSB: int64(1)<<40 + int64(i), LSB: 0x4000000000000001}
		t := time.Now()
		_, err := srv.Upsert(ns, id, fmt.Sprintf(`{"n":-1,"bucket":-1,"single":%d}`, i), auth.Principal{})
		must(err)
		lat = append(lat, time.Since(t))
	}
	p50, p99 := pct(lat)
	out["single_commit_p50_ms"], out["single_commit_p99_ms"] = p50, p99
	out["single_commits_per_sec"] = int(float64(count) / sum(lat).Seconds())
}

func measureReads(srv *server.KdbServerRuntime, n, reads int, out report) {
	rng := rand.New(rand.NewSource(1))
	lat := make([]time.Duration, 0, reads)
	for i := 0; i < reads; i++ {
		id := docID(rng.Intn(n))
		t := time.Now()
		_, _, found, err := srv.GetDocument(ns, id)
		must(err)
		if !found {
			fmt.Fprintln(os.Stderr, "document missing:", id)
			os.Exit(1)
		}
		lat = append(lat, time.Since(t))
	}
	out["get_p50_us"], out["get_p99_us"] = pctUS(lat)

	sch := srv.Schema()
	engine := srv.SQLEngine()
	run := func(label, q string, wantRows int, repeats int) {
		var l []time.Duration
		for r := 0; r < repeats; r++ {
			t := time.Now()
			res, err := engine.Execute(q, sql.QueryContext{NamespaceID: ns, Schema: sch, MaxRows: 10000})
			must(err)
			l = append(l, time.Since(t))
			if len(res.Rows) != wantRows {
				fmt.Fprintf(os.Stderr, "%s: got %d rows, want %d\n", label, len(res.Rows), wantRows)
				os.Exit(1)
			}
		}
		p50, p99 := pct(l)
		out[label+"_p50_ms"], out[label+"_p99_ms"] = p50, p99
		out[label+"_first_ms"] = ms(l[0]) // the first query after open/load: includes any cold-start cost
	}
	// A selective filter over a schemaless namespace is a full scan: it examines every document.
	repeats := 5
	if n >= 500000 {
		repeats = 3
	}
	run("select_scan_one_row", "SELECT kdb_id FROM docs WHERE n = "+strconv.Itoa(n-1), 1, repeats)
	run("select_scan_bucket", "SELECT kdb_id FROM docs WHERE bucket = 7 LIMIT 100", min(100, n/100), repeats)
	run("select_by_kdb_id", "SELECT n FROM docs WHERE kdb_id = '"+docID(n/2).String()+"'", 1, 200)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func rssMB() int {
	b, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil {
		return -1
	}
	kb, _ := strconv.Atoi(strings.TrimSpace(string(b)))
	return kb >> 10
}

func diskMB(dir string) int {
	b, err := exec.Command("du", "-sk", dir).Output()
	if err != nil {
		return -1
	}
	kb, _ := strconv.Atoi(strings.Fields(string(b))[0])
	return kb >> 10
}

func pct(l []time.Duration) (p50, p99 float64) {
	s := append([]time.Duration(nil), l...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	at := func(q float64) time.Duration { return s[min(len(s)-1, int(q*float64(len(s))))] }
	return ms(at(0.50)), ms(at(0.99))
}

func pctUS(l []time.Duration) (p50, p99 float64) {
	s := append([]time.Duration(nil), l...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	at := func(q float64) time.Duration { return s[min(len(s)-1, int(q*float64(len(s))))] }
	us := func(d time.Duration) float64 { return round2(float64(d) / float64(time.Microsecond)) }
	return us(at(0.50)), us(at(0.99))
}

func sum(l []time.Duration) (t time.Duration) {
	for _, d := range l {
		t += d
	}
	return
}

func ms(d time.Duration) float64           { return round2(float64(d) / float64(time.Millisecond)) }
func float64Round(d time.Duration) float64 { return ms(d) }
func round2(f float64) float64             { return float64(int(f*100+0.5)) / 100 }
