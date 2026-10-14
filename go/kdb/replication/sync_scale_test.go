package replication

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/storage"
)

// TestSyncScale measures what node-to-node sync costs as history grows: the baseline the sync
// work in docs/kdb-memory-cassandra-plan.md (Track C) is judged against. Two scenarios per size n:
//
//   - join:     B is empty and pulls A's n commits.
//   - catch-up: B holds A's first n commits; A writes n more and B pulls them.
//   - push:     A pushes its 2n commits into an empty node C.
//
// Both nodes are file-backed and live in this process, so the heap figures cover both sides of the
// sync - the host's diff work and the receiver's backlog alike. Reported per scenario: wall time,
// bytes on the wire each way, requests sent (frames), the peak heap above the pre-sync baseline,
// the bytes allocated during the sync, and the heap still live after it.
//
// Off by default; KDB_SYNC_SCALE lists the sizes, e.g. KDB_SYNC_SCALE=10000,100000. Timings need an
// otherwise idle machine; bytes, requests and allocations do not.
//
// "peak heap+" samples HeapAlloc, which includes garbage the collector has not reached yet, so it
// moves with GC pacing. KDB_SYNC_LIVE=1 adds "peak live+": a collection before every sample, so
// it is the live heap the sync held at its worst - the figure that has to fit the memory budget.
// The forced collections slow the sync, so take wall times from a run without it.
// KDB_SYNC_ADOPT_BYTES sets V2ClientConfig.AdoptEveryBytes (-1: adopt only at the end).
func TestSyncScale(t *testing.T) {
	spec := os.Getenv("KDB_SYNC_SCALE")
	if spec == "" {
		t.Skip("set KDB_SYNC_SCALE=10000,100000 to run")
	}
	if v, err := strconv.Atoi(os.Getenv("KDB_SYNC_ADOPT_BYTES")); err == nil {
		testAdoptEveryBytes = v
		t.Cleanup(func() { testAdoptEveryBytes = 0 })
	}
	bodyBytes := 200
	if v, err := strconv.Atoi(os.Getenv("KDB_SYNC_BODY_BYTES")); err == nil && v > 0 {
		bodyBytes = v
	}
	for _, field := range strings.Split(spec, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(field))
		if err != nil || n <= 0 {
			t.Fatalf("KDB_SYNC_SCALE: bad size %q", field)
		}
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) { runSyncScale(t, n, bodyBytes) })
	}
}

func runSyncScale(t *testing.T, n, bodyBytes int) {
	ns := "app/data"
	a, b := fileNode(t, ns), fileNode(t, ns)
	ma := listen(t, a, ns)
	pad := strings.Repeat("x", bodyBytes)
	// Each commit rewrites one of n/10 documents, so history is ten versions deep per document.
	docs := make([]codec.UUID, max(n/10, 1))
	for i := range docs {
		docs[i], _ = codec.RandomUUID()
	}
	seed := func(from, to int) {
		start := time.Now()
		for i := from; i < to; i++ {
			body := fmt.Sprintf(`{"i":%d,"pad":%q}`, i, pad)
			if _, err := a.Upsert(ns, docs[i%len(docs)], body, auth.Principal{}); err != nil {
				t.Fatal(err)
			}
		}
		t.Logf("seeded commits %d..%d in %s", from, to, time.Since(start).Round(time.Millisecond))
	}

	seed(0, n)
	t.Logf("%-9s %10s %12s %12s %9s %12s %12s %12s %12s %6s", "scenario", "commits", "recv", "sent", "requests", "peak heap+", "peak live+", "allocated", "live after", "early")
	report := func(name string, commits int, m syncMeasure) {
		live := "-"
		if m.peakLive > 0 {
			live = mib(m.peakLive)
		}
		t.Logf("%-9s %10d %12s %12s %9d %12s %12s %12s %12s %6d   %s", name, commits, mib(m.recv), mib(m.sent), m.requests,
			mib(m.peakHeap), live, mib(m.allocated), mib(m.liveAfter), m.early, m.wall.Round(time.Millisecond))
	}
	m := measureSync(t, func(tr *countingTransport) int {
		r, err := syncWith(t, b, ma, ns, peersync.SyncPull, tr)
		if err != nil {
			t.Fatal(err)
		}
		return r.EarlyAdoptions
	})
	if head(t, a) != head(t, b) {
		t.Fatal("join: A and B differ")
	}
	report("join", n, m)

	seed(n, 2*n)
	m = measureSync(t, func(tr *countingTransport) int {
		r, err := syncWith(t, b, ma, ns, peersync.SyncPull, tr)
		if err != nil {
			t.Fatal(err)
		}
		return r.EarlyAdoptions
	})
	if head(t, a) != head(t, b) {
		t.Fatal("catch-up: A and B differ")
	}
	report("catch-up", n, m)

	// push: A pushes its whole history (2n commits) into an empty node C, which is the side
	// whose memory is measured here - a node receiving a long push.
	c := fileNode(t, ns)
	mc := listen(t, c, ns)
	m = measureSync(t, func(tr *countingTransport) int {
		if _, err := syncWith(t, a, mc, ns, peersync.SyncPush, tr); err != nil {
			t.Fatal(err)
		}
		return 0
	})
	if head(t, a) != head(t, c) {
		t.Fatal("push: A and C differ")
	}
	report("push", 2*n, m)
}

type syncMeasure struct {
	wall                           time.Duration
	recv, sent                     int64
	requests                       int64
	peakHeap, allocated, liveAfter int64
	peakLive                       int64
	early                          int
}

// measureSync runs sync with a counting transport and samples the heap while it runs.
func measureSync(t *testing.T, sync func(*countingTransport) int) syncMeasure {
	t.Helper()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	var peak, peakLive atomic.Uint64
	sampleLive := os.Getenv("KDB_SYNC_LIVE") == "1"
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		var ms runtime.MemStats
		every := 20 * time.Millisecond
		if sampleLive {
			every = 100 * time.Millisecond
		}
		tick := time.NewTicker(every)
		defer tick.Stop()
		for {
			runtime.ReadMemStats(&ms)
			if ms.HeapAlloc > peak.Load() {
				peak.Store(ms.HeapAlloc)
			}
			if sampleLive {
				runtime.GC()
				runtime.ReadMemStats(&ms)
				if ms.HeapAlloc > peakLive.Load() {
					peakLive.Store(ms.HeapAlloc)
				}
			}
			select {
			case <-stop:
				return
			case <-tick.C:
			}
		}
	}()
	tr := newCounting()
	start := time.Now()
	early := sync(tr)
	wall := time.Since(start)
	close(stop)
	<-done
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	runtime.GC()
	var live runtime.MemStats
	runtime.ReadMemStats(&live)
	out := syncMeasure{
		wall: wall, recv: tr.recv.Load(), sent: tr.sent.Load(), requests: tr.sentFrames.Load(),
		peakHeap:  int64(peak.Load()) - int64(before.HeapAlloc),
		allocated: int64(after.TotalAlloc - before.TotalAlloc),
		liveAfter: int64(live.HeapAlloc) - int64(before.HeapAlloc),
		early:     early,
	}
	if sampleLive {
		out.peakLive = int64(peakLive.Load()) - int64(before.HeapAlloc)
	}
	return out
}

// fileNode is newNode on disk: a file-backed runtime, so what a sync leaves resident is what a
// real node would keep, not the whole history. Writes are acknowledged before fsync - the seeding
// would otherwise be bound by the disk, and the receiver's fsyncs are not what is measured.
//
// It starts from what a deployed node gets (embed.FileRuntimeOptionsFromEnv: the graph file on, and
// KDB_TREE_FILE / KDB_BOUNDED_LIVE_BODIES honoured, so the scale tests can be run with them), with
// asynchronous durability.
func fileNode(t *testing.T, ns string) *server.KdbServerRuntime {
	t.Helper()
	opts := embed.FileRuntimeOptionsFromEnv()
	opts.Storage.Durability = storage.DurabilityAsync
	rt, err := embed.OpenFileRuntimeWithOptions(t.TempDir(), "app", ns, schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Close)
	srv := server.NewKdbServerRuntime(rt)
	id, _ := codec.RandomUUID()
	srv.NodeID = id
	return srv
}

func mib(b int64) string {
	return fmt.Sprintf("%.1f MiB", float64(b)/(1<<20))
}
