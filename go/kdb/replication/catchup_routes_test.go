package replication

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/wire"
)

// TestCatchUpRoutes measures the two catch-up routes of docs/kdb-memory-cassandra-plan.md C7 on
// a hot-set workload: B is in sync with A, then A writes a backlog of commits that keep rewriting
// a small set of documents, and two receivers catch up, one by each route.
//
//   - replay: today's pull - every commit of the backlog fetched and applied.
//   - squash: peersync.CatchUpSquash - the tree diff (TREE_NODES), the changed bodies
//     (OBJECT_FETCH), the install and its checkpoint.
//
// The ratio between the two, across backlog/hot-set shapes, sets the auto route's threshold.
//
// Off by default: KDB_CATCHUP_SCALE lists backlog:hot pairs, e.g. 100000:1000,100000:50000.
// KDB_SYNC_BODY_BYTES sets the body size (200 by default). Timings need an otherwise idle machine.
func TestCatchUpRoutes(t *testing.T) {
	spec := os.Getenv("KDB_CATCHUP_SCALE")
	if spec == "" {
		t.Skip("set KDB_CATCHUP_SCALE=100000:1000 (backlog:hot,...) to run")
	}
	bodyBytes := 200
	if v, err := strconv.Atoi(os.Getenv("KDB_SYNC_BODY_BYTES")); err == nil && v > 0 {
		bodyBytes = v
	}
	t.Logf("%-8s %9s %7s %12s %12s %9s %12s %12s", "route", "backlog", "hot", "recv", "sent", "requests", "allocated", "peak heap+")
	for _, field := range strings.Split(spec, ",") {
		var backlog, hot int
		if _, err := fmt.Sscanf(strings.TrimSpace(field), "%d:%d", &backlog, &hot); err != nil || backlog <= 0 || hot <= 0 {
			t.Fatalf("KDB_CATCHUP_SCALE: bad pair %q", field)
		}
		t.Run(fmt.Sprintf("backlog=%d/hot=%d", backlog, hot), func(t *testing.T) { runCatchUpRoutes(t, backlog, hot, bodyBytes) })
	}
}

func runCatchUpRoutes(t *testing.T, backlog, hot, bodyBytes int) {
	ns := "app/data"
	a := fileNode(t, ns)
	ma := listen(t, a, ns)
	// One receiver per route, both in sync with A before the backlog.
	squashed, replayed := fileNode(t, ns), fileNode(t, ns)
	pad := strings.Repeat("x", bodyBytes)
	docs := make([]codec.UUID, hot)
	for i := range docs {
		docs[i], _ = codec.RandomUUID()
	}
	write := func(from, to int) {
		for i := from; i < to; i++ {
			if _, err := a.Upsert(ns, docs[i%hot], fmt.Sprintf(`{"i":%d,"pad":%q}`, i, pad), auth.Principal{}); err != nil {
				t.Fatal(err)
			}
		}
	}
	write(0, hot)
	for _, n := range []*server.KdbServerRuntime{squashed, replayed} {
		if _, err := syncWith(t, n, ma, ns, peersync.SyncPull, newCounting()); err != nil {
			t.Fatal(err)
		}
	}
	start := time.Now()
	write(hot, hot+backlog)
	t.Logf("wrote a backlog of %d commits over %d documents in %s", backlog, hot, time.Since(start).Round(time.Millisecond))

	report := func(route string, m syncMeasure) {
		t.Logf("%-8s %9d %7d %12s %12s %9d %12s %12s   %s", route, backlog, hot, mib(m.recv), mib(m.sent), m.requests,
			mib(m.allocated), mib(m.peakHeap), m.wall.Round(time.Millisecond))
	}
	route := func(n *server.KdbServerRuntime, r peersync.CatchUpRoute) func(*countingTransport) int {
		return func(tr *countingTransport) int {
			res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tr, peersync.V2ClientConfig{
				NodeID: n.NodeID.String(), PeerURI: ma.addr, Namespaces: []string{ns}, Mode: peersync.SyncPull,
				Local: n.PeerNamespaces(), CreateLocal: true, Timeout: 60 * time.Second, CatchUp: r,
			})
			if err != nil {
				t.Fatal(err)
			}
			if got := res.Namespaces[0]; got.Err != nil {
				t.Fatal(got.Err)
			} else if (r == peersync.CatchUpSquash) != (got.Squashed != "") {
				t.Fatalf("route %v: squashed=%q", r, got.Squashed)
			}
			return 0
		}
	}
	report("squash", measureSync(t, route(squashed, peersync.CatchUpSquash)))
	report("replay", measureSync(t, route(replayed, peersync.CatchUpReplay)))
	for _, n := range []*server.KdbServerRuntime{squashed, replayed} {
		if head(t, a) != head(t, n) {
			t.Fatal("a receiver's head differs from A's")
		}
	}
}
