package replication

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/wire"
)

// TestSnapshotScale measures a snapshot bootstrap - an empty node B installing A's state at its
// head (V2ClientConfig.PreferSnapshot) - as the namespace grows: the sibling of TestSyncScale for
// the snapshot path (docs/kdb-memory-cassandra-plan.md C5). Per size: wall time, bytes on the wire,
// requests, the peak heap and (KDB_SYNC_LIVE=1) peak live heap above the pre-sync baseline, bytes
// allocated, and the longest B's write gate was unavailable to a local writer during the
// bootstrap ("gate held"), measured by a probe that takes the gate in a loop.
//
// Off by default; KDB_SNAPSHOT_SCALE lists the sizes in documents, e.g.
// KDB_SNAPSHOT_SCALE=100000,1000000. KDB_SYNC_BODY_BYTES sets the body size (default 200).
// Both nodes are file-backed and in this process, so heap figures cover both sides.
func TestSnapshotScale(t *testing.T) {
	spec := os.Getenv("KDB_SNAPSHOT_SCALE")
	if spec == "" {
		t.Skip("set KDB_SNAPSHOT_SCALE=100000,1000000 to run")
	}
	bodyBytes := 200
	if v, err := strconv.Atoi(os.Getenv("KDB_SYNC_BODY_BYTES")); err == nil && v > 0 {
		bodyBytes = v
	}
	for _, field := range strings.Split(spec, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(field))
		if err != nil || n <= 0 {
			t.Fatalf("KDB_SNAPSHOT_SCALE: bad size %q", field)
		}
		t.Run(fmt.Sprintf("docs=%d", n), func(t *testing.T) { runSnapshotScale(t, n, bodyBytes) })
	}
}

func runSnapshotScale(t *testing.T, n, bodyBytes int) {
	ns := "app/data"
	a, b := fileNode(t, ns), fileNode(t, ns)
	b.WriteTimeout = time.Hour // the probe measures how long the gate is held; it must not give up
	ma := listen(t, a, ns)
	pad := strings.Repeat("x", bodyBytes)
	start := time.Now()
	const perCommit = 1_000
	for from := 0; from < n; from += perCommit {
		head, err := a.Runtime.DAG.Head()
		if err != nil {
			t.Fatal(err)
		}
		ops := make([]document.Op, 0, perCommit)
		for i := from; i < min(from+perCommit, n); i++ {
			id, _ := codec.RandomUUID()
			ops = append(ops, document.WriteOp{DocID: id, Patch: fmt.Sprintf(`{"i":%d,"pad":%q}`, i, pad)})
		}
		txID, _ := codec.RandomUUID()
		if _, err := a.Commit(ns, document.Transaction{ID: txID, BaseVersion: head, Operations: ops, Timestamp: codec.TimestampNow()}, "", auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("seeded %d documents in %d commits in %s", n, (n+perCommit-1)/perCommit, time.Since(start).Round(time.Millisecond))

	var gateHeld time.Duration
	m := measureSync(t, func(tr *countingTransport) int {
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			node := b.PeerSyncNode()
			for {
				select {
				case <-stop:
					return
				default:
				}
				t0 := time.Now()
				_ = node.Exclusive(func() error { return nil })
				if w := time.Since(t0); w > gateHeld {
					gateHeld = w
				}
				time.Sleep(time.Millisecond)
			}
		}()
		res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tr, peersync.V2ClientConfig{
			NodeID: b.NodeID.String(), PeerURI: ma.addr, Namespaces: []string{ns}, Mode: peersync.SyncPull,
			Local: b.PeerNamespaces(), CreateLocal: true, Timeout: 10 * time.Minute, PreferSnapshot: true,
		})
		close(stop)
		wg.Wait()
		if err != nil {
			t.Fatal(err)
		}
		if r := res.Namespaces[0]; r.Err != nil || r.Snapshot == "" {
			t.Fatalf("bootstrap: snapshot %q, %v", r.Snapshot, r.Err)
		}
		return 0
	})
	if head(t, a) != head(t, b) {
		t.Fatal("snapshot: A and B differ")
	}
	live := "-"
	if m.peakLive > 0 {
		live = mib(m.peakLive)
	}
	t.Logf("%-9s %10s %12s %9s %12s %12s %12s %12s %12s %10s", "scenario", "docs", "recv", "requests", "peak heap+", "peak live+", "allocated", "live after", "gate held", "wall")
	t.Logf("%-9s %10d %12s %9d %12s %12s %12s %12s %12s %10s", "snapshot", n, mib(m.recv), m.requests,
		mib(m.peakHeap), live, mib(m.allocated), mib(m.liveAfter), gateHeld.Round(time.Millisecond), m.wall.Round(time.Millisecond))
}
