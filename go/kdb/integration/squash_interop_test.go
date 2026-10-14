package integration

import (
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

// TestKotlinReadsAGoNamespaceThatCaughtUpBySquash: B, a Go file-backed namespace, catches up with
// A by squash (docs/kdb-memory-cassandra-plan.md C7) - it jumps to A's head, which it admits as a
// shallow root that only its checkpoint records, since a commit's operations are relative to its
// real parent and could not be replayed onto B's old head. The Kotlin CLI then reads the
// directory, and every document must read exactly as Go holds it. A replay of the log alone cannot
// get there - with a write on top of the squash the log holds a commit whose parent it lacks, and
// without one it ends at B's head before the squash, whose older state a replay would serve - so
// Kotlin opens it from Go's checkpoint (CheckpointOpen.kt; it refused such namespaces before it
// read checkpoints, #209). Once B has deepened the gap from A, its log is whole again, and the
// reads must still be exact.
func TestKotlinReadsAGoNamespaceThatCaughtUpBySquash(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the Kotlin CLI through gradle")
	}
	repo := findRepoRoot(t)
	t.Run("write-on-top", func(t *testing.T) { runKotlinReadsSquash(t, repo, true, false) })
	t.Run("squash-only", func(t *testing.T) { runKotlinReadsSquash(t, repo, false, false) })
	t.Run("deepened", func(t *testing.T) { runKotlinReadsSquash(t, repo, true, true) })
}

func runKotlinReadsSquash(t *testing.T, repo string, writeOnTop, deepen bool) {
	root := t.TempDir()
	ns := "app/data"
	mrt, err := embed.OpenMemoryRuntime("app", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	a := server.NewKdbServerRuntime(mrt)
	a.NodeID = randomID(t)
	fileRT, err := embed.OpenFileRuntime(root, "app", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	b := server.NewKdbServerRuntime(fileRT)
	b.NodeID = randomID(t)
	closed := false
	defer func() {
		if !closed {
			fileRT.Close()
		}
	}()

	want := map[codec.UUID]string{}
	upsert := func(rt *server.KdbServerRuntime, id codec.UUID, body string) {
		t.Helper()
		if _, err := rt.Upsert(ns, id, body, auth.Principal{}); err != nil {
			t.Fatal(err)
		}
		want[id] = body
	}
	ids := make([]codec.UUID, 6)
	for i := range ids {
		ids[i] = randomID(t)
		upsert(a, ids[i], fmt.Sprintf(`{"v":%d}`, i))
	}
	ln, err := server.ListenPeerSync("tcp://127.0.0.1:0?bind=true", a, ns)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	pull := func(route peersync.CatchUpRoute) peersync.NamespaceSyncResult {
		t.Helper()
		res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
			NodeID: b.NodeID.String(), PeerURI: "tcp://" + ln.Addr().String(), Namespaces: []string{ns},
			Local: b.PeerNamespaces(), Mode: peersync.SyncPull, CatchUp: route,
		})
		if err != nil || len(res.Namespaces) != 1 || res.Namespaces[0].Err != nil {
			t.Fatalf("pull: %v %+v", err, res)
		}
		return res.Namespaces[0]
	}
	pull(peersync.CatchUpReplay)

	// The backlog: rewrites, a delete and a new document - every kind of change a squash carries.
	for i := 0; i < 60; i++ {
		upsert(a, ids[i%len(ids)], fmt.Sprintf(`{"v":%d,"round":%d}`, i, i/len(ids)))
	}
	head, err := a.PeerIngestEnv().DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	txID := randomID(t)
	if _, err := a.Commit(ns, document.Transaction{ID: txID, BaseVersion: head,
		Operations: []document.Op{document.DeleteOp{DocID: ids[0]}}, Timestamp: codec.TimestampNow()}, "", auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	delete(want, ids[0])
	upsert(a, randomID(t), `{"fresh":true}`)

	aHead, _ := a.PeerIngestEnv().DAG.Head()
	if res := pull(peersync.CatchUpSquash); res.Squashed != aHead.Hex() {
		t.Fatalf("expected a squash to %s, got %+v", aHead.Hex(), res)
	}
	if writeOnTop {
		upsert(b, randomID(t), `{"local":"on top of the squash"}`)
	}
	if deepen {
		results, err := b.Deepen(func() (*peersync.RepairSession, error) {
			return peersync.OpenRepairSession(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
				NodeID: b.NodeID.String(), PeerURI: "tcp://" + ln.Addr().String(), Namespaces: []string{ns},
			})
		})
		if err != nil {
			t.Fatalf("deepen across the squash gap: %v (%+v)", err, results)
		}
		if roots := b.PeerIngestEnv().DAG.ShallowRoots(); len(roots) != 0 {
			t.Fatalf("after deepening, %d shallow roots remain", len(roots))
		}
	}
	for id, body := range want {
		got, _, found, err := b.GetDocument(ns, id)
		if err != nil || !found || !sameJSON(got, body) {
			t.Fatalf("Go holds %q for %s, want %s (%v)", got, id, body, err)
		}
	}
	fileRT.Close()
	closed = true

	// Deleted by the backlog: Kotlin must not serve its old value.
	got := checkKotlinReads(t, repo, root, &cpDoc{live: want, gone: map[codec.UUID]bool{ids[0]: true}})
	if !deepen && !got.fromCheckpoint {
		t.Fatalf("Kotlin read a squashed namespace without its checkpoint; stderr:\n%s", tail(got.stderr))
	}
	t.Logf("from checkpoint %v (v%d, %s)", got.fromCheckpoint, got.version, openLine(got.stderr))
}
