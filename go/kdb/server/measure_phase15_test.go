package server

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

// Phase 15 gate measurements (docs/kdb-distributed-self-healing-research.md). Not part of the
// suite: run with KDB_MEASURE=1 go test ./kdb/server/ -run Measure -v.

func requireMeasure(t *testing.T) {
	if os.Getenv("KDB_MEASURE") == "" {
		t.Skip("measurement; set KDB_MEASURE=1")
	}
}

func pullOnce(t *testing.T, from, to *KdbServerRuntime, extra ...codec.Hash) peersync.NamespaceSyncResult {
	t.Helper()
	ln, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", to, "app/data")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
		NodeID: from.NodeID.String(), PeerURI: "tcp://" + ln.Addr().String(), Namespaces: []string{"app/data"},
		Local: from.PeerNamespaces(), Mode: peersync.SyncPull, ExtraHaves: map[string][]codec.Hash{"app/data": extra},
	})
	if err != nil || res.Namespaces[0].Err != nil {
		t.Fatalf("pull: %v %+v", err, res)
	}
	return res.Namespaces[0]
}

// M1: how many commits does have/want negotiation send that the receiver already holds, as the
// two sides' divergence grows? The haves are the head plus first-parent ancestors at distances
// 1, 2, 4 ... (at most 32), so the overshoot is bounded by the local side's own divergence.
func TestMeasureNegotiationOvershoot(t *testing.T) {
	requireMeasure(t)
	const shared = 2000
	for _, d := range []int{1, 10, 100, 1000, 5000} {
		a, b := newMetaNode(t).data, newMetaNode(t).data
		for i := 0; i < shared; i++ {
			if _, err := b.Upsert("app/data", mustRandomUUID(t), fmt.Sprintf(`{"s":%d}`, i), auth.Principal{}); err != nil {
				t.Fatal(err)
			}
		}
		pullOnce(t, a, b)
		lastRemote := mustHeadOf(t, b) // what the replicator records as the peer's main
		for i := 0; i < d; i++ {
			if _, err := a.Upsert("app/data", mustRandomUUID(t), fmt.Sprintf(`{"a":%d}`, i), auth.Principal{}); err != nil {
				t.Fatal(err)
			}
			if _, err := b.Upsert("app/data", mustRandomUUID(t), fmt.Sprintf(`{"b":%d}`, i), auth.Principal{}); err != nil {
				t.Fatal(err)
			}
		}
		withRemote := os.Getenv("KDB_MEASURE_REMOTE_MAIN") != ""
		var r peersync.NamespaceSyncResult
		if withRemote {
			r = pullOnce(t, a, b, lastRemote)
		} else {
			r = pullOnce(t, a, b)
		}
		t.Logf("M1 (last remote main as a have: %v)", withRemote)
		t.Logf("M1 divergence d=%5d each side: needed %5d, received %5d, overshoot %5d (%.1f%% of what was needed, %.2f%% of shared history)",
			d, r.Pulled, r.Received, r.Received-r.Pulled, 100*float64(r.Received-r.Pulled)/float64(max(r.Pulled, 1)),
			100*float64(r.Received-r.Pulled)/float64(shared))
	}
}

// M2: in a projection delta, how much of what the source sends is deletes of documents the
// replica never held? Every changed document that does not match the filter goes as a delete.
func TestMeasureProjectionDeleteNoise(t *testing.T) {
	requireMeasure(t)
	for _, pct := range []int{1, 10, 50} {
		f := newProjectionFixture(t, "region = 'EU'")
		const docs = 5000
		ids := make([]codec.UUID, docs)
		r := rand.New(rand.NewSource(int64(pct)))
		for i := range ids {
			ids[i] = mustRandomUUID(t)
			region := "US"
			if r.Intn(100) < pct {
				region = "EU"
			}
			f.put(ids[i], fmt.Sprintf(`{"region":%q,"v":0}`, region))
		}
		syncLargePages(t, f)
		held := len(f.held())
		// A round of updates to 2000 random documents, keeping their regions.
		for i := 0; i < 2000; i++ {
			id := ids[r.Intn(docs)]
			body, _, _, _ := f.source.GetDocument("app/data", id)
			region := "US"
			if len(body) > 12 && body[11:13] == "EU" {
				region = "EU"
			}
			f.put(id, fmt.Sprintf(`{"region":%q,"v":%d}`, region, i+1))
		}
		res := syncLargePages(t, f)
		t.Logf("M2 filter matches ~%2d%% (%4d held): delta sent %4d writes and %4d deletes - %.0f%% of the delta's entries are deletes",
			pct, held, res.Writes, res.Deletes, 100*float64(res.Deletes)/float64(max(res.Writes+res.Deletes, 1)))
	}
}

func syncLargePages(t *testing.T, f *projectionFixture) peersync.ProjectionResult {
	t.Helper()
	res, err := peersync.SyncProjection(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.ProjectionConfig{
		NodeID: "replica", PeerURI: f.addr, Namespace: "app/data", Filter: f.filter, Target: f.replica.ProjectionTarget(),
	})
	if err != nil {
		t.Fatalf("projection sync: %v", err)
	}
	return res
}
