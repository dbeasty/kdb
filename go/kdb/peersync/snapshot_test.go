package peersync

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/wire"
)

// seedHistory writes n commits of distinct documents, some rewritten, on ns in p.
func seedHistory(t *testing.T, p *testNamespaces, ns string, n int) {
	t.Helper()
	s := p.side(ns)
	for i := 0; i < n; i++ {
		id := newUUID(t)
		writeAtHead(t, s, ns, id, fmt.Sprintf(`{"i":%d,"pad":%q}`, i, strings.Repeat("y", 300)))
		if i%3 == 0 {
			writeAtHead(t, s, ns, id, fmt.Sprintf(`{"i":%d,"rewritten":true}`, i))
		}
	}
}

func TestSnapshotBootstrapEmptyNode(t *testing.T) {
	ns := "app/snap"
	remote := newTestNamespaces(t, ns)
	local := newTestNamespaces(t, ns)
	seedHistory(t, remote, ns, 60)
	v2Hub(t, "hub-snap-boot", newHost(remote), nil)

	res := syncV2(t, "hub-snap-boot", local, V2ClientConfig{Namespaces: []string{ns}, Mode: SyncPull, PreferSnapshot: true, PageBytes: 4_000})
	rh := mustHead(t, remote.side(ns))
	if res.Namespaces[0].Snapshot != rh.Hex() {
		t.Fatalf("expected a snapshot at the remote head, got %q", res.Namespaces[0].Snapshot)
	}
	l := local.side(ns)
	if mustHead(t, l) != rh || !l.dag.IsShallow(rh) {
		t.Fatal("local main is not the remote head as a shallow root")
	}
	if res.Namespaces[0].Pulled != 0 {
		t.Fatalf("a snapshot bootstrap should not also fetch history, pulled %d", res.Namespaces[0].Pulled)
	}

	// From here on it is an ordinary peer: new remote commits fast-forward it.
	writeAtHead(t, remote.side(ns), ns, newUUID(t), `{"after":"snapshot"}`)
	res = syncV2(t, "hub-snap-boot", local, V2ClientConfig{Namespaces: []string{ns}, Mode: SyncBoth})
	if res.Namespaces[0].Local["branch:main"] != wire.RefFastForwarded || mustHead(t, l) != mustHead(t, remote.side(ns)) {
		t.Fatalf("after the snapshot, sync did not fast-forward: %+v", res.Namespaces[0])
	}
}

// TestSnapshotRejectsTamperedBody: a snapshot whose documents do not build the tree its commit
// declares is refused and leaves the namespace empty.
func TestSnapshotRejectsTamperedBody(t *testing.T) {
	ns := "app/snap-tamper"
	remote := newTestNamespaces(t, ns)
	local := newTestNamespaces(t, ns)
	seedHistory(t, remote, ns, 10)
	w := wire.NewCodec(wire.EncodingJSON)
	tampering := newHost(remote)
	v2HubRaw(t, "hub-snap-tamper", func(frame []byte) []byte {
		out, _ := tampering.HandleFrame(frame)
		msg, _ := w.Decode(out)
		if page, ok := msg.(wire.SnapshotPageMessage); ok && len(page.Docs) > 0 {
			page.Docs[0].Body = `{"tampered":true}`
			out, _ = w.Encode(page)
		}
		return out
	})
	cfg := V2ClientConfig{PeerURI: "memory://hub-snap-tamper", Local: local, NodeID: "c", Namespaces: []string{ns}, Mode: SyncPull, PreferSnapshot: true}
	res, err := SyncV2(w, stream.NewInMemoryTransport(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	var mismatch *TreeMismatchError
	if !errors.As(res.Namespaces[0].Err, &mismatch) {
		t.Fatalf("expected a tree mismatch, got %v", res.Namespaces[0].Err)
	}
	l := local.side(ns)
	_, head, _, _ := l.dag.HeadCommit()
	if len(head.ParentHashes) != 0 {
		t.Fatal("a refused snapshot moved main")
	}
	n := 0
	if err := l.storage.(storage.TreeWalker).WalkTree(ns, head.DocumentTreeHash, func(codec.UUID, codec.Hash) bool { n++; return true }); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("a refused snapshot left %d documents behind", n)
	}
}

// v2HubRaw serves frames on an in-memory hub through an arbitrary function.
func v2HubRaw(t *testing.T, hub string, serve func([]byte) []byte) {
	t.Helper()
	h := stream.HubFor(hub)
	h.ServerHandler = func(frame []byte) {
		if out := serve(frame); out != nil {
			h.ServerSend(out)
		}
	}
	t.Cleanup(func() { h.ServerHandler = nil })
}

// TestSyncFallsBackToSnapshotBelowHorizon: a peer that itself only holds history from a shallow
// root cannot send an empty node its history from the beginning, so the empty node bootstraps
// from a snapshot without being asked to.
func TestSyncFallsBackToSnapshotBelowHorizon(t *testing.T) {
	ns := "app/snap-horizon"
	origin := newTestNamespaces(t, ns)
	middle := newTestNamespaces(t, ns)
	edge := newTestNamespaces(t, ns)
	seedHistory(t, origin, ns, 20)
	v2Hub(t, "hub-snap-origin", newHost(origin), nil)
	syncV2(t, "hub-snap-origin", middle, V2ClientConfig{Namespaces: []string{ns}, Mode: SyncPull, PreferSnapshot: true})
	writeAtHead(t, middle.side(ns), ns, newUUID(t), `{"on":"middle"}`)

	v2Hub(t, "hub-snap-middle", newHost(middle), nil)
	res := syncV2(t, "hub-snap-middle", edge, V2ClientConfig{Namespaces: []string{ns}, Mode: SyncPull})
	if res.Namespaces[0].Snapshot == "" {
		t.Fatal("an empty node syncing from a shallow peer did not bootstrap from a snapshot")
	}
	if mustHead(t, edge.side(ns)) != mustHead(t, middle.side(ns)) {
		t.Fatal("edge did not reach middle's head")
	}
}
