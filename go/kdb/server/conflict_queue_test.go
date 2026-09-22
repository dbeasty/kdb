package server

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

// TestConflictQueuedAndResolvedReplicates: a refused same-document push is queued with a
// tracking branch holding the peer's side; resolving it takes the chosen side into a merge,
// clears the entry and the branch, and the peer then fast-forwards to the resolution.
func TestConflictQueuedAndResolvedReplicates(t *testing.T) {
	rt := newTestRuntime(t)
	ns := rt.Runtime.DefaultNamespace
	shared := mustRandomUUID(t)
	if _, err := rt.Upsert(ns, shared, `{"v":"local"}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	peer := newPeerFixture(t, rt)
	genesis, _ := peer.dag.Head()
	remote := pushDoc(t, peer.dag, peer.storage, ns, genesis, shared, `{"v":"remote"}`)
	if _, err := peer.push(remote); err == nil {
		t.Fatal("expected the push to be refused as a conflict")
	}

	entries := rt.Conflicts.List()
	if len(entries) != 1 || entries[0].Kind != peersync.ConflictDivergence || entries[0].Ref != "branch:main" {
		t.Fatalf("expected one divergence of main, got %+v", entries)
	}
	e := entries[0]
	if b, ok := rt.dag.GetBranch(e.TrackingRef); !ok || b.HeadHash != remote.Hash {
		t.Fatalf("tracking branch %q does not hold the peer's side: %+v", e.TrackingRef, b)
	}
	if len(e.Items) != 1 || e.Items[0].DocumentID != shared.String() {
		t.Fatalf("conflict items: %+v", e.Items)
	}

	// Pushing the same thing again updates the entry, it does not add another.
	if _, err := peer.push(remote); err == nil {
		t.Fatal("expected the repeated push to be refused again")
	}
	if got := rt.Conflicts.List(); len(got) != 1 || got[0].Seen != 2 {
		t.Fatalf("expected one entry seen twice, got %+v", got)
	}

	resolved, err := rt.ResolveConflict(e.ID, map[codec.UUID]peersync.Choice{shared: {Take: "remote"}}, auth.Principal{})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if json, _, _, _ := rt.GetDocument(ns, shared); json != `{"v":"remote"}` {
		t.Fatalf("after resolving for remote the document is %s", json)
	}
	if rt.Conflicts.Len() != 0 {
		t.Fatalf("entry not cleared: %+v", rt.Conflicts.List())
	}
	if _, ok := rt.dag.GetBranch(e.TrackingRef); ok {
		t.Fatal("tracking branch not removed")
	}

	// The resolution is an ordinary merge: the peer takes it as a fast-forward.
	ln, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", rt, ns)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	client := peersync.NewClient(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peer.dag, peer.storage)
	session, err := client.Connect(peersync.ClientConfig{NamespaceID: ns, NodeID: "fixture", PeerURI: "tcp://" + ln.Addr().String(), ApplyToStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect()
	res, err := session.PullMissing()
	if err != nil || res.Conflict != nil {
		t.Fatalf("peer pull after resolution: %v %+v", err, res.Conflict)
	}
	if res.FinalHead != resolved.Hash {
		t.Fatalf("peer at %s, resolution is %s", res.FinalHead.Hex(), resolved.Hash.Hex())
	}
}

// TestResolveConflictNeedsEveryDocument: a resolution that leaves a conflicting document
// undecided leaves the conflict open.
func TestResolveConflictNeedsEveryDocument(t *testing.T) {
	rt := newTestRuntime(t)
	ns := rt.Runtime.DefaultNamespace
	x, y := mustRandomUUID(t), mustRandomUUID(t)
	rt.Upsert(ns, x, `{"v":"local"}`, auth.Principal{})
	rt.Upsert(ns, y, `{"v":"local"}`, auth.Principal{})
	peer := newPeerFixture(t, rt)
	g, _ := peer.dag.Head()
	c1 := pushDoc(t, peer.dag, peer.storage, ns, g, x, `{"v":"remote"}`)
	c2 := pushDoc(t, peer.dag, peer.storage, ns, c1.Hash, y, `{"v":"remote"}`)
	peer.push(c1, c2)
	e := rt.Conflicts.List()[0]
	if _, err := rt.ResolveConflict(e.ID, map[codec.UUID]peersync.Choice{x: {Take: "remote"}}, auth.Principal{}); err == nil {
		t.Fatal("a partial resolution was accepted")
	}
	if rt.Conflicts.Len() != 1 {
		t.Fatal("a failed resolution cleared the conflict")
	}
	body := `{"v":"merged by hand"}`
	if _, err := rt.ResolveConflict(e.ID, map[codec.UUID]peersync.Choice{x: {Take: "local"}, y: {Body: &body}}, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	if got, _, _, _ := rt.GetDocument(ns, x); got != `{"v":"local"}` {
		t.Fatalf("x = %s", got)
	}
	if got, _, _, _ := rt.GetDocument(ns, y); got != body {
		t.Fatalf("y = %s", got)
	}
}
