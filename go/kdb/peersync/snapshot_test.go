package peersync

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
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

// TestSnapshotReplicaFollowsMainDespiteBranchBelowRoot: the peer has a side branch forked below
// the commit this node was bootstrapped from. Its parents can never arrive, so the branch is
// reported and skipped - and main keeps syncing, now and on every later sync.
func TestSnapshotReplicaFollowsMainDespiteBranchBelowRoot(t *testing.T) {
	ns := "app/snap-side"
	remote := newTestNamespaces(t, ns)
	local := newTestNamespaces(t, ns)
	r := remote.side(ns)
	f := writeAtHead(t, r, ns, newUUID(t), `{"a":1}`)
	writeAtHead(t, r, ns, newUUID(t), `{"b":1}`)
	root := writeAtHead(t, r, ns, newUUID(t), `{"c":1}`)
	v2Hub(t, "hub-snap-side", newHost(remote), nil)
	syncV2(t, "hub-snap-side", local, V2ClientConfig{Namespaces: []string{ns}, Mode: SyncPull, PreferSnapshot: true})

	side := writeDoc(t, r, ns, f.Hash, newUUID(t), `{"side":1}`)
	if _, err := r.dag.CreateBranch("feature", side.Hash); err != nil {
		t.Fatal(err)
	}
	setHead(t, r, root.Hash)
	next := writeAtHead(t, r, ns, newUUID(t), `{"d":1}`)
	for i := 0; i < 2; i++ {
		res := syncV2(t, "hub-snap-side", local, V2ClientConfig{Namespaces: []string{ns}, Mode: SyncPull})
		if res.Namespaces[0].RefErrors["branch:feature"] == "" {
			t.Fatalf("sync %d: the unreachable branch was not reported: %+v", i, res.Namespaces[0])
		}
		if mustHead(t, local.side(ns)) != next.Hash {
			t.Fatalf("sync %d: main stopped following the peer", i)
		}
	}
}

// TestSnapshotNotMadeDurableLeavesNamespaceEmpty: when the snapshot cannot be made durable, main
// goes back to where it was and the documents go with it - nothing is left for later commits to
// be logged on top of that a restart could not bring back.
func TestSnapshotNotMadeDurableLeavesNamespaceEmpty(t *testing.T) {
	ns := "app/snap-persist"
	remote := newTestNamespaces(t, ns)
	local := newTestNamespaces(t, ns)
	local.installed = func(document.Commit) error { return errors.New("disk full") }
	seedHistory(t, remote, ns, 10)
	v2Hub(t, "hub-snap-persist", newHost(remote), nil)
	l := local.side(ns)
	before := mustHead(t, l)

	cfg := V2ClientConfig{PeerURI: "memory://hub-snap-persist", Local: local, NodeID: "c", Namespaces: []string{ns}, Mode: SyncPull, PreferSnapshot: true}
	res, _ := SyncV2(wire.NewCodec(wire.EncodingJSON), stream.NewInMemoryTransport(), cfg)
	if err := res.Namespaces[0].Err; err == nil || !strings.Contains(err.Error(), "disk full") {
		t.Fatalf("expected the persist failure to be reported, got %v", err)
	}
	if mustHead(t, l) != before {
		t.Fatal("main moved to a snapshot that was not made durable")
	}
	_, head, _, _ := l.dag.HeadCommit()
	n := 0
	if err := l.storage.(storage.TreeWalker).WalkTree(ns, head.DocumentTreeHash, func(codec.UUID, codec.Hash) bool { n++; return true }); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("an undone snapshot left %d documents behind", n)
	}
	// And nothing of the snapshot's root is left in the DAG either. It used to stay resident
	// with nothing pointing at it, which is worse than untidy: a file runtime counts commits to
	// decide whether a namespace is still fresh enough to bootstrap, so the orphan refused every
	// later attempt for the life of the process. See TestSnapshotCanBeRetriedAfterAFailedInstall.
	if n := l.dag.CommitCount(); n != 1 {
		t.Fatalf("an undone snapshot left the DAG holding %d commits, want 1 (genesis)", n)
	}
	if roots := l.dag.ShallowRoots(); len(roots) != 0 {
		t.Fatalf("an undone snapshot left shallow roots behind: %v", roots)
	}
}

// TestSnapshotCanBeRetriedAfterAFailedInstall: the regression this exists for. A node whose
// bootstrap fails part-way - here because the snapshot could not be made durable - can be
// bootstrapped again once the cause is gone, in the same process. Before the root was taken back
// out of the DAG, the namespace was left holding a commit it had never bootstrapped from, and the
// up-front freshness check refused every retry for as long as the process lived.
func TestSnapshotCanBeRetriedAfterAFailedInstall(t *testing.T) {
	ns := "app/snap-retry"
	remote := newTestNamespaces(t, ns)
	local := newTestNamespaces(t, ns)
	seedHistory(t, remote, ns, 12)
	v2Hub(t, "hub-snap-retry", newHost(remote), nil)
	l := local.side(ns)

	// The same rule a file-backed runtime applies (embed.CanInstallSnapshot): a namespace holding
	// anything more than its genesis commit is no longer fresh enough to bootstrap.
	notFresh := errors.New("a snapshot can only bootstrap a namespace that has never had a commit")
	local.canInstall = func() error {
		if l.dag.CommitCount() > 1 {
			return notFresh
		}
		return nil
	}
	failing := true
	local.installed = func(document.Commit) error {
		if failing {
			return errors.New("disk full")
		}
		return nil
	}

	cfg := V2ClientConfig{PeerURI: "memory://hub-snap-retry", Local: local, NodeID: "c",
		Namespaces: []string{ns}, Mode: SyncPull, PreferSnapshot: true, PageBytes: 1_000}
	res, _ := SyncV2(wire.NewCodec(wire.EncodingJSON), stream.NewInMemoryTransport(), cfg)
	if err := res.Namespaces[0].Err; err == nil || !strings.Contains(err.Error(), "disk full") {
		t.Fatalf("expected the first attempt to fail on the persist hook, got %v", err)
	}
	if err := local.canInstall(); err != nil {
		t.Fatalf("the namespace is no longer bootstrappable after a failed attempt: %v", err)
	}

	failing = false
	res = syncV2(t, "hub-snap-retry", local, V2ClientConfig{Namespaces: []string{ns},
		Mode: SyncPull, PreferSnapshot: true, PageBytes: 1_000})
	rh := mustHead(t, remote.side(ns))
	if res.Namespaces[0].Snapshot != rh.Hex() {
		t.Fatalf("the retry did not install a snapshot: %+v", res.Namespaces[0])
	}
	if mustHead(t, l) != rh || !l.dag.IsShallow(rh) {
		t.Fatal("after the retry, local main is not the remote head as a shallow root")
	}
	// And the state came with it: the retry holds every document the peer's head tree does.
	count := func(s side, at codec.Hash) int {
		t.Helper()
		c, err := s.dag.GetCommitOrThrow(at)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		if err := s.storage.(storage.TreeWalker).WalkTree(ns, c.DocumentTreeHash, func(codec.UUID, codec.Hash) bool {
			n++
			return true
		}); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if got, want := count(l, rh), count(remote.side(ns), rh); got != want || want == 0 {
		t.Fatalf("the retried snapshot holds %d documents, the peer's head tree holds %d", got, want)
	}
}
