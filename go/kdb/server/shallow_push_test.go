package server

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

// pushOnly has local dial target and push to it, nothing else: target never dials anyone, which
// is the hub that #111's pull-side fix could not reach - a shallow node that is only ever dialled.
func pushOnly(t *testing.T, local, target *KdbServerRuntime) peersync.NamespaceSyncResult {
	t.Helper()
	ln, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", target, "app/data")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
		NodeID: local.NodeID.String(), PeerURI: "tcp://" + ln.Addr().String(), Namespaces: []string{"app/data"},
		Mode: peersync.SyncPush, Local: local.PeerNamespaces(),
	})
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	return res.Namespaces[0]
}

// TestPushIntoASnapshotNodeCarriesTheForkPointBelowItsRoot: the push direction of
// TestSnapshotNodeTakesABranchForkedBelowItsRoot. J, file-backed and bootstrapped from A's snapshot
// at S, never dials anyone; A dials J and pushes M, whose side C sits on P, a parent of S. A counts
// P as held by J (it is below S, which J advertises), so it never sends it, and J - which cannot
// fetch from a node that dialled it - refused C with "missing parent" on every push, for ever. The
// pusher now sends P's state on the same session and J takes it in as it would on a pull.
func TestPushIntoASnapshotNodeCarriesTheForkPointBelowItsRoot(t *testing.T) {
	t.Run("memory pusher", func(t *testing.T) { pushCarriesTheForkPoint(t, false) })
	t.Run("file pusher", func(t *testing.T) { pushCarriesTheForkPoint(t, true) })
}

func pushCarriesTheForkPoint(t *testing.T, filePusher bool) {
	dir := t.TempDir()
	rt, j := openFileNode(t, dir)
	defer func() { rt.Close() }()
	a, _, s, x, shared := boundaryFork(t, j, filePusher)

	r := pushOnly(t, a, j)
	if r.Err != nil {
		t.Fatalf("the push into the snapshot node failed: %v", r.Err)
	}
	if got := r.Remote["branch:main"]; got != wire.RefFastForwarded {
		t.Fatalf("J should fast-forward to A's head, got %q", got)
	}
	if len(r.BoundarySent) != 1 {
		t.Fatalf("exactly one fork point should have been sent, got %v", r.BoundarySent)
	}
	requireSameState(t, j, a)
	requireDoc(t, j, "J", shared, `{"v":"c"}`)
	if j.dag.HasCommit(x) {
		t.Fatal("J took history below its root; the push should carry only the fork point's state")
	}
	if !j.dag.IsShallow(s) {
		t.Fatal("the snapshot root should still be shallow")
	}
	head := mustHeadOf(t, j)

	// Durable: a clean restart keeps it, and J keeps taking pushes.
	rt.Close()
	rt, j = openFileNode(t, dir)
	if mustHeadOf(t, j) != head {
		t.Fatal("a clean restart lost the pushed head")
	}
	requireDoc(t, j, "J after a restart", shared, `{"v":"c"}`)
	putJSON(t, a, shared, `{"v":"after"}`)
	if r := pushOnly(t, a, j); r.Err != nil || len(r.BoundarySent) != 0 {
		t.Fatalf("the next push: %v (sent %v)", r.Err, r.BoundarySent)
	}
	requireSameState(t, j, a)
}

// TestPushMergesIntoASnapshotNodeAcrossItsBoundary: J has written since its bootstrap, so the push
// is a real merge on J, which has to trace the value C wrote down to the fork point below its root.
func TestPushMergesIntoASnapshotNodeAcrossItsBoundary(t *testing.T) {
	dir := t.TempDir()
	rt, j := openFileNode(t, dir)
	defer func() { rt.Close() }()
	a, _, _, _, shared := boundaryFork(t, j, true)
	mine := mustRandomUUID(t)
	putJSON(t, j, mine, `{"from":"j"}`)

	r := pushOnly(t, a, j)
	if r.Err != nil {
		t.Fatalf("the push into the snapshot node failed: %v", r.Err)
	}
	if got := r.Remote["branch:main"]; got != wire.RefMerged {
		t.Fatalf("J should merge A's head, got %q", got)
	}
	requireDoc(t, j, "J", shared, `{"v":"c"}`)
	requireDoc(t, j, "J", mine, `{"from":"j"}`)
	// A takes J's merge back and both agree.
	if r := pullOnly(t, a, j); r.Err != nil {
		t.Fatal(r.Err)
	}
	requireSameState(t, j, a)
	head := mustHeadOf(t, j)
	rt.Close()
	rt, j = openFileNode(t, dir)
	if mustHeadOf(t, j) != head {
		t.Fatal("a restart lost the merge")
	}
	requireDoc(t, j, "J after a restart", shared, `{"v":"c"}`)
}
