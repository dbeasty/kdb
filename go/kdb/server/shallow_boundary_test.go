package server

import (
	"os"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

// pullOnly pulls app/data from source into local and nothing else: source never sees local's
// history, which is how the test below shapes a DAG deterministically.
func pullOnly(t *testing.T, local, source *KdbServerRuntime) peersync.NamespaceSyncResult {
	t.Helper()
	ln, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", source, "app/data")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
		NodeID: local.NodeID.String(), PeerURI: "tcp://" + ln.Addr().String(), Namespaces: []string{"app/data"},
		Mode: peersync.SyncPull, Local: local.PeerNamespaces(),
	})
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	return res.Namespaces[0]
}

func putJSON(t *testing.T, rt *KdbServerRuntime, id codec.UUID, body string) codec.Hash {
	t.Helper()
	c, err := rt.PutJSON("app/data", id, body, nil, auth.Principal{})
	if err != nil {
		t.Fatal(err)
	}
	return c.Hash
}

func requireDoc(t *testing.T, rt *KdbServerRuntime, label string, id codec.UUID, want string) {
	t.Helper()
	body, _, found, err := rt.GetDocument("app/data", id)
	if err != nil || !found || body != want {
		t.Fatalf("%s: document %s = %q found=%v err=%v, want %q", label, id, body, found, err, want)
	}
}

// boundaryFork builds the multi-writer topology the late-joiner e2e test hit:
//
//	... -> X -> A1 ----\
//	         \          S (A's merge of A1 and P) -> M (A merges C)
//	          -> P ----/                                         /
//	              \-> C (B, still on P, never saw S) -----------/
//
// J is bootstrapped from A's snapshot at S - it holds S and nothing behind it - and then A takes C.
// A's pack for J is "everything from M not below J's haves"; P is below S, so it is never sent,
// although J does not hold it. It returns S, X and the document C rewrote.
//
// A is file-backed when fileSource is set: it is the node that has to serve P's state, and P's tree
// is one it never built - it merged P rather than fast-forwarding onto it.
func boundaryFork(t *testing.T, j *KdbServerRuntime, fileSource bool) (a, b *KdbServerRuntime, s, x codec.Hash, shared codec.UUID) {
	t.Helper()
	a, b = newMetaNode(t).data, newMetaNode(t).data
	if fileSource {
		var art interface{ Close() }
		art, a = openFileNode(t, t.TempDir())
		t.Cleanup(func() { art.Close() })
	}
	writeN(t, a, "history", 6)
	x = mustHeadOf(t, a)
	if r := pullOnly(t, b, a); r.Err != nil {
		t.Fatalf("B takes A's history: %v", r.Err)
	}
	putJSON(t, a, mustRandomUUID(t), `{"side":"a1"}`)
	shared = mustRandomUUID(t)
	p := putJSON(t, b, shared, `{"v":"p"}`)
	if r := pullOnly(t, a, b); r.Err != nil {
		t.Fatalf("A takes P: %v", r.Err)
	}
	s = mustHeadOf(t, a)
	if sc, _ := a.dag.GetCommit(s); len(sc.ParentHashes) != 2 || (sc.ParentHashes[0] != p && sc.ParentHashes[1] != p) {
		t.Fatalf("A's head should be a merge with P as a parent: %+v", sc.ParentHashes)
	}
	if root := bootstrapFrom(t, j, a); root != s {
		t.Fatalf("J was bootstrapped at %s, want %s", root.Hex(), s.Hex())
	}
	c := putJSON(t, b, shared, `{"v":"c"}`) // on P: B never pulled S
	if cc, _ := b.dag.GetCommit(c); len(cc.ParentHashes) != 1 || cc.ParentHashes[0] != p {
		t.Fatalf("C should sit directly on P: %+v", cc.ParentHashes)
	}
	if r := pullOnly(t, a, b); r.Err != nil {
		t.Fatalf("A takes C: %v", r.Err)
	}
	requireDoc(t, a, "A after merging C", shared, `{"v":"c"}`)
	return a, b, s, x, shared
}

// TestSnapshotNodeTakesABranchForkedBelowItsRoot: a node bootstrapped from a snapshot follows a
// peer whose history gained a branch forked from behind the snapshot's root. Before the fix every
// sync failed, forever, on "missing parent": the peer counted the fork point as held because it
// is an ancestor of the joiner's root. The joiner now takes the fork point's state - not its
// history - and converges, durably.
func TestSnapshotNodeTakesABranchForkedBelowItsRoot(t *testing.T) {
	t.Run("memory source", func(t *testing.T) { takesABranchForkedBelowItsRoot(t, false) })
	t.Run("file source", func(t *testing.T) { takesABranchForkedBelowItsRoot(t, true) })
}

func takesABranchForkedBelowItsRoot(t *testing.T, fileSource bool) {
	dir := t.TempDir()
	rt, j := openFileNode(t, dir)
	defer func() { rt.Close() }()
	a, _, s, x, shared := boundaryFork(t, j, fileSource)
	bootstrapCheckpoint := checkpointFiles(t, dir)

	r := pullOnly(t, j, a)
	if r.Err != nil {
		t.Fatalf("the snapshot node could not take the peer's head: %v", r.Err)
	}
	if len(r.BoundaryExtended) != 1 {
		t.Fatalf("exactly one fork point should have been taken in, got %v", r.BoundaryExtended)
	}
	requireSameState(t, j, a)
	requireDoc(t, j, "J", shared, `{"v":"c"}`)
	if j.dag.HasCommit(x) {
		t.Fatal("J fetched history below its root; it should have taken only the fork point's state")
	}
	if n, full := j.dag.CommitCount(), a.dag.CommitCount(); n >= full {
		t.Fatalf("J holds %d commits, A %d: that is the full history, not a snapshot's", n, full)
	}
	if !j.dag.IsShallow(s) {
		t.Fatal("the snapshot root should still be shallow")
	}
	head := mustHeadOf(t, j)

	// Clean restart.
	rt.Close()
	rt, j = openFileNode(t, dir)
	if mustHeadOf(t, j) != head {
		t.Fatal("a clean restart lost the head")
	}
	requireDoc(t, j, "J after a restart", shared, `{"v":"c"}`)
	rt.Close()

	// A crash before any checkpoint after the bootstrap: the bootstrap's checkpoint plus the whole
	// log tail, which now holds the fork point taken in without its parents.
	for p := range checkpointFiles(t, dir) {
		if _, ok := bootstrapCheckpoint[p]; !ok {
			if err := os.Remove(p); err != nil {
				t.Fatal(err)
			}
		}
	}
	for p, data := range bootstrapCheckpoint {
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	rt, j = openFileNode(t, dir)
	if mustHeadOf(t, j) != head {
		t.Fatalf("replaying the log onto the bootstrap checkpoint: head %s, want %s", mustHeadOf(t, j).Hex(), head.Hex())
	}
	requireDoc(t, j, "J after replaying onto the bootstrap checkpoint", shared, `{"v":"c"}`)

	// And it keeps following.
	putJSON(t, a, shared, `{"v":"after"}`)
	if r := pullOnly(t, j, a); r.Err != nil {
		t.Fatalf("after the restart: %v", r.Err)
	}
	requireSameState(t, j, a)
}

// TestSnapshotNodeMergesABranchForkedBelowItsRoot: as above, but J has written since its bootstrap,
// so taking the peer's head is a real merge, and the value C wrote has to be traced back to the
// fork point below J's root. Both sides then agree.
func TestSnapshotNodeMergesABranchForkedBelowItsRoot(t *testing.T) {
	dir := t.TempDir()
	rt, j := openFileNode(t, dir)
	defer func() { rt.Close() }()
	a, _, _, x, shared := boundaryFork(t, j, true)
	mine := mustRandomUUID(t)
	putJSON(t, j, mine, `{"from":"j"}`)

	if err := trySync(t, j, a); err != nil {
		t.Fatalf("J could not merge the peer's head: %v", err)
	}
	requireSameState(t, j, a)
	requireDoc(t, j, "J", shared, `{"v":"c"}`)
	requireDoc(t, a, "A", mine, `{"from":"j"}`)
	if j.dag.HasCommit(x) {
		t.Fatal("J fetched history below its root")
	}
	head := mustHeadOf(t, j)
	rt.Close()
	rt, j = openFileNode(t, dir)
	if mustHeadOf(t, j) != head {
		t.Fatal("a restart lost the merge")
	}
	requireDoc(t, j, "J after a restart", shared, `{"v":"c"}`)
	requireDoc(t, j, "J after a restart", mine, `{"from":"j"}`)
}

// TestDeepenAfterTakingAForkPointBelowTheRoot: deepening a node that took a fork point in below its
// root - the recovery a node may still run later - leaves no shallow roots, keeps the head, and
// survives a restart and a replay of the log with no checkpoint (the fork point's frame is logged
// before the history below it).
func TestDeepenAfterTakingAForkPointBelowTheRoot(t *testing.T) {
	dir := t.TempDir()
	rt, j := openFileNode(t, dir)
	defer func() { rt.Close() }()
	a, _, _, x, shared := boundaryFork(t, j, true)
	if r := pullOnly(t, j, a); r.Err != nil || len(r.BoundaryExtended) != 1 {
		t.Fatalf("pull: %v %v", r.Err, r.BoundaryExtended)
	}
	if _, err := j.Deepen(openerFor(t, a)); err != nil {
		t.Fatalf("deepen: %v", err)
	}
	if roots := j.ShallowRoots(); len(roots) != 0 {
		t.Fatalf("no shallow roots should remain after deepening: %v", roots)
	}
	if !j.dag.HasCommit(x) {
		t.Fatal("deepening should have fetched the history below the root")
	}
	head := mustHeadOf(t, j)
	rt.Close()

	rt, j = openFileNode(t, dir)
	if mustHeadOf(t, j) != head || len(j.ShallowRoots()) != 0 {
		t.Fatal("a clean restart lost the deepened history")
	}
	requireDoc(t, j, "J after a restart", shared, `{"v":"c"}`)
	rt.Close()

	for p := range checkpointFiles(t, dir) {
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
	}
	rt, j = openFileNode(t, dir)
	if mustHeadOf(t, j) != head {
		t.Fatal("a replay of the log with no checkpoint lost the head")
	}
	requireDoc(t, j, "J after a replay", shared, `{"v":"c"}`)
}
