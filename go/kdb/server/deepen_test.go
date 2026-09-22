package server

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

// bootstrapFrom has local, empty, install source's current state by snapshot.
func bootstrapFrom(t *testing.T, local, source *KdbServerRuntime) codec.Hash {
	t.Helper()
	ln, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", source, "app/data")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
		NodeID: local.NodeID.String(), PeerURI: "tcp://" + ln.Addr().String(), Namespaces: []string{"app/data"},
		Local: local.PeerNamespaces(), PreferSnapshot: true,
	})
	if err != nil || res.Namespaces[0].Err != nil || res.Namespaces[0].Snapshot == "" {
		t.Fatalf("snapshot bootstrap: %+v %v", res, err)
	}
	h, _ := codec.HashFromHex(res.Namespaces[0].Snapshot)
	return h
}

func openerFor(t *testing.T, peer *KdbServerRuntime) func() (*peersync.RepairSession, error) {
	t.Helper()
	ln, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", peer, "app/data")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	return func() (*peersync.RepairSession, error) {
		return peersync.OpenRepairSession(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
			NodeID: "deepener", PeerURI: "tcp://" + ln.Addr().String(), Namespaces: []string{"app/data"},
		})
	}
}

func writeN(t *testing.T, rt *KdbServerRuntime, prefix string, n int) []codec.Hash {
	t.Helper()
	var out []codec.Hash
	for i := 0; i < n; i++ {
		c, err := rt.Upsert("app/data", mustRandomUUID(t), fmt.Sprintf(`{"%s":%d}`, prefix, i), auth.Principal{})
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, c.Hash)
	}
	return out
}

func trySync(t *testing.T, from, to *KdbServerRuntime) error {
	t.Helper()
	ln, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", to, "app/data")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
		NodeID: from.NodeID.String(), PeerURI: "tcp://" + ln.Addr().String(), Namespaces: []string{"app/data"}, Local: from.PeerNamespaces(),
	})
	if err != nil {
		return err
	}
	return res.Namespaces[0].Err
}

// TestDeepenLetsASnapshotRootedNodeSyncWithAnIndependentOne: a node bootstrapped by snapshot cannot
// sync with a node that has history of its own (the root's parents are nowhere on it). Deepening
// from the snapshot's source fetches that history, and the two nodes then sync and merge like any
// others.
func TestDeepenLetsASnapshotRootedNodeSyncWithAnIndependentOne(t *testing.T) {
	dir := t.TempDir()
	rt, a := openFileNode(t, dir)
	defer func() { rt.Close() }()
	b, c := newMetaNode(t).data, newMetaNode(t).data
	history := writeN(t, b, "b", 5)
	root := bootstrapFrom(t, a, b)
	writeN(t, a, "a", 2)
	writeN(t, c, "c", 2)

	var unrelated *peersync.UnrelatedHistoryError
	if err := trySync(t, c, a); !errors.As(err, &unrelated) {
		t.Fatalf("before deepening, C and A should be unrelated histories, got %v", err)
	}

	results, err := a.Deepen(openerFor(t, b))
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Root != root || !results[0].Unshallowed || results[0].Fetched != len(history)-1 || len(results[0].Horizon) != 0 {
		t.Fatalf("deepen: %+v", results)
	}
	if roots := a.ShallowRoots(); len(roots) != 0 {
		t.Fatalf("no shallow roots should remain: %v", roots)
	}
	if !a.dag.HasCommit(history[0]) || !a.dag.IsAncestor(history[0], root) {
		t.Fatal("B's first commit should now be A's history, below the old root")
	}

	// And now C and A sync, both ways, and converge.
	if err := trySync(t, c, a); err != nil {
		t.Fatalf("after deepening, C and A should sync: %v", err)
	}
	if mustHeadOf(t, a) != mustHeadOf(t, c) {
		t.Fatal("C and A did not converge")
	}
}

// A deepened namespace keeps its history across a clean restart: the old root is no longer shallow,
// historical commits' operations load from the log, and the snapshot's documents still read.
func TestDeepenedHistoryIsDurable(t *testing.T) {
	dir := t.TempDir()
	rt, a := openFileNode(t, dir)
	b := newMetaNode(t).data
	ids := make([]codec.UUID, 4)
	for i := range ids {
		ids[i] = mustRandomUUID(t)
		if _, err := b.Upsert("app/data", ids[i], fmt.Sprintf(`{"v":%d}`, i), auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	first, _ := b.dag.GetCommit(mustHeadOf(t, b))
	firstCommit := first.ParentHashes[0] // an ancestor of the snapshot root
	root := bootstrapFrom(t, a, b)
	if _, err := a.Deepen(openerFor(t, b)); err != nil {
		t.Fatal(err)
	}
	rt.Close()

	rt, a = openFileNode(t, dir)
	defer rt.Close()
	if a.dag.IsShallow(root) || len(a.ShallowRoots()) != 0 {
		t.Fatal("the old root must stay unshallowed across a restart")
	}
	if _, err := a.dag.GetCommitOrThrow(firstCommit); err != nil {
		t.Fatalf("a deepened commit and its operations must be readable after a restart: %v", err)
	}
	if !a.dag.IsAncestor(firstCommit, root) {
		t.Fatal("ancestry across the old root must hold after a restart")
	}
	for i, id := range ids {
		if body, _, found, err := a.GetDocument("app/data", id); err != nil || !found || body != fmt.Sprintf(`{"v":%d}`, i) {
			t.Fatalf("snapshot document %d: %s %v %v", i, body, found, err)
		}
	}
}

func checkpointFiles(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.Contains(filepath.ToSlash(p), "/snap/kdb_checkpoint_") {
			b, _ := os.ReadFile(p)
			out[p] = b
		}
		return nil
	})
	return out
}

// A partial deepen - from a peer whose own history is shallow - leaves the peer's horizon as this
// namespace's new shallow root. The deepened commits are then in the log with a parentless one
// among them: a replay (as after a crash, before any newer checkpoint) must admit it as the
// horizon the marker names, not fail the open. Deepening again from a node with the whole history
// then finishes the job.
func TestPartialDeepenSurvivesReplayAndCanBeFinished(t *testing.T) {
	full := newMetaNode(t).data
	writeN(t, full, "full", 3)
	mid := newMetaNode(t).data
	midRoot := bootstrapFrom(t, mid, full) // mid's history starts at midRoot
	writeN(t, mid, "mid", 2)

	dir := t.TempDir()
	rt, a := openFileNode(t, dir)
	bootstrapFrom(t, a, mid)
	snapshotCheckpoint := checkpointFiles(t, dir)
	if len(snapshotCheckpoint) == 0 {
		t.Fatal("no snapshot checkpoint to roll back to")
	}

	res, err := a.Deepen(openerFor(t, mid))
	if err != nil {
		t.Fatal(err)
	}
	if len(res) == 0 || len(res[0].Horizon) != 1 || res[0].Horizon[0] != midRoot {
		t.Fatalf("the peer's own root should become the new horizon: %+v", res)
	}
	if roots := a.ShallowRoots(); len(roots) != 1 || roots[0] != midRoot.Hex() {
		t.Fatalf("shallow roots: %v", roots)
	}
	head := mustHeadOf(t, a)
	rt.Close()

	// Roll the checkpoint back to the snapshot's: what a crash before the next checkpoint leaves.
	for p, b := range snapshotCheckpoint {
		if err := os.WriteFile(p, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	rt, a = openFileNode(t, dir)
	if mustHeadOf(t, a) != head {
		t.Fatal("the head must survive a replay of the deepened log")
	}
	if !a.dag.IsShallow(midRoot) || !a.dag.HasCommit(midRoot) {
		t.Fatal("the horizon must come back as a shallow root")
	}

	// A node with the whole history finishes it.
	if _, err := a.Deepen(openerFor(t, full)); err != nil {
		t.Fatal(err)
	}
	if roots := a.ShallowRoots(); len(roots) != 0 {
		t.Fatalf("deepening from the full node should leave no shallow roots: %v", roots)
	}
	rt.Close()
	rt, a = openFileNode(t, dir)
	defer rt.Close()
	if len(a.ShallowRoots()) != 0 || mustHeadOf(t, a) != head {
		t.Fatal("a fully deepened namespace must reopen whole")
	}
}

// Deepening a namespace with no shallow roots does nothing and asks no peer.
func TestDeepenWithoutShallowRootsIsANoOp(t *testing.T) {
	a := newMetaNode(t).data
	writeN(t, a, "x", 2)
	res, err := a.Deepen(func() (*peersync.RepairSession, error) {
		return nil, errors.New("must not connect")
	})
	if err != nil || len(res) != 0 {
		t.Fatalf("%+v %v", res, err)
	}
}

// A crash after the history is logged but before the marker drops the root leaves the root marked
// shallow over history the namespace holds. Deepen run again finishes without fetching anything.
func TestDeepenInterruptedBeforeTheMarkerIsFinishedByRunningAgain(t *testing.T) {
	dir := t.TempDir()
	rt, a := openFileNode(t, dir)
	b := newMetaNode(t).data
	writeN(t, b, "b", 4)
	root := bootstrapFrom(t, a, b)
	snapshotCheckpoint := checkpointFiles(t, dir)
	metaPath := filepath.Join(dir, "ns", "app", "data", "meta.json")
	marker, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Deepen(openerFor(t, b)); err != nil {
		t.Fatal(err)
	}
	rt.Close()
	// Put back the pre-deepen marker and checkpoint: the history is in the log, nothing else.
	if err := os.WriteFile(metaPath, marker, 0o644); err != nil {
		t.Fatal(err)
	}
	for p, bytes := range snapshotCheckpoint {
		_ = os.WriteFile(p, bytes, 0o644)
	}

	rt, a = openFileNode(t, dir)
	defer rt.Close()
	if !a.dag.IsShallow(root) {
		t.Fatal("with the old marker, the root opens shallow")
	}
	res, err := a.Deepen(openerFor(t, b))
	if err != nil || len(res) != 1 || res[0].Fetched != 0 || !res[0].Unshallowed {
		t.Fatalf("running deepen again should finish without fetching: %+v %v", res, err)
	}
	if len(a.ShallowRoots()) != 0 {
		t.Fatal("no shallow roots should remain")
	}
}
