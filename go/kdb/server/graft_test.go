package server

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

func allowUnrelated() *peersync.ResolutionChain {
	return &peersync.ResolutionChain{AllowUnrelated: true, Rules: []peersync.ResolutionRule{{Kind: peersync.RuleLastWrite}}}
}

// graftScenario is Phase 11's case on real runtimes: A was bootstrapped from B's snapshot and B is
// gone; C has history of its own. It returns A's snapshot root and a document of B's.
func graftScenario(t *testing.T, a, c *KdbServerRuntime) (root codec.Hash, fromB codec.UUID) {
	t.Helper()
	b := newMetaNode(t).data
	writeN(t, b, "b", 4)
	fromB = mustRandomUUID(t)
	if _, err := b.Upsert("app/data", fromB, `{"from":"b"}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	root = bootstrapFrom(t, a, b)
	writeN(t, a, "a", 2)
	writeN(t, c, "c", 3)
	a.SetResolutionChain(allowUnrelated())
	c.SetResolutionChain(allowUnrelated())
	return root, fromB
}

func requireSameState(t *testing.T, x, y *KdbServerRuntime) {
	t.Helper()
	if mustHeadOf(t, x) != mustHeadOf(t, y) {
		t.Fatalf("heads differ: %s vs %s", mustHeadOf(t, x).Hex(), mustHeadOf(t, y).Hex())
	}
	hx, _ := x.HeadTree()
	hy, _ := y.HeadTree()
	if hx.TreeHash != hy.TreeHash {
		t.Fatalf("trees differ: %s vs %s", hx.TreeHash.Hex(), hy.TreeHash.Hex())
	}
}

// TestGraftMergesAnUnrelatedHistoryDurably: C, file-backed, syncs with A, whose history nobody
// else holds. C grafts A's root, merges, and both converge; C keeps it all across a clean restart
// and across a replay of its log with no checkpoint (a crash before the first one).
func TestGraftMergesAnUnrelatedHistoryDurably(t *testing.T) {
	a := newMetaNode(t).data
	dir := t.TempDir()
	rt, c := openFileNode(t, dir)
	root, fromB := graftScenario(t, a, c)

	if err := trySync(t, c, a); err != nil {
		t.Fatalf("with allowUnrelated, C should graft and merge: %v", err)
	}
	requireSameState(t, c, a)
	if !c.dag.IsShallow(root) {
		t.Fatal("A's root should be a shallow root on C")
	}
	head := mustHeadOf(t, c)
	readB := func(c *KdbServerRuntime) {
		t.Helper()
		if body, _, found, err := c.GetDocument("app/data", fromB); err != nil || !found || body != `{"from":"b"}` {
			t.Fatalf("B's document on C: %q %v %v", body, found, err)
		}
	}
	readB(c)
	rt.Close()

	rt, c = openFileNode(t, dir)
	if mustHeadOf(t, c) != head || !c.dag.IsShallow(root) {
		t.Fatal("a clean restart lost the graft")
	}
	readB(c)
	rt.Close()

	for p := range checkpointFiles(t, dir) {
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
	}
	rt, c = openFileNode(t, dir)
	defer rt.Close()
	if mustHeadOf(t, c) != head || !c.dag.IsShallow(root) {
		t.Fatal("a replay of the log without a checkpoint lost the graft")
	}
	readB(c)
	c.SetResolutionChain(allowUnrelated())

	// And they keep syncing as ordinary peers.
	writeN(t, a, "after", 1)
	if err := trySync(t, c, a); err != nil {
		t.Fatal(err)
	}
	requireSameState(t, c, a)
}

// TestGraftPushedToAHostThatOnlyEverIsDialled: A, snapshot-rooted, dials C and is the only one
// that ever does - an edge dialling its hub. A merges C's history (it needs no graft: C's history
// reaches genesis) and pushes the merge; C, which lacks everything below A's root, takes the root
// by GRAFT_PUSH first. C is file-backed and keeps it across a restart.
func TestGraftPushedToAHostThatOnlyEverIsDialled(t *testing.T) {
	a := newMetaNode(t).data
	dir := t.TempDir()
	rt, c := openFileNode(t, dir)
	root, fromB := graftScenario(t, a, c)
	if err := trySync(t, a, c); err != nil {
		t.Fatalf("A should merge C's history and push it: %v", err)
	}
	requireSameState(t, a, c)
	head := mustHeadOf(t, c)
	rt.Close()

	rt, c = openFileNode(t, dir)
	defer rt.Close()
	if mustHeadOf(t, c) != head || !c.dag.IsShallow(root) {
		t.Fatal("the pushed graft did not survive a restart")
	}
	if body, _, found, err := c.GetDocument("app/data", fromB); err != nil || !found || body != `{"from":"b"}` {
		t.Fatalf("B's document on C after restart: %q %v %v", body, found, err)
	}
}

// TestGraftRefusedUnderHistoryNone: without tree objects a grafted tree has nowhere to live, so a
// history=none namespace refuses with a reason naming deepen, and stays as it was.
func TestGraftRefusedUnderHistoryNone(t *testing.T) {
	a := newMetaNode(t).data
	opts := embed.FileRuntimeOptions{}
	opts.Storage.HistoryMode = storage.HistoryModeNone
	rt, err := embed.OpenFileRuntimeWithOptions(t.TempDir(), "app", "app/data", schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	c := NewKdbServerRuntime(rt)
	c.NodeID = codec.DerivedUUID("graft-none")
	root, _ := graftScenario(t, a, c)
	before := mustHeadOf(t, c)

	err = trySync(t, c, a)
	var unrelated *peersync.UnrelatedHistoryError
	if !errors.As(err, &unrelated) || !strings.Contains(err.Error(), "deepen") {
		t.Fatalf("want an unrelated-history refusal pointing at deepen, got %v", err)
	}
	if c.dag.HasCommit(root) || mustHeadOf(t, c) != before {
		t.Fatal("a refused graft changed C")
	}
}
