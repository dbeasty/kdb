package server

import (
	"errors"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

func syncPatterns(t *testing.T, from, to metaNode, patterns ...string) (peersync.V2Result, error) {
	t.Helper()
	ln, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", to.data, "app/data")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
		NodeID: from.data.NodeID.String(), PeerURI: "tcp://" + ln.Addr().String(), Namespaces: patterns, Local: from.data.PeerNamespaces(),
	})
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestSingleHomeRefusesWritesElsewhere: once a namespace is assigned a home, only that node takes
// its writes; the others still receive them by replication and refuse local writes with the
// home's address.
func TestSingleHomeRefusesWritesElsewhere(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	h, err := a.store.AssignHome("app/data", a.data.NodeID.String(), "tcp://a:9090")
	if err != nil || h.Fence != 1 {
		t.Fatalf("assign: %+v %v", h, err)
	}
	c, err := a.data.Upsert("app/data", mustRandomUUID(t), `{"on":"home"}`, auth.Principal{})
	if err != nil {
		t.Fatalf("the home refused its own write: %v", err)
	}
	if node, fence, ok := parseHomeStamp(c.Message); !ok || node != a.data.NodeID.String() || fence != 1 {
		t.Fatalf("the home's commit is not stamped: %q", c.Message)
	}
	if _, err := syncPatterns(t, a, b, "**"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "B to learn the assignment", func() bool { _, ok := b.data.HomeOf(); return ok })
	_, err = b.data.Upsert("app/data", mustRandomUUID(t), `{"on":"not home"}`, auth.Principal{})
	var nh *NotHomeError
	if !errors.As(err, &nh) || nh.Home.Addr != "tcp://a:9090" {
		t.Fatalf("expected NotHomeError naming the home, got %v", err)
	}
	if ha, _ := a.data.dag.Head(); ha != mustHeadOf(t, b.data) {
		t.Fatal("the home's write did not replicate to B")
	}
}

// TestStaleFenceRefusedAfterHandover: the home moves to B; A, not yet aware, writes at the old
// fence; B refuses to adopt that write, and A refuses writes once it learns.
func TestStaleFenceRefusedAfterHandover(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	if _, err := a.store.AssignHome("app/data", a.data.NodeID.String(), "tcp://a"); err != nil {
		t.Fatal(err)
	}
	if _, err := syncPatterns(t, a, b, "**"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "B to learn the first assignment", func() bool { h, ok := b.data.HomeOf(); return ok && h.Fence == 1 })

	// Handover on B; A has not heard.
	if h, err := b.store.AssignHome("app/data", b.data.NodeID.String(), "tcp://b"); err != nil || h.Fence != 2 {
		t.Fatalf("handover: %+v %v", h, err)
	}
	stale, err := a.data.Upsert("app/data", mustRandomUUID(t), `{"written":"after handover"}`, auth.Principal{})
	if err != nil {
		t.Fatalf("A does not know yet and should still write: %v", err)
	}
	res, err := syncPatterns(t, a, b, "app/data")
	if err != nil {
		t.Fatal(err)
	}
	if b.data.dag.IsAncestor(stale.Hash, mustHeadOf(t, b.data)) {
		t.Fatal("B adopted a commit made at a superseded fence")
	}
	if len(res.Namespaces) == 0 || res.Namespaces[0].Err == nil {
		t.Fatalf("the push of a stale write should have been refused: %+v", res)
	}

	// A learns the handover and stops accepting writes.
	if _, err := syncPatterns(t, b, a, "**"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "A to learn the handover", func() bool { h, _ := a.data.HomeOf(); return h.Fence == 2 })
	_, err = a.data.Upsert("app/data", mustRandomUUID(t), `{"x":1}`, auth.Principal{})
	var nh *NotHomeError
	if !errors.As(err, &nh) {
		t.Fatalf("the former home still accepts writes: %v", err)
	}
}

func mustHeadOf(t *testing.T, rt *KdbServerRuntime) codec.Hash {
	t.Helper()
	h, err := rt.dag.Head()
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// TestCrossNamespaceCommitRefusedAcrossHomes: a cross-namespace transaction can be atomic only
// where one node decides every part, so a part in a namespace homed elsewhere refuses the whole
// group.
func TestCrossNamespaceCommitRefusedAcrossHomes(t *testing.T) {
	rts := newNamespaceSetRuntimes(t, "a/local", "a/remote")
	other := mustRandomUUID(t)
	rts["a/remote"].SetHome(&Home{Node: other.String(), Addr: "tcp://elsewhere", Fence: 1})
	set := rts["a/local"].Namespaces
	var parts []NamespaceTransaction
	for _, ns := range []string{"a/local", "a/remote"} {
		head, _ := rts[ns].dag.Head()
		id := mustRandomUUID(t)
		parts = append(parts, NamespaceTransaction{Namespace: ns, Tx: document.Transaction{
			ID: mustRandomUUID(t), BaseVersion: head, Timestamp: codec.TimestampNow(),
			Operations: []document.Op{document.WriteOp{DocID: id, Patch: `{"x":1}`}},
		}})
	}
	_, err := set.CommitAcross(parts, auth.Principal{})
	var nh *NotHomeError
	if !errors.As(err, &nh) {
		t.Fatalf("expected the group refused with NotHomeError, got %v", err)
	}
}

// TestHandoverOnOldHomeKeepsItsWrites: a handover made on the current home captures everything
// it wrote - including writes it acknowledged but had not yet replicated - so the new home
// accepts them when they arrive, and takes no writes of its own until it has them.
func TestHandoverOnOldHomeKeepsItsWrites(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	if _, err := a.store.AssignHome("app/data", a.data.NodeID.String(), "tcp://a"); err != nil {
		t.Fatal(err)
	}
	if _, err := syncPatterns(t, a, b, "**"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "B to learn A is home", func() bool { _, ok := b.data.HomeOf(); return ok })
	late, err := a.data.Upsert("app/data", mustRandomUUID(t), `{"acknowledged":"before handover"}`, auth.Principal{})
	if err != nil {
		t.Fatal(err)
	}
	// Handover made on A, naming B; only the metadata reaches B first.
	h, err := a.store.AssignHome("app/data", b.data.NodeID.String(), "tcp://b")
	if err != nil || h.Since != late.Hash.Hex() {
		t.Fatalf("handover point %q, want A's head %s (%v)", h.Since, late.Hash.Hex(), err)
	}
	if _, err := a.data.Upsert("app/data", mustRandomUUID(t), `{"x":1}`, auth.Principal{}); !errors.As(err, new(*NotHomeError)) {
		t.Fatalf("A still accepts writes after handing over: %v", err)
	}
	if _, err := syncPatterns(t, a, b, metaOnly()...); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "B to learn it is home", func() bool { h, _ := b.data.HomeOf(); return h.Node == b.data.NodeID.String() })
	var unavailable *UnavailableError
	if _, err := b.data.Upsert("app/data", mustRandomUUID(t), `{"x":1}`, auth.Principal{}); !errors.As(err, &unavailable) {
		t.Fatalf("B took a write before holding the handover point: %v", err)
	}
	// Now the data: A's late write is part of the handover and is adopted.
	if _, err := syncPatterns(t, a, b, "app/data"); err != nil {
		t.Fatal(err)
	}
	if !b.data.dag.IsAncestor(late.Hash, mustHeadOf(t, b.data)) && mustHeadOf(t, b.data) != late.Hash {
		t.Fatal("B refused a write A made before the handover")
	}
	if _, err := b.data.Upsert("app/data", mustRandomUUID(t), `{"on":"new home"}`, auth.Principal{}); err != nil {
		t.Fatalf("the new home refused a write once it held the handover point: %v", err)
	}
}

func metaOnly() []string { return []string{MetaNamespace} }
