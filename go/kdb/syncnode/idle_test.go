package syncnode

import (
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/replication"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
)

// fileNode is a sync node over a file host at dir, with idle close configured.
func fileNode(t *testing.T, dir string, idle *IdleConfig) (testNode, *embed.Host) {
	t.Helper()
	host, err := embed.OpenFileHost(dir, embed.FileRuntimeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	rt, err := host.Namespace(embed.CatalogFromNamespace("zolik/cloud"), "zolik/cloud", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	primary := server.NewKdbServerRuntime(rt)
	primary.NodeID = codec.DerivedUUID("idle-cloud:" + dir)
	set := server.NewNamespaceSet(host.Transactions())
	if err := set.Add(primary); err != nil {
		t.Fatal(err)
	}
	node, err := Open(host, set, primary, Config{Idle: idle, DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	set.SetOpener(node.Opener(nil))
	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
	return testNode{node: node, set: set, primary: primary}, host
}

// TestIdleNamespacesCloseAndReopenOnSync (G6): the cloud closes namespaces nobody used; a phone's
// sync reaches one anyway - it is still served - and the cloud reopens it with its data and its
// definitions; across a restart the namespaces on disk are served without being opened.
func TestIdleNamespacesCloseAndReopenOnSync(t *testing.T) {
	dir := t.TempDir()
	cloud, host := fileNode(t, dir, &IdleConfig{MaxOpen: 1000, IdleAfter: time.Hour, MinIdle: time.Second})
	lw := peersync.ResolutionChain{Rules: []peersync.ResolutionRule{{Kind: peersync.RuleLastWrite}}}
	if err := cloud.node.Meta().SetResolution("zolik/u/*", lw); err != nil {
		t.Fatal(err)
	}
	profile := mustID(t)
	cloud.put(t, "zolik/u/1", profile, `{"name":"ada"}`)
	cloud.put(t, "zolik/u/2", mustID(t), `{"name":"bob"}`)

	closed := cloud.node.CloseIdle(time.Now().Add(2 * time.Hour))
	if len(closed) != 2 {
		t.Fatalf("both user namespaces should close, closed %v", closed)
	}
	if _, open := cloud.set.Get("zolik/u/1"); open {
		t.Fatal("zolik/u/1 is still open")
	}
	if _, open := cloud.set.Get("zolik/cloud"); !open {
		t.Fatal("the primary must never close")
	}

	ln, err := cloud.node.Listen("tcp://127.0.0.1:0?bind=true")
	if err != nil {
		t.Fatal(err)
	}
	phone := newTestNode(t, "zolik/phone", Config{Peers: []replication.PeerConfig{{
		Name: "cloud", Addr: "tcp://" + ln.Addr().String(), Namespaces: []string{"zolik/u/1"},
		Mode: peersync.SyncBoth, Interval: time.Hour, CreateLocal: true, ScopedMeta: true,
	}}})
	if err := phone.node.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := phone.node.SyncNow("cloud"); err != nil {
		t.Fatal(err)
	}
	if phone.get("zolik/u/1", profile) != `{"name":"ada"}` {
		t.Fatal("the phone should get the closed namespace's data")
	}
	reopened, open := cloud.set.Get("zolik/u/1")
	if !open || reopened.ResolutionChainOf().Hash() != lw.Hash() {
		t.Fatal("the reopened namespace should have its definitions applied afresh")
	}
	cloud.node.Close()
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}

	// Restarted: nothing but the primary opened, yet everything on disk is served.
	cloud2, host2 := fileNode(t, dir, &IdleConfig{IdleAfter: time.Hour})
	defer host2.Close()
	defer cloud2.node.Close()
	known := cloud2.set.KnownNamespaces()
	has := map[string]bool{}
	for _, ns := range known {
		has[ns] = true
	}
	if !has["zolik/u/1"] || !has["zolik/u/2"] {
		t.Fatalf("namespaces on disk should be known after a restart: %v", known)
	}
	if _, open := cloud2.set.Get("zolik/u/2"); open {
		t.Fatal("a restart should not open every namespace")
	}
}
