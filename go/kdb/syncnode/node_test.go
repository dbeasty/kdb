package syncnode

import (
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/replication"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
)

// testNode is an in-memory sync node: a namespace set with primary, opened through the node's
// default opener.
type testNode struct {
	node    *Node
	set     *server.NamespaceSet
	primary *server.KdbServerRuntime
}

func newTestNode(t *testing.T, primaryNS string, cfg Config) testNode {
	t.Helper()
	rt, err := embed.OpenMemoryRuntime(embed.CatalogFromNamespace(primaryNS), primaryNS, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	primary := server.NewKdbServerRuntime(rt)
	primary.NodeID, _ = codec.RandomUUID() // one process, several nodes
	primary.PeerCreateOnPush = true
	set := server.NewNamespaceSet(nil)
	if err := set.Add(primary); err != nil {
		t.Fatal(err)
	}
	if cfg.Debounce == 0 {
		cfg.Debounce = 5 * time.Millisecond
	}
	node, err := Open(nil, set, primary, cfg)
	if err != nil {
		t.Fatal(err)
	}
	set.SetOpener(node.Opener(nil))
	t.Cleanup(func() { node.Close() })
	return testNode{node: node, set: set, primary: primary}
}

func (n testNode) put(t *testing.T, ns string, id codec.UUID, body string) {
	t.Helper()
	rt, err := n.set.Resolve(ns, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Upsert(ns, id, body, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
}

func (n testNode) get(ns string, id codec.UUID) string {
	rt, ok := n.set.Get(ns)
	if !ok {
		return ""
	}
	body, _, found, err := rt.GetDocument(ns, id)
	if err != nil || !found {
		return ""
	}
	return body
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func mustID(t *testing.T) codec.UUID {
	t.Helper()
	id, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// cloudAndPhone is a cloud node listening for peers and a phone that dials it for patterns.
func cloudAndPhone(t *testing.T, patterns ...string) (cloud, phone testNode) {
	t.Helper()
	cloud = newTestNode(t, "zolik/cloud", Config{})
	if err := cloud.node.Start(); err != nil {
		t.Fatal(err)
	}
	ln, err := cloud.node.Listen("tcp://127.0.0.1:0?bind=true")
	if err != nil {
		t.Fatal(err)
	}
	phone = newTestNode(t, "zolik/phone", Config{Peers: []replication.PeerConfig{{
		Name: "cloud", Addr: "tcp://" + ln.Addr().String(), Namespaces: patterns,
		Mode: peersync.SyncBoth, Interval: 50 * time.Millisecond, CreateLocal: true,
	}}})
	return cloud, phone
}

// TestNodeSyncsNamespacesOpenedThroughTheSet: a node built from an application's set and primary
// replicates the namespaces its peer patterns select - in both directions, created on demand -
// and nothing else.
func TestNodeSyncsNamespacesOpenedThroughTheSet(t *testing.T) {
	cloud, phone := cloudAndPhone(t, "zolik/u/1")
	profile, other := mustID(t), mustID(t)
	cloud.put(t, "zolik/u/1", profile, `{"name":"ada"}`)
	cloud.put(t, "zolik/u/2", other, `{"name":"bob"}`)
	if err := phone.node.Start(); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the phone to pull its user's namespace", func() bool { return phone.get("zolik/u/1", profile) != "" })

	settings := mustID(t)
	phone.put(t, "zolik/u/1", settings, `{"theme":"dark"}`)
	eventually(t, "the phone's write to reach the cloud", func() bool { return cloud.get("zolik/u/1", settings) != "" })
	if _, ok := phone.set.Get("zolik/u/2"); ok {
		t.Fatal("the phone holds another user's namespace it never asked for")
	}
}

// TestPeerNamespacesChangeWhileRunning (G5): joining a match adds its namespace to the running
// replicator; the next cycle syncs it, and the peer's progress on what it already synced stays.
func TestPeerNamespacesChangeWhileRunning(t *testing.T) {
	cloud, phone := cloudAndPhone(t, "zolik/u/1")
	profile, move := mustID(t), mustID(t)
	cloud.put(t, "zolik/u/1", profile, `{"name":"ada"}`)
	cloud.put(t, "zolik/m/9", move, `{"move":"e4"}`)
	if err := phone.node.Start(); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the phone to pull its user's namespace", func() bool { return phone.get("zolik/u/1", profile) != "" })
	r := phone.node.Replicator()
	// Progress is recorded when the cycle that brought the document ends, a moment after it lands.
	eventually(t, "the user's namespace progress to be recorded", func() bool {
		return r.Status()[0].State.Namespaces["zolik/u/1"].RemoteMain != ""
	})
	before := r.Status()[0].State.Namespaces["zolik/u/1"]

	if err := r.AddNamespaces("cloud", "zolik/m/9"); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the joined match to sync", func() bool { return phone.get("zolik/m/9", move) != "" })
	st := r.Status()[0]
	if after := st.State.Namespaces["zolik/u/1"]; after.RemoteMain == "" || before.RemoteMain == "" {
		t.Fatalf("progress on the user's namespace was lost: before %+v after %+v", before, after)
	}
	if got := st.Namespaces; len(got) != 3 { // u/1, m/9 and the metadata namespace
		t.Fatalf("peer patterns after joining: %v", got)
	}

	// Leaving the match stops syncing it; what was synced stays.
	if err := r.RemoveNamespaces("cloud", "zolik/m/9"); err != nil {
		t.Fatal(err)
	}
	// A cycle already under way read the old patterns when it began; SyncNow waits for it, and
	// every cycle after reads the new ones.
	if _, err := phone.node.SyncNow("cloud"); err != nil {
		t.Fatal(err)
	}
	later := mustID(t)
	cloud.put(t, "zolik/m/9", later, `{"move":"e5"}`)
	if _, err := phone.node.SyncNow("cloud"); err != nil {
		t.Fatal(err)
	}
	if phone.get("zolik/m/9", later) != "" || phone.get("zolik/m/9", move) == "" {
		t.Fatal("after leaving, the match should keep what it had and receive nothing new")
	}
	if err := r.SetPeerNamespaces("cloud", nil); err == nil {
		t.Fatal("an empty pattern set must be refused")
	}
}

// TestDefinitionsTravelThroughTheNode: a resolution chain recorded on the cloud reaches the phone
// through the metadata namespace the node adds to every peer, and applies to the phone's
// namespace.
func TestDefinitionsTravelThroughTheNode(t *testing.T) {
	cloud, phone := cloudAndPhone(t, "zolik/u/1")
	cloud.put(t, "zolik/u/1", mustID(t), `{"x":1}`)
	chain := &peersync.ResolutionChain{Rules: []peersync.ResolutionRule{{Kind: peersync.RuleLastWrite}}}
	if err := cloud.node.Meta().SetResolution("zolik/u/1", *chain); err != nil {
		t.Fatal(err)
	}
	if err := phone.node.Start(); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the chain to reach the phone and apply", func() bool {
		rt, ok := phone.set.Get("zolik/u/1")
		return ok && rt.ResolutionChainOf().Hash() == chain.Hash()
	})
}
