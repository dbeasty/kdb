package syncnode

import (
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/replication"
	"github.com/limidus/kdb/go/kdb/server"
)

// metaBodies is every definition document a node's metadata namespace holds at head.
func metaBodies(t *testing.T, n testNode) []string {
	t.Helper()
	var out []string
	for _, def := range must(n.node.Meta().View(func(string) bool { return true })) {
		out = append(out, def.Body)
	}
	return out
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// TestScopedPeerGetsOnlyWhatItMaySee (G4): a phone syncing its own user's namespace with
// meta=scoped receives the pattern definitions and its namespace's own, never another user's or a
// match's it is not in - and still merges, because its chain resolves to the cloud's.
func TestScopedPeerGetsOnlyWhatItMaySee(t *testing.T) {
	cloud := newTestNode(t, "zolik/cloud", Config{})
	if err := cloud.node.Start(); err != nil {
		t.Fatal(err)
	}
	ln, err := cloud.node.Listen("tcp://127.0.0.1:0?bind=true")
	if err != nil {
		t.Fatal(err)
	}
	lw := peersync.ResolutionChain{Rules: []peersync.ResolutionRule{{Kind: peersync.RuleLastWrite}}}
	q := peersync.ResolutionChain{Rules: []peersync.ResolutionRule{{Kind: peersync.RuleQueue}}}
	meta := cloud.node.Meta()
	for _, err := range []error{
		meta.SetResolution("zolik/u/*", lw),
		meta.SetResolution("zolik/u/2", q), // another user's override: private
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := meta.AssignHome("zolik/m/9", cloud.primary.NodeID.String(), "tcp://cloud"); err != nil {
		t.Fatal(err)
	}
	doc := mustID(t)
	cloud.put(t, "zolik/u/1", doc, `{"v":"base"}`)
	cloud.put(t, "zolik/u/2", mustID(t), `{"secret":true}`)

	phone := newTestNode(t, "zolik/phone", Config{Peers: []replication.PeerConfig{{
		Name: "cloud", Addr: "tcp://" + ln.Addr().String(), Namespaces: []string{"zolik/u/1"},
		Mode: peersync.SyncBoth, Interval: time.Hour, CreateLocal: true, ScopedMeta: true,
	}}})
	if err := phone.node.Start(); err != nil {
		t.Fatal(err)
	}
	res, err := phone.node.SyncNow("cloud")
	if err != nil {
		t.Fatal(err)
	}
	if res.MetaDefinitions != 1 {
		t.Fatalf("the phone should receive exactly the pattern (u/1 has no definition of its own), got %d", res.MetaDefinitions)
	}
	for _, ns := range res.Namespaces {
		if ns.Namespace == server.MetaNamespace {
			t.Fatal("a scoped peer must not sync the metadata namespace whole")
		}
	}
	for _, body := range metaBodies(t, phone) {
		if strings.Contains(body, "zolik/u/2") || strings.Contains(body, "zolik/m/9") {
			t.Fatalf("the phone learned another namespace's definition: %s", body)
		}
	}
	phoneU1, ok := phone.set.Get("zolik/u/1")
	cloudU1, _ := cloud.set.Get("zolik/u/1")
	if !ok || phoneU1.ResolutionChainOf().Hash() != cloudU1.ResolutionChainOf().Hash() || cloudU1.ResolutionChainOf().Hash() != lw.Hash() {
		t.Fatal("the phone's chain for its namespace should resolve to the cloud's")
	}

	// Because the chains agree, a divergent write merges instead of being held.
	phone.put(t, "zolik/u/1", doc, `{"v":"phone"}`)
	time.Sleep(2 * time.Millisecond)
	cloud.put(t, "zolik/u/1", doc, `{"v":"cloud"}`)
	res, err = phone.node.SyncNow("cloud")
	if err != nil {
		t.Fatal(err)
	}
	for _, ns := range res.Namespaces {
		if ns.Namespace == "zolik/u/1" && (ns.ResolutionMismatch || len(ns.Conflicts) > 0) {
			t.Fatalf("the namespace should merge by the shared chain: %+v", ns)
		}
	}
	if phone.get("zolik/u/1", doc) != cloud.get("zolik/u/1", doc) || cloud.get("zolik/u/1", doc) != `{"v":"cloud"}` {
		t.Fatalf("after merge: phone %s cloud %s", phone.get("zolik/u/1", doc), cloud.get("zolik/u/1", doc))
	}
}
