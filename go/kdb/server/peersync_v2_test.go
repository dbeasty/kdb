package server

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

// newNamespaceSetRuntimes opens one memory runtime per namespace, all in one NamespaceSet - the
// shape of a kdb-service process serving several namespaces.
func newNamespaceSetRuntimes(t *testing.T, namespaces ...string) map[string]*KdbServerRuntime {
	t.Helper()
	set := NewNamespaceSet(nil)
	out := map[string]*KdbServerRuntime{}
	for _, ns := range namespaces {
		rt, err := embed.OpenMemoryRuntime("app", ns, schema.None())
		if err != nil {
			t.Fatal(err)
		}
		srv := NewKdbServerRuntime(rt)
		if err := set.Add(srv); err != nil {
			t.Fatal(err)
		}
		srv.Namespaces = set
		out[ns] = srv
	}
	return out
}

// TestV2SyncsSeveralNamespacesOverOneConnection: one v2 session against a real listener syncs
// every namespace the pattern selects, both ways, through each namespace's own write gate.
func TestV2SyncsSeveralNamespacesOverOneConnection(t *testing.T) {
	a := newNamespaceSetRuntimes(t, "site/one", "site/two", "other/x")
	b := newNamespaceSetRuntimes(t, "site/one", "site/two", "other/x")
	for ns, rt := range a {
		if _, err := rt.Upsert(ns, mustRandomUUID(t), `{"from":"a"}`, auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	for ns, rt := range b {
		if _, err := rt.Upsert(ns, mustRandomUUID(t), `{"from":"b"}`, auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	ln, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", b["site/one"], "site/one")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
		NodeID: a["site/one"].NodeID.String() + "-a", PeerURI: "tcp://" + ln.Addr().String(),
		Namespaces: []string{"site/*"}, Local: a["site/one"].PeerNamespaces(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Namespaces) != 2 {
		t.Fatalf("expected site/one and site/two, got %+v", res.Namespaces)
	}
	for _, ns := range []string{"site/one", "site/two"} {
		ha, _ := a[ns].dag.Head()
		hb, _ := b[ns].dag.Head()
		if ha != hb {
			t.Fatalf("%s: heads differ after sync", ns)
		}
	}
	ha, _ := a["other/x"].dag.Head()
	hb, _ := b["other/x"].dag.Head()
	if ha == hb {
		t.Fatal("other/x was synced although the pattern excludes it")
	}
}
