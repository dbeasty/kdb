package replication

import (
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/wire"
)

func newNode(t *testing.T, ns string) *server.KdbServerRuntime {
	t.Helper()
	rt, err := embed.OpenMemoryRuntime("app", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	srv := server.NewKdbServerRuntime(rt)
	id, _ := codec.RandomUUID()
	srv.NodeID = id // one process, two nodes: give each its own identity
	return srv
}

func put(t *testing.T, srv *server.KdbServerRuntime, body string) {
	t.Helper()
	id, _ := codec.RandomUUID()
	if _, err := srv.Upsert(srv.Runtime.DefaultNamespace, id, body, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
}

func head(t *testing.T, srv *server.KdbServerRuntime) codec.Hash {
	t.Helper()
	h, err := srv.Runtime.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func eventually(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestReplicatorConvergesTwoNodes: a node configured with one peer and nothing else keeps both
// in step - its own writes go out on the commit trigger, the peer's come in on the tick.
func TestReplicatorConvergesTwoNodes(t *testing.T) {
	ns := "app/data"
	a, b := newNode(t, ns), newNode(t, ns)
	ln, err := server.ListenPeerSync("tcp://127.0.0.1:0?bind=true", b, ns)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	r, err := New(Config{
		NodeID: a.NodeID.String(), Local: a.PeerNamespaces(),
		Peers: []PeerConfig{{Name: "b", Addr: "tcp://" + ln.Addr().String(), Namespaces: []string{ns}, Mode: peersync.SyncBoth, Interval: 100 * time.Millisecond}},
	})
	if err != nil {
		t.Fatal(err)
	}
	a.CommitListener = func(n string, _ document.Commit) { r.OnLocalCommit(n) }
	r.Start()
	defer r.Stop()

	put(t, a, `{"from":"a"}`)
	eventually(t, 3*time.Second, "a's write to reach b", func() bool { return head(t, a) == head(t, b) })
	put(t, b, `{"from":"b"}`)
	eventually(t, 3*time.Second, "b's write to reach a", func() bool { return head(t, a) == head(t, b) })

	st := r.Status()
	if len(st) != 1 || st[0].State.LastSuccess.IsZero() || st[0].State.PeerNodeID != b.NodeID.String() {
		t.Fatalf("status after syncing: %+v", st)
	}
	if ns := st[0].State.Namespaces[ns]; ns.RemoteMain == "" || ns.Pushed == 0 || ns.Pulled == 0 {
		t.Fatalf("namespace progress not recorded: %+v", ns)
	}
}

// TestReplicatorBackoffAndRecovery: a peer that does not answer counts failures and backs off;
// once it answers, the next attempt succeeds and the count resets.
func TestReplicatorBackoffAndRecovery(t *testing.T) {
	ns := "app/data"
	a, b := newNode(t, ns), newNode(t, ns)
	put(t, b, `{"from":"b"}`)
	hub := "hub-replicator-backoff"
	r, err := New(Config{
		NodeID: a.NodeID.String(), Local: a.PeerNamespaces(), Timeout: 50 * time.Millisecond,
		Peers:      []PeerConfig{{Name: "b", Addr: "memory://" + hub, Namespaces: []string{ns}, Interval: 20 * time.Millisecond}},
		Transport:  func(PeerConfig) stream.Transport { return stream.NewInMemoryTransport() },
		MaxBackoff: 80 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	r.Start()
	defer r.Stop()
	eventually(t, 3*time.Second, "failures to accumulate", func() bool { return r.Status()[0].State.ConsecutiveFailures >= 2 })

	host := peersync.NewV2Host(wire.NewCodec(wire.EncodingJSON), peersync.V2HostConfig{NodeID: b.NodeID.String(), Namespaces: b.PeerNamespaces()}, auth.AllowAll, auth.EmptyContext)
	h := stream.HubFor(hub)
	h.ServerHandler = func(frame []byte) {
		if out, err := host.HandleFrame(frame); err == nil && out != nil {
			h.ServerSend(out)
		}
	}
	defer func() { h.ServerHandler = nil }()
	eventually(t, 3*time.Second, "recovery", func() bool {
		s := r.Status()[0].State
		return s.ConsecutiveFailures == 0 && !s.LastSuccess.IsZero() && head(t, a) == head(t, b)
	})
}

// TestPeerStatePersistsAcrossRestart: what a replicator learned survives it.
func TestPeerStatePersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	st, err := NewStateStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Save(PeerState{Name: "cloud", PeerNodeID: "n1", Namespaces: map[string]NamespaceState{"a/b": {RemoteMain: "abc", Pulled: 3}}}); err != nil {
		t.Fatal(err)
	}
	again, _ := NewStateStore(dir)
	got, err := again.Load("cloud")
	if err != nil || got.PeerNodeID != "n1" || got.Namespaces["a/b"].RemoteMain != "abc" || got.Namespaces["a/b"].Pulled != 3 {
		t.Fatalf("reloaded %+v, %v", got, err)
	}
}
