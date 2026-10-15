package replication

import (
	"errors"
	"fmt"
	"github.com/limidus/kdb/go/kdb/internal/testtime"
	"io"
	"net"
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
	deadline := time.Now().Add(testtime.Scale(within))
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

	// The heads meet as soon as the pull is adopted, a moment before the cycle that did it records
	// its progress - so the status is waited for, not read at once.
	eventually(t, 3*time.Second, "the sync's progress to be recorded", func() bool {
		st := r.Status()
		if len(st) != 1 {
			return false
		}
		p := st[0].State.Namespaces[ns]
		return p.RemoteMain != "" && p.Pushed > 0 && p.Pulled > 0
	})
	st := r.Status()
	if st[0].State.LastSuccess.IsZero() || st[0].State.PeerNodeID != b.NodeID.String() {
		t.Fatalf("status after syncing: %+v", st)
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

// TestPeerFloorOnlyCountsActivePushPeers: the retention floor for a namespace is the oldest last
// sync among peers that receive it from this node and have been heard from within the grace.
func TestPeerFloorOnlyCountsActivePushPeers(t *testing.T) {
	now := time.Now()
	st, _ := NewStateStore("")
	save := func(name string, lastSuccess, synced time.Time) {
		_ = st.Save(PeerState{Name: name, LastSuccess: lastSuccess, Namespaces: map[string]NamespaceState{"app/data": {LastSync: synced}}})
	}
	save("fresh", now, now.Add(-time.Minute))
	save("behind", now.Add(-time.Hour), now.Add(-time.Hour))
	save("silent", now.Add(-30*24*time.Hour), now.Add(-30*24*time.Hour))
	save("puller", now, now.Add(-48*time.Hour))
	r, err := New(Config{Local: newNode(t, "app/data").PeerNamespaces(), State: st, Peers: []PeerConfig{
		{Name: "fresh", Addr: "tcp://x:1", Namespaces: []string{"app/*"}, Mode: peersync.SyncBoth},
		{Name: "behind", Addr: "tcp://x:2", Namespaces: []string{"app/data"}, Mode: peersync.SyncPush},
		{Name: "silent", Addr: "tcp://x:3", Namespaces: []string{"**"}, Mode: peersync.SyncBoth},
		{Name: "puller", Addr: "tcp://x:4", Namespaces: []string{"**"}, Mode: peersync.SyncPull},
	}})
	if err != nil {
		t.Fatal(err)
	}
	floor, ok := r.PeerFloor("app/data", 7*24*time.Hour, now)
	if !ok || !floor.Equal(now.Add(-time.Hour)) {
		t.Fatalf("floor %v %v, want the behind peer's last sync", floor, ok)
	}
	if _, ok := r.PeerFloor("other/ns", 7*24*time.Hour, now); ok {
		t.Fatal("a namespace no pushing peer syncs has no floor")
	}
}

// TestReplicatorDeepensASnapshotBootstrap: a peer configured bootstrap=snapshot,deepen=true
// installs the peer's state by snapshot, then fetches the history below it - so the namespace
// ends with no shallow roots and the peer's first commit in its history.
func TestReplicatorDeepensASnapshotBootstrap(t *testing.T) {
	ns := "app/data"
	a, b := newNode(t, ns), newNode(t, ns)
	put(t, b, `{"n":1}`)
	put(t, b, `{"n":2}`)
	put(t, b, `{"n":3}`)
	var first codec.Hash
	{
		h := head(t, b)
		for {
			c, _ := b.Runtime.DAG.GetCommitOrThrow(h)
			if len(c.ParentHashes) == 0 {
				break
			}
			first, h = h, c.ParentHashes[0]
		}
	}
	ln, err := server.ListenPeerSync("tcp://127.0.0.1:0?bind=true", b, ns)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	r, err := New(Config{
		NodeID: a.NodeID.String(), Local: a.PeerNamespaces(),
		Peers: []PeerConfig{{Name: "b", Addr: "tcp://" + ln.Addr().String(), Namespaces: []string{ns}, Mode: peersync.SyncPull,
			Interval: 100 * time.Millisecond, PreferSnapshot: true, Deepen: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	r.Start()
	defer r.Stop()
	eventually(t, 3*time.Second, "the snapshot to be installed and deepened", func() bool {
		return head(t, a) == head(t, b) && len(a.ShallowRoots()) == 0 && a.Runtime.DAG.(interface{ HasCommit(codec.Hash) bool }).HasCommit(first)
	})
}

// A repair asks the healthiest peer first: one that keeps failing goes to the back, whatever its
// name.
func TestRepairPeersAreAskedHealthiestFirst(t *testing.T) {
	st, _ := NewStateStore("")
	r, err := New(Config{
		NodeID: "x", Local: newNode(t, "app/data").PeerNamespaces(), State: st,
		Peers: []PeerConfig{{Name: "a-down", Addr: "tcp://127.0.0.1:1"}, {Name: "b-stale", Addr: "tcp://127.0.0.1:2"}, {Name: "c-fresh", Addr: "tcp://127.0.0.1:3"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for _, s := range []PeerState{
		{Name: "a-down", ConsecutiveFailures: 4, LastSuccess: now.Add(-time.Hour)},
		{Name: "b-stale", LastSuccess: now.Add(-time.Minute)},
		{Name: "c-fresh", LastSuccess: now},
	} {
		if err := st.Save(s); err != nil {
			t.Fatal(err)
		}
	}
	got := r.healthiestFirst()
	if len(got) != 3 || got[0] != "c-fresh" || got[1] != "b-stale" || got[2] != "a-down" {
		t.Fatalf("expected c-fresh, b-stale, a-down; got %v", got)
	}
}

// TestDeepenRunsAfterAShallowBoundaryError: a joiner whose pull failed because the peer could not
// supply a fork point's state behind its snapshot root (a ShallowBoundaryError, typically from an
// older peer) must still be deepened when deepen=true - that error names deepen as the cure. Any
// other failure still skips deepen. Before, every failed sync skipped it, so a stuck joiner
// configured with deepen=true stayed stuck (found by the mixed-version upgrade test, #125).
func TestDeepenRunsAfterAShallowBoundaryError(t *testing.T) {
	boundary := &peersync.ShallowBoundaryError{Namespace: "app/data", Cause: errors.New("peer: INTERNAL: no tree")}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"sync succeeded", nil, true},
		{"shallow boundary", boundary, true},
		{"shallow boundary, wrapped", fmt.Errorf("namespace app/data: %w", boundary), true},
		{"connection lost", errors.New("dial tcp: connection refused"), false},
	}
	for _, c := range cases {
		if got := deepenAfter(peersync.NamespaceSyncResult{Namespace: "app/data", Err: c.err}); got != c.want {
			t.Errorf("%s: deepenAfter = %v, want %v", c.name, got, c.want)
		}
	}
}

// slowAcceptProxy forwards connections to target, each only after delay: a peer that is slow to
// answer, so the peers dialled alongside it reliably go first.
func slowAcceptProxy(t *testing.T, target string, delay time.Duration) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			in, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer in.Close()
				time.Sleep(delay)
				out, err := net.Dial("tcp", target)
				if err != nil {
					return
				}
				defer out.Close()
				go io.Copy(out, in)
				io.Copy(in, out)
			}()
		}
	}()
	return ln.Addr().String()
}

// TestSnapshotPeerBootstrapsBeforeOtherPeers: a node joining two peers, one with bootstrap=snapshot,
// is bootstrapped from that peer's snapshot even when the other peer answers first. A node's pulls
// of a namespace take turns, and the snapshot decision is made from what the node holds when its
// turn comes: before, the plain peer's pull usually won the race, the namespace was no longer empty
// by the snapshot peer's turn, and its whole history was fetched instead (test_deepen.py flaked 7
// times in 10).
func TestSnapshotPeerBootstrapsBeforeOtherPeers(t *testing.T) {
	ns := "app/data"
	a, b, c := newNode(t, ns), newNode(t, ns), newNode(t, ns)
	for i := 0; i < 5; i++ {
		put(t, b, fmt.Sprintf(`{"from":"b","n":%d}`, i))
	}
	put(t, c, `{"from":"c"}`)
	var first codec.Hash // b's oldest commit above its root: a snapshot of b's main leaves it out
	{
		h := head(t, b)
		for {
			cm, _ := b.Runtime.DAG.GetCommitOrThrow(h)
			if len(cm.ParentHashes) == 0 {
				break
			}
			first, h = h, cm.ParentHashes[0]
		}
	}
	lnB, err := server.ListenPeerSync("tcp://127.0.0.1:0?bind=true", b, ns)
	if err != nil {
		t.Fatal(err)
	}
	defer lnB.Close()
	lnC, err := server.ListenPeerSync("tcp://127.0.0.1:0?bind=true", c, ns)
	if err != nil {
		t.Fatal(err)
	}
	defer lnC.Close()
	r, err := New(Config{
		NodeID: a.NodeID.String(), Local: a.PeerNamespaces(),
		Peers: []PeerConfig{
			{Name: "b", Addr: "tcp://" + slowAcceptProxy(t, lnB.Addr().String(), 300*time.Millisecond), Namespaces: []string{ns},
				Mode: peersync.SyncPull, Interval: 100 * time.Millisecond, PreferSnapshot: true},
			{Name: "c", Addr: "tcp://" + lnC.Addr().String(), Namespaces: []string{ns}, Mode: peersync.SyncPull, Interval: 100 * time.Millisecond},
		},
		Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	r.Start()
	defer r.Stop()
	eventually(t, 10*time.Second, "a to hold both peers' heads", func() bool {
		return a.Runtime.DAG.(interface{ HasCommit(codec.Hash) bool }).HasCommit(head(t, b)) &&
			a.Runtime.DAG.(interface{ HasCommit(codec.Hash) bool }).HasCommit(head(t, c))
	})
	if len(a.ShallowRoots()) == 0 || a.Runtime.DAG.(interface{ HasCommit(codec.Hash) bool }).HasCommit(first) {
		t.Fatalf("a fetched b's whole history instead of bootstrapping from its snapshot (shallow roots %d)", len(a.ShallowRoots()))
	}
}

// A snapshot peer that never answers delays the other peers' first sync by at most a request
// timeout; it does not block them.
func TestDownSnapshotPeerDoesNotBlockOtherPeers(t *testing.T) {
	ns := "app/data"
	a, c := newNode(t, ns), newNode(t, ns)
	put(t, c, `{"from":"c"}`)
	lnC, err := server.ListenPeerSync("tcp://127.0.0.1:0?bind=true", c, ns)
	if err != nil {
		t.Fatal(err)
	}
	defer lnC.Close()
	// Accepts and never answers: the snapshot peer's sync hangs until its request timeout.
	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer dead.Close()
	r, err := New(Config{
		NodeID: a.NodeID.String(), Local: a.PeerNamespaces(),
		Peers: []PeerConfig{
			{Name: "b", Addr: "tcp://" + dead.Addr().String(), Namespaces: []string{ns}, Mode: peersync.SyncPull,
				Interval: time.Hour, PreferSnapshot: true},
			{Name: "c", Addr: "tcp://" + lnC.Addr().String(), Namespaces: []string{ns}, Mode: peersync.SyncPull, Interval: 100 * time.Millisecond},
		},
		Timeout: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	r.Start()
	defer r.Stop()
	eventually(t, 5*time.Second, "c's write to reach a", func() bool { return head(t, a) == head(t, c) })
}
