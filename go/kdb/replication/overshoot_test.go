package replication

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

// countingTransport wraps TCP and counts the bytes of every frame received from and sent to the
// peer, so a test can see what a sync cost on the wire.
type countingTransport struct {
	inner    stream.Transport
	recv     *atomic.Int64
	sent     *atomic.Int64
	killRecv int64         // when > 0, the connection is closed once this many bytes have been received
	delay    time.Duration // slows every received frame by this much
}

func newCounting() *countingTransport {
	return &countingTransport{inner: tcp.NewTransport(core.DefaultConnectOptions()), recv: &atomic.Int64{}, sent: &atomic.Int64{}}
}

func (t *countingTransport) Connect(uri string) (stream.ConnectionHandle, error) {
	c, err := t.inner.Connect(uri)
	if err != nil {
		return nil, err
	}
	cc := &countingConn{inner: c, t: t, in: make(chan []byte, 64), stop: make(chan struct{})}
	go cc.pump()
	return cc, nil
}

type countingConn struct {
	inner stream.ConnectionHandle
	t     *countingTransport
	in    chan []byte
	stop  chan struct{}
	once  sync.Once
}

func (c *countingConn) pump() {
	defer close(c.in)
	var got int64
	for {
		select {
		case f, ok := <-c.inner.Incoming():
			if !ok {
				return
			}
			if c.t.delay > 0 {
				time.Sleep(c.t.delay)
			}
			c.t.recv.Add(int64(len(f)))
			got += int64(len(f))
			if c.t.killRecv > 0 && got >= c.t.killRecv {
				_ = c.Close()
				return
			}
			select {
			case c.in <- f:
			case <-c.stop:
				return
			}
		case <-c.stop:
			return
		}
	}
}

func (c *countingConn) Send(f []byte) error {
	c.t.sent.Add(int64(len(f)))
	return c.inner.Send(f)
}
func (c *countingConn) Incoming() <-chan []byte { return c.in }
func (c *countingConn) TryPoll() []byte         { return nil }
func (c *countingConn) Close() error {
	c.once.Do(func() { close(c.stop) })
	return c.inner.Close()
}

type meshNode struct {
	srv  *server.KdbServerRuntime
	addr string
}

func listen(t *testing.T, srv *server.KdbServerRuntime, ns string) meshNode {
	t.Helper()
	ln, err := server.ListenPeerSync("tcp://127.0.0.1:0?bind=true", srv, ns)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	return meshNode{srv: srv, addr: "tcp://" + ln.Addr().String()}
}

func syncWith(t *testing.T, from *server.KdbServerRuntime, to meshNode, ns string, mode peersync.SyncMode, tr stream.Transport) (peersync.NamespaceSyncResult, error) {
	return syncPaged(t, from, to, ns, mode, tr, 0)
}

func syncPaged(t *testing.T, from *server.KdbServerRuntime, to meshNode, ns string, mode peersync.SyncMode, tr stream.Transport, pageBytes int) (peersync.NamespaceSyncResult, error) {
	t.Helper()
	res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tr, peersync.V2ClientConfig{
		NodeID: from.NodeID.String(), PeerURI: to.addr, Namespaces: []string{ns}, Mode: mode,
		Local: from.PeerNamespaces(), CreateLocal: true, Timeout: 30 * time.Second, PageBytes: pageBytes,
	})
	if err != nil {
		return peersync.NamespaceSyncResult{}, err
	}
	for _, r := range res.Namespaces {
		if r.Namespace == ns {
			return r, r.Err
		}
	}
	return peersync.NamespaceSyncResult{}, fmt.Errorf("namespace %s not in result", ns)
}

// seedMesh builds A with n commits of bodyBytes-sized documents, and B holding the same history.
func seedMesh(t *testing.T, ns string, n, bodyBytes int) (meshNode, meshNode) {
	t.Helper()
	a, b := newNode(t, ns), newNode(t, ns)
	pad := strings.Repeat("x", bodyBytes)
	for i := 0; i < n; i++ {
		put(t, a, fmt.Sprintf(`{"i":%d,"pad":%q}`, i, pad))
	}
	ma, mb := listen(t, a, ns), listen(t, b, ns)
	if _, err := syncWith(t, b, ma, ns, peersync.SyncPull, newCounting()); err != nil {
		t.Fatal(err)
	}
	if head(t, a) != head(t, b) {
		t.Fatal("seed: A and B differ")
	}
	return ma, mb
}

type pullStats struct {
	received, pulled int
	bytes            int64
}

func (p pullStats) String() string {
	return fmt.Sprintf("received=%d pulled=%d overshoot=%d bytesToC=%d", p.received, p.pulled, p.received-p.pulled, p.bytes)
}

// TestJoinTwoPeersOvershoot measures what an empty node C costs joining a mesh of A and B that
// hold the same history, pulling from both concurrently (as the replicator's per-peer loops do)
// and one after the other.
func TestJoinTwoPeersOvershoot(t *testing.T) {
	cases := []struct {
		n, body int
	}{{100, 16}, {1000, 16}, {300, 4096}}
	for _, tc := range cases {
		for _, concurrent := range []bool{false, true} {
			name := fmt.Sprintf("n=%d/body=%d/concurrent=%v", tc.n, tc.body, concurrent)
			t.Run(name, func(t *testing.T) {
				ns := "app/data"
				ma, mb := seedMesh(t, ns, tc.n, tc.body)
				c := newNode(t, ns)
				var total pullStats
				var mu sync.Mutex
				run := func(peer meshNode) {
					tr := newCounting()
					r, err := syncWith(t, c, peer, ns, peersync.SyncPull, tr)
					if err != nil {
						t.Error(err)
					}
					mu.Lock()
					total.received += r.Received
					total.pulled += r.Pulled
					total.bytes += tr.recv.Load()
					mu.Unlock()
				}
				if concurrent {
					var wg sync.WaitGroup
					for _, p := range []meshNode{ma, mb} {
						wg.Add(1)
						go func() { defer wg.Done(); run(p) }()
					}
					wg.Wait()
				} else {
					run(ma)
					run(mb)
				}
				if head(t, c) != head(t, ma.srv) {
					t.Fatal("C did not converge")
				}
				t.Logf("%s: %s", name, total)
				// Concurrent or not, the history crosses the wire once.
				if total.received != tc.n || total.pulled != tc.n {
					t.Fatalf("expected the %d commits sent once, got %s", tc.n, total)
				}
			})
		}
	}
}

// TestMeasurePushOvershoot: A and B, holding the same history, both push to an empty C at once.
func TestMeasurePushOvershoot(t *testing.T) {
	for _, n := range []int{100, 1000} {
		for _, concurrent := range []bool{false, true} {
			name := fmt.Sprintf("n=%d/concurrent=%v", n, concurrent)
			t.Run(name, func(t *testing.T) {
				ns := "app/data"
				ma, mb := seedMesh(t, ns, n, 16)
				mc := listen(t, newNode(t, ns), ns)
				var pushed int
				var sentBytes atomic.Int64
				var mu sync.Mutex
				run := func(from meshNode) {
					tr := newCounting()
					r, err := syncWith(t, from.srv, mc, ns, peersync.SyncPush, tr)
					if err != nil {
						t.Error(err)
					}
					mu.Lock()
					pushed += r.Pushed
					mu.Unlock()
					sentBytes.Add(tr.sent.Load())
				}
				if concurrent {
					var wg sync.WaitGroup
					for _, p := range []meshNode{ma, mb} {
						wg.Add(1)
						go func() { defer wg.Done(); run(p) }()
					}
					wg.Wait()
				} else {
					run(ma)
					run(mb)
				}
				if head(t, mc.srv) != head(t, ma.srv) {
					t.Fatal("C did not converge")
				}
				t.Logf("%s: pushed(new to C)=%d bytesToC=%d", name, pushed, sentBytes.Load())
			})
		}
	}
}

// TestMeasureSteadyState: peers already in sync reconnecting exchange O(1).
func TestMeasureSteadyState(t *testing.T) {
	for _, n := range []int{100, 1000} {
		ns := "app/data"
		ma, _ := seedMesh(t, ns, n, 16)
		c := newNode(t, ns)
		if _, err := syncWith(t, c, ma, ns, peersync.SyncBoth, newCounting()); err != nil {
			t.Fatal(err)
		}
		tr := newCounting()
		r, err := syncWith(t, c, ma, ns, peersync.SyncBoth, tr)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("n=%d steady state: received=%d pulled=%d pushed=%d bytesIn=%d bytesOut=%d", n, r.Received, r.Pulled, r.Pushed, tr.recv.Load(), tr.sent.Load())
		if r.Received != 0 || r.Pushed != 0 || tr.recv.Load() > 2048 || tr.sent.Load() > 2048 {
			t.Fatalf("in-sync peers should exchange a constant few frames, got %+v and %d/%d bytes", r, tr.recv.Load(), tr.sent.Load())
		}
	}
}

// TestJoinPeerDropsMidPull: C pulls from A and B at once and A's connection dies part way. Nothing
// may go missing - B's pull, waiting its turn, must fetch the rest - and B is not sent what A's
// pull already stored.
func TestJoinPeerDropsMidPull(t *testing.T) {
	ns := "app/data"
	const n = 1000
	ma, mb := seedMesh(t, ns, n, 16)
	c := newNode(t, ns)
	full := newCounting()
	if _, err := syncPaged(t, newNode(t, ns), ma, ns, peersync.SyncPull, full, 4096); err != nil {
		t.Fatal(err)
	}
	// Small pages, slowed, and cut halfway through the history.
	dying := newCounting()
	dying.killRecv, dying.delay = full.recv.Load()/2, time.Millisecond
	var ra peersync.NamespaceSyncResult
	var errA error
	done := make(chan struct{})
	go func() {
		defer close(done)
		ra, errA = syncPaged(t, c, ma, ns, peersync.SyncPull, dying, 4096)
	}()
	eventually(t, 5*time.Second, "A's pull to store its first page", func() bool {
		return dying.recv.Load() > 4096
	})
	rb, errB := syncPaged(t, c, mb, ns, peersync.SyncPull, newCounting(), 4096)
	<-done
	if errA == nil {
		t.Fatal("expected A's pull to fail when its connection was cut")
	}
	if errB != nil {
		t.Fatalf("B's pull: %v", errB)
	}
	if head(t, c) != head(t, mb.srv) {
		t.Fatal("C did not converge on B's head")
	}
	t.Logf("A (cut): received=%d pulled=%d; B: received=%d pulled=%d", ra.Received, ra.Pulled, rb.Received, rb.Pulled)
	if ra.Pulled == 0 || rb.Received >= n {
		t.Fatalf("expected B to resume from what A stored, not refetch: A pulled %d, B received %d", ra.Pulled, rb.Received)
	}
	if over := rb.Received - rb.Pulled; over != 0 {
		t.Fatalf("B was sent %d commits C already held", over)
	}
}

// TestReplicatorJoinsTwoPeersOnce: the real replicator, configured with two peers holding the same
// history, brings an empty node up to date with the history sent once, and exports overshoot.
func TestReplicatorJoinsTwoPeersOnce(t *testing.T) {
	ns := "app/data"
	const n = 1000
	ma, mb := seedMesh(t, ns, n, 16)
	c := newNode(t, ns)
	trs := map[string]*countingTransport{"a": newCounting(), "b": newCounting()}
	r, err := New(Config{
		NodeID: c.NodeID.String(), Local: c.PeerNamespaces(),
		Peers: []PeerConfig{
			{Name: "a", Addr: ma.addr, Namespaces: []string{ns}, Mode: peersync.SyncBoth, Interval: time.Hour},
			{Name: "b", Addr: mb.addr, Namespaces: []string{ns}, Mode: peersync.SyncBoth, Interval: time.Hour},
		},
		Transport: func(p PeerConfig) stream.Transport { return trs[p.Name] },
	})
	if err != nil {
		t.Fatal(err)
	}
	r.Start()
	defer r.Stop()
	eventually(t, 10*time.Second, "both peers' first cycle", func() bool {
		st := r.Status()
		return head(t, c) == head(t, ma.srv) && !st[0].State.LastSuccess.IsZero() && !st[1].State.LastSuccess.IsZero()
	})
	var pulled, overshoot, bytes int64
	for _, p := range r.Status() {
		pulled += p.State.Namespaces[ns].Pulled
		overshoot += p.State.Namespaces[ns].Overshoot
		bytes += trs[p.Name].recv.Load()
	}
	var m strings.Builder
	r.WriteMetrics(&m)
	t.Logf("replicator join: pulled=%d overshoot=%d bytesToC=%d", pulled, overshoot, bytes)
	if pulled != n || overshoot != 0 {
		t.Fatalf("expected %d commits pulled once, got pulled=%d overshoot=%d", n, pulled, overshoot)
	}
	if !strings.Contains(m.String(), `kdb_replication_overshoot_commits_total{peer="a",namespace="app/data"} 0`) {
		t.Fatalf("overshoot metric missing:\n%s", m.String())
	}
}

// TestTwoNodesSyncEachOtherAtOnce: each node pulls from the other while the other pulls from it;
// the pull gate is per node and held only on the pulling side, so neither waits on the other.
func TestTwoNodesSyncEachOtherAtOnce(t *testing.T) {
	ns := "app/data"
	ma, mb := seedMesh(t, ns, 200, 16)
	for i := 0; i < 50; i++ {
		put(t, ma.srv, fmt.Sprintf(`{"a":%d}`, i))
		put(t, mb.srv, fmt.Sprintf(`{"b":%d}`, i))
	}
	done := make(chan error, 2)
	go func() { _, err := syncWith(t, ma.srv, mb, ns, peersync.SyncBoth, newCounting()); done <- err }()
	go func() { _, err := syncWith(t, mb.srv, ma, ns, peersync.SyncBoth, newCounting()); done <- err }()
	for i := 0; i < 2; i++ {
		select {
		case <-done: // a concurrent ref update may be refused as a conflict; a later sync settles it
		case <-time.After(20 * time.Second):
			t.Fatal("two nodes syncing each other deadlocked")
		}
	}
	for i := 0; i < 3 && head(t, ma.srv) != head(t, mb.srv); i++ {
		if _, err := syncWith(t, ma.srv, mb, ns, peersync.SyncBoth, newCounting()); err != nil {
			t.Fatal(err)
		}
	}
	if head(t, ma.srv) != head(t, mb.srv) {
		t.Fatal("A and B did not converge")
	}
}
