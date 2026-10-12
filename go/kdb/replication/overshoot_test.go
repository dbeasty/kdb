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
	killRecv int64 // when > 0, the connection is closed once this many bytes have been received
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
	t.Helper()
	res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tr, peersync.V2ClientConfig{
		NodeID: from.NodeID.String(), PeerURI: to.addr, Namespaces: []string{ns}, Mode: mode,
		Local: from.PeerNamespaces(), CreateLocal: true, Timeout: 30 * time.Second,
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

// TestMeasureJoinOvershoot measures what an empty node C costs joining a mesh of A and B that
// hold the same history, pulling from both concurrently (as the replicator's per-peer loops do)
// and one after the other.
func TestMeasureJoinOvershoot(t *testing.T) {
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
	}
}
