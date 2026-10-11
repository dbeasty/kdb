package syncnode

import (
	"fmt"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/replication"
)

const meshNS = "app/data"

// freeAddrs reserves n loopback ports for nodes that must name each other before any listens.
func freeAddrs(t *testing.T, n int) []string {
	t.Helper()
	out := make([]string, n)
	for i := range out {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		out[i] = l.Addr().String()
		l.Close()
	}
	return out
}

// meshNodes starts n nodes listening on addrs, node i dialing the peers peersOf(i) names.
func meshNodes(t *testing.T, addrs []string, peersOf func(i int) []replication.PeerConfig, debounce time.Duration) []testNode {
	t.Helper()
	nodes := make([]testNode, len(addrs))
	for i := range nodes {
		nodes[i] = newTestNode(t, meshNS, Config{Peers: peersOf(i), Debounce: debounce})
		if _, err := nodes[i].node.Listen("tcp://" + addrs[i] + "?bind=true"); err != nil {
			t.Fatal(err)
		}
	}
	for _, n := range nodes {
		if err := n.node.Start(); err != nil {
			t.Fatal(err)
		}
	}
	return nodes
}

func (n testNode) head(t *testing.T) codec.Hash {
	t.Helper()
	rt, ok := n.set.Get(meshNS)
	if !ok {
		t.Fatal("namespace not open")
	}
	h, err := rt.Runtime.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func (n testNode) commits(t *testing.T) int {
	rt, _ := n.set.Get(meshNS)
	return rt.PeerIngestEnv().DAG.CommitCount()
}

func (n testNode) putJSON(t *testing.T, id codec.UUID, body string) {
	t.Helper()
	rt, err := n.set.Resolve(meshNS, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.PutJSON(meshNS, id, body, nil, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
}

func sameHeads(t *testing.T, nodes []testNode) ([]codec.Hash, bool) {
	hs := make([]codec.Hash, len(nodes))
	same := true
	for i, n := range nodes {
		hs[i] = n.head(t)
		if hs[i] != hs[0] {
			same = false
		}
	}
	return hs, same
}

// TestReplicatorRingSettlesOnOneHead: four sync nodes, each pulling from the next through its
// replicator, driven one sync at a time. Each wrote a document of its own. Once every node holds
// all four - a couple of rounds - the ring must settle on one head and stay there. Before merges
// of same-content heads were canonical every pull made a new empty merge and the ring never
// settled: the failure a five-node kdb-service mesh showed under load, where overlapping syncs
// have this shape.
func TestReplicatorRingSettlesOnOneHead(t *testing.T) {
	const n = 4
	addrs := freeAddrs(t, n)
	nodes := meshNodes(t, addrs, func(i int) []replication.PeerConfig {
		return []replication.PeerConfig{{
			Name: "next", Addr: "tcp://" + addrs[(i+1)%n], Namespaces: []string{meshNS},
			Mode: peersync.SyncPull, Interval: time.Hour, CreateLocal: true,
		}}
	}, time.Hour) // only SyncNow syncs
	ids := make([]codec.UUID, n)
	for i, node := range nodes {
		ids[i] = mustID(t)
		node.putJSON(t, ids[i], fmt.Sprintf(`{"from":%d}`, i))
	}
	round := func() {
		for _, node := range nodes {
			res, err := node.node.SyncNow("next")
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range res.Namespaces {
				if r.Err != nil {
					t.Fatalf("sync of %s: %v", r.Namespace, r.Err)
				}
			}
		}
	}
	settled := false
	for r := 1; r <= 2*n; r++ {
		round()
		if _, same := sameHeads(t, nodes); same {
			settled = true
			break
		}
	}
	if hs, _ := sameHeads(t, nodes); !settled {
		t.Fatalf("the ring never settled on one head: %v", shortHeads(hs))
	}
	for _, node := range nodes {
		for i, id := range ids {
			if got := node.get(meshNS, id); got != fmt.Sprintf(`{"from":%d}`, i) {
				t.Fatalf("a settled node holds %q for node %d's document", got, i)
			}
		}
	}
	before, _ := sameHeads(t, nodes)
	counts := make([]int, n)
	for i, node := range nodes {
		counts[i] = node.commits(t)
	}
	round()
	after, same := sameHeads(t, nodes)
	if !same || after[0] != before[0] {
		t.Fatalf("a settled ring moved: %v -> %v", shortHeads(before), shortHeads(after))
	}
	for i, node := range nodes {
		if node.commits(t) != counts[i] {
			t.Fatalf("node %d made commits after settling: %d -> %d", i, counts[i], node.commits(t))
		}
	}
}

// TestFullMeshSettlesAfterConcurrentWrites: five nodes in a full mesh, every replicator syncing
// on its own schedule and on every commit, take concurrent writes; once the writes stop the mesh
// must reach one head and stay on it.
func TestFullMeshSettlesAfterConcurrentWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("soak")
	}
	const n = 5
	addrs := freeAddrs(t, n)
	nodes := meshNodes(t, addrs, func(i int) []replication.PeerConfig {
		var peers []replication.PeerConfig
		for j := 0; j < n; j++ {
			if j != i {
				peers = append(peers, replication.PeerConfig{
					Name: fmt.Sprintf("n%d", j), Addr: "tcp://" + addrs[j], Namespaces: []string{meshNS},
					Mode: peersync.SyncBoth, Interval: 15 * time.Millisecond, CreateLocal: true,
				})
			}
		}
		return peers
	}, 2*time.Millisecond)
	ids := make([]codec.UUID, 40)
	for i := range ids {
		ids[i] = mustID(t)
	}
	var wg sync.WaitGroup
	var writes atomic.Int64
	stop := time.Now().Add(1500 * time.Millisecond)
	for w := 0; w < n; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w)))
			for k := 0; time.Now().Before(stop); k++ {
				// Distinct documents per writer: the default strict policy holds same-document
				// conflicts for an operator, which is not what this test is about.
				id := ids[w*len(ids)/n+rng.Intn(len(ids)/n)]
				rt, err := nodes[w].set.Resolve(meshNS, true)
				if err != nil {
					t.Error(err)
					return
				}
				if _, err := rt.PutJSON(meshNS, id, fmt.Sprintf(`{"w":%d,"k":%d}`, w, k), nil, auth.Principal{}); err != nil {
					continue // the gate was busy with a sync; this is load, not a failure
				}
				writes.Add(1)
				time.Sleep(time.Duration(rng.Intn(3)) * time.Millisecond)
			}
		}(w)
	}
	wg.Wait()
	if writes.Load() == 0 {
		t.Fatal("no writes landed")
	}
	// One head, held for a while: settled, not just momentarily equal.
	deadline := time.Now().Add(30 * time.Second)
	var stableSince time.Time
	var last codec.Hash
	for {
		hs, same := sameHeads(t, nodes)
		switch {
		case same && hs[0] == last && !stableSince.IsZero():
			if time.Since(stableSince) > 500*time.Millisecond {
				return
			}
		case same:
			last, stableSince = hs[0], time.Now()
		default:
			stableSince = time.Time{}
		}
		if time.Now().After(deadline) {
			t.Fatalf("after %d writes the mesh did not settle on one head within 30s: %v", writes.Load(), shortHeads(hs))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// shortHeads names heads by their first eight hex digits, for failure messages.
func shortHeads(hs []codec.Hash) []string {
	out := make([]string, len(hs))
	for i, h := range hs {
		out[i] = h.Hex()[:8]
	}
	return out
}
