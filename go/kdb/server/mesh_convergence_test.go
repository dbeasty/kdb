package server

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/transaction"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

// Merge convergence in a mesh.
//
// A peer-sync merge is a deterministic function of the two heads it joins, so merge(a, b) is the
// same commit on every node. That is not enough for three or more nodes: merge(merge(a, b), c)
// and merge(merge(a, c), b) are different commits holding the same documents. Once writes stop,
// every node holds the same tree, yet each sync of two different heads still made a new merge -
// a head the other nodes did not have - and when syncs overlap, new heads were made faster than
// they spread. A five-node kdb-service mesh under load sat for minutes with every node's head
// different and still changing, every sync succeeding, thousands of empty merges a minute.

const meshNS = "app/data"

func newMeshNode(t *testing.T) *KdbServerRuntime {
	t.Helper()
	rt, err := embed.OpenMemoryRuntime("app", meshNS, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rt.Close() })
	srv := NewKdbServerRuntime(rt)
	srv.NodeID = mustRandomUUID(t)
	return srv
}

func meshPut(t *testing.T, rt *KdbServerRuntime, id codec.UUID, body string) {
	t.Helper()
	if _, err := rt.PutJSON(meshNS, id, body, nil, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
}

func meshHead(t *testing.T, rt *KdbServerRuntime) codec.Hash {
	t.Helper()
	h, err := rt.dag.Head()
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func meshHeads(t *testing.T, nodes []*KdbServerRuntime) []codec.Hash {
	out := make([]codec.Hash, len(nodes))
	for i, n := range nodes {
		out[i] = meshHead(t, n)
	}
	return out
}

func allEqual(hs []codec.Hash) bool {
	for _, h := range hs[1:] {
		if h != hs[0] {
			return false
		}
	}
	return true
}

func commitCount(nodes []*KdbServerRuntime) int {
	n := 0
	for _, rt := range nodes {
		n += rt.dag.CommitCount()
	}
	return n
}

// pullOverWire pulls main from the node behind ln into dst over a real v2 session: pull only, as
// a node whose push to that peer failed or has not happened yet would.
func pullOverWire(t *testing.T, dst *KdbServerRuntime, ln *Listener) {
	t.Helper()
	res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
		NodeID: dst.NodeID.String(), PeerURI: "tcp://" + ln.Addr().String(), Namespaces: []string{meshNS},
		Local: dst.PeerNamespaces(), Mode: peersync.SyncPull,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res.Namespaces {
		if r.Namespace == meshNS && r.Err != nil {
			t.Fatalf("pull: %v", r.Err)
		}
	}
}

// TestPullRingConvergesOnceWritesStop: three nodes, each pulling from the next in a ring - the
// shape of a mesh whose syncs overlap, where each node learns of the others through a third
// party before they learn of it. Each wrote one document. Within a few rounds every node holds
// all three, and then the ring must settle on one head and stop making commits. Before merges
// were made canonical it never did: every pull of a different head made a new empty merge.
func TestPullRingConvergesOnceWritesStop(t *testing.T) {
	nodes := []*KdbServerRuntime{newMeshNode(t), newMeshNode(t), newMeshNode(t)}
	lns := make([]*Listener, len(nodes))
	for i, n := range nodes {
		ln, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", n, meshNS)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { ln.Close() })
		lns[i] = ln
	}
	ids := make([]codec.UUID, len(nodes))
	for i, n := range nodes {
		ids[i] = mustRandomUUID(t)
		meshPut(t, n, ids[i], fmt.Sprintf(`{"from":%d}`, i))
	}
	round := func() {
		for i := range nodes {
			pullOverWire(t, nodes[i], lns[(i+1)%len(nodes)])
		}
	}
	converged := -1
	for r := 1; r <= 8; r++ {
		round()
		if allEqual(meshHeads(t, nodes)) {
			converged = r
			break
		}
	}
	if converged < 0 {
		t.Fatalf("the ring never settled on one head: %v (commits held: %d)", shortHeads(meshHeads(t, nodes)), commitCount(nodes))
	}
	if converged > 4 {
		t.Fatalf("the ring took %d rounds to settle; a ring of three needs at most 4", converged)
	}
	// Settled means settled: another round makes nothing.
	before, count := meshHeads(t, nodes), commitCount(nodes)
	round()
	if after := meshHeads(t, nodes); !allEqual(after) || after[0] != before[0] || commitCount(nodes) != count {
		t.Fatalf("a settled ring kept making commits: %d -> %d", count, commitCount(nodes))
	}
	for _, n := range nodes {
		for i, id := range ids {
			if body, _, found, err := n.GetDocument(meshNS, id); err != nil || !found || body != fmt.Sprintf(`{"from":%d}`, i) {
				t.Fatalf("document %d on a settled node: %q %v %v", i, body, found, err)
			}
		}
	}
}

// pullAt is one in-process pull: dst takes src's history up to at - a head src had at some
// moment, not necessarily its current one, which is what a sync that overlaps others sees - and
// decides its own head as the wire client would.
func pullAt(t *testing.T, dst, src *KdbServerRuntime, at codec.Hash) {
	t.Helper()
	commits, stubs, err := peersync.MissingCommits(src.dag, at, []codec.Hash{meshHead(t, dst)}, math.MaxInt)
	if err != nil {
		t.Fatal(err)
	}
	env := dst.PeerIngestEnv()
	if _, err := peersync.StoreCommits(env, commits, stubs); err != nil {
		t.Fatal(err)
	}
	res, err := peersync.Adopt(env, at)
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome.Kind == peersync.OutcomeConflict {
		t.Fatalf("unexpected conflict under last-write: %+v", res.Outcome.Report)
	}
}

// TestMeshConvergesUnderOverlappingSyncs is the property: any number of nodes, random writes to
// shared documents, random syncs that each see a peer's head as of some earlier moment. Once
// writes stop, rounds in which every node pulls from every other - each pull against the head the
// peer had when the round began, in a random order - must settle every node on one head within
// three rounds, and a settled mesh must make no further commits.
//
// Three rounds is the bound the canonical merge gives: after the first every node holds every
// write, after the second every node holds every commit that carries content, and every merge of
// two heads holding the same content is then the one commit naming that content's tips.
func TestMeshConvergesUnderOverlappingSyncs(t *testing.T) {
	seeds := 12
	if v, err := strconv.Atoi(os.Getenv("TEST_SEEDS")); err == nil && v > 0 {
		seeds = v
	}
	for seed := 1; seed <= seeds; seed++ {
		n := 3 + seed%3
		t.Run(fmt.Sprintf("seed=%d/nodes=%d", seed, n), func(t *testing.T) { meshProperty(t, int64(seed), n) })
	}
}

func meshProperty(t *testing.T, seed int64, n int) {
	rng := rand.New(rand.NewSource(seed))
	nodes := make([]*KdbServerRuntime, n)
	for i := range nodes {
		nodes[i] = newMeshNode(t)
		nodes[i].PeerSyncConflictPolicy = transaction.ConflictPolicyLastWrite
	}
	docs := make([]codec.UUID, 8)
	for i := range docs {
		docs[i] = mustRandomUUID(t)
	}
	// Recent heads per node: a pull sees one of them, as an overlapping sync would.
	seen := make([][]codec.Hash, n)
	note := func(i int) {
		seen[i] = append(seen[i], meshHead(t, nodes[i]))
		if len(seen[i]) > 3 {
			seen[i] = seen[i][1:]
		}
	}
	for i := range nodes {
		note(i)
	}
	for step := 0; step < 150; step++ {
		if rng.Intn(2) == 0 {
			k := rng.Intn(n)
			meshPut(t, nodes[k], docs[rng.Intn(len(docs))], fmt.Sprintf(`{"node":%d,"step":%d}`, k, step))
			note(k)
			continue
		}
		dst := rng.Intn(n)
		src := (dst + 1 + rng.Intn(n-1)) % n
		pullAt(t, nodes[dst], nodes[src], seen[src][rng.Intn(len(seen[src]))])
		note(dst)
	}

	// Writes have stopped.
	for r := 1; ; r++ {
		start := meshHeads(t, nodes)
		count := commitCount(nodes)
		type pair struct{ dst, src int }
		var pairs []pair
		for i := 0; i < n; i++ {
			for j := 0; j < n; j++ {
				if i != j {
					pairs = append(pairs, pair{i, j})
				}
			}
		}
		rng.Shuffle(len(pairs), func(a, b int) { pairs[a], pairs[b] = pairs[b], pairs[a] })
		for _, p := range pairs {
			pullAt(t, nodes[p.dst], nodes[p.src], start[p.src])
		}
		end := meshHeads(t, nodes)
		if allEqual(start) && allEqual(end) && start[0] == end[0] {
			if c := commitCount(nodes); c != count {
				t.Fatalf("seed %d: a settled mesh made %d commits in a round", seed, c-count)
			}
			break
		}
		if r > 3 {
			t.Fatalf("seed %d: %d nodes still on different heads after %d rounds of all-pairs syncs: %v", seed, n, r, shortHeads(end))
		}
	}
	// One tree, and it is every node's live state.
	h := meshHead(t, nodes[0])
	for i, node := range nodes {
		for _, id := range docs {
			want, _, wantFound, _ := nodes[0].GetDocument(meshNS, id)
			got, _, found, err := node.GetDocument(meshNS, id)
			if err != nil || found != wantFound || got != want {
				t.Fatalf("seed %d: node %d at %s disagrees on %s: %q vs %q", seed, i, h.Hex(), id, got, want)
			}
		}
	}
}

// TestRingJoinSurvivesRestart: a node that moved main onto a canonical join - which need not name
// its previous head as a parent - must come back at the same head when its log is replayed, as a
// kill -9 before the next checkpoint leaves it. Replay admitted a logged commit only when main
// was one of its parents, so such a node reopened at its previous head.
func TestRingJoinSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	rt, fileNode := openFileNode(t, dir)
	closed := false
	defer func() {
		if !closed {
			rt.Close()
		}
	}()
	const f = 2 // the file-backed node; in this ring it is the one that adopts a join sideways
	nodes := []*KdbServerRuntime{newMeshNode(t), newMeshNode(t), fileNode}
	lns := make([]*Listener, len(nodes))
	listen := func(i int) {
		ln, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", nodes[i], meshNS)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { ln.Close() })
		lns[i] = ln
	}
	for i := range nodes {
		listen(i)
	}
	for i, n := range nodes {
		meshPut(t, n, mustRandomUUID(t), fmt.Sprintf(`{"from":%d}`, i))
	}
	movedSideways := false
	for r := 0; r < 6 && !allEqual(meshHeads(t, nodes)); r++ {
		for i := range nodes {
			before := meshHead(t, nodes[i])
			pullOverWire(t, nodes[i], lns[(i+1)%len(nodes)])
			after := meshHead(t, nodes[i])
			if i == f && after != before && !nodes[f].dag.IsAncestor(before, after) {
				movedSideways = true
			}
		}
	}
	heads := meshHeads(t, nodes)
	if !allEqual(heads) {
		t.Fatalf("the ring did not settle: %v", shortHeads(heads))
	}
	if !movedSideways {
		t.Fatal("the file-backed node never moved main onto a join that does not descend from its head - the test does not exercise replaying one")
	}

	lns[f].Close()
	rt.Close()
	closed = true
	removeCheckpointFiles(t, dir)
	rt, reopened := openFileNode(t, dir)
	closed = false
	if got := meshHead(t, reopened); got != heads[f] {
		t.Fatalf("head after replay %s, before the restart %s", got.Hex(), heads[f].Hex())
	}
	nodes[f] = reopened
	listen(f)
	count := commitCount(nodes)
	for i := range nodes {
		pullOverWire(t, nodes[i], lns[(i+1)%len(nodes)])
	}
	if after := meshHeads(t, nodes); !allEqual(after) || after[0] != heads[0] || commitCount(nodes) != count {
		t.Fatalf("the ring moved after the restart: %v (commits %d -> %d)", shortHeads(after), count, commitCount(nodes))
	}
	// The live tree is the head's: a write on top of it keeps every document the head holds.
	settled, err := reopened.dag.GetCommitOrThrow(heads[0])
	if err != nil {
		t.Fatal(err)
	}
	c, err := reopened.PutJSON(meshNS, mustRandomUUID(t), `{"after":"restart"}`, nil, auth.Principal{})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range docIDsAt(t, reopened, settled.DocumentTreeHash) {
		if d, err := reopened.Runtime.Storage.GetDocument(meshNS, id, c.DocumentTreeHash); err != nil || d == nil {
			t.Fatalf("document %s lost from the live tree after replay", id)
		}
	}
}

func docIDsAt(t *testing.T, rt *KdbServerRuntime, tree codec.Hash) []codec.UUID {
	t.Helper()
	walker, ok := rt.Runtime.Storage.(storage.TreeWalker)
	if !ok {
		t.Fatal("storage cannot walk trees")
	}
	var ids []codec.UUID
	if err := walker.WalkTree(meshNS, tree, func(id codec.UUID, _ codec.Hash) bool {
		ids = append(ids, id)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 3 {
		t.Fatalf("documents at the settled head: %d, want 3", len(ids))
	}
	return ids
}

// shortHeads names heads by their first eight hex digits, for failure messages.
func shortHeads(hs []codec.Hash) []string {
	out := make([]string, len(hs))
	for i, h := range hs {
		out[i] = h.Hex()[:8]
	}
	return out
}
