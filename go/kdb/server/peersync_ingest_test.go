package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/index/stores"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/sql"
	mem "github.com/limidus/kdb/go/kdb/storage/mem"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

// peerFixture is a client-side replica of the test runtime's namespace, able to write commits
// the server has never seen and push them over a real peer-sync socket.
type peerFixture struct {
	t       *testing.T
	ns      string
	dag     *dag.InMemoryCommitDag
	storage *mem.InMemoryStorageAdapter
	addr    string
}

func newPeerFixture(t *testing.T, rt *KdbServerRuntime) *peerFixture {
	t.Helper()
	ns := rt.Runtime.DefaultNamespace
	ln, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", rt, ns)
	if err != nil {
		t.Fatalf("ListenPeerSync: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	d, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatal(err)
	}
	return &peerFixture{t: t, ns: ns, dag: d, storage: mem.NewInMemoryStorageAdapter(), addr: "tcp://" + ln.Addr().String()}
}

func (p *peerFixture) write(body string) (codec.UUID, document.Commit) {
	p.t.Helper()
	id, err := codec.RandomUUID()
	if err != nil {
		p.t.Fatal(err)
	}
	head, _ := p.dag.Head()
	return id, pushDoc(p.t, p.dag, p.storage, p.ns, head, id, body)
}

func (p *peerFixture) push(commits ...document.Commit) (int, error) {
	p.t.Helper()
	client := peersync.NewClient(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), p.dag, p.storage)
	session, err := client.Connect(peersync.ClientConfig{NamespaceID: p.ns, NodeID: "fixture", PeerURI: p.addr})
	if err != nil {
		p.t.Fatalf("connect: %v", err)
	}
	defer client.Disconnect()
	return session.PushCommits(commits)
}

// TestPeerFastForwardDoesNotLoseConcurrentLocalCommit is D2: the peer host used to read the
// head, decide "fast-forward", and SetHead unconditionally - all outside the write gate - so a
// local commit landing in between was silently dropped from main. The push now queues on the
// same gate as local writers and reads the head inside it, so whichever order they run in, both
// writes stay reachable.
func TestPeerFastForwardDoesNotLoseConcurrentLocalCommit(t *testing.T) {
	for round := 0; round < 20; round++ {
		rt := newTestRuntime(t)
		peer := newPeerFixture(t, rt)
		_, pushed := peer.write(`{"from":"peer"}`)

		release, err := rt.AcquireWriteSlotForTest()
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		var pushErr, localErr error
		var local document.Commit
		wg.Add(2)
		go func() { defer wg.Done(); _, pushErr = peer.push(pushed) }()
		go func() {
			defer wg.Done()
			id, _ := codec.RandomUUID()
			local, localErr = rt.Upsert(rt.Runtime.DefaultNamespace, id, `{"from":"local"}`, auth.Principal{})
		}()
		time.Sleep(20 * time.Millisecond) // both are now queued behind the held gate
		release()
		wg.Wait()
		if pushErr != nil || localErr != nil {
			t.Fatalf("round %d: push=%v local=%v", round, pushErr, localErr)
		}
		head, _ := rt.dag.Head()
		for name, h := range map[string]codec.Hash{"peer": pushed.Hash, "local": local.Hash} {
			if head != h && !rt.dag.IsAncestor(h, head) {
				t.Fatalf("round %d: %s commit %s is not reachable from main %s", round, name, h.Hex(), head.Hex())
			}
		}
	}
}

// TestPeerIngestUpdatesIndexes is D8 for indexes: a document that arrives by peer sync must be
// findable through the index exactly like one written locally.
func TestPeerIngestUpdatesIndexes(t *testing.T) {
	rt := newTestRuntime(t)
	provider, err := rt.OpenIndexes(stores.Options{})
	if err != nil {
		t.Fatalf("OpenIndexes: %v", err)
	}
	if err := provider.CreateIndex(sql.StmtCreateIndex{
		Name: "docs_text", Table: "docs", Fields: []sql.IndexField{{Path: "title", Weight: 1}}, Using: "FULLTEXT",
	}, sql.QueryContext{NamespaceID: rt.Runtime.DefaultNamespace}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	peer := newPeerFixture(t, rt)
	id, c := peer.write(`{"title":"replicated zebra"}`)
	if _, err := peer.push(c); err != nil {
		t.Fatalf("push: %v", err)
	}
	hits, _, err := provider.Search(context.Background(), wire.SearchMessage{
		Namespace: rt.Runtime.DefaultNamespace,
		Text:      &wire.SearchTextArm{Index: "docs_text", Query: "zebra"},
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) != 1 || hits[0].DocID != id {
		t.Fatalf("expected the replicated document in the index, got %+v", hits)
	}
}

// TestPeerIngestFiresCommitListener is D8 for stream subscribers: CommitListener is how the
// stream hub learns about writes, and it used to hear only about local ones.
func TestPeerIngestFiresCommitListener(t *testing.T) {
	rt := newTestRuntime(t)
	var mu sync.Mutex
	var heard []codec.Hash
	rt.CommitListener = func(_ string, c document.Commit) {
		mu.Lock()
		heard = append(heard, c.Hash)
		mu.Unlock()
	}
	peer := newPeerFixture(t, rt)
	_, c1 := peer.write(`{"n":1}`)
	_, c2 := peer.write(`{"n":2}`)
	if _, err := peer.push(c1, c2); err != nil {
		t.Fatalf("push: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(heard) != 2 || heard[0] != c1.Hash || heard[1] != c2.Hash {
		t.Fatalf("expected listener to hear both commits parents-first, got %v", heard)
	}
}

// TestPeerIngestUniqueDuplicateRecorded: two nodes each gave email x to a different document
// while disconnected. The replicated history is accepted - refusing it would leave the nodes
// unable to converge - and the duplicate is recorded rather than silently kept.
func TestPeerIngestUniqueDuplicateRecorded(t *testing.T) {
	rt := newTestRuntimeWithSchema(t, uniqueEmailSchema(t))
	local, _ := codec.RandomUUID()
	if _, err := rt.Upsert(rt.Runtime.DefaultNamespace, local, `{"email":"x@example.com"}`, auth.Principal{}); err != nil {
		t.Fatalf("local upsert: %v", err)
	}
	peer := newPeerFixture(t, rt)
	_, c := peer.write(`{"email":"x@example.com"}`)
	if _, err := peer.push(c); err != nil {
		t.Fatalf("push: %v", err)
	}
	head, _ := rt.dag.Head()
	if !rt.dag.IsAncestor(c.Hash, head) {
		t.Fatal("replicated commit was not merged into main")
	}
	issues := rt.Conflicts.List()
	if len(issues) != 1 || issues[0].Kind != peersync.ConflictUniqueDuplicate {
		t.Fatalf("expected one unique-duplicate conflict, got %+v", issues)
	}
	if got := rt.UniqueKeys.Len(); got != 1 {
		t.Fatalf("expected the contested key to have exactly one owner, got %d keys", got)
	}
}

// TestForeignGroupPartIsFlagged is D10's interim fix: a replicated commit that is one part of a
// cross-namespace group decided on another host used to be read as committed with nothing said.
// It still is - this node cannot judge a decision log it does not have - but it is now recorded.
func TestForeignGroupPartIsFlagged(t *testing.T) {
	rt := newTestRuntime(t)
	peer := newPeerFixture(t, rt)
	group, _ := codec.RandomUUID()
	marker := embed.GroupMarker{Host: "some-other-host", Epoch: 3, Group: group, Parts: []string{"app/data", "app/other"}}
	id, _ := codec.RandomUUID()
	body := `{"part":"one"}`
	if err := peer.storage.PutDocument(peer.ns, document.Document{ID: id, JSON: body}); err != nil {
		t.Fatal(err)
	}
	head, _ := peer.dag.Head()
	parent, _ := peer.dag.GetCommitOrThrow(head)
	tree, err := peer.storage.CommitTree(peer.ns, parent.DocumentTreeHash)
	if err != nil {
		t.Fatal(err)
	}
	tx := document.Transaction{
		ID: group, BaseVersion: head, Timestamp: codec.TimestampNow(), AuthorNodeID: group,
		Operations: []document.Op{document.WriteOp{DocID: id, Patch: body}},
	}
	c, err := peer.dag.AppendCommit(tx, head, tree, nil, marker.Message())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := peer.push(c); err != nil {
		t.Fatalf("push: %v", err)
	}
	for _, issue := range rt.Conflicts.List() {
		if issue.Kind == peersync.ConflictForeignGroupPart && issue.IncomingHex == c.Hash.Hex() {
			return
		}
	}
	t.Fatalf("foreign group part not flagged; conflicts: %+v", rt.Conflicts.List())
}

// TestTwoServersConvergeWithIndexes is Phase 0's exit criterion: two server runtimes that each
// took 1,000 writes while apart converge over a real socket - one merge, one head - and each
// one's index agrees with its documents afterwards.
func TestTwoServersConvergeWithIndexes(t *testing.T) {
	a, b := newTestRuntime(t), newTestRuntime(t)
	providers := map[string]*RegistryIndexProvider{}
	for name, rt := range map[string]*KdbServerRuntime{"a": a, "b": b} {
		p, err := rt.OpenIndexes(stores.Options{})
		if err != nil {
			t.Fatal(err)
		}
		if err := p.CreateIndex(sql.StmtCreateIndex{
			Name: "tags", Table: "docs", Fields: []sql.IndexField{{Path: "tag", Weight: 1}}, Using: "FULLTEXT",
		}, sql.QueryContext{NamespaceID: rt.Runtime.DefaultNamespace}); err != nil {
			t.Fatal(err)
		}
		providers[name] = p
		for i := 0; i < 1000; i++ {
			putDoc(t, rt, `{"tag":"from`+name+`"}`)
		}
	}
	ln, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", b, b.Runtime.DefaultNamespace)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	client := peersync.NewClient(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), a.dag, a.Runtime.Storage)
	session, err := client.Connect(peersync.ClientConfig{
		NamespaceID: a.Runtime.DefaultNamespace, NodeID: "a", PeerURI: "tcp://" + ln.Addr().String(),
		Node: a.PeerSyncNode(), ApplyToStorage: true, PageCommits: 300,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect()
	result, err := session.SyncBidirectional()
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if result.Conflict != nil {
		t.Fatalf("disjoint writes reported a conflict: %+v", result.Conflict)
	}
	ha, _ := a.dag.Head()
	hb, _ := b.dag.Head()
	if ha != hb {
		t.Fatalf("heads differ after sync: a=%s b=%s", ha.Hex(), hb.Hex())
	}
	for name, p := range providers {
		for _, tag := range []string{"froma", "fromb"} {
			hits, _, err := p.Search(context.Background(), wire.SearchMessage{
				Namespace: a.Runtime.DefaultNamespace,
				Text:      &wire.SearchTextArm{Index: "tags", Query: tag},
				Limit:     5000,
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(hits) != 1000 {
				t.Errorf("node %s: index finds %d documents tagged %s, want 1000", name, len(hits), tag)
			}
		}
	}
}

// TestServerStampsAuthorNodeID: a commit records the node that made it, not the random UUID a
// client put in the transaction.
func TestServerStampsAuthorNodeID(t *testing.T) {
	rt := newTestRuntime(t)
	id, _ := codec.RandomUUID()
	c, err := rt.Upsert(rt.Runtime.DefaultNamespace, id, `{"a":1}`, auth.Principal{})
	if err != nil {
		t.Fatal(err)
	}
	if c.AuthorNodeID != rt.NodeID || rt.NodeID == (codec.UUID{}) {
		t.Fatalf("commit authored by %s, runtime node is %s", c.AuthorNodeID, rt.NodeID)
	}
}

// TestPeerHandshakeRefusesOwnNodeID: a peer presenting this node's own identity is a copied data
// root, and two nodes recording progress as one would corrupt both.
func TestPeerHandshakeRefusesOwnNodeID(t *testing.T) {
	rt := newTestRuntime(t)
	peer := newPeerFixture(t, rt)
	client := peersync.NewClient(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peer.dag, peer.storage)
	_, err := client.Connect(peersync.ClientConfig{NamespaceID: peer.ns, NodeID: rt.NodeID.String(), PeerURI: peer.addr})
	if err == nil {
		client.Disconnect()
		t.Fatal("a peer with this node's own identity was accepted")
	}
}

// TestPeerSyncSurvivesRestartAfterConflictAndMerge: delta replay applies every logged commit to
// the live tree in log order and moves main to each. Peer sync used to log every commit it
// stored before deciding anything, so the losing side of a refused conflict came back on
// restart as if it had been on main. Only adopted commits are logged now, in adoption order.
func TestPeerSyncSurvivesRestartAfterConflictAndMerge(t *testing.T) {
	dataDir := t.TempDir()
	ns := "app/data"
	rt, err := embed.OpenFileRuntime(dataDir, "app", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	srv := NewKdbServerRuntime(rt)
	shared := mustRandomUUID(t)
	if _, err := srv.Upsert(ns, shared, `{"v":"local"}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}

	// A peer that wrote the same document: refused under the default strict policy.
	loser := newPeerFixture(t, srv)
	genesis, _ := loser.dag.Head()
	loserOnly := mustRandomUUID(t)
	c1 := pushDoc(t, loser.dag, loser.storage, ns, genesis, shared, `{"v":"remote"}`)
	c2 := pushDoc(t, loser.dag, loser.storage, ns, c1.Hash, loserOnly, `{"from":"loser"}`)
	if _, err := loser.push(c1, c2); err == nil {
		t.Fatal("expected the same-document push to be refused as a conflict")
	}

	// A peer that wrote something else: merged.
	winner := newPeerFixture(t, srv)
	_, merged := winner.write(`{"from":"winner"}`)
	if _, err := winner.push(merged); err != nil {
		t.Fatalf("disjoint push: %v", err)
	}
	head, _ := srv.dag.Head()
	rt.Close()
	// A clean close checkpoints, and reopening from a checkpoint never replays the log. Remove
	// it - the state a kill before the next checkpoint leaves - so the log alone has to rebuild
	// main.
	removeCheckpointFiles(t, dataDir)

	reopened, err := embed.OpenFileRuntime(dataDir, "app", ns, schema.None())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	got, err := reopened.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	if got != head {
		t.Fatalf("head after restart %s, before %s", got.Hex(), head.Hex())
	}
	// Reads at a commit's tree go through stored tree objects and look right either way; the
	// damage a bad replay does is to the live tree, which the next write builds on. So write.
	after := NewKdbServerRuntime(reopened)
	next, err := after.Upsert(ns, mustRandomUUID(t), `{"after":"restart"}`, auth.Principal{})
	if err != nil {
		t.Fatalf("write after restart: %v", err)
	}
	hc := next
	doc, err := reopened.Storage.GetDocument(ns, shared, hc.DocumentTreeHash)
	if err != nil || doc == nil || doc.JSON != `{"v":"local"}` {
		t.Fatalf("after restart the shared document is %+v (%v), want the local write", doc, err)
	}
	if d, _ := reopened.Storage.GetDocument(ns, loserOnly, hc.DocumentTreeHash); d != nil {
		t.Fatal("a document from the refused push is on main after restart")
	}
}

func removeCheckpointFiles(t *testing.T, root string) {
	t.Helper()
	removed := 0
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		if strings.Contains(filepath.ToSlash(path), "/snap/kdb_checkpoint_") {
			removed++
			return os.Remove(path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if removed == 0 {
		t.Fatal("no checkpoint found to remove - the test would not exercise log replay")
	}
}
