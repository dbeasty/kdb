package replication

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/wire"
)

func commitCount(srv *server.KdbServerRuntime) int {
	return srv.PeerIngestEnv().DAG.CommitCount()
}

// fileNodeAt is fileNode on a given directory, so a test can close and reopen it.
func fileNodeAt(t *testing.T, dir, ns string) (*server.KdbServerRuntime, func()) {
	t.Helper()
	rt, err := embed.OpenFileRuntimeWithOptions(dir, "app", ns, schema.None(),
		embed.FileRuntimeOptions{Storage: embed.StorageOptions{Durability: storage.DurabilityAsync}})
	if err != nil {
		t.Fatal(err)
	}
	srv := server.NewKdbServerRuntime(rt)
	id, _ := codec.RandomUUID()
	srv.NodeID = id
	return srv, rt.Close
}

// saveCheckpointFiles reads every checkpoint file under dir; restoreCheckpointFiles puts exactly
// those back, removing any written since - the state a kill leaves.
func saveCheckpointFiles(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	err := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.Contains(filepath.ToSlash(p), "/snap/kdb_checkpoint_") {
			return err
		}
		b, err := os.ReadFile(p)
		out[p] = b
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) == 0 {
		t.Fatal("no checkpoint on disk after the squash")
	}
	return out
}

func restoreCheckpointFiles(t *testing.T, dir string, saved map[string][]byte) {
	t.Helper()
	_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.Contains(filepath.ToSlash(p), "/snap/kdb_checkpoint_") {
			if _, keep := saved[p]; !keep {
				_ = os.Remove(p)
			}
		}
		return nil
	})
	for p, b := range saved {
		if err := os.WriteFile(p, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func syncRoute(t *testing.T, from *server.KdbServerRuntime, to meshNode, ns string, route peersync.CatchUpRoute) peersync.NamespaceSyncResult {
	t.Helper()
	res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), newCounting(), peersync.V2ClientConfig{
		NodeID: from.NodeID.String(), PeerURI: to.addr, Namespaces: []string{ns}, Mode: peersync.SyncPull,
		Local: from.PeerNamespaces(), CreateLocal: true, Timeout: 30 * time.Second, CatchUp: route,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res.Namespaces {
		if r.Namespace == ns {
			if r.Err != nil {
				t.Fatal(r.Err)
			}
			return r
		}
	}
	t.Fatalf("namespace %s not in the result", ns)
	return peersync.NamespaceSyncResult{}
}

// requireSameDocuments checks b holds exactly a's documents at their heads.
func requireSameDocuments(t *testing.T, a, b *server.KdbServerRuntime, ns string, ids []codec.UUID) {
	t.Helper()
	at, err := a.HeadTree()
	if err != nil {
		t.Fatal(err)
	}
	bt, err := b.HeadTree()
	if err != nil {
		t.Fatal(err)
	}
	if at.TreeHash != bt.TreeHash {
		t.Fatalf("trees differ: %s against %s", at.TreeHash.Hex(), bt.TreeHash.Hex())
	}
	for _, id := range ids {
		da, err := a.Runtime.Storage.GetDocument(ns, id, at.TreeHash)
		if err != nil {
			t.Fatal(err)
		}
		db, err := b.Runtime.Storage.GetDocument(ns, id, bt.TreeHash)
		if err != nil {
			t.Fatal(err)
		}
		if (da == nil) != (db == nil) || (da != nil && da.JSON != db.JSON) {
			t.Fatalf("document %s differs", id)
		}
	}
}

// A squash moves B straight to A's head: same head hash, same documents (rewritten, created and
// deleted ones), without the backlog's commits. It survives a local write and a reopen, and the
// next sync is an ordinary replay on top of it. Run with B's whole log in its open segment (the
// squash checkpoint claims the state before the first segment), and with B reopened before the
// backlog - its first segment sealed and checkpointed, the checkpoint a reopen would fall back to
// were the squash not to write a newer one.
func TestSquashCatchUpJumpsToThePeersHead(t *testing.T) {
	t.Run("one-segment", func(t *testing.T) { runSquashCatchUp(t, false) })
	t.Run("sealed-segment", func(t *testing.T) { runSquashCatchUp(t, true) })
}

func runSquashCatchUp(t *testing.T, sealedBefore bool) {
	ns := "app/data"
	a := fileNode(t, ns)
	ma := listen(t, a, ns)
	dirB := t.TempDir()
	b, closeB := fileNodeAt(t, dirB, ns)

	ids := make([]codec.UUID, 20)
	for i := range ids {
		ids[i], _ = codec.RandomUUID()
	}
	upsert := func(srv *server.KdbServerRuntime, id codec.UUID, body string) {
		t.Helper()
		if _, err := srv.Upsert(ns, id, body, auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	// Random padding fills B's small segments, so it seals them: the log is zstd'd, and a
	// repeated pad would compress to nothing.
	for i, id := range ids {
		pad := make([]byte, 300)
		_, _ = rand.Read(pad)
		upsert(a, id, fmt.Sprintf(`{"v":%d,"pad":%q}`, i, hex.EncodeToString(pad)))
	}
	syncRoute(t, b, ma, ns, peersync.CatchUpReplay)
	if head(t, a) != head(t, b) {
		t.Fatal("initial sync: heads differ")
	}
	// Writes of B's own, one log append each, so its small segments seal; A takes them, so the
	// two still share one history.
	for i := 0; i < 15; i++ {
		id, _ := codec.RandomUUID()
		pad := make([]byte, 300)
		_, _ = rand.Read(pad)
		upsert(b, id, fmt.Sprintf(`{"b":%d,"pad":%q}`, i, hex.EncodeToString(pad)))
		ids = append(ids, id)
	}
	syncRoute(t, a, listen(t, b, ns), ns, peersync.CatchUpReplay)
	if head(t, a) != head(t, b) {
		t.Fatal("after A took B's writes: heads differ")
	}
	if sealedBefore {
		// A checkpoint from before the squash, as a node that has been running has: the squash
		// must write a newer one, or a reopen falls back to this. Reopening seals the first
		// segment, which maintenance then checkpoints through.
		closeB()
		b, closeB = fileNodeAt(t, dirB, ns)
		if _, err := b.Runtime.Maintain(); err != nil {
			t.Fatal(err)
		}
		saveCheckpointFiles(t, dirB) // fails the test when there is none to fall back to
	}
	before := commitCount(b)

	// The backlog: 500 rewrites of the same documents, one deleted, one created.
	for i := 0; i < 500; i++ {
		upsert(a, ids[i%len(ids)], fmt.Sprintf(`{"v":%d,"round":%d}`, i, i/len(ids)))
	}
	txID, _ := codec.RandomUUID()
	if _, err := a.Commit(ns, document.Transaction{ID: txID, BaseVersion: head(t, a),
		Operations: []document.Op{document.DeleteOp{DocID: ids[0]}}, Timestamp: codec.TimestampNow()}, "", auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	fresh, _ := codec.RandomUUID()
	upsert(a, fresh, `{"fresh":true}`)
	ids = append(ids, fresh)

	res := syncRoute(t, b, ma, ns, peersync.CatchUpSquash)
	if res.Squashed != head(t, a).Hex() {
		t.Fatalf("expected a squash to %s, got %+v", head(t, a).Hex(), res)
	}
	if head(t, a) != head(t, b) {
		t.Fatal("after the squash: heads differ")
	}
	requireSameDocuments(t, a, b, ns, ids)
	if added := commitCount(b) - before; added > 2 {
		t.Fatalf("the squash stored %d commits; it should take the head alone", added)
	}

	// A local write on top of the squashed head, then a reopen as after a kill: the checkpoint
	// rolled back to the one the squash wrote (a clean close would write a newer one). That
	// checkpoint is what restores the jump, and the log replays the write on top of it.
	squashCheckpoint := saveCheckpointFiles(t, dirB)
	local, _ := codec.RandomUUID()
	upsert(b, local, `{"local":true}`)
	bHead := head(t, b)
	closeB()
	restoreCheckpointFiles(t, dirB, squashCheckpoint)
	b, closeB = fileNodeAt(t, dirB, ns)
	t.Cleanup(closeB)
	if head(t, b) != bHead {
		t.Fatalf("after reopen main is %s, want the local write %s on top of the squash", head(t, b).Hex(), bHead.Hex())
	}
	tree, err := b.HeadTree()
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range append([]codec.UUID{local}, ids[1:]...) {
		if d, err := b.Runtime.Storage.GetDocument(ns, id, tree.TreeHash); err != nil || d == nil {
			t.Fatalf("after reopen document %s is missing (%v)", id, err)
		}
	}

	// The next sync from A is ordinary: A writes more, B replays it on top of the squash root,
	// and A takes B's local write.
	upsert(a, ids[3], `{"after":true}`)
	if res := syncRoute(t, b, ma, ns, peersync.CatchUpReplay); res.Squashed != "" {
		t.Fatalf("a replay sync squashed: %+v", res)
	}
	mb := listen(t, b, ns)
	syncRoute(t, a, mb, ns, peersync.CatchUpReplay)
	if head(t, a) != head(t, b) {
		t.Fatalf("after syncing both ways the heads differ: %s and %s", head(t, a).Hex(), head(t, b).Hex())
	}

	// The gap the squash left is ordinary shallow history: deepening from A fetches the skipped
	// commits, and B ends with A's whole history and no shallow root.
	if len(b.PeerIngestEnv().DAG.ShallowRoots()) == 0 {
		t.Fatal("the squash root is not a shallow root")
	}
	results, err := b.Deepen(func() (*peersync.RepairSession, error) {
		return peersync.OpenRepairSession(wire.NewCodec(wire.EncodingJSON), newCounting(), peersync.V2ClientConfig{
			NodeID: b.NodeID.String(), PeerURI: ma.addr, Namespaces: []string{ns},
		})
	})
	if err != nil {
		t.Fatalf("deepen across the squash gap: %v (%+v)", err, results)
	}
	if roots := b.PeerIngestEnv().DAG.ShallowRoots(); len(roots) != 0 {
		t.Fatalf("after deepening, %d shallow roots remain", len(roots))
	}
	if commitCount(b) != commitCount(a) {
		t.Fatalf("after deepening B holds %d commits, A %d", commitCount(b), commitCount(a))
	}
}

// auto replays a backlog too small to be worth a squash, and a squash is never taken by a node
// with commits of its own the peer lacks: it replays, which merges them.
func TestSquashDeclinedForSmallBacklogsAndDivergence(t *testing.T) {
	ns := "app/data"
	a, b := fileNode(t, ns), fileNode(t, ns)
	ma := listen(t, a, ns)
	id, _ := codec.RandomUUID()
	for i := 0; i < 5; i++ {
		if _, err := a.Upsert(ns, id, fmt.Sprintf(`{"v":%d}`, i), auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	syncRoute(t, b, ma, ns, peersync.CatchUpReplay)
	for i := 0; i < 50; i++ {
		if _, err := a.Upsert(ns, id, fmt.Sprintf(`{"v":%d}`, 100+i), auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	if res := syncRoute(t, b, ma, ns, peersync.CatchUpAuto); res.Squashed != "" {
		t.Fatalf("auto squashed a %d-commit backlog, below its minimum: %+v", 50, res)
	}
	if head(t, a) != head(t, b) {
		t.Fatal("auto replay: heads differ")
	}

	// Diverged: B writes, A writes; a squash route must not drop B's commit.
	mine, _ := codec.RandomUUID()
	if _, err := b.Upsert(ns, mine, `{"mine":true}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if _, err := a.Upsert(ns, id, fmt.Sprintf(`{"v":%d}`, 200+i), auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	if res := syncRoute(t, b, ma, ns, peersync.CatchUpSquash); res.Squashed != "" {
		t.Fatalf("a diverged node squashed: %+v", res)
	}
	tree, err := b.HeadTree()
	if err != nil {
		t.Fatal(err)
	}
	if d, err := b.Runtime.Storage.GetDocument(ns, mine, tree.TreeHash); err != nil || d == nil {
		t.Fatalf("B's own document is gone after a squash-route sync (%v)", err)
	}
}

// A namespace whose catch-up policy refuses squash keeps full history on every replica: a host
// holding the policy answers SQUASH_HEAD with a refusal, and a catchup=squash peer replays the
// whole backlog instead, converging with every commit and no shallow root. A receiver holding
// the policy does not squash either, even from a host that does not know it yet.
func TestSquashRefusedByCatchUpPolicy(t *testing.T) {
	ns := "app/data"
	a, b := fileNode(t, ns), fileNode(t, ns)
	ma := listen(t, a, ns)
	ids := make([]codec.UUID, 10)
	for i := range ids {
		ids[i], _ = codec.RandomUUID()
	}
	backlog := func(from int) {
		t.Helper()
		for i := 0; i < 200; i++ {
			if _, err := a.Upsert(ns, ids[i%len(ids)], fmt.Sprintf(`{"v":%d}`, from+i), auth.Principal{}); err != nil {
				t.Fatal(err)
			}
		}
	}
	backlog(0)
	syncRoute(t, b, ma, ns, peersync.CatchUpReplay)

	// The host refuses, and says why.
	a.SetCatchUpPolicy(&server.CatchUpPolicy{RefuseSquash: true, Reason: "audit: full history everywhere"})
	backlog(1000)
	_, local, _, err := b.PeerIngestEnv().DAG.HeadCommit()
	if err != nil {
		t.Fatal(err)
	}
	reply := squashHeadOf(t, b, ma, ns, local.Hash)
	if reply.Refused != "audit: full history everywhere" {
		t.Fatalf("SQUASH_HEAD from a refusing host: %+v", reply)
	}
	if res := syncRoute(t, b, ma, ns, peersync.CatchUpSquash); res.Squashed != "" {
		t.Fatalf("a squash was taken from a host that refuses them: %+v", res)
	}
	requireFullHistory(t, a, b, ns, ids)

	// The receiver holds the policy, the host no longer does: still a replay.
	a.SetCatchUpPolicy(nil)
	b.SetCatchUpPolicy(&server.CatchUpPolicy{RefuseSquash: true})
	backlog(2000)
	if reply := squashHeadOf(t, b, ma, ns, head(t, b)); reply.Refused != "" || !reply.Descends {
		t.Fatalf("the host stopped refusing, but SQUASH_HEAD says %+v", reply)
	}
	if res := syncRoute(t, b, ma, ns, peersync.CatchUpSquash); res.Squashed != "" {
		t.Fatalf("a receiver whose policy refuses squash took one: %+v", res)
	}
	requireFullHistory(t, a, b, ns, ids)

	// With neither refusing, the same route squashes - the refusal above was the policy's doing.
	b.SetCatchUpPolicy(nil)
	backlog(3000)
	if res := syncRoute(t, b, ma, ns, peersync.CatchUpSquash); res.Squashed != head(t, a).Hex() {
		t.Fatalf("with no policy the squash route did not squash: %+v", res)
	}
}

// requireFullHistory checks b converged with a on every document and every commit, with no
// shallow root - what replaying, rather than squashing, leaves.
func requireFullHistory(t *testing.T, a, b *server.KdbServerRuntime, ns string, ids []codec.UUID) {
	t.Helper()
	if head(t, a) != head(t, b) {
		t.Fatalf("heads differ: %s and %s", head(t, a).Hex(), head(t, b).Hex())
	}
	requireSameDocuments(t, a, b, ns, ids)
	if commitCount(b) != commitCount(a) {
		t.Fatalf("B holds %d commits, A %d", commitCount(b), commitCount(a))
	}
	if roots := b.PeerIngestEnv().DAG.ShallowRoots(); len(roots) != 0 {
		t.Fatalf("B has %d shallow roots", len(roots))
	}
}

// squashHeadOf asks the node listening at to, as from, what a squash of ns from have would do.
func squashHeadOf(t *testing.T, from *server.KdbServerRuntime, to meshNode, ns string, have codec.Hash) wire.SquashHeadResultMessage {
	t.Helper()
	s, err := peersync.OpenRepairSession(wire.NewCodec(wire.EncodingJSON), newCounting(), peersync.V2ClientConfig{
		NodeID: from.NodeID.String(), PeerURI: to.addr, Namespaces: []string{ns},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	reply, err := s.SquashHead(ns, have)
	if err != nil {
		t.Fatal(err)
	}
	return reply
}
