package server

import (
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
)

// peerPush is what a peer's push to rt carries: every commit reachable from the source's head
// that rt's main does not have, parents first.
func peerPush(t *testing.T, source, rt *KdbServerRuntime) ([]document.Commit, codec.Hash) {
	t.Helper()
	srcHead, err := source.dag.Head()
	if err != nil {
		t.Fatal(err)
	}
	have, err := rt.dag.Head()
	if err != nil {
		t.Fatal(err)
	}
	commits, _, err := peersync.MissingCommits(source.dag, srcHead, []codec.Hash{have}, 1000)
	if err != nil {
		t.Fatal(err)
	}
	return commits, srcHead
}

// fileNode opens (and reopens) a file-backed node at dir.
func fileNode(t *testing.T, dir, ns string) func() *KdbServerRuntime {
	return func() *KdbServerRuntime {
		t.Helper()
		rt, err := embed.OpenFileRuntime(dir, "app", ns, schema.None())
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		srv := NewKdbServerRuntime(rt)
		srv.NodeID = mustRandomUUID(t)
		return srv
	}
}

// peerAhead gives b a logged write of its own - so its log has a segment and a clean close
// writes a checkpoint - and returns a peer that holds b's history plus writes of its own, so a
// push from it to b is a fast-forward.
func peerAhead(t *testing.T, ns string, b *KdbServerRuntime, writes int) (*KdbServerRuntime, []codec.UUID) {
	t.Helper()
	if _, err := b.Upsert(ns, mustRandomUUID(t), `{"local":true}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	source := newTestRuntime(t)
	commits, head := peerPush(t, b, source)
	if _, err := peersync.Ingest(source.PeerIngestEnv(), commits, head); err != nil {
		t.Fatalf("seeding the peer: %v", err)
	}
	var ids []codec.UUID
	for i := 0; i < writes; i++ {
		id := mustRandomUUID(t)
		ids = append(ids, id)
		if _, err := source.Upsert(ns, id, fmt.Sprintf(`{"i":%d}`, i), auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	return source, ids
}

// TestPeerCommitStoredDuringDrainSurvivesCleanRestart is the 3-node e2e drain failure at the layer
// it happens in. A peer's push lands as the node starts draining: StoreCommits admits the commits
// to the DAG (outside the write serialization, by design), and Adopt is then refused because the
// node is draining - so they are never logged. The clean close that follows checkpointed the whole
// in-memory DAG, these commits included, without their operations (a checkpoint carries
// operations only for branch heads and shallow roots; everything else is read back from the
// log). The reopened node held commits whose operations nothing could produce, and every later
// sync that reached them failed with "has no frame in the delta log", forever.
func TestPeerCommitStoredDuringDrainSurvivesCleanRestart(t *testing.T) {
	ns := "app/data"
	open := fileNode(t, t.TempDir(), ns)
	b := open()
	source, ids := peerAhead(t, ns, b, 3)
	commits, srcHead := peerPush(t, source, b)
	b.BeginDraining()
	_, err := peersync.Ingest(b.PeerIngestEnv(), commits, srcHead)
	var unavailable *UnavailableError
	if !errors.As(err, &unavailable) {
		t.Fatalf("a push while draining: got %v, want the drain's refusal", err)
	}
	if !b.dag.HasCommit(srcHead) {
		t.Fatal("precondition: the refused push's commits are expected to be stored, only not adopted")
	}
	b.Release()

	b = open()
	defer b.Release()
	for _, c := range commits {
		if !b.dag.HasCommit(c.Hash) {
			continue
		}
		if _, err := b.dag.CommitOperations(c.Hash); err != nil {
			t.Errorf("reopened node holds commit %s but cannot read its operations: %v", c.Hash.Hex(), err)
		}
	}

	// The same push, retried after the restart, goes through.
	commits, srcHead = peerPush(t, source, b)
	if _, err := peersync.Ingest(b.PeerIngestEnv(), commits, srcHead); err != nil {
		t.Fatalf("sync after restart: %v", err)
	}
	if h, _ := b.dag.Head(); h != srcHead {
		t.Fatalf("head after sync %s, want %s", h.Hex(), srcHead.Hex())
	}
	for i, id := range ids {
		if body, _, found, err := b.GetDocument(ns, id); err != nil || !found || body != fmt.Sprintf(`{"i":%d}`, i) {
			t.Fatalf("document %d after sync: %q found=%v err=%v", i, body, found, err)
		}
	}
}

// TestPeerCommitCheckpointedUnloggedByOlderVersionHeals covers a data directory a released
// version already left in the broken state: its checkpoint names a commit received from a peer
// that never reached the log. A peer's copy of that commit carries its operations, and the hash
// covers them, so receiving it again puts them back - and the sync that was failing forever goes
// through.
func TestPeerCommitCheckpointedUnloggedByOlderVersionHeals(t *testing.T) {
	ns := "app/data"
	open := fileNode(t, t.TempDir(), ns)
	b := open()
	source, ids := peerAhead(t, ns, b, 1)
	id := ids[0]
	commits, srcHead := peerPush(t, source, b)
	// What v0.6.2's StoreCommits did: admit to the DAG with nothing recording that the commit is
	// not in the log, so the clean close below checkpoints it.
	for _, c := range commits {
		if err := b.dag.PutCommit(c, true); err != nil {
			t.Fatal(err)
		}
	}
	b.Release()

	b = open()
	if !b.dag.HasCommit(srcHead) {
		t.Fatal("precondition: the checkpoint is expected to restore the unlogged commit")
	}
	if _, err := b.dag.CommitOperations(srcHead); err == nil {
		t.Fatal("precondition: the restored commit's operations are expected to be unreadable")
	}

	commits, srcHead = peerPush(t, source, b)
	if _, err := peersync.Ingest(b.PeerIngestEnv(), commits, srcHead); err != nil {
		t.Fatalf("sync after restart: %v", err)
	}
	if body, _, found, err := b.GetDocument(ns, id); err != nil || !found || body != `{"i":0}` {
		t.Fatalf("document after sync: %q found=%v err=%v", body, found, err)
	}
	b.Release()

	// And now that it is adopted, it is in the log: a reopen reads it back.
	b = open()
	defer b.Release()
	if _, err := b.dag.CommitOperations(srcHead); err != nil {
		t.Fatalf("adopted commit after another restart: %v", err)
	}
}

// TestPeerSyncListenerCloseEndsInboundSessions: closing the peer-sync listener used to stop only
// the accept loop. A session a peer had already opened went on running - storing pushed commits
// into the DAG while the node drained, checkpointed and closed its storage underneath it. Close
// now ends every inbound session and waits for its handler to return, so the shutdown sequence
// can stop peer sync before the final checkpoint.
func TestPeerSyncListenerCloseEndsInboundSessions(t *testing.T) {
	rt := newTestRuntime(t)
	ln, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", rt, "app/data")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// Give the accept loop time to hand the connection to its handler.
	time.Sleep(100 * time.Millisecond)

	closed := make(chan struct{})
	go func() {
		_ = ln.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("listener Close did not return")
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("read a byte from a session that should have been ended")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("the inbound session is still open after the listener closed")
	}
}
