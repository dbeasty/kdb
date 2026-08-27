package server

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/peersync"
	mem "github.com/limidus/kdb/go/kdb/storage/mem"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

// TestListenPeerSyncPushIsVisibleToServerQueries is the front-door regression test: a real
// peersync.Client, over a real TCP socket, pushes a commit at a server started via
// ListenPeerSync - the same wiring go/cmd/kdb-service now uses (--peer-addr, previously always
// "disabled" with nothing behind it). The commit must both fast-forward the server's DAG head
// (peersync's own contract, already tested) and be readable back through the server's ordinary
// document API (GetDocument), which only works if MaterializeCommit actually replayed the
// commit's ops into runtime storage - dag.PutCommit alone does not do that.
func TestListenPeerSyncPushIsVisibleToServerQueries(t *testing.T) {
	const ns = "app/data" // matches newTestRuntime's fixed namespace
	runtime := newTestRuntime(t)

	listener, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", runtime, ns)
	if err != nil {
		t.Fatalf("ListenPeerSync: %v", err)
	}
	defer listener.Close()

	clientDag, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatalf("client dag: %v", err)
	}
	clientStorage := mem.NewInMemoryStorageAdapter()
	genesis, err := clientDag.Head()
	if err != nil {
		t.Fatalf("client genesis head: %v", err)
	}

	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatalf("random uuid: %v", err)
	}
	docJSON := `{"greeting":"hello from a real tcp peer"}`
	clientCommit := pushDoc(t, clientDag, clientStorage, ns, genesis, docID, docJSON)

	w := wire.NewCodec(wire.EncodingJSON)
	transport := tcp.NewTransport(core.DefaultConnectOptions())
	client := peersync.NewClient(w, transport, clientDag, clientStorage)
	session, err := client.Connect(peersync.ClientConfig{
		NamespaceID: ns,
		NodeID:      "test-client",
		PeerURI:     "tcp://" + listener.Addr().String(),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Disconnect()

	pushed, err := session.PushCommits([]document.Commit{clientCommit})
	if err != nil {
		t.Fatalf("pushCommits: %v", err)
	}
	if pushed != 1 {
		t.Fatalf("expected 1 commit applied, got %d", pushed)
	}

	head, err := runtime.dag.Head()
	if err != nil {
		t.Fatalf("server head: %v", err)
	}
	if head != clientCommit.Hash {
		t.Fatalf("server did not fast-forward to the pushed commit: head=%s pushed=%s", head.Hex(), clientCommit.Hash.Hex())
	}

	json, commitHex, found, err := runtime.GetDocument(ns, docID)
	if err != nil {
		t.Fatalf("GetDocument: %v", err)
	}
	if !found {
		t.Fatal("pushed document not visible via GetDocument - MaterializeCommit did not replay it into storage")
	}
	if json != docJSON {
		t.Fatalf("expected JSON %q, got %q", docJSON, json)
	}
	if commitHex != clientCommit.Hash.Hex() {
		t.Fatalf("expected commit hex %s, got %s", clientCommit.Hash.Hex(), commitHex)
	}
}

// pushDoc writes docID directly against d/store (bypassing transaction.Engine, same shape as
// kdb/peersync's own writeDoc test helper) and appends a commit on parent.
func pushDoc(t *testing.T, d *dag.InMemoryCommitDag, store *mem.InMemoryStorageAdapter, ns string, parent codec.Hash, docID codec.UUID, jsonText string) document.Commit {
	t.Helper()
	if err := store.PutDocument(ns, document.Document{ID: docID, JSON: jsonText}); err != nil {
		t.Fatalf("putDocument: %v", err)
	}
	parentCommit, err := d.GetCommitOrThrow(parent)
	if err != nil {
		t.Fatalf("getCommitOrThrow(parent): %v", err)
	}
	tree, err := store.CommitTree(ns, parentCommit.DocumentTreeHash)
	if err != nil {
		t.Fatalf("commitTree: %v", err)
	}
	txID, err := codec.RandomUUID()
	if err != nil {
		t.Fatalf("random uuid: %v", err)
	}
	authorID, err := codec.RandomUUID()
	if err != nil {
		t.Fatalf("random uuid: %v", err)
	}
	tx := document.Transaction{
		ID:           txID,
		BaseVersion:  parent,
		Operations:   []document.Op{document.WriteOp{DocID: docID, Patch: jsonText}},
		Timestamp:    codec.TimestampNow(),
		AuthorNodeID: authorID,
	}
	commit, err := d.AppendCommit(tx, parent, tree, nil, "test push")
	if err != nil {
		t.Fatalf("appendCommit: %v", err)
	}
	return commit
}
