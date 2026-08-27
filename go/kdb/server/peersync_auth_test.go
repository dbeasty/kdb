package server

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/peersync"
	mem "github.com/limidus/kdb/go/kdb/storage/mem"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

// TestListenPeerSyncEnforcesAuth is the regression test for the finding recorded in
// docs/kdb-finish-up-plan.md as 1-G9: with --rbac configured, ListenPeerSync's frameHandler
// stored an auth.Engine/ConnectionContext but never called either, so any TCP peer could
// CommitFetch/CommitPush the whole namespace regardless of RBAC. Proves, over a real TCP socket:
// a peer with no credentials is rejected, a peer with credentials but no "sync" grant is
// rejected, and a peer with a "sync" grant is accepted and can actually fetch/push.
func TestListenPeerSyncEnforcesAuth(t *testing.T) {
	const ns = "app/data"
	runtime := newTestRuntime(t)
	engine, store := newTestRegistryAuthEngine(t)
	runtime.AuthEngine = engine

	if err := store.CreateRole("syncer", []string{"sync:" + ns}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRole("reader-only", []string{"read:" + ns}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateUser("syncer-user", "sync-pw", []string{"syncer"}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateUser("reader-user", "reader-pw", []string{"reader-only"}); err != nil {
		t.Fatal(err)
	}

	listener, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", runtime, ns)
	if err != nil {
		t.Fatalf("ListenPeerSync: %v", err)
	}
	defer listener.Close()

	connect := func(t *testing.T, connCtx auth.ConnectionContext) error {
		t.Helper()
		clientDag, err := dag.NewInMemoryCommitDag(ns)
		if err != nil {
			t.Fatal(err)
		}
		w := wire.NewCodec(wire.EncodingJSON)
		transport := tcp.NewTransport(core.DefaultConnectOptions())
		client := peersync.NewClient(w, transport, clientDag, mem.NewInMemoryStorageAdapter())
		_, err = client.Connect(peersync.ClientConfig{
			NamespaceID:       ns,
			NodeID:            "test-client",
			PeerURI:           "tcp://" + listener.Addr().String(),
			ConnectionContext: connCtx,
		})
		return err
	}

	t.Run("no credentials rejected", func(t *testing.T) {
		if err := connect(t, auth.ConnectionContext{}); err == nil {
			t.Fatal("expected handshake to be rejected for a peer with no credentials")
		}
	})

	t.Run("credentials without sync grant rejected", func(t *testing.T) {
		err := connect(t, auth.ConnectionContext{User: ptr("reader-user"), Password: ptr("reader-pw")})
		if err == nil {
			t.Fatal("expected handshake to be rejected for a principal with only a read grant")
		}
	})

	t.Run("credentials with sync grant accepted", func(t *testing.T) {
		clientDag, err := dag.NewInMemoryCommitDag(ns)
		if err != nil {
			t.Fatal(err)
		}
		clientStorage := mem.NewInMemoryStorageAdapter()
		genesis, err := clientDag.Head()
		if err != nil {
			t.Fatal(err)
		}
		docID, err := codec.RandomUUID()
		if err != nil {
			t.Fatal(err)
		}
		clientCommit := pushDoc(t, clientDag, clientStorage, ns, genesis, docID, `{"greeting":"authorized peer"}`)

		w := wire.NewCodec(wire.EncodingJSON)
		transport := tcp.NewTransport(core.DefaultConnectOptions())
		client := peersync.NewClient(w, transport, clientDag, clientStorage)
		session, err := client.Connect(peersync.ClientConfig{
			NamespaceID: ns,
			NodeID:      "test-client",
			PeerURI:     "tcp://" + listener.Addr().String(),
			ConnectionContext: auth.ConnectionContext{
				User:     ptr("syncer-user"),
				Password: ptr("sync-pw"),
			},
		})
		if err != nil {
			t.Fatalf("expected handshake to be accepted for a principal with a sync grant: %v", err)
		}
		defer client.Disconnect()
		pushed, err := session.PushCommits([]document.Commit{clientCommit})
		if err != nil {
			t.Fatalf("PushCommits for an authorized peer: %v", err)
		}
		if pushed != 1 {
			t.Fatalf("expected 1 commit applied, got %d", pushed)
		}
	})
}
