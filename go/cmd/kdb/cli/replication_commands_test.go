package cli

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
)

// TestSyncPullsFromALivePeer: `kdb sync` against a running peer-sync listener brings the local
// data directory up to the peer's head, through the same ingest a service would use.
func TestSyncPullsFromALivePeer(t *testing.T) {
	ns := "app/data"
	rt, err := embed.OpenMemoryRuntime("app", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	peer := server.NewKdbServerRuntime(rt)
	id, _ := codec.RandomUUID()
	c, err := peer.Upsert(ns, id, `{"from":"peer"}`, auth.Principal{})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := server.ListenPeerSync("tcp://127.0.0.1:0?bind=true", peer, ns)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	dir := t.TempDir()
	if code := Run([]string{"--data-dir", dir, "--quiet", "sync", ns, "tcp://" + ln.Addr().String(), "--pull"}); code != 0 {
		t.Fatalf("kdb sync exited %d", code)
	}
	local, err := embed.OpenFileRuntime(dir, "app", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	if h, _ := local.DAG.Head(); h != c.Hash {
		t.Fatalf("local head %s, peer head %s", h.Hex(), c.Hash.Hex())
	}
	if code := Run([]string{"--data-dir", dir, "node", "status"}); code != 0 {
		t.Fatalf("kdb node status exited %d", code)
	}
}
