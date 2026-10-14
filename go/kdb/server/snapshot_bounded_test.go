package server

import (
	"fmt"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage/engine"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

// TestSnapshotBootstrapBodiesAreEvictable: under bounded live bodies, a namespace bootstrapped from
// a peer's snapshot holds no more of the snapshot's bodies than its cache allows. They used to be
// pinned until a durability notice that a snapshot batch never gets - every body of the namespace,
// for the life of the process. The bodies are in the blob store from the moment they arrive, read
// back exactly, and survive a reopen.
func TestSnapshotBootstrapBodiesAreEvictable(t *testing.T) {
	ns := "app/data"
	source := newTestRuntime(t)
	texts := map[codec.UUID]string{}
	for i := 0; i < 2400; i++ { // ~12 MB of bodies against a 4 MiB version-store floor
		id := mustRandomUUID(t)
		texts[id] = fmt.Sprintf(`{"i":%d,"pad":%q}`, i, strings.Repeat(string(rune('a'+i%26)), 5000))
		if _, err := source.Upsert(ns, id, texts[id], auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	ln, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", source, ns)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	dir := t.TempDir()
	open := func() *KdbServerRuntime {
		t.Helper()
		opts := embed.FileRuntimeOptions{}
		opts.Storage.MemoryBudgetBytes = 4 << 20
		opts.Storage.DocumentCacheBytes = 1 << 20
		opts.Storage.BoundedLiveBodies = true
		rt, err := embed.OpenFileRuntimeWithOptions(dir, "app", ns, schema.None(), opts)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		srv := NewKdbServerRuntime(rt)
		srv.NodeID = mustRandomUUID(t)
		return srv
	}
	requireAll := func(srv *KdbServerRuntime, where string) {
		t.Helper()
		for id, want := range texts {
			body, _, found, err := srv.GetDocument(ns, id)
			if err != nil || !found || body != want {
				t.Fatalf("%s: document %s found=%v err=%v, content right=%v", where, id, found, err, body == want)
			}
		}
	}
	b := open()
	res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
		NodeID: b.NodeID.String(), PeerURI: "tcp://" + ln.Addr().String(), Namespaces: []string{ns},
		Mode: peersync.SyncPull, Local: b.PeerNamespaces(), PreferSnapshot: true, PageBytes: 256 << 10,
	})
	if err != nil || res.Namespaces[0].Err != nil || res.Namespaces[0].Snapshot == "" {
		t.Fatalf("bootstrap: %v %+v", err, res)
	}
	eng, ok := b.Runtime.Storage.(*engine.ServerEngine)
	if !ok {
		t.Skip("not a server engine")
	}
	if n := eng.PendingDurablePins(); n > 0 {
		t.Fatalf("%d snapshot versions pinned waiting for a log write that will never come", n)
	}
	if live := eng.MemoryBreakdown().DocumentBodiesLive; live > 6<<20 {
		t.Fatalf("version store holds %d live body bytes of ~12 MB bootstrapped against a 4 MiB floor", live)
	}
	if !eng.LiveBodiesDurable() {
		t.Fatal("a bootstrapped body is not in the blob store")
	}
	requireAll(b, "after bootstrap")
	if live := eng.MemoryBreakdown().DocumentBodiesLive; live > 6<<20 {
		t.Fatalf("after reading every document the version store holds %d live body bytes against a 4 MiB floor", live)
	}
	b.Runtime.Close()

	reopened := open()
	defer reopened.Runtime.Close()
	requireAll(reopened, "after reopen")
}
