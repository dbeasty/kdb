package cli

import (
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

// TestCLIDeepen: a data directory bootstrapped from a peer by snapshot has a shallow root;
// `kdb deepen` fetches the history below it and leaves none (exit 0).
func TestCLIDeepen(t *testing.T) {
	ns := "app/data"
	peerRT, err := embed.OpenMemoryRuntime("app", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	peer := server.NewKdbServerRuntime(peerRT)
	for i := 0; i < 3; i++ {
		id, _ := codec.RandomUUID()
		if _, err := peer.Upsert(ns, id, `{"n":1}`, auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	ln, err := server.ListenPeerSync("tcp://127.0.0.1:0?bind=true", peer, ns)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	addr := "tcp://" + ln.Addr().String()

	dir := t.TempDir()
	rt, err := embed.OpenFileRuntime(dir, "app", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	local := server.NewKdbServerRuntime(rt)
	res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
		NodeID: "local", PeerURI: addr, Namespaces: []string{ns}, Local: local.PeerNamespaces(), PreferSnapshot: true,
	})
	if err != nil || res.Namespaces[0].Snapshot == "" {
		t.Fatalf("bootstrap: %+v %v", res, err)
	}
	rt.Close()

	code, out := captureRun(t, "--data-dir", dir, "deepen", ns, addr)
	if code != 0 || !strings.Contains(out, "history complete") || !strings.Contains(out, "0 shallow root(s) left") {
		t.Fatalf("kdb deepen: %d %q", code, out)
	}
	code, out = captureRun(t, "--data-dir", dir, "deepen", ns, addr)
	if code != 0 || !strings.Contains(out, "0 shallow root(s) left") {
		t.Fatalf("a second deepen has nothing to do: %d %q", code, out)
	}
}
