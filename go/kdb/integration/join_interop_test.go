package integration

import (
	"fmt"
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

// TestKotlinReadsAGoNamespaceSettledOnACanonicalJoin: four nodes in a pull ring settle on one
// canonical join - a content-free merge naming three content-carrying commits - which one Go
// file-backed node logs. Kotlin does not make such joins, but it must store and replay what it
// is given: the Kotlin CLI reads the directory and every document reads exactly as Go holds it.
func TestKotlinReadsAGoNamespaceSettledOnACanonicalJoin(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the Kotlin CLI through gradle")
	}
	repo := findRepoRoot(t)
	root := t.TempDir()
	ns := "app/data"
	fileRT, err := embed.OpenFileRuntime(root, "app", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	f := server.NewKdbServerRuntime(fileRT)
	f.NodeID = randomID(t)
	nodes := []*server.KdbServerRuntime{f}
	for i := 0; i < 3; i++ {
		rt, err := embed.OpenMemoryRuntime("app", ns, schema.None())
		if err != nil {
			t.Fatal(err)
		}
		s := server.NewKdbServerRuntime(rt)
		s.NodeID = randomID(t)
		nodes = append(nodes, s)
	}
	lns := make([]*server.Listener, len(nodes))
	for i, n := range nodes {
		ln, err := server.ListenPeerSync("tcp://127.0.0.1:0?bind=true", n, ns)
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()
		lns[i] = ln
	}
	want := map[codec.UUID]string{}
	for i, n := range nodes {
		id := randomID(t)
		// A whole-document replace on top of the first write: a delete and a write in one commit.
		for _, body := range []string{fmt.Sprintf(`{"from":%d,"v":1}`, i), fmt.Sprintf(`{"from":%d,"v":2}`, i)} {
			if _, err := n.PutJSON(ns, id, body, nil, auth.Principal{}); err != nil {
				t.Fatal(err)
			}
			want[id] = body
		}
	}
	heads := func() []codec.Hash {
		out := make([]codec.Hash, len(nodes))
		for i, n := range nodes {
			out[i], _ = n.PeerIngestEnv().DAG.Head()
		}
		return out
	}
	settled := func() bool {
		hs := heads()
		for _, h := range hs[1:] {
			if h != hs[0] {
				return false
			}
		}
		return true
	}
	for r := 0; r < 8 && !settled(); r++ {
		for i := range nodes {
			res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
				NodeID: nodes[i].NodeID.String(), PeerURI: "tcp://" + lns[(i+1)%len(nodes)].Addr().String(),
				Namespaces: []string{ns}, Local: nodes[i].PeerNamespaces(), Mode: peersync.SyncPull,
			})
			if err != nil || res.Namespaces[0].Err != nil {
				t.Fatalf("pull: %v %+v", err, res)
			}
		}
	}
	if !settled() {
		t.Fatalf("the ring did not settle: %v", heads())
	}
	d := f.PeerIngestEnv().DAG
	head, _ := d.Head()
	join, err := d.GetCommitOrThrow(head)
	if err != nil {
		t.Fatal(err)
	}
	if !d.IsContentFreeMerge(join) || len(join.ParentHashes) < 3 {
		t.Fatalf("the ring settled on %s with %d parents; the test wants a join of three or more", head.Hex(), len(join.ParentHashes))
	}
	for id, body := range want {
		got, _, found, err := f.GetDocument(ns, id)
		if err != nil || !found || !sameJSON(got, body) {
			t.Fatalf("Go holds %q for %s, want %s (%v)", got, id, body, err)
		}
	}
	fileRT.Close()

	for id, body := range want {
		if out := lastLine(kotlinCLI(t, repo, "--data-dir", root, "get", ns, id.String())); !sameJSON(out, body) {
			t.Fatalf("Kotlin read %q for %s, Go holds %s", out, id, body)
		}
	}
}
