package server

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

type checkpointFile struct {
	path string
	data []byte
}

func readCheckpoints(t *testing.T, root string) []checkpointFile {
	t.Helper()
	var out []checkpointFile
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.Contains(filepath.ToSlash(path), "/snap/kdb_checkpoint_") && !strings.Contains(path, ".tmp-") {
			b, _ := os.ReadFile(path)
			out = append(out, checkpointFile{path, b})
		}
		return nil
	})
	if len(out) == 0 {
		t.Fatal("no checkpoint found")
	}
	return out
}

// TestSnapshotBootstrapSurvivesRestart: a file-backed namespace bootstrapped from a peer's
// snapshot keeps the snapshot's documents across a clean restart and across a crash after later
// writes (the bootstrap checkpoint plus the log tail), and refuses to open - rather than opening
// empty - if that checkpoint is lost.
func TestSnapshotBootstrapSurvivesRestart(t *testing.T) {
	ns := "app/data"
	source := newTestRuntime(t)
	var ids []codec.UUID
	for i := 0; i < 40; i++ {
		id := mustRandomUUID(t)
		ids = append(ids, id)
		if _, err := source.Upsert(ns, id, fmt.Sprintf(`{"i":%d}`, i), auth.Principal{}); err != nil {
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
		rt, err := embed.OpenFileRuntime(dir, "app", ns, schema.None())
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		srv := NewKdbServerRuntime(rt)
		srv.NodeID = mustRandomUUID(t)
		return srv
	}
	b := open()
	res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
		NodeID: b.NodeID.String(), PeerURI: "tcp://" + ln.Addr().String(), Namespaces: []string{ns},
		Mode: peersync.SyncPull, Local: b.PeerNamespaces(), PreferSnapshot: true, PageBytes: 200,
	})
	if err != nil || res.Namespaces[0].Err != nil || res.Namespaces[0].Snapshot == "" {
		t.Fatalf("bootstrap: %v %+v", err, res)
	}
	root, _ := source.dag.Head()
	bootstrapCheckpoint := readCheckpoints(t, dir)

	local := mustRandomUUID(t)
	after, err := b.Upsert(ns, local, `{"after":"bootstrap"}`, auth.Principal{})
	if err != nil {
		t.Fatalf("write after bootstrap: %v", err)
	}
	b.Runtime.Close()

	check := func(label string, wantHead codec.Hash) {
		t.Helper()
		srv := open()
		defer srv.Runtime.Close()
		if h, _ := srv.dag.Head(); h != wantHead {
			t.Fatalf("%s: head %s, want %s", label, h.Hex(), wantHead.Hex())
		}
		if !srv.dag.IsShallow(root) {
			t.Fatalf("%s: the snapshot root is not a shallow root after reopening", label)
		}
		for i, id := range ids {
			body, _, found, err := srv.GetDocument(ns, id)
			if err != nil || !found || body != fmt.Sprintf(`{"i":%d}`, i) {
				t.Fatalf("%s: snapshot document %d = %q found=%v err=%v", label, i, body, found, err)
			}
		}
		if body, _, found, _ := srv.GetDocument(ns, local); !found || body != `{"after":"bootstrap"}` {
			t.Fatalf("%s: the post-bootstrap write is %q", label, body)
		}
		// The next write builds on what was restored.
		if _, err := srv.Upsert(ns, mustRandomUUID(t), `{"next":true}`, auth.Principal{}); err != nil {
			t.Fatalf("%s: write after reopening: %v", label, err)
		}
	}
	check("clean restart", after.Hash)

	// A crash between the post-bootstrap write and the next checkpoint: only the bootstrap's
	// checkpoint and the log exist. (The previous check added one more commit; roll the whole
	// directory's checkpoints back to the bootstrap's.)
	for _, f := range bootstrapCheckpoint {
		if err := os.WriteFile(f.path, f.data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	srv := open()
	h, _ := srv.dag.Head()
	srv.Runtime.Close()
	if h == root {
		t.Fatal("after a crash the log tail was not replayed onto the bootstrap checkpoint")
	}

	for _, f := range readCheckpoints(t, dir) {
		os.Remove(f.path)
	}
	_, err = embed.OpenFileRuntime(dir, "app", ns, schema.None())
	var missing *embed.SnapshotCheckpointMissingError
	if !errors.As(err, &missing) {
		t.Fatalf("opening without the bootstrap checkpoint: want SnapshotCheckpointMissingError, got %v", err)
	}
}

// TestNestedNamespacesBootstrapFromSnapshots: two namespaces on one file-backed node, where one's
// name is a prefix of the other's, can both be bootstrapped from a peer's snapshot and both
// reopen afterwards.
//
// Nesting is how namespace patterns are meant to be used ("u/<account>" for what a person writes,
// "u/<account>/ro" for what the cloud writes for them), and it used to be impossible here: the
// checkpoint kept the id's slash as a real path separator, so the outer namespace's checkpoint
// was the FILE snap/kdb_checkpoint_u/<account> and the nested one needed that same path to be a
// directory. The second bootstrap failed with "not a directory" - and did not recover, because by
// then the namespace had a commit, so every later attempt was refused as needing a fresh
// namespace and it could never be bootstrapped on that node again.
func TestNestedNamespacesBootstrapFromSnapshots(t *testing.T) {
	outer, nested := "repro/account", "repro/account/ro"
	dir := t.TempDir()

	// One source per namespace, each serving its own listener.
	serve := func(ns string, body string) (string, codec.UUID) {
		t.Helper()
		rt, err := embed.OpenMemoryRuntime(embed.CatalogFromNamespace(ns), ns, schema.None())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { rt.Close() })
		src := NewKdbServerRuntime(rt)
		id := mustRandomUUID(t)
		if _, err := src.Upsert(ns, id, body, auth.Principal{}); err != nil {
			t.Fatal(err)
		}
		ln, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", src, ns)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { ln.Close() })
		return ln.Addr().String(), id
	}
	outerAddr, outerDoc := serve(outer, `{"who":"outer"}`)
	nestedAddr, nestedDoc := serve(nested, `{"who":"nested"}`)

	open := func(ns string) *KdbServerRuntime {
		t.Helper()
		rt, err := embed.OpenFileRuntime(dir, embed.CatalogFromNamespace(ns), ns, schema.None())
		if err != nil {
			t.Fatalf("open %s: %v", ns, err)
		}
		srv := NewKdbServerRuntime(rt)
		srv.NodeID = mustRandomUUID(t)
		return srv
	}
	bootstrap := func(ns, addr string) {
		t.Helper()
		b := open(ns)
		defer b.Runtime.Close()
		res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
			NodeID: b.NodeID.String(), PeerURI: "tcp://" + addr, Namespaces: []string{ns},
			Mode: peersync.SyncPull, Local: b.PeerNamespaces(), PreferSnapshot: true,
		})
		if err != nil {
			t.Fatalf("bootstrapping %s: %v", ns, err)
		}
		if res.Namespaces[0].Err != nil {
			t.Fatalf("bootstrapping %s: %v", ns, res.Namespaces[0].Err)
		}
		if res.Namespaces[0].Snapshot == "" {
			t.Fatalf("bootstrapping %s did not install a snapshot: %+v", ns, res.Namespaces[0])
		}
	}
	// The outer namespace first, so its checkpoint is the one already on disk when the nested
	// namespace needs the same path.
	bootstrap(outer, outerAddr)
	bootstrap(nested, nestedAddr)

	// Both reopen from what is on disk, each holding its own snapshot's document - and each
	// namespace's checkpoint still describes its own namespace, not its neighbour's.
	for _, c := range []struct {
		ns, want string
		doc      codec.UUID
	}{
		{outer, `{"who":"outer"}`, outerDoc},
		{nested, `{"who":"nested"}`, nestedDoc},
	} {
		srv := open(c.ns)
		body, _, found, err := srv.GetDocument(c.ns, c.doc)
		if err != nil || !found || body != c.want {
			t.Fatalf("%s after reopening: document = %q found=%v err=%v, want %q", c.ns, body, found, err, c.want)
		}
		h, err := srv.dag.Head()
		if err != nil {
			t.Fatalf("%s: head after reopening: %v", c.ns, err)
		}
		if !srv.dag.IsShallow(h) {
			t.Fatalf("%s: the snapshot root is not a shallow root after reopening", c.ns)
		}
		srv.Runtime.Close()
	}
}

// TestSnapshotRefusedIntoANamespaceWithHistory: a file-backed namespace that has commits of its
// own cannot be bootstrapped - the snapshot's root would have no place in its log.
func TestSnapshotRefusedIntoANamespaceWithHistory(t *testing.T) {
	ns := "app/data"
	dir := t.TempDir()
	rt, err := embed.OpenFileRuntime(dir, "app", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	if err := rt.CanInstallSnapshot(); err != nil {
		t.Fatalf("a fresh namespace should accept a snapshot: %v", err)
	}
	srv := NewKdbServerRuntime(rt)
	if _, err := srv.Upsert(ns, mustRandomUUID(t), `{"x":1}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	if err := rt.CanInstallSnapshot(); !errors.Is(err, embed.ErrSnapshotNeedsFreshNamespace) {
		t.Fatalf("want ErrSnapshotNeedsFreshNamespace, got %v", err)
	}
}
