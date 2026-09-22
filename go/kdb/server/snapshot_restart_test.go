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
