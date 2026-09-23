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
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/io"
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

// failingSnapshotInstall wraps a provider so the durable half of a snapshot bootstrap
// (PersistSnapshot plus the unique-key rebuild) can be made to fail on demand - a full disk, a
// directory that cannot be written. Everything else about the node is real.
type failingSnapshotInstall struct {
	inner peersync.NamespaceProvider
	fail  *bool
}

func (f failingSnapshotInstall) List() []string { return f.inner.List() }

func (f failingSnapshotInstall) Env(ns string, create bool) (peersync.IngestEnv, error) {
	env, err := f.inner.Env(ns, create)
	if err != nil {
		return env, err
	}
	real := env.SnapshotInstalled
	env.SnapshotInstalled = func(root document.Commit) error {
		if *f.fail {
			return errors.New("injected: no space left on device")
		}
		return real(root)
	}
	return env, nil
}

// TestSnapshotBootstrapCanBeRetriedAfterAFailedInstall: a file-backed node whose bootstrap fails
// after the documents are in - the checkpoint could not be written - can be bootstrapped again
// once the cause is gone, in the same process, and what it ends up with survives a restart.
//
// The failure used to be permanent. InstallSnapshot put main back and deleted the documents but
// left the snapshot's root resident in the DAG, and CanInstallSnapshot refuses a namespace
// holding more than its genesis commit - so every later attempt was refused as needing a fresh
// namespace, for the life of the process, with no way back but a restart. It is the same shape
// as the nested-namespace failure above, reached by a different cause.
func TestSnapshotBootstrapCanBeRetriedAfterAFailedInstall(t *testing.T) {
	ns := "app/data"
	source := newTestRuntime(t)
	var ids []codec.UUID
	for i := 0; i < 12; i++ {
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
	root, _ := source.dag.Head()

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
	fail := true
	bootstrap := func() peersync.V2Result {
		t.Helper()
		res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
			NodeID: b.NodeID.String(), PeerURI: "tcp://" + ln.Addr().String(), Namespaces: []string{ns},
			Mode: peersync.SyncPull, Local: failingSnapshotInstall{inner: b.PeerNamespaces(), fail: &fail},
			PreferSnapshot: true, PageBytes: 200,
		})
		if err != nil {
			t.Fatalf("sync: %v", err)
		}
		return res
	}

	res := bootstrap()
	if err := res.Namespaces[0].Err; err == nil || !strings.Contains(err.Error(), "no space left") {
		t.Fatalf("expected the bootstrap to fail on the persist hook, got %v", err)
	}
	if err := b.Runtime.CanInstallSnapshot(); err != nil {
		t.Fatalf("after a failed bootstrap the namespace refuses to be bootstrapped again: %v", err)
	}
	if n := b.dag.CommitCount(); n != 1 {
		t.Fatalf("a failed bootstrap left %d commits in the DAG, want 1 (genesis)", n)
	}

	fail = false
	res = bootstrap()
	if res.Namespaces[0].Err != nil || res.Namespaces[0].Snapshot != root.Hex() {
		t.Fatalf("the retry did not bootstrap: %+v", res.Namespaces[0])
	}
	if h, _ := b.dag.Head(); h != root || !b.dag.IsShallow(root) {
		t.Fatal("after the retry, main is not the peer's head as a shallow root")
	}
	for i, id := range ids {
		body, _, found, err := b.GetDocument(ns, id)
		if err != nil || !found || body != fmt.Sprintf(`{"i":%d}`, i) {
			t.Fatalf("document %d after the retry = %q found=%v err=%v", i, body, found, err)
		}
	}
	b.Runtime.Close()

	// The retry's own durability is real, not just its in-memory result.
	reopened := open()
	defer reopened.Runtime.Close()
	if h, _ := reopened.dag.Head(); h != root {
		t.Fatalf("after reopening, head is %s, want the snapshot root %s", h.Hex(), root.Hex())
	}
	for i, id := range ids {
		body, _, found, err := reopened.GetDocument(ns, id)
		if err != nil || !found || body != fmt.Sprintf(`{"i":%d}`, i) {
			t.Fatalf("document %d after reopening = %q found=%v err=%v", i, body, found, err)
		}
	}
}

// uniqueSchema declares one indexed, unique field.
func uniqueSchema(t *testing.T, field string) schema.KdbSchema {
	t.Helper()
	f, err := schema.NewField(field, schema.StringType{}, false, true, true)
	if err != nil {
		t.Fatal(err)
	}
	sch, err := schema.Build([]schema.Field{f}, 1, codec.TimestampNow(), "")
	if err != nil {
		t.Fatal(err)
	}
	return sch
}

// snapshotSource serves a peer's namespace over peer sync and returns its address.
func snapshotSource(t *testing.T, ns string, bodies ...string) (addr string, head codec.Hash) {
	t.Helper()
	rt, err := embed.OpenMemoryRuntime("demo", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rt.Close() })
	src := NewKdbServerRuntime(rt)
	for _, body := range bodies {
		if _, err := src.Upsert(ns, mustRandomUUID(t), body, auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	ln, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", src, ns)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	h, _ := src.dag.Head()
	return ln.Addr().String(), h
}

// TestSnapshotInstallWritesNothingDurableUntilItCanSucceed: the durable half of the install hook
// runs last, so a bootstrap refused by anything after the documents are in leaves no checkpoint
// and no namespace marker behind.
//
// The refusal here is the real one: the peer's documents violate a unique constraint this node's
// schema declares, which the peer - not having that schema - never had to honour. When
// PersistSnapshot ran first, that left a checkpoint on disk claiming a bootstrap that
// InstallSnapshot had just rolled back in memory: a namespace that looks fresh, takes local
// writes on genesis, and reopens bootstrapped from a checkpoint its log does not descend from.
func TestSnapshotInstallWritesNothingDurableUntilItCanSucceed(t *testing.T) {
	ns := "app/data"
	addr, _ := snapshotSource(t, ns, `{"email":"a@example.com"}`, `{"email":"a@example.com"}`)

	dir := t.TempDir()
	open := func() *KdbServerRuntime {
		t.Helper()
		rt, err := embed.OpenFileRuntime(dir, "app", ns, uniqueSchema(t, "email"))
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		srv := NewKdbServerRuntime(rt)
		srv.NodeID = mustRandomUUID(t)
		return srv
	}
	b := open()
	res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
		NodeID: b.NodeID.String(), PeerURI: "tcp://" + addr, Namespaces: []string{ns},
		Mode: peersync.SyncPull, Local: b.PeerNamespaces(), PreferSnapshot: true,
	})
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if res.Namespaces[0].Err == nil {
		t.Fatal("a snapshot violating this node's unique constraint was installed")
	}

	// Nothing durable: no checkpoint, and no shallow root in the namespace marker.
	if n := len(findCheckpoints(dir)); n != 0 {
		t.Fatalf("a refused bootstrap wrote %d checkpoint(s)", n)
	}
	if marker, err := os.ReadFile(filepath.Join(dir, "ns", "app", "data", "meta.json")); err == nil {
		if strings.Contains(string(marker), "shallowRoots\":[\"") {
			t.Fatalf("a refused bootstrap recorded a shallow root in the marker: %s", marker)
		}
	}
	// Nothing in memory either, so the node can still be bootstrapped, and the registry holds no
	// claims from documents that were rolled back.
	if err := b.Runtime.CanInstallSnapshot(); err != nil {
		t.Fatalf("after a refused bootstrap the namespace cannot be bootstrapped again: %v", err)
	}
	if n := b.UniqueKeys.Len(); n != 0 {
		t.Fatalf("the unique registry kept %d claim(s) from a rolled-back snapshot", n)
	}
	if _, err := b.Upsert(ns, mustRandomUUID(t), `{"email":"a@example.com"}`, auth.Principal{}); err != nil {
		t.Fatalf("a local write was refused by a claim no live document holds: %v", err)
	}
	b.Runtime.Close()

	// And it reopens as the namespace it is - one local write on genesis - rather than from a
	// checkpoint describing a bootstrap that never happened.
	reopened := open()
	defer reopened.Runtime.Close()
	n := 0
	if err := reopened.Runtime.Storage.(storage.TreeWalker).WalkTree(ns, mustHeadTree(t, reopened), func(codec.UUID, codec.Hash) bool {
		n++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("after reopening, the namespace holds %d documents, want 1 (the local write)", n)
	}
}

// findCheckpoints lists the checkpoint files under root, without failing when there are none.
func findCheckpoints(root string) []string {
	var out []string
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.Contains(filepath.ToSlash(path), "/snap/kdb_checkpoint_") && !strings.Contains(path, ".tmp-") {
			out = append(out, path)
		}
		return nil
	})
	return out
}

func mustHeadTree(t *testing.T, srv *KdbServerRuntime) codec.Hash {
	t.Helper()
	_, head, ok, err := srv.dag.HeadCommit()
	if err != nil || !ok {
		t.Fatalf("head commit: ok=%v err=%v", ok, err)
	}
	return head.DocumentTreeHash
}

// TestSnapshotInstallDropsUniqueClaimsWhenItCannotBeMadeDurable: the other half of running the
// unique-key rebuild first. The rebuild succeeds and claims every key the snapshot's documents
// hold; if the checkpoint then cannot be written, InstallSnapshot takes those documents away
// again, so the claims have to go with them - otherwise the node refuses later local writes as
// duplicates of documents it no longer holds.
func TestSnapshotInstallDropsUniqueClaimsWhenItCannotBeMadeDurable(t *testing.T) {
	ns := "app/data"
	addr, _ := snapshotSource(t, ns, `{"email":"a@example.com"}`, `{"email":"b@example.com"}`)

	dir := t.TempDir()
	rt, err := embed.OpenFileRuntime(dir, "app", ns, uniqueSchema(t, "email"))
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	b := NewKdbServerRuntime(rt)
	b.NodeID = mustRandomUUID(t)

	// A directory where the checkpoint file belongs: the rename that publishes it cannot land.
	// If this path were wrong the bootstrap would simply succeed, and the assertions below say so.
	blocked := filepath.Join(dir, "snap", io.SnapFileName("kdb:checkpoint:"+ns))
	if err := os.MkdirAll(blocked, 0o755); err != nil {
		t.Fatal(err)
	}

	res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
		NodeID: b.NodeID.String(), PeerURI: "tcp://" + addr, Namespaces: []string{ns},
		Mode: peersync.SyncPull, Local: b.PeerNamespaces(), PreferSnapshot: true,
	})
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if res.Namespaces[0].Err == nil {
		t.Fatal("the bootstrap succeeded although its checkpoint could not be written")
	}
	if err := b.Runtime.CanInstallSnapshot(); err != nil {
		t.Fatalf("after the failed bootstrap the namespace cannot be bootstrapped again: %v", err)
	}
	if n := b.UniqueKeys.Len(); n != 0 {
		t.Fatalf("the unique registry kept %d claim(s) from a snapshot that was rolled back", n)
	}
	// The proof that matters to a client: the value the rolled-back snapshot held is free again.
	if _, err := b.Upsert(ns, mustRandomUUID(t), `{"email":"a@example.com"}`, auth.Principal{}); err != nil {
		t.Fatalf("a local write was refused by a claim no live document holds: %v", err)
	}
}
