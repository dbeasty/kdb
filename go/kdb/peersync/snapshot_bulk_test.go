package peersync

import (
	"errors"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
	"github.com/limidus/kdb/go/kdb/storage/io"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/wire"
)

// engineSide is a side whose storage is a server engine with a blob store, so InstallSnapshot
// takes the bulk path (storage.SnapshotApplier) rather than PutDocument and CommitTree.
func engineSide(t *testing.T, ns string) (side, *engine.ServerEngine) {
	t.Helper()
	d, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatal(err)
	}
	// A tiny memtable budget, so the install flushes the blob store several times on the way.
	eng := engine.NewServerEngine(ns, storage.StorageEngineConfig{IOShim: io.NewInMemoryPlatformIO(), MemtableFlushBytes: 4 << 10}, nil)
	t.Cleanup(func() { _ = eng.Close() })
	return side{dag: d, storage: eng}, eng
}

// TestBulkSnapshotInstallKeepsInstallSnapshotsChecks: the bulk path refuses a tampered body with
// the same tree mismatch and undoes it the same way - main unmoved, no documents, the DAG back to
// genesis - and a durable-hook failure undoes it too. A clean install afterwards reads back every
// document exactly, and with every body already in the blob store.
func TestBulkSnapshotInstallKeepsInstallSnapshotsChecks(t *testing.T) {
	ns := "app/snap-bulk"
	remote := newTestNamespaces(t, ns)
	seedHistory(t, remote, ns, 40)
	local := newTestNamespaces(t, ns)
	l, eng := engineSide(t, ns)
	local.sides[ns] = l
	w := wire.NewCodec(wire.EncodingJSON)

	host := newHost(remote)
	tamper := true
	v2HubRaw(t, "hub-snap-bulk", func(frame []byte) []byte {
		out, _ := host.HandleFrame(frame)
		msg, _ := w.Decode(out)
		if page, ok := msg.(wire.SnapshotPageMessage); ok && tamper && len(page.Docs) > 0 {
			page.Docs[len(page.Docs)-1].Body = `{"tampered":true}`
			out, _ = w.Encode(page)
		}
		return out
	})
	cfg := V2ClientConfig{PeerURI: "memory://hub-snap-bulk", Local: local, NodeID: "c", Namespaces: []string{ns},
		Mode: SyncPull, PreferSnapshot: true, PageBytes: 2_000}
	empty := func(label string) {
		t.Helper()
		_, head, _, _ := l.dag.HeadCommit()
		if len(head.ParentHashes) != 0 || l.dag.CommitCount() != 1 {
			t.Fatalf("%s: main moved or the DAG kept the root (%d commits)", label, l.dag.CommitCount())
		}
		if n := eng.LiveTree().Size(); n != 0 {
			t.Fatalf("%s: a refused snapshot left %d documents behind", label, n)
		}
	}

	res, err := SyncV2(w, stream.NewInMemoryTransport(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	var mismatch *TreeMismatchError
	if !errors.As(res.Namespaces[0].Err, &mismatch) {
		t.Fatalf("expected a tree mismatch, got %v", res.Namespaces[0].Err)
	}
	empty("tampered")

	tamper = false
	local.installed = func(document.Commit) error { return errors.New("disk full") }
	res, _ = SyncV2(w, stream.NewInMemoryTransport(), cfg)
	if err := res.Namespaces[0].Err; err == nil {
		t.Fatal("expected the persist failure to be reported")
	}
	empty("not durable")

	local.installed = func(document.Commit) error { return eng.MaterializeLiveBodies() }
	res, err = SyncV2(w, stream.NewInMemoryTransport(), cfg)
	if err != nil || res.Namespaces[0].Err != nil || res.Namespaces[0].Snapshot == "" {
		t.Fatalf("clean install: %v %+v", err, res.Namespaces[0])
	}
	r := remote.side(ns)
	rh := mustHead(t, r)
	if mustHead(t, l) != rh {
		t.Fatal("local main is not the remote head")
	}
	rc, _ := r.dag.GetCommitOrThrow(rh)
	n := 0
	if err := r.storage.(storage.TreeWalker).WalkTree(ns, rc.DocumentTreeHash, func(id codec.UUID, _ codec.Hash) bool {
		n++
		want, _ := r.storage.GetDocument(ns, id, rc.DocumentTreeHash)
		got, err := eng.GetDocument(ns, id, rc.DocumentTreeHash)
		if err != nil || got == nil || want == nil || got.JSON != want.JSON {
			t.Fatalf("document %s: got %v (%v), want %v", id, got, err, want)
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if n == 0 || eng.LiveTree().Size() != n {
		t.Fatalf("installed %d documents, the peer's tree holds %d", eng.LiveTree().Size(), n)
	}
	if !eng.LiveBodiesDurable() {
		t.Fatal("a bootstrapped body is not in the blob store")
	}
}
