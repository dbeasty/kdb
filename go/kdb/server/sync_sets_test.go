package server

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
	mem "github.com/limidus/kdb/go/kdb/storage/mem"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

// pushGroupPart pushes, into rt's namespace, one part of a cross-namespace group decided on
// another host.
func pushGroupPart(t *testing.T, rt *KdbServerRuntime, marker embed.GroupMarker) {
	t.Helper()
	ns := rt.Runtime.DefaultNamespace
	ln, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", rt, ns)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	d, _ := dag.NewInMemoryCommitDag(ns)
	store := mem.NewInMemoryStorageAdapter()
	head, _ := d.Head()
	hc, _ := d.GetCommitOrThrow(head)
	id := mustRandomUUID(t)
	body := `{"group":"` + marker.Group.String() + `"}`
	_ = store.PutDocument(ns, document.Document{ID: id, JSON: body})
	tree, err := store.CommitTree(ns, hc.DocumentTreeHash)
	if err != nil {
		t.Fatal(err)
	}
	c, err := d.AppendCommit(document.Transaction{
		ID: marker.Group, BaseVersion: head, Timestamp: codec.TimestampNow(),
		Operations: []document.Op{document.WriteOp{DocID: id, Patch: body}},
	}, head, tree, nil, marker.Message())
	if err != nil {
		t.Fatal(err)
	}
	client := peersync.NewClient(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), d, store)
	session, err := client.Connect(peersync.ClientConfig{NamespaceID: ns, NodeID: "remote-host", PeerURI: "tcp://" + ln.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect()
	if _, err := session.PushCommits([]document.Commit{c}); err != nil {
		t.Fatal(err)
	}
}

func foreignFlags(rt *KdbServerRuntime) int {
	n := 0
	for _, e := range rt.Conflicts.List() {
		if e.Kind == peersync.ConflictForeignGroupPart {
			n++
		}
	}
	return n
}

// TestGroupFlaggedUntilAllPartsArrive: a replicated cross-namespace group is flagged in every
// namespace a part has reached while any part is still to come, and the flags clear when the
// last one arrives.
func TestGroupFlaggedUntilAllPartsArrive(t *testing.T) {
	rts := newNamespaceSetRuntimes(t, "a/one", "a/two")
	group, _ := codec.RandomUUID()
	m := embed.GroupMarker{Host: "elsewhere", Epoch: 1, Group: group, Parts: []string{"a/one", "a/two"}}
	pushGroupPart(t, rts["a/one"], m)
	if foreignFlags(rts["a/one"]) != 1 {
		t.Fatalf("the first part of an incomplete group was not flagged: %+v", rts["a/one"].Conflicts.List())
	}
	pushGroupPart(t, rts["a/two"], m)
	if foreignFlags(rts["a/one"]) != 0 || foreignFlags(rts["a/two"]) != 0 {
		t.Fatalf("flags remain after every part arrived: %+v %+v", rts["a/one"].Conflicts.List(), rts["a/two"].Conflicts.List())
	}
}

// TestGroupFlaggedWhenPartitionMissing: a group with a part in a namespace this node does not
// replicate can never be whole here, and says so.
func TestGroupFlaggedWhenPartitionMissing(t *testing.T) {
	rts := newNamespaceSetRuntimes(t, "a/one")
	group, _ := codec.RandomUUID()
	pushGroupPart(t, rts["a/one"], embed.GroupMarker{Host: "elsewhere", Epoch: 1, Group: group, Parts: []string{"a/one", "b/unreplicated"}})
	entries := rts["a/one"].Conflicts.List()
	if len(entries) != 1 || entries[0].Kind != peersync.ConflictForeignGroupPart {
		t.Fatalf("expected the part flagged, got %+v", entries)
	}
}

// TestNamespaceAutoCreatedOnReplica: a pull that may create namespaces opens the ones its peer has
// through the process's namespace opener.
func TestNamespaceAutoCreatedOnReplica(t *testing.T) {
	source := newNamespaceSetRuntimes(t, "site/one", "site/two")
	for ns, rt := range source {
		if _, err := rt.Upsert(ns, mustRandomUUID(t), `{"at":"`+ns+`"}`, auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	ln, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", source["site/one"], "site/one")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	replica := newNamespaceSetRuntimes(t, "local/only")["local/only"]
	replica.Namespaces.SetOpener(func(id string, create bool) (*KdbServerRuntime, error) {
		rt, err := embed.OpenMemoryRuntime("site", id, schema.None())
		if err != nil {
			return nil, err
		}
		srv := NewKdbServerRuntime(rt)
		srv.Namespaces = replica.Namespaces
		return srv, nil
	})
	res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
		NodeID: "replica", PeerURI: "tcp://" + ln.Addr().String(), Namespaces: []string{"site/*"},
		Mode: peersync.SyncPull, Local: replica.PeerNamespaces(), CreateLocal: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Namespaces) != 2 {
		t.Fatalf("synced %+v", res.Namespaces)
	}
	for ns, src := range source {
		rt, ok := replica.Namespaces.Get(ns)
		if !ok {
			t.Fatalf("%s was not created on the replica", ns)
		}
		hs, _ := src.dag.Head()
		hr, _ := rt.dag.Head()
		if hs != hr {
			t.Fatalf("%s not in sync", ns)
		}
	}
}
