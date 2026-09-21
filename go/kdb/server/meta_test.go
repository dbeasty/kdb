package server

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/index/stores"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

// metaNode is one process's worth of runtimes for these tests: a data namespace with indexes, the
// metadata namespace, and the store joining them.
type metaNode struct {
	data  *KdbServerRuntime
	meta  *KdbServerRuntime
	store *MetaStore
}

func newMetaNode(t *testing.T) metaNode {
	t.Helper()
	set := NewNamespaceSet(nil)
	rt, err := embed.OpenMemoryRuntime("app", "app/data", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	data := NewKdbServerRuntime(rt)
	data.NodeID = mustRandomUUID(t)
	if _, err := data.OpenIndexes(stores.Options{}); err != nil {
		t.Fatal(err)
	}
	data.Namespaces = set
	if err := set.Add(data); err != nil {
		t.Fatal(err)
	}
	mrt, err := embed.OpenMemoryRuntime("_kdb", MetaNamespace, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	meta := NewKdbServerRuntime(mrt)
	meta.NodeID = data.NodeID
	set.AddSystem(meta)
	store := NewMetaStore(meta, set)
	t.Cleanup(store.Close)
	return metaNode{data: data, meta: meta, store: store}
}

func syncNodes(t *testing.T, from, to metaNode) {
	t.Helper()
	ln, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", to.data, "app/data")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
		NodeID: from.data.NodeID.String(), PeerURI: "tcp://" + ln.Addr().String(),
		Namespaces: []string{"**"}, Local: from.data.PeerNamespaces(),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, ns := range res.Namespaces {
		if ns.Err != nil {
			t.Fatalf("%s: %v", ns.Namespace, ns.Err)
		}
	}
}

// TestIndexCreatedOnAIsBuiltOnB: CREATE INDEX on one node reaches the other through the metadata
// namespace and is built there, so a search on either finds the same documents.
func TestIndexCreatedOnAIsBuiltOnB(t *testing.T) {
	a, b := newMetaNode(t), newMetaNode(t)
	pa := a.data.SQLIndexProvider().(*RegistryIndexProvider)
	if err := pa.CreateIndex(sql.StmtCreateIndex{
		Name: "titles", Table: "docs", Fields: []sql.IndexField{{Path: "title", Weight: 1}}, Using: "FULLTEXT",
	}, sql.QueryContext{NamespaceID: "app/data"}); err != nil {
		t.Fatal(err)
	}
	putDoc(t, a.data, `{"title":"replicated definitions"}`)
	syncNodes(t, a, b)

	pb := b.data.SQLIndexProvider().(*RegistryIndexProvider)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		hits, _, err := pb.Search(context.Background(), wire.SearchMessage{
			Namespace: "app/data", Text: &wire.SearchTextArm{Index: "titles", Query: "definitions"}, Limit: 5,
		})
		if err == nil && len(hits) == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the index created on A was never built on B")
}

// TestSchemaSurvivesRestartThroughMeta: a schema set on a running namespace is recorded in the
// metadata namespace and applied again after the process restarts - before this, a CREATE TABLE
// on the Go server lived only in memory.
func TestSchemaSurvivesRestartThroughMeta(t *testing.T) {
	dir := t.TempDir()
	sch := uniqueEmailSchema(t)
	open := func() (*embed.Host, metaNode) {
		host, err := embed.OpenFileHost(dir, embed.FileRuntimeOptions{})
		if err != nil {
			t.Fatal(err)
		}
		set := NewNamespaceSet(host.Transactions())
		rt, err := host.Namespace("app", "app/data", schema.None())
		if err != nil {
			t.Fatal(err)
		}
		data := NewKdbServerRuntime(rt)
		data.Namespaces = set
		if err := set.Add(data); err != nil {
			t.Fatal(err)
		}
		mrt, err := host.Namespace("_kdb", MetaNamespace, schema.None())
		if err != nil {
			t.Fatal(err)
		}
		meta := NewKdbServerRuntime(mrt)
		set.AddSystem(meta)
		return host, metaNode{data: data, meta: meta, store: NewMetaStore(meta, set)}
	}
	host, n := open()
	if err := n.data.SetSchemaChecked(sch); err != nil {
		t.Fatal(err)
	}
	if err := n.store.RecordSchema("app/data", sch); err != nil {
		t.Fatal(err)
	}
	n.store.Close()
	host.Close()

	host, n = open()
	defer host.Close()
	defer n.store.Close()
	if n.data.Schema().SchemaHash == sch.SchemaHash {
		t.Fatal("test is not exercising anything: the schema was restored without the metadata namespace")
	}
	if err := n.store.ReconcileAll(); err != nil {
		t.Fatal(err)
	}
	if n.data.Schema().SchemaHash != sch.SchemaHash {
		t.Fatal("the schema was not restored from the metadata namespace")
	}
}

// TestSchemaMigrationInReplicatedCommitApplies: a peer's commit carrying a schema migration
// changes this node's schema, not only its history.
func TestSchemaMigrationInReplicatedCommitApplies(t *testing.T) {
	rt := newTestRuntime(t)
	peer := newPeerFixture(t, rt)
	field := schema.MustField("title", schema.StringType{}, false, true, false)
	mig := schema.SchemaMigration{MigrationID: mustRandomUUID(t), FromVersion: rt.Schema().Version, ToVersion: rt.Schema().Version + 1,
		Steps: []schema.MigrationStep{schema.AddFieldStep{Field: field}}}
	raw, err := mig.ToBytes()
	if err != nil {
		t.Fatal(err)
	}
	head, _ := peer.dag.Head()
	hc, _ := peer.dag.GetCommitOrThrow(head)
	tree, _ := peer.dag.GetDocumentTree(hc.DocumentTreeHash)
	txID, _ := codec.RandomUUID()
	c, err := peer.dag.AppendCommit(document.Transaction{
		ID: txID, BaseVersion: head, Timestamp: codec.TimestampNow(),
		Operations: []document.Op{document.SchemaMigrationOp{MigrationID: mig.MigrationID, MigrationPayload: hex.EncodeToString(raw)}},
	}, head, tree, nil, "migrate")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := peer.push(c); err != nil {
		t.Fatalf("push: %v", err)
	}
	if !rt.Schema().HasField("title") {
		t.Fatal("the replicated migration did not reach the schema")
	}
}
