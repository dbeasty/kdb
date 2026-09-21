package control

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/peersync"
	mem "github.com/limidus/kdb/go/kdb/storage/mem"
)

// queueConflict makes cs's namespace refuse a peer's same-document write, queuing a conflict.
func queueConflict(t *testing.T, cs *Server, body string) codec.UUID {
	t.Helper()
	docID, _, err := document.ResolveID(body)
	if err != nil {
		t.Fatal(err)
	}
	d, err := dag.NewInMemoryCommitDag("demo/users")
	if err != nil {
		t.Fatal(err)
	}
	store := mem.NewInMemoryStorageAdapter()
	g, _ := d.Head()
	gc, _ := d.GetCommitOrThrow(g)
	if err := store.PutDocument("demo/users", document.Document{ID: docID, JSON: `{"id":"doc-a","v":"remote"}`}); err != nil {
		t.Fatal(err)
	}
	tree, err := store.CommitTree("demo/users", gc.DocumentTreeHash)
	if err != nil {
		t.Fatal(err)
	}
	txID, _ := codec.RandomUUID()
	c, err := d.AppendCommit(document.Transaction{
		ID: txID, BaseVersion: g, Timestamp: codec.TimestampNow(),
		Operations: []document.Op{document.WriteOp{DocID: docID, Patch: `{"id":"doc-a","v":"remote"}`}},
	}, g, tree, nil, "remote")
	if err != nil {
		t.Fatal(err)
	}
	env := cs.opts.Runtime.PeerIngestEnv()
	env.Peer = "peer-x"
	res, err := peersync.Ingest(env, []document.Commit{c}, c.Hash)
	if err != nil || res.Outcome.Kind != peersync.OutcomeConflict {
		t.Fatalf("expected a queued conflict, got %v %v", res.Outcome.Kind, err)
	}
	return docID
}

func TestConflictEndpoints(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	seed(t, cs, `{"id":"doc-a","v":"local"}`)
	docID := queueConflict(t, cs, `{"id":"doc-a"}`)

	res, body := get(t, base, "/v1/ns/demo%2Fusers/conflicts")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("list: %d %v", res.StatusCode, body)
	}
	list := body["conflicts"].([]any)
	if len(list) != 1 {
		t.Fatalf("expected one conflict, got %v", list)
	}
	id := list[0].(map[string]any)["id"].(string)

	res, body = postJSON(t, base, "/v1/ns/demo%2Fusers/conflicts/"+id+"/resolve",
		fmt.Sprintf(`{"choices":{%q:{"take":"remote"}}}`, docID.String()))
	if res.StatusCode != http.StatusOK || body["commit"] == "" {
		t.Fatalf("resolve: %d %v", res.StatusCode, body)
	}
	if cs.opts.Runtime.Conflicts.Len() != 0 {
		t.Fatal("resolved conflict still queued")
	}
	res, _ = postJSON(t, base, "/v1/ns/demo%2Fusers/conflicts/"+id+"/resolve", `{"choices":{}}`)
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("resolving a cleared conflict: %d", res.StatusCode)
	}
}

func TestResolveNeedsWriteEnabled(t *testing.T) {
	cs, base := newFixture(t)
	seed(t, cs, `{"id":"doc-a","v":"local"}`)
	queueConflict(t, cs, `{"id":"doc-a"}`)
	id := cs.opts.Runtime.Conflicts.List()[0].ID
	res, _ := postJSON(t, base, "/v1/ns/demo%2Fusers/conflicts/"+id+"/resolve", `{"choices":{}}`)
	if res.StatusCode != http.StatusForbidden {
		t.Fatalf("a read-only control plane resolved a conflict: %d", res.StatusCode)
	}
}

func TestPeersEndpointWithoutReplication(t *testing.T) {
	_, base := newFixture(t)
	res, body := get(t, base, "/v1/peers")
	if res.StatusCode != http.StatusOK || body["configured"] != false {
		t.Fatalf("peers without replication: %d %v", res.StatusCode, body)
	}
}
