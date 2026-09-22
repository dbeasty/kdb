package control

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
	mem "github.com/limidus/kdb/go/kdb/storage/mem"
)

// withMeta gives the fixture's runtime a metadata namespace, which resolution chains live in.
func withMeta(t *testing.T) func(*Options) {
	return func(o *Options) {
		set := server.NewNamespaceSet(nil)
		if err := set.Add(o.Runtime); err != nil {
			t.Fatal(err)
		}
		o.Runtime.Namespaces = set
		mrt, err := embed.OpenMemoryRuntime("_kdb", server.MetaNamespace, schema.None())
		if err != nil {
			t.Fatal(err)
		}
		meta := server.NewKdbServerRuntime(mrt)
		set.AddSystem(meta)
		store := server.NewMetaStore(meta, set)
		t.Cleanup(store.Close)
	}
}

func TestResolutionChainEndpoints(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true }, withMeta(t))
	res, body := get(t, base, "/v1/ns/demo%2Fusers/resolution")
	if res.StatusCode != http.StatusOK || body["hash"] != "" || len(body["rules"].([]any)) != 0 {
		t.Fatalf("no chain yet: %d %v", res.StatusCode, body)
	}
	if res, body := sendJSON(t, http.MethodPut, base, "/v1/ns/demo%2Fusers/resolution", `{"rules":[{"kind":"coin-flip"}]}`); res.StatusCode != http.StatusBadRequest {
		t.Fatalf("an invalid chain must be refused: %d %v", res.StatusCode, body)
	}
	node := cs.opts.Runtime.NodeID.String()
	res, body = sendJSON(t, http.MethodPut, base, "/v1/ns/demo%2Fusers/resolution",
		fmt.Sprintf(`{"rules":[{"kind":"source-priority","nodes":[%q]},{"kind":"authority","pending":"provisional","timeout":"24h"}]}`, node))
	if res.StatusCode != http.StatusOK || body["hash"] == "" {
		t.Fatalf("set: %d %v", res.StatusCode, body)
	}
	hash := body["hash"]
	res, body = get(t, base, "/v1/ns/demo%2Fusers/resolution")
	if body["hash"] != hash || len(body["rules"].([]any)) != 2 {
		t.Fatalf("read back: %d %v", res.StatusCode, body)
	}
	if a := cs.opts.Runtime.ResolutionChainOf().Authority(); a == nil || a.Pending != peersync.PendingProvisional {
		t.Fatal("the chain was not applied to the runtime")
	}
	if res, _ := sendJSON(t, http.MethodPut, base, "/v1/ns/demo%2Fusers/resolution", `{"rules":[]}`); res.StatusCode != http.StatusOK {
		t.Fatalf("clear: %d", res.StatusCode)
	}
	if cs.opts.Runtime.ResolutionChainOf() != nil {
		t.Fatal("an empty chain should clear it")
	}
}

func TestResolutionChainNeedsAMetadataNamespace(t *testing.T) {
	_, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	if res, _ := sendJSON(t, http.MethodPut, base, "/v1/ns/demo%2Fusers/resolution", `{"rules":[{"kind":"queue"}]}`); res.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 without a metadata namespace, got %d", res.StatusCode)
	}
}

func TestAuthorityConflictEndpoints(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true }, withMeta(t))
	if err := cs.opts.Runtime.Meta.SetResolution("demo/users", peersync.ResolutionChain{Rules: []peersync.ResolutionRule{{Kind: peersync.RuleAuthority}}}); err != nil {
		t.Fatal(err)
	}
	seed(t, cs, `{"id":"doc-a","v":"local"}`)
	docID := queueConflict(t, cs, `{"id":"doc-a"}`)

	count := func(query string) int {
		t.Helper()
		res, body := get(t, base, "/v1/ns/demo%2Fusers/conflicts"+query)
		if res.StatusCode != http.StatusOK {
			t.Fatalf("list %s: %d %v", query, res.StatusCode, body)
		}
		return len(body["conflicts"].([]any))
	}
	if count("?authority=true") != 1 || count("?undelivered=true") != 1 || count("?doc="+docID.String()) != 1 {
		t.Fatal("the held conflict should be listed for the authority, undelivered, and by its document")
	}
	if count("?doc=00000000-0000-0000-0000-000000000000") != 0 {
		t.Fatal("a document with no conflict lists nothing")
	}
	id := cs.opts.Runtime.Conflicts.List()[0].ID

	if res, _ := postJSON(t, base, "/v1/ns/demo%2Fusers/conflicts/"+id+"/ack", ``); res.StatusCode != http.StatusNoContent {
		t.Fatalf("ack: %d", res.StatusCode)
	}
	if count("?authority=true&undelivered=true") != 0 {
		t.Fatal("an acknowledged conflict is no longer undelivered")
	}
	if res, _ := postJSON(t, base, "/v1/ns/demo%2Fusers/conflicts/nope/ack", ``); res.StatusCode != http.StatusNotFound {
		t.Fatalf("ack of an unknown conflict: %d", res.StatusCode)
	}

	if res, _ := postJSON(t, base, "/v1/ns/demo%2Fusers/conflicts/resolve-all", `{"take":"theirs"}`); res.StatusCode != http.StatusBadRequest {
		t.Fatalf("take must be local or remote: %d", res.StatusCode)
	}
	res, body := postJSON(t, base, "/v1/ns/demo%2Fusers/conflicts/resolve-all", `{"take":"remote","filter":{"peer":"peer-x"},"dryRun":true}`)
	results := body["results"].([]any)
	if res.StatusCode != http.StatusOK || len(results) != 1 || results[0].(map[string]any)["commitHex"] != nil {
		t.Fatalf("dry run: %d %v", res.StatusCode, body)
	}
	res, body = postJSON(t, base, "/v1/ns/demo%2Fusers/conflicts/resolve-all", `{"take":"remote","filter":{"peer":"peer-x"}}`)
	results = body["results"].([]any)
	if res.StatusCode != http.StatusOK || len(results) != 1 || results[0].(map[string]any)["commitHex"] == "" {
		t.Fatalf("resolve-all: %d %v", res.StatusCode, body)
	}
	if count("") != 0 {
		t.Fatal("resolve-all should clear the queue")
	}
}

// With an authority in the chain, a principal holding write but not resolve gets 403.
func TestAuthorityConflictResolutionIsForbiddenWithoutResolve(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true }, withMeta(t))
	if err := cs.opts.Runtime.Meta.SetResolution("demo/users", peersync.ResolutionChain{Rules: []peersync.ResolutionRule{{Kind: peersync.RuleAuthority}}}); err != nil {
		t.Fatal(err)
	}
	seed(t, cs, `{"id":"doc-a","v":"local"}`)
	docID := queueConflict(t, cs, `{"id":"doc-a"}`)
	id := cs.opts.Runtime.Conflicts.List()[0].ID

	usersDag, _ := dag.NewInMemoryCommitDag(auth.UsersNamespace)
	rolesDag, _ := dag.NewInMemoryCommitDag(auth.RolesNamespace)
	store, err := auth.NewRegistryAuthStore(usersDag, rolesDag, mem.NewInMemoryStorageAdapter())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRole("writer", []string{"read:demo/users", "write:demo/users"}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateUser("alice", "secret", []string{"writer"}); err != nil {
		t.Fatal(err)
	}
	cs.opts.Runtime.AuthEngine = auth.NewRegistryAuthEngine(store)

	for path, body := range map[string]string{
		"/conflicts/" + id + "/resolve": fmt.Sprintf(`{"choices":{%q:{"take":"remote"}}}`, docID),
		"/conflicts/" + id + "/ack":     ``,
		"/conflicts/resolve-all":        `{"take":"remote"}`,
	} {
		if res, got := postJSON(t, base, "/v1/ns/demo%2Fusers"+path, body); res.StatusCode != http.StatusForbidden {
			t.Fatalf("%s: expected 403, got %d %v", path, res.StatusCode, got)
		}
	}
	if res, _ := sendJSON(t, http.MethodDelete, base, "/v1/ns/demo%2Fusers/conflicts/"+id, ``); res.StatusCode != http.StatusForbidden {
		t.Fatalf("dismiss: expected 403, got %d", res.StatusCode)
	}
	if cs.opts.Runtime.Conflicts.Len() != 1 {
		t.Fatal("nothing may have been settled")
	}
}

func TestProcedureEndpoints(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true }, withMeta(t))
	res, body := get(t, base, "/v1/ns/demo%2Fusers/procedures")
	if res.StatusCode != http.StatusOK || len(body["procedures"].([]any)) != 0 {
		t.Fatalf("no procedures yet: %d %v", res.StatusCode, body)
	}
	if res, body := sendJSON(t, http.MethodPut, base, "/v1/ns/demo%2Fusers/procedures/broken",
		`{"source":"function main( {"}`); res.StatusCode != http.StatusBadRequest {
		t.Fatalf("source that will not compile must be refused: %d %v", res.StatusCode, body)
	}
	res, body = sendJSON(t, http.MethodPut, base, "/v1/ns/demo%2Fusers/procedures/keepLocal",
		`{"source":"function main(c) { return { take: \"local\" }; }"}`)
	if res.StatusCode != http.StatusOK || body["sourceHash"] == "" {
		t.Fatalf("define: %d %v", res.StatusCode, body)
	}
	hash := body["sourceHash"]
	if got := cs.opts.Runtime.ProcedureHash("keepLocal"); got != hash {
		t.Fatalf("the runtime holds %q, the endpoint reported %q", got, hash)
	}
	res, body = get(t, base, "/v1/ns/demo%2Fusers/procedures/keepLocal")
	if res.StatusCode != http.StatusOK || body["sourceHash"] != hash {
		t.Fatalf("read back: %d %v", res.StatusCode, body)
	}
	if res, _ := get(t, base, "/v1/ns/demo%2Fusers/procedures/absent"); res.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for a procedure this node does not hold, got %d", res.StatusCode)
	}
	if res, body := sendJSON(t, http.MethodDelete, base, "/v1/ns/demo%2Fusers/procedures/keepLocal", ``); res.StatusCode != http.StatusOK {
		t.Fatalf("drop: %d %v", res.StatusCode, body)
	}
	if _, ok := cs.opts.Runtime.ProcedureSource("keepLocal"); ok {
		t.Fatal("the dropped procedure is still on the runtime")
	}
}

func TestProcedureNeedsAMetadataNamespace(t *testing.T) {
	_, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	if res, _ := sendJSON(t, http.MethodPut, base, "/v1/ns/demo%2Fusers/procedures/p",
		`{"source":"function main() { return { defer: true }; }"}`); res.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 without a metadata namespace, got %d", res.StatusCode)
	}
}
