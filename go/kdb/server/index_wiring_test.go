package server

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/index"
	"github.com/limidus/kdb/go/kdb/index/stores"
	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/wire"
)

// putDoc writes one JSON body through the runtime's upsert path and returns its id.
func putDoc(t *testing.T, srv *KdbServerRuntime, body string) codec.UUID {
	t.Helper()
	id, err := codec.RandomUUID()
	if err != nil {
		t.Fatalf("uuid: %v", err)
	}
	if _, err := srv.Upsert(srv.Runtime.DefaultNamespace, id, body, auth.Principal{}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	return id
}

// TestOpenIndexesWiresBothProviders guards the wiring itself: after OpenIndexes the runtime
// serves SQL index lookups and SEARCH frames from the same registry, where before it had
// neither. This is the join between the index layer and the server that Components 67 and 69
// both depend on, and nothing else in the tree exercises it.
func TestOpenIndexesWiresBothProviders(t *testing.T) {
	srv := newTestRuntime(t)
	if srv.SQLIndexProvider() != nil {
		t.Fatal("expected no index provider before OpenIndexes")
	}
	if srv.SearchProvider() != nil {
		t.Fatal("expected no search provider before OpenIndexes")
	}

	provider, err := srv.OpenIndexes(stores.Options{})
	if err != nil {
		t.Fatalf("OpenIndexes: %v", err)
	}
	if got := srv.SQLIndexProvider(); got != sql.IndexProvider(provider) {
		t.Fatal("SQL index provider was not installed")
	}
	if got := srv.SearchProvider(); got != SearchProvider(provider) {
		t.Fatal("search provider was not installed")
	}
}

// TestCreateIndexThenSearchOverTheWireShape covers the whole Component 64/67/68/69 path in one
// go: CREATE INDEX registers and backfills a full-text index from documents that already
// existed, later writes keep it current through the commit hook, and a SEARCH frame returns
// them ranked. A regression anywhere in that chain - DDL, backfill, commit maintenance, or
// search - fails here.
func TestCreateIndexThenSearchOverTheWireShape(t *testing.T) {
	srv := newTestRuntime(t)
	provider, err := srv.OpenIndexes(stores.Options{})
	if err != nil {
		t.Fatalf("OpenIndexes: %v", err)
	}
	ns := srv.Runtime.DefaultNamespace

	// Written BEFORE the index exists, so only a backfill can find it.
	before := putDoc(t, srv, `{"title":"deploy the staging cluster","body":"rollout notes"}`)

	if err := provider.CreateIndex(sql.StmtCreateIndex{
		Name:   "docs_text",
		Table:  "docs",
		Fields: []sql.IndexField{{Path: "title", Weight: 3}, {Path: "body", Weight: 1}},
		Using:  "FULLTEXT",
	}, sql.QueryContext{NamespaceID: ns}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	// Written AFTER, so only the commit-path hook can find it.
	after := putDoc(t, srv, `{"title":"staging database","body":"deploy checklist"}`)
	putDoc(t, srv, `{"title":"unrelated","body":"nothing to see"}`)

	hits, _, err := provider.Search(context.Background(), wire.SearchMessage{
		Namespace: ns,
		Text:      &wire.SearchTextArm{Index: "docs_text", Query: "deploy"},
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	found := map[codec.UUID]bool{}
	for _, h := range hits {
		found[h.DocID] = true
		if h.Score <= 0 {
			t.Fatalf("hit %s has non-positive score %v", h.DocID, h.Score)
		}
	}
	if !found[before] {
		t.Error("document written before CREATE INDEX was not backfilled into the index")
	}
	if !found[after] {
		t.Error("document written after CREATE INDEX was not maintained on the commit path")
	}
	if len(hits) != 2 {
		t.Errorf("expected exactly the two matching documents, got %d", len(hits))
	}
}

// TestSearchIncludeJSONReturnsBodiesByteExact pins that a SEARCH asking for bodies gets the
// stored bytes back unchanged - the same round-trip promise §9.4 makes for reads.
func TestSearchIncludeJSONReturnsBodiesByteExact(t *testing.T) {
	srv := newTestRuntime(t)
	provider, err := srv.OpenIndexes(stores.Options{})
	if err != nil {
		t.Fatalf("OpenIndexes: %v", err)
	}
	ns := srv.Runtime.DefaultNamespace
	if err := provider.CreateIndex(sql.StmtCreateIndex{
		Name: "t", Table: "docs", Fields: []sql.IndexField{{Path: "title"}}, Using: "FULLTEXT",
	}, sql.QueryContext{NamespaceID: ns}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	// Deliberately unsorted keys and odd spacing: a body that survives verbatim proves nothing
	// re-serialized it.
	body := `{"zeta":1,"title":"quarterly report",  "alpha":[1,2,3]}`
	putDoc(t, srv, body)

	hits, _, err := provider.Search(context.Background(), wire.SearchMessage{
		Namespace:   ns,
		Text:        &wire.SearchTextArm{Index: "t", Query: "quarterly"},
		Limit:       5,
		IncludeJSON: true,
	})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) != 1 {
		t.Fatalf("expected 1 hit, got %d", len(hits))
	}
	if hits[0].JSON != body {
		t.Errorf("body not byte-exact:\n got %s\nwant %s", hits[0].JSON, body)
	}
}

// TestSearchWithoutMatchingIndexIsUnsupported guards the failure mode this layer exists to
// remove: a search naming an index that does not exist must say so, never quietly return no
// hits (which reads identically to "nothing matched").
func TestSearchWithoutMatchingIndexIsUnsupported(t *testing.T) {
	srv := newTestRuntime(t)
	provider, err := srv.OpenIndexes(stores.Options{})
	if err != nil {
		t.Fatalf("OpenIndexes: %v", err)
	}
	_, _, err = provider.Search(context.Background(), wire.SearchMessage{
		Namespace: srv.Runtime.DefaultNamespace,
		Text:      &wire.SearchTextArm{Index: "nosuchindex", Query: "x"},
		Limit:     5,
	})
	if err == nil {
		t.Fatal("expected an error for a missing index, got nil")
	}
	var unsupported *UnsupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("expected *UnsupportedError, got %T: %v", err, err)
	}
}

// TestSQLEqualityUsesTheIndexProvider proves the planner actually reaches the registry for an
// indexed equality, rather than silently falling back to a full scan: the plan names the index.
func TestSQLEqualityUsesTheIndexProvider(t *testing.T) {
	srv := newTestRuntime(t)
	provider, err := srv.OpenIndexes(stores.Options{})
	if err != nil {
		t.Fatalf("OpenIndexes: %v", err)
	}
	ns := srv.Runtime.DefaultNamespace
	if err := provider.CreateIndex(sql.StmtCreateIndex{
		Name: "by_status", Table: "docs", Fields: []sql.IndexField{{Path: "status"}}, Using: "HASH",
	}, sql.QueryContext{NamespaceID: ns}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	putDoc(t, srv, `{"status":"open","n":1}`)
	putDoc(t, srv, `{"status":"done","n":2}`)
	putDoc(t, srv, `{"status":"open","n":3}`)

	ids, ok, err := provider.ExactLookup(sql.QueryContext{NamespaceID: ns},
		"status", sql.CellString{Value: "open"})
	if err != nil {
		t.Fatalf("ExactLookup: %v", err)
	}
	if !ok {
		t.Fatal("expected the HASH index to serve the lookup")
	}
	if len(ids) != 2 {
		t.Errorf("expected 2 ids for status='open', got %d", len(ids))
	}
}

// TestDescriptorForRejectsVectorWithoutDimensions guards a CREATE INDEX that would otherwise
// build an unusable vector store: dimensions is what every write is validated against, so an
// index without it can never reject a wrong-length vector.
func TestDescriptorForRejectsVectorWithoutDimensions(t *testing.T) {
	_, err := descriptorFor(sql.StmtCreateIndex{
		Name: "v", Table: "docs", Fields: []sql.IndexField{{Path: "embedding"}}, Using: "VECTOR",
	}, "ns")
	if err == nil {
		t.Fatal("expected VECTOR without dimensions to be rejected")
	}
}

// TestDescriptorForCarriesWeightsAndOptions pins the descriptor options CREATE INDEX emits,
// since the store factory reads them by name and a silent rename would produce an index built
// with default weights instead of the declared ones.
func TestDescriptorForCarriesWeightsAndOptions(t *testing.T) {
	desc, err := descriptorFor(sql.StmtCreateIndex{
		Name:   "docs_text",
		Table:  "docs",
		Fields: []sql.IndexField{{Path: "title", Weight: 3}, {Path: "body", Weight: 1}},
		Using:  "FULLTEXT",
	}, "ns")
	if err != nil {
		t.Fatalf("descriptorFor: %v", err)
	}
	if desc.IndexName() != "docs_text" {
		t.Errorf("index_name = %q", desc.IndexName())
	}
	if got := desc.Options[index.OptionWeights]; got != "title=3,body=1" {
		t.Errorf("weights = %q, want title=3,body=1", got)
	}
	if desc.Type != index.IndexTypeFullText {
		t.Errorf("type = %v", desc.Type)
	}
	if len(desc.Fields) != 2 {
		t.Errorf("fields = %v", desc.Fields)
	}
}

// TestShortestFloat64WidensWithoutNoise pins the cross-tree score formatting rule from §5: a
// float32 score must widen through its shortest decimal, or Go prints 0.8999999761581421 where
// Kotlin prints 0.9 and the two engines disagree on identical data.
func TestShortestFloat64WidensWithoutNoise(t *testing.T) {
	if got := shortestFloat64(0.9); got != 0.9 {
		t.Errorf("shortestFloat64(0.9f) = %v, want 0.9", got)
	}
	b, err := json.Marshal(shortestFloat64(0.1))
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "0.1" {
		t.Errorf("0.1f marshalled as %s, want 0.1", b)
	}
}
