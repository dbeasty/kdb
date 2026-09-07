package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/index"
	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/wire"
)

// fakeSearchProvider is a stand-in for the real index-backed provider: it records the request it
// was handed and returns a fixed ranking, so the dispatch path can be tested before any index
// exists.
type fakeSearchProvider struct {
	hits     []SearchHit
	resolved codec.Hash
	err      error

	lastRequest chan wire.SearchMessage
}

func (p *fakeSearchProvider) Search(_ context.Context, req wire.SearchMessage) ([]SearchHit, codec.Hash, error) {
	if p.lastRequest != nil {
		select {
		case p.lastRequest <- req:
		default:
		}
	}
	return p.hits, p.resolved, p.err
}

func searchRequest(t *testing.T, c *rawWireClient, namespace, query string, includeJSON bool) wire.SearchResultMessage {
	t.Helper()
	msg := wire.SearchMessage{
		H:           wire.Header{MessageType: wire.MsgSearch, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.nextCorrelation()},
		Namespace:   namespace,
		Text:        &wire.SearchTextArm{Index: "tasks_text", Query: query},
		Limit:       10,
		IncludeJSON: includeJSON,
	}
	reply := c.request(t, msg)
	result, ok := reply.(wire.SearchResultMessage)
	if !ok {
		t.Fatalf("expected SearchResultMessage, got %T", reply)
	}
	return result
}

// TestSearchWithoutProviderIsUnsupported is Component 68's default state: until the runtime's
// index layer is wired, a SEARCH gets a typed UNSUPPORTED refusal naming what is missing, rather
// than an INTERNAL error or an empty result that would read as "nothing matched".
func TestSearchWithoutProviderIsUnsupported(t *testing.T) {
	rt := newTestRuntime(t)
	addr := listenFor(t, rt)
	c := dialRawWireClient(t, addr)
	c.handshake(t, wire.ClientSQL, "app/data")

	result := searchRequest(t, c, "app/data", "deploy staging", false)
	if result.Error == nil {
		t.Fatal("expected an error when no SearchProvider is configured")
	}
	if *result.Error != ErrSearchNotConfigured.Error() {
		t.Fatalf("error = %q, want %q", *result.Error, ErrSearchNotConfigured.Error())
	}
	if result.ErrorCode == nil || *result.ErrorCode != wire.ErrorCodeUnsupported {
		t.Fatalf("errorCode = %v, want UNSUPPORTED", result.ErrorCode)
	}
	if len(result.Hits) != 0 {
		t.Fatalf("hits = %v, want none", result.Hits)
	}
}

// TestSearchDispatchesToProvider: with a provider set, the decoded request reaches it and its
// ranking comes back as hits, with bodies attached only when the request asked for them.
func TestSearchDispatchesToProvider(t *testing.T) {
	rt := newTestRuntime(t)
	first, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	second, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	head, err := rt.Runtime.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	provider := &fakeSearchProvider{
		hits: []SearchHit{
			{DocID: first, Score: 2.5, JSON: `{"title":"deploy staging"}`},
			{DocID: second, Score: 0.5, JSON: `{"title":"deploy prod"}`},
		},
		resolved:    head,
		lastRequest: make(chan wire.SearchMessage, 1),
	}
	rt.SetSearchProvider(provider)
	addr := listenFor(t, rt)
	c := dialRawWireClient(t, addr)
	c.handshake(t, wire.ClientSQL, "app/data")

	result := searchRequest(t, c, "app/data", "deploy staging", true)
	if result.Error != nil {
		t.Fatalf("search errored: %s", *result.Error)
	}
	if result.ResolvedCommitHex != head.Hex() {
		t.Fatalf("resolvedCommitHex = %s, want %s", result.ResolvedCommitHex, head.Hex())
	}
	if len(result.Hits) != 2 {
		t.Fatalf("hits = %+v, want 2", result.Hits)
	}
	if result.Hits[0].DocID != first.String() || result.Hits[0].Score != 2.5 {
		t.Fatalf("first hit = %+v", result.Hits[0])
	}
	if result.Hits[0].JSON == nil || *result.Hits[0].JSON != `{"title":"deploy staging"}` {
		t.Fatalf("first hit body = %+v", result.Hits[0].JSON)
	}
	select {
	case req := <-provider.lastRequest:
		if req.Namespace != "app/data" || req.Text == nil || req.Text.Query != "deploy staging" || req.Limit != 10 {
			t.Fatalf("provider saw %+v", req)
		}
	case <-time.After(time.Second):
		t.Fatal("the provider never saw the request")
	}

	// Without IncludeJSON the bodies are withheld even though the provider supplied them.
	plain := searchRequest(t, c, "app/data", "deploy staging", false)
	if plain.Error != nil {
		t.Fatalf("search errored: %s", *plain.Error)
	}
	if plain.Hits[0].JSON != nil {
		t.Fatalf("body returned without includeJson: %v", *plain.Hits[0].JSON)
	}
}

// TestSearchRequiresAuthenticationAndAnArm covers the two refusals that happen before the
// provider is consulted: a search before the handshake, and one naming neither arm.
func TestSearchRequiresAuthenticationAndAnArm(t *testing.T) {
	rt := newTestRuntime(t)
	rt.SetSearchProvider(&fakeSearchProvider{})
	addr := listenFor(t, rt)

	unauthenticated := dialRawWireClient(t, addr)
	result := searchRequest(t, unauthenticated, "app/data", "q", false)
	if result.Error == nil || *result.Error != "not authenticated" {
		t.Fatalf("pre-handshake search = %+v, want a not-authenticated error", result)
	}

	c := dialRawWireClient(t, addr)
	c.handshake(t, wire.ClientSQL, "app/data")
	reply := c.request(t, wire.SearchMessage{
		H:         wire.Header{MessageType: wire.MsgSearch, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.nextCorrelation()},
		Namespace: "app/data",
		Limit:     10,
	})
	armless, ok := reply.(wire.SearchResultMessage)
	if !ok {
		t.Fatalf("expected SearchResultMessage, got %T", reply)
	}
	if armless.Error == nil {
		t.Fatal("expected an error for a search with neither arm")
	}
}

// TestSearchProviderErrorIsClassified: an error from the provider reaches the client with the
// code its concrete type implies, not as bare prose.
func TestSearchProviderErrorIsClassified(t *testing.T) {
	rt := newTestRuntime(t)
	rt.SetSearchProvider(&fakeSearchProvider{err: &UnsupportedError{Reason: "no VECTOR index on embedding"}})
	addr := listenFor(t, rt)
	c := dialRawWireClient(t, addr)
	c.handshake(t, wire.ClientSQL, "app/data")

	result := searchRequest(t, c, "app/data", "q", false)
	if result.ErrorCode == nil || *result.ErrorCode != wire.ErrorCodeUnsupported {
		t.Fatalf("errorCode = %v, want UNSUPPORTED", result.ErrorCode)
	}

	rt.SetSearchProvider(&fakeSearchProvider{err: errors.New("index corrupt")})
	generic := searchRequest(t, c, "app/data", "q", false)
	if generic.ErrorCode == nil || *generic.ErrorCode != wire.ErrorCodeInternal {
		t.Fatalf("errorCode = %v, want INTERNAL for an unclassified provider error", generic.ErrorCode)
	}
}

// TestSearchIsClassifiedAsScanForAdmission pins the frame admitter's view of SEARCH: it reads
// documents like a scan does, so it is shed in the same zones a SELECT is, and a shed SEARCH
// gets a typed SearchResult refusal rather than being dropped.
func TestSearchIsClassifiedAsScanForAdmission(t *testing.T) {
	class, ok := opClassForMessage(wire.MsgSearch)
	if !ok || class != ClassScan {
		t.Fatalf("opClassForMessage(SEARCH) = %v, %v; want ClassScan, true", class, ok)
	}
	reply, ok := rejectionMessage(
		wire.Header{MessageType: wire.MsgSearch, CorrelationID: 3},
		&MemoryPressureError{Zone: ZoneHigh, Class: ClassScan, RetryAfterMs: 25},
	)
	if !ok {
		t.Fatal("SEARCH has no typed rejection message - a shed search would be dropped silently")
	}
	result, isSearch := reply.(wire.SearchResultMessage)
	if !isSearch {
		t.Fatalf("rejection is a %T, want SearchResultMessage", reply)
	}
	if result.H.CorrelationID != 3 || result.ErrorCode == nil || *result.ErrorCode != wire.ErrorCodeBusy {
		t.Fatalf("rejection = %+v", result)
	}
}

// stubIndexProvider is a minimal sql.IndexProvider that reports "no index" for every lookup, and
// records that index DDL reached it.
type stubIndexProvider struct{ created, dropped int }

func (p *stubIndexProvider) FullTextSearch(sql.QueryContext, string, string, int) ([]index.RankedResult, bool, error) {
	return nil, false, nil
}

func (p *stubIndexProvider) VectorSearch(sql.QueryContext, string, []float32, int) ([]index.RankedResult, bool, error) {
	return nil, false, nil
}

func (p *stubIndexProvider) ExactLookup(sql.QueryContext, string, sql.Cell) ([]codec.UUID, bool, error) {
	return nil, false, nil
}

func (p *stubIndexProvider) RangeLookup(sql.QueryContext, string, sql.Cell, sql.Cell, bool, bool) ([]codec.UUID, bool, error) {
	return nil, false, nil
}

func (p *stubIndexProvider) CreateIndex(sql.StmtCreateIndex, sql.QueryContext) error {
	p.created++
	return nil
}

func (p *stubIndexProvider) DropIndex(sql.StmtDropIndex, sql.QueryContext) error {
	p.dropped++
	return nil
}

// TestSQLIndexProviderWiring: with no provider the engine refuses index DDL (the pre-Layer-16
// behaviour, and the reason a nil provider must not be handed to NewEngineWithIndexes as a
// non-nil interface); installing one rebuilds the engine so CREATE/DROP INDEX reach it.
func TestSQLIndexProviderWiring(t *testing.T) {
	rt := newTestRuntime(t)
	addr := listenFor(t, rt)
	c := dialRawWireClient(t, addr)
	c.handshake(t, wire.ClientSQL, "app/data")
	sess := c.sessionBegin(t, "app/data", "READ_COMMITTED")

	if rt.SQLIndexProvider() != nil {
		t.Fatal("a fresh runtime should have no index provider")
	}
	if r := c.sqlExec(t, "app/data", sess.SessionID, `CREATE TABLE t (v VARCHAR NOT NULL)`); r.Error != nil {
		t.Fatalf("create table: %s", *r.Error)
	}
	refused := c.sqlExec(t, "app/data", sess.SessionID, `CREATE INDEX t_v ON t (v)`)
	if refused.Error == nil {
		t.Fatal("expected CREATE INDEX to be refused with no index provider configured")
	}

	provider := &stubIndexProvider{}
	rt.SetSQLIndexProvider(provider)
	if rt.SQLIndexProvider() == nil {
		t.Fatal("provider not installed")
	}
	if r := c.sqlExec(t, "app/data", sess.SessionID, `CREATE INDEX t_v ON t (v)`); r.Error != nil {
		t.Fatalf("create index: %s", *r.Error)
	}
	if r := c.sqlExec(t, "app/data", sess.SessionID, `DROP INDEX t_v ON t`); r.Error != nil {
		t.Fatalf("drop index: %s", *r.Error)
	}
	if provider.created != 1 || provider.dropped != 1 {
		t.Fatalf("provider saw created=%d dropped=%d, want 1 and 1", provider.created, provider.dropped)
	}
	// Queries still run against the expiry-filtering storage view after the rebuild.
	if r := c.sqlExec(t, "app/data", sess.SessionID, `SELECT COUNT(*) AS n FROM t`); r.Error != nil {
		t.Fatalf("select after rebuild: %s", *r.Error)
	}
}
