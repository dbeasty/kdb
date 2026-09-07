package client_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/client"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/wire"
)

// stubSearchProvider answers every search with a fixed ranking, and records what it was asked, so
// the client's request/response mapping can be exercised over a real listener before any index
// exists.
type stubSearchProvider struct {
	hits     []server.SearchHit
	resolved codec.Hash
	requests chan wire.SearchMessage
}

func (p *stubSearchProvider) Search(_ context.Context, req wire.SearchMessage) ([]server.SearchHit, codec.Hash, error) {
	select {
	case p.requests <- req:
	default:
	}
	return p.hits, p.resolved, nil
}

// TestSearchRoundTripsAgainstAListener is Component 69's client half: a SearchRequest crosses the
// wire, reaches the server's provider with both arms and every option intact, and the ranked hits
// come back with their bodies.
func TestSearchRoundTripsAgainstAListener(t *testing.T) {
	addr, rt := startTestServer(t)
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
	provider := &stubSearchProvider{
		hits: []server.SearchHit{
			{DocID: first, Score: 3.25, JSON: `{"title":"deploy staging"}`},
			{DocID: second, Score: 1.5, JSON: `{"title":"deploy prod"}`},
		},
		resolved: head,
		requests: make(chan wire.SearchMessage, 1),
	}
	rt.SetSearchProvider(provider)

	c := connectTestClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	weight := 0.6
	result, err := c.Search(ctx, client.SearchRequest{
		Namespace:   "app/data",
		Text:        &client.SearchTextArm{Index: "tasks_text", Query: "deploy staging", Depth: 100, Weight: &weight},
		Vector:      &client.SearchVectorArm{Index: "embedding", Vector: []float64{0.5, -0.25}},
		Fusion:      "weighted",
		Limit:       20,
		IncludeJSON: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.ResolvedCommitHex != head.Hex() {
		t.Fatalf("resolvedCommitHex = %s, want %s", result.ResolvedCommitHex, head.Hex())
	}
	if len(result.Hits) != 2 {
		t.Fatalf("hits = %+v, want 2", result.Hits)
	}
	if result.Hits[0].DocID != first.String() || result.Hits[0].Score != 3.25 {
		t.Fatalf("first hit = %+v", result.Hits[0])
	}
	if string(result.Hits[0].JSON) != `{"title":"deploy staging"}` {
		t.Fatalf("first hit body = %s", result.Hits[0].JSON)
	}
	select {
	case req := <-provider.requests:
		if req.Namespace != "app/data" || req.Limit != 20 || req.Fusion != "weighted" || !req.IncludeJSON {
			t.Fatalf("provider saw %+v", req)
		}
		if req.Text == nil || req.Text.Index != "tasks_text" || req.Text.Query != "deploy staging" || req.Text.Depth != 100 {
			t.Fatalf("text arm = %+v", req.Text)
		}
		if req.Text.Weight == nil || *req.Text.Weight != weight {
			t.Fatalf("text weight = %v", req.Text.Weight)
		}
		if req.Vector == nil || req.Vector.Index != "embedding" || len(req.Vector.Vector) != 2 || req.Vector.Vector[1] != -0.25 {
			t.Fatalf("vector arm = %+v", req.Vector)
		}
	case <-time.After(time.Second):
		t.Fatal("the provider never saw the request")
	}
}

// TestSearchWithoutProviderReturnsUnsupported: against a server with no search index configured,
// the client gets a typed ErrUnsupported rather than prose it would have to parse.
func TestSearchWithoutProviderReturnsUnsupported(t *testing.T) {
	addr, _ := startTestServer(t)
	c := connectTestClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := c.Search(ctx, client.SearchRequest{
		Namespace: "app/data",
		Text:      &client.SearchTextArm{Index: "tasks_text", Query: "deploy"},
		Limit:     10,
	})
	if err == nil {
		t.Fatal("expected an error from a server with no search provider")
	}
	if !errors.Is(err, client.ErrUnsupported) {
		t.Fatalf("error %v does not unwrap to ErrUnsupported", err)
	}
}

// TestSearchRejectsAnEmptyRequestLocally: a request naming no arm and one naming no namespace are
// caught client-side, without a round trip the server would only refuse anyway.
func TestSearchRejectsAnEmptyRequestLocally(t *testing.T) {
	addr, _ := startTestServer(t)
	c := connectTestClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := c.Search(ctx, client.SearchRequest{Namespace: "app/data", Limit: 10}); err == nil {
		t.Fatal("expected an error for a search with neither arm")
	}
	if _, err := c.Search(ctx, client.SearchRequest{Text: &client.SearchTextArm{Index: "i", Query: "q"}}); err == nil {
		t.Fatal("expected an error for a search with no namespace")
	}
}
