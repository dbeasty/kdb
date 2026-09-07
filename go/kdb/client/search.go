package client

import (
	"context"
	"fmt"

	"github.com/limidus/kdb/go/kdb/wire"
)

// SearchTextArm is the lexical arm of a SearchRequest - see wire.SearchTextArm.
type SearchTextArm struct {
	Index    string
	Query    string
	Depth    int
	MinScore *float64
	Weight   *float64
}

// SearchVectorArm is the vector arm of a SearchRequest - see wire.SearchVectorArm.
type SearchVectorArm struct {
	Index    string
	Vector   []float64
	Depth    int
	MinScore *float64
	Weight   *float64
}

// SearchRequest mirrors the SEARCH wire body (kdb-spec-layer16 §11): one or both arms, an
// optional fusion mode ("rrf" or "weighted") when both are present, the result cap, whether to
// return each hit's document body, and an optional historical commit to search at.
type SearchRequest struct {
	Namespace   string
	Text        *SearchTextArm
	Vector      *SearchVectorArm
	Fusion      string
	Limit       int
	IncludeJSON bool
	AtCommitHex string
}

// SearchHit is one ranked document. JSON is nil unless the request set IncludeJSON.
type SearchHit struct {
	DocID string
	Score float64
	JSON  []byte
}

// SearchResult is what Search returns: the hits, best first, and the commit they were ranked at.
type SearchResult struct {
	Hits              []SearchHit
	ResolvedCommitHex string
}

// Search runs a full-text, vector, or fused search against ns's configured indexes. Sessionless
// (like GetJSON): no session is opened and no transaction state is touched. A server with no
// search index configured answers with an error carrying wire.ErrorCodeUnsupported.
func (c *Client) Search(ctx context.Context, req SearchRequest) (SearchResult, error) {
	if req.Namespace == "" {
		return SearchResult{}, fmt.Errorf("kdb: search: Namespace is required")
	}
	if req.Text == nil && req.Vector == nil {
		return SearchResult{}, fmt.Errorf("kdb: search: at least one of Text or Vector is required")
	}
	msg := wire.SearchMessage{
		H:           wire.Header{MessageType: wire.MsgSearch, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.nextCorrelation()},
		Namespace:   req.Namespace,
		Fusion:      req.Fusion,
		Limit:       req.Limit,
		IncludeJSON: req.IncludeJSON,
		AtCommitHex: req.AtCommitHex,
	}
	if req.Text != nil {
		msg.Text = &wire.SearchTextArm{
			Index: req.Text.Index, Query: req.Text.Query, Depth: req.Text.Depth,
			MinScore: req.Text.MinScore, Weight: req.Text.Weight,
		}
	}
	if req.Vector != nil {
		msg.Vector = &wire.SearchVectorArm{
			Index: req.Vector.Index, Vector: req.Vector.Vector, Depth: req.Vector.Depth,
			MinScore: req.Vector.MinScore, Weight: req.Vector.Weight,
		}
	}
	reply, err := c.request(ctx, msg)
	if err != nil {
		return SearchResult{}, err
	}
	result, ok := reply.(wire.SearchResultMessage)
	if !ok {
		return SearchResult{}, fmt.Errorf("kdb: expected SearchResult, got %T", reply)
	}
	if result.Error != nil {
		return SearchResult{}, classifiedError(*result.Error, result.ErrorCode, result.RetryAfterMs)
	}
	hits := make([]SearchHit, len(result.Hits))
	for i, h := range result.Hits {
		hits[i] = SearchHit{DocID: h.DocID, Score: h.Score}
		if h.JSON != nil {
			hits[i].JSON = []byte(*h.JSON)
		}
	}
	return SearchResult{Hits: hits, ResolvedCommitHex: result.ResolvedCommitHex}, nil
}
