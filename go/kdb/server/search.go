package server

import (
	"context"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/wire"
)

// SearchHit is one ranked document a SearchProvider returns. JSON is filled only when the
// request asked for IncludeJSON; the server sends it through verbatim.
type SearchHit struct {
	DocID codec.UUID
	Score float64
	JSON  string
}

// SearchProvider answers SEARCH frames (kdb-spec-layer16 §11, Component 68) once the runtime's
// fulltext/vector indexes are wired: given the decoded request it returns the ranked hits and
// the commit the search resolved at (head, or req.AtCommitHex when set). Authorization and
// admission have already happened by the time it is called. Implementations must be safe for
// concurrent use - frames on one connection are dispatched concurrently.
type SearchProvider interface {
	Search(ctx context.Context, req wire.SearchMessage) (hits []SearchHit, resolvedCommit codec.Hash, err error)
}

// UnsupportedError reports a request the server understood but has nothing configured to serve -
// a SEARCH with no SearchProvider. Classified as wire.ErrorCodeUnsupported: never retry
// unmodified, the fix is on the operator's side.
type UnsupportedError struct{ Reason string }

func (e *UnsupportedError) Error() string { return "unsupported: " + e.Reason }

// ErrSearchNotConfigured is the UnsupportedError every search gets while SearchProvider is nil.
var ErrSearchNotConfigured = &UnsupportedError{Reason: "no index configured for search"}

// handleSearch authorizes the request as a DocumentRead on the namespace (sessionless, like
// DocumentGet), takes a scan-class admission grant, and hands it to the runtime's SearchProvider.
func (h *sqlWireConnHandler) handleSearch(msg wire.SearchMessage) wire.Message {
	principal, authenticated := h.principalSnapshot()
	if !authenticated {
		return searchError(msg, "not authenticated")
	}
	action := auth.DocumentReadAction{Namespace: msg.Namespace}
	if err := h.runtime.AuthEngine.Authorizer().Authorize(context.Background(), principal, action); err != nil {
		return searchErrorClassified(msg, (&AuthorizationError{Cause: err}).Error(), &AuthorizationError{Cause: err})
	}
	if msg.Text == nil && msg.Vector == nil {
		return searchError(msg, "search needs a text arm, a vector arm, or both")
	}
	if adm := h.runtime.admission; adm != nil {
		// Sized like a small ordered scan: a search materializes at most Limit hits (plus their
		// bodies when asked for), never the namespace.
		head, err := h.runtime.Runtime.DAG.Head()
		if err != nil {
			return searchErrorClassified(msg, err.Error(), err)
		}
		limit := msg.Limit
		if limit <= 0 {
			limit = 10
		}
		estimate := adm.Costs().EstimateScan(ScanEstimateInput{
			Namespace: msg.Namespace,
			Shape:     sql.QueryShape{HasOrderBy: true, HasPredicate: true},
			TreeSize:  h.runtime.treeSizeAt(head),
			MaxRows:   limit,
			RowBudget: int(adm.ScanRowBudget()),
		})
		actx, cancel := context.WithTimeout(context.Background(), readAcquireTimeout)
		grant, err := adm.AcquireBytes(actx, ClassScan, estimate)
		cancel()
		if err != nil {
			return searchErrorClassified(msg, err.Error(), err)
		}
		defer grant.Release()
	}
	provider := h.runtime.SearchProvider()
	if provider == nil {
		return searchErrorClassified(msg, ErrSearchNotConfigured.Error(), ErrSearchNotConfigured)
	}
	hits, resolved, err := provider.Search(context.Background(), msg)
	if err != nil {
		return searchErrorClassified(msg, err.Error(), err)
	}
	out := make([]wire.SearchHit, len(hits))
	for i, hit := range hits {
		out[i] = wire.SearchHit{DocID: hit.DocID.String(), Score: hit.Score}
		if msg.IncludeJSON {
			body := hit.JSON
			out[i].JSON = &body
		}
	}
	return wire.SearchResultMessage{
		H:                 header(msg.H.CorrelationID, wire.MsgSearchResult),
		Namespace:         msg.Namespace,
		Hits:              out,
		ResolvedCommitHex: resolved.Hex(),
	}
}

func searchError(msg wire.SearchMessage, errMsg string) wire.Message {
	return wire.SearchResultMessage{
		H:         header(msg.H.CorrelationID, wire.MsgSearchResult),
		Namespace: msg.Namespace,
		Hits:      []wire.SearchHit{},
		Error:     &errMsg,
	}
}

func searchErrorClassified(msg wire.SearchMessage, errMsg string, err error) wire.Message {
	code, retryAfterMs := classifyError(err)
	return wire.SearchResultMessage{
		H:            header(msg.H.CorrelationID, wire.MsgSearchResult),
		Namespace:    msg.Namespace,
		Hits:         []wire.SearchHit{},
		Error:        &errMsg,
		ErrorCode:    &code,
		RetryAfterMs: retryAfterMs,
	}
}
