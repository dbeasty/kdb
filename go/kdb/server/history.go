package server

import (
	"context"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/wire"
)

// HISTORY_LIST and REVERT (kdb-spec-layer6-component17's history surface,
// reached over the wire rather than only through SQL).
//
// Sessionless, like DOCUMENT_GET and SEARCH: a listing reads metadata the
// DAG already holds, and a revert is an ordinary write that computes its
// own operations. Neither needs a session pin, because neither spans
// round trips.

// defaultHistoryLimit bounds a listing whose caller named no limit.
//
// A client that omits the limit wants commits, not an empty page - but it
// also does not want a million of them, and there is no way for it to know
// how long the history is before asking. Fifty is a screenful, and the
// caller pages with Skip from there.
const defaultHistoryLimit = 50

// maxHistoryLimit caps what one frame will carry however much is asked
// for. A listing is metadata-only (operations are deliberately left out,
// see wire.HistoryCommit) but it is still one frame, and an unbounded one
// is a way to make the server allocate arbitrarily on a client's say-so.
const maxHistoryLimit = 1000

func (h *sqlWireConnHandler) handleHistoryList(msg wire.HistoryListMessage) wire.Message {
	principal, authenticated := h.principalSnapshot()
	if !authenticated {
		return historyError(msg, "not authenticated", nil)
	}
	action := auth.DocumentReadAction{Namespace: msg.Namespace}
	if err := h.runtime.AuthEngine.Authorizer().Authorize(context.Background(), principal, action); err != nil {
		wrapped := &AuthorizationError{Cause: err}
		return historyError(msg, wrapped.Error(), wrapped)
	}

	nav, ok := h.runtime.Runtime.DAG.(dag.HistoryNavigator)
	if !ok {
		err := &UnsupportedError{Reason: "this namespace's commit graph does not navigate"}
		return historyError(msg, err.Error(), err)
	}
	from := msg.From
	if from == "" {
		from = "head"
	}
	resolved, err := nav.ResolveRevision(from)
	if err != nil {
		return historyError(msg, err.Error(), err)
	}
	limit := msg.Limit
	if limit <= 0 {
		limit = defaultHistoryLimit
	}
	if limit > maxHistoryLimit {
		limit = maxHistoryLimit
	}
	entries, err := nav.ListCommits(resolved, msg.Skip, limit)
	if err != nil {
		return historyError(msg, err.Error(), err)
	}
	out := make([]wire.HistoryCommit, len(entries))
	for i, e := range entries {
		parents := make([]string, len(e.ParentHashes))
		for j, p := range e.ParentHashes {
			parents[j] = p.Hex()
		}
		out[i] = wire.HistoryCommit{
			CommitHex:       e.Hash.Hex(),
			ParentHexes:     parents,
			TransactionID:   e.TransactionID.String(),
			TimestampMicros: e.Timestamp.EpochMicros(),
			AuthorNodeID:    e.AuthorNodeID.String(),
			TreeHex:         e.TreeHash.Hex(),
			Message:         e.Message,
			OperationCount:  e.OperationCount,
			Stubbed:         e.Stubbed,
		}
	}
	return wire.HistoryResultMessage{
		H:           header(msg.H.CorrelationID, wire.MsgHistoryResult),
		Namespace:   msg.Namespace,
		Commits:     out,
		ResolvedHex: resolved.Hex(),
	}
}

// handleRevert authorizes as a namespace-wide write, because a revert can
// touch any document in the namespace and there is no way to know which
// before computing the diff.
func (h *sqlWireConnHandler) handleRevert(msg wire.RevertMessage) wire.Message {
	principal, authenticated := h.principalSnapshot()
	if !authenticated {
		return revertError(msg, "not authenticated", nil)
	}
	action := auth.DocumentWriteAction{Namespace: msg.Namespace}
	if err := h.runtime.AuthEngine.Authorizer().Authorize(context.Background(), principal, action); err != nil {
		wrapped := &AuthorizationError{Cause: err}
		return revertError(msg, wrapped.Error(), wrapped)
	}
	if msg.To == "" {
		return revertError(msg, "revert needs a revision to restore", nil)
	}
	// Through the write gate, like every other write: a revert reads the
	// head tree, computes a diff against it and commits, and interleaving
	// that with another writer would commit a tree built against a head
	// that has moved.
	release, err := h.runtime.writeGate.acquire(context.Background())
	if err != nil {
		return revertError(msg, err.Error(), err)
	}
	defer release()

	res, err := embed.RevertTo(h.runtime.Runtime, msg.Namespace, msg.To)
	if err != nil {
		return revertError(msg, err.Error(), err)
	}
	return wire.RevertResultMessage{
		H:         header(msg.H.CorrelationID, wire.MsgRevertResult),
		Namespace: msg.Namespace,
		CommitHex: res.Commit.Hex(),
		TargetHex: res.Target.Hex(),
		Restored:  res.Restored,
		Removed:   res.Removed,
	}
}

func historyError(msg wire.HistoryListMessage, errMsg string, err error) wire.Message {
	out := wire.HistoryResultMessage{
		H:         header(msg.H.CorrelationID, wire.MsgHistoryResult),
		Namespace: msg.Namespace,
		Commits:   []wire.HistoryCommit{},
		Error:     &errMsg,
	}
	if err != nil {
		code, retryAfterMs := classifyError(err)
		out.ErrorCode = &code
		out.RetryAfterMs = retryAfterMs
	}
	return out
}

func revertError(msg wire.RevertMessage, errMsg string, err error) wire.Message {
	out := wire.RevertResultMessage{
		H:         header(msg.H.CorrelationID, wire.MsgRevertResult),
		Namespace: msg.Namespace,
		Error:     &errMsg,
	}
	if err != nil {
		code, retryAfterMs := classifyError(err)
		out.ErrorCode = &code
		out.RetryAfterMs = retryAfterMs
	}
	return out
}
