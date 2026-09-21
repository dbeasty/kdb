package client

import (
	"context"
	"errors"
	"fmt"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/wire"
)

// CrossNamespaceResult is a committed CommitAcross.
type CrossNamespaceResult struct {
	// GroupID is the transaction id every participant commit carries - the way to find the other
	// halves of the transaction from any one of its commits.
	GroupID string
	// Commits maps each namespace to the commit hash the transaction produced there, usable as
	// that namespace's next BaseVersion.
	Commits map[string]string
}

// CrossNamespaceError names the namespace that refused a CommitAcross. Nothing was written in
// any namespace. Err is what that namespace said - a *ConflictError, *PreconditionError,
// *BusyError and so on - and errors.Is / errors.As reach it through this wrapper.
type CrossNamespaceError struct {
	Namespace string
	Err       error
}

func (e *CrossNamespaceError) Error() string {
	if e.Namespace == "" {
		return e.Err.Error()
	}
	return fmt.Sprintf("kdb: cross-namespace transaction refused by namespace %s: %v", e.Namespace, e.Err)
}

func (e *CrossNamespaceError) Unwrap() error { return e.Err }

// CommitAcross commits one transaction per namespace atomically: every namespace gets its commit,
// or none does - including across a server crash at any point. Each Transaction must name a
// different namespace.
//
// Each Transaction's BaseVersion anchors conflict detection in its own namespace, exactly as for
// Commit: take it from a GetJSON on that namespace, or from a previous commit's result. An empty
// BaseVersion anchors on the namespace's head at the moment the transaction commits - a blind
// write, which never conflicts.
//
// On a refusal the error is a *CrossNamespaceError naming the namespace that refused. A conflict
// in any participant satisfies errors.Is(err, ErrConflict).
func (c *Client) CommitAcross(ctx context.Context, txs []Transaction) (CrossNamespaceResult, error) {
	if len(txs) == 0 {
		return CrossNamespaceResult{}, errors.New("kdb: cross-namespace transaction has no participants")
	}
	parts := make([]wire.TxCommitMultiPart, len(txs))
	seen := make(map[string]struct{}, len(txs))
	for i, tx := range txs {
		if tx.Namespace == "" {
			return CrossNamespaceResult{}, errors.New("kdb: cross-namespace transaction names an empty namespace")
		}
		if _, dup := seen[tx.Namespace]; dup {
			return CrossNamespaceResult{}, fmt.Errorf("kdb: namespace %q appears twice; merge its writes into one Transaction", tx.Namespace)
		}
		seen[tx.Namespace] = struct{}{}
		ops, err := tx.operations()
		if err != nil {
			return CrossNamespaceResult{}, err
		}
		var base codec.Hash
		if tx.BaseVersion != "" {
			if base, err = codec.HashFromHex(tx.BaseVersion); err != nil {
				return CrossNamespaceResult{}, fmt.Errorf("kdb: invalid BaseVersion for namespace %q: %w", tx.Namespace, err)
			}
		}
		txID, err := codec.RandomUUID()
		if err != nil {
			return CrossNamespaceResult{}, err
		}
		encoded, err := wire.EncodeTransaction(document.Transaction{
			ID:           txID,
			BaseVersion:  base,
			Operations:   ops,
			Timestamp:    codec.TimestampNow(),
			AuthorNodeID: c.authorNodeID,
		})
		if err != nil {
			return CrossNamespaceResult{}, err
		}
		parts[i] = wire.TxCommitMultiPart{Namespace: tx.Namespace, TransactionBytes: encoded}
	}
	reply, err := c.request(ctx, wire.TxCommitMultiMessage{
		H:     wire.Header{MessageType: wire.MsgTxCommitMulti, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.nextCorrelation()},
		Parts: parts,
	})
	if err != nil {
		return CrossNamespaceResult{}, err
	}
	result, ok := reply.(wire.TxCommitMultiResultMessage)
	if !ok {
		return CrossNamespaceResult{}, fmt.Errorf("kdb: unexpected cross-namespace commit response %T", reply)
	}
	if result.Error != nil {
		var cause error
		if len(result.ConflictReport) > 0 {
			cause = decodeConflictError(result.ConflictReport, result.RetryAfterMs)
		} else {
			cause = classifiedError(*result.Error, result.ErrorCode, result.RetryAfterMs)
		}
		return CrossNamespaceResult{}, &CrossNamespaceError{Namespace: result.FailedNamespace, Err: cause}
	}
	out := CrossNamespaceResult{GroupID: result.GroupID, Commits: make(map[string]string, len(result.Parts))}
	for _, p := range result.Parts {
		out.Commits[p.Namespace] = p.CommitHex
		c.advanceHead(p.Namespace, p.CommitHex)
	}
	return out, nil
}

// advanceHead records a commit this client made as its namespace's tracked head, when the client
// is tracking that namespace at all. A namespace reached only through CommitAcross has no session
// here and nothing to advance.
func (c *Client) advanceHead(ns, commitHex string) {
	c.nsMu.Lock()
	st, ok := c.namespaces[ns]
	c.nsMu.Unlock()
	if !ok {
		return
	}
	h, err := codec.HashFromHex(commitHex)
	if err != nil {
		return
	}
	st.mu.Lock()
	st.head = h
	st.mu.Unlock()
}
