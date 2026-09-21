package server

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/wire"
)

// TX_COMMIT_MULTI: a transaction spanning namespaces, over the wire. See
// docs/kdb-cross-namespace-transactions-plan.md and NamespaceSet.CommitAcross, which does all of
// the work - this only decodes, authorizes and shapes the reply.

// namespaceSet is the set a listener serving this runtime commits cross-namespace transactions
// through: the one the process configured (Namespaces), or - for a runtime nobody put in a set -
// a set of just this runtime, so a TX_COMMIT_MULTI naming only this namespace still works and one
// naming any other is refused as an unknown namespace rather than silently written here.
func (s *KdbServerRuntime) namespaceSet() *NamespaceSet {
	if s.Namespaces != nil {
		return s.Namespaces
	}
	s.soloSetOnce.Do(func() {
		set := NewNamespaceSet(s.Runtime.TxnCoordinator())
		_ = set.Add(s)
		s.soloSet = set
	})
	return s.soloSet
}

// runtimeFor resolves the runtime a sessionless frame naming namespace should be served by. With
// no NamespaceSet configured every frame is served by this runtime, whatever namespace it names -
// the behaviour before namespaces could be routed at all. Reads never create a namespace.
func (h *sqlWireConnHandler) runtimeFor(namespace string) (*KdbServerRuntime, error) {
	set := h.runtime.Namespaces
	if set == nil || namespace == "" || namespace == h.runtime.Runtime.DefaultNamespace {
		return h.runtime, nil
	}
	return set.Resolve(namespace, false)
}

func (h *sqlWireConnHandler) handleTxCommitMulti(msg wire.TxCommitMultiMessage) wire.Message {
	principal, authenticated := h.principalSnapshot()
	if !authenticated {
		return multiCommitError(msg, "", errors.New("not authenticated"))
	}
	if msg.SessionID != "" {
		if _, ok := h.sessions.Get(msg.SessionID); !ok {
			return multiCommitError(msg, "", errors.New("unknown session: "+msg.SessionID))
		}
	}
	set := h.runtime.namespaceSet()
	parts := make([]NamespaceTransaction, 0, len(msg.Parts))
	for _, p := range msg.Parts {
		tx, err := wire.DecodeTransaction(p.TransactionBytes)
		if err != nil {
			return multiCommitError(msg, p.Namespace, errors.New("invalid transactionBytes: "+err.Error()))
		}
		rt, err := set.Resolve(p.Namespace, true)
		if err != nil {
			return multiCommitError(msg, p.Namespace, err)
		}
		// The namespace-level grant TX_COMMIT's session begin would have checked. Per-document
		// write grants are checked again inside CommitAcross, as they are for every commit.
		if err := rt.AuthEngine.Authorizer().Authorize(context.Background(), principal, auth.TxCommitAction{Namespace: p.Namespace}); err != nil {
			return multiCommitError(msg, p.Namespace, &AuthorizationError{Cause: err})
		}
		parts = append(parts, NamespaceTransaction{Namespace: p.Namespace, Tx: tx, SessionID: msg.SessionID})
	}
	result, err := set.CommitAcross(parts, principal)
	if err != nil {
		failed := ""
		var cne *CrossNamespaceError
		if errors.As(err, &cne) {
			failed = cne.Namespace
		}
		return multiCommitError(msg, failed, err)
	}
	reply := wire.TxCommitMultiResultMessage{
		H:       header(msg.H.CorrelationID, wire.MsgTxCommitMultiResult),
		GroupID: result.Group.String(),
		Parts:   make([]wire.TxCommitMultiResultPart, len(result.Commits)),
	}
	for i, c := range result.Commits {
		reply.Parts[i] = wire.TxCommitMultiResultPart{Namespace: c.Namespace, CommitHex: c.Commit.Hash.Hex()}
	}
	return reply
}

func multiCommitError(msg wire.TxCommitMultiMessage, namespace string, err error) wire.Message {
	errMsg := err.Error()
	code, retryAfterMs := classifyError(err)
	reply := wire.TxCommitMultiResultMessage{
		H:               header(msg.H.CorrelationID, wire.MsgTxCommitMultiResult),
		FailedNamespace: namespace,
		Error:           &errMsg,
		ErrorCode:       &code,
		RetryAfterMs:    retryAfterMs,
	}
	var conflict *ConflictError
	if errors.As(err, &conflict) {
		reply.ConflictReport, _ = json.Marshal(conflict.Report)
	}
	return reply
}

// soloSet state lives on the runtime; declared here to keep it next to its only user.
type soloNamespaceSet struct {
	soloSetOnce sync.Once
	soloSet     *NamespaceSet
}
