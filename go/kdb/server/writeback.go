package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/wire"
)

// Write-back projections: a filtered projection that also takes writes, commits them locally at
// once - so they are readable here, offline or not - and sends them to the source on the next
// sync, where each one lands only if the documents it touched are still what the projection had.
//
// The projection's own DAG is the outbox. A local write is a commit whose message records the
// content hash of every document it replaced; a decision on it is a later commit naming it. So a
// write is pending exactly while no decision names it, and whatever a crash interrupts is found
// again by reading the DAG: no second log to keep consistent with the first.

const (
	localWritePrefix = "kdb:writeback-local/1 "
	resolvedPrefix   = "kdb:writeback/1 "
)

// ErrProjectionWriteOp refuses an operation a write-back projection cannot send to its source.
var ErrProjectionWriteOp = errors.New("a write-back projection takes only document writes and deletes")

// writeBackState is a write-back projection's list of undecided local writes. It is a cache of
// what the DAG says, rebuilt from it whenever it may be wrong.
type writeBackState struct {
	// mu serializes the projection's writers - client writes, pulled pages, decisions - so the
	// set of pending documents a page must not overwrite cannot change under it.
	mu      sync.Mutex
	loaded  bool
	pending []peersync.PendingWrite
}

func (s *KdbServerRuntime) writeBackOn() bool { return s.ProjectionOf != "" && s.ProjectionWriteBack }

// admitProjectionWrite is admitWrite's rule for a projection.
func (s *KdbServerRuntime) admitProjectionWrite(tx document.Transaction) error {
	if !s.ProjectionWriteBack {
		return &ProjectionReadOnlyError{Namespace: s.Runtime.DefaultNamespace, Source: s.ProjectionOf}
	}
	for _, op := range tx.Operations {
		switch op.(type) {
		case document.WriteOp, document.DeleteOp:
		default:
			return fmt.Errorf("%w; %T is not one", ErrProjectionWriteOp, op)
		}
	}
	return nil
}

// localWriteMessage is the message of a client commit on a write-back projection: the content
// hash each document had before it ("-" for absent), read under the write gate from the tree the
// commit lands on. Those are what the source must still hold for the write to apply.
func (s *KdbServerRuntime) localWriteMessage(tx document.Transaction) string {
	_, head, ok, err := s.dag.HeadCommit()
	var b strings.Builder
	b.WriteString(localWritePrefix)
	seen := map[codec.UUID]bool{}
	for _, op := range tx.Operations {
		id := opDocID(op)
		if seen[id] {
			continue
		}
		seen[id] = true
		base := "-"
		if err == nil && ok {
			if d, _ := s.Runtime.Storage.GetDocument(s.Runtime.DefaultNamespace, id, head.DocumentTreeHash); d != nil {
				if h, err := d.ContentHash(); err == nil {
					base = h.Hex()
				}
			}
		}
		fmt.Fprintf(&b, "%s=%s ", id, base)
	}
	return strings.TrimSpace(b.String())
}

func opDocID(op document.Op) codec.UUID {
	switch o := op.(type) {
	case document.WriteOp:
		return o.DocID
	case document.DeleteOp:
		return o.DocID
	}
	return codec.UUID{}
}

// parseLocalWrite reads a local write's bases, in the order the message lists them.
func parseLocalWrite(msg string) (ids []codec.UUID, bases map[codec.UUID]string, ok bool) {
	if !strings.HasPrefix(msg, localWritePrefix) {
		return nil, nil, false
	}
	bases = map[codec.UUID]string{}
	for _, f := range strings.Fields(strings.TrimPrefix(msg, localWritePrefix)) {
		k, v, _ := strings.Cut(f, "=")
		id, err := codec.ParseUUID(k)
		if err != nil {
			continue
		}
		if v == "-" {
			v = ""
		}
		ids = append(ids, id)
		bases[id] = v
	}
	return ids, bases, true
}

// pendingWrite turns a local write commit into what is sent: each document's final state, from
// the commit's ops (which hold full bodies), and its base.
func pendingWrite(c document.Commit) (peersync.PendingWrite, bool) {
	ids, bases, ok := parseLocalWrite(c.Message)
	if !ok {
		return peersync.PendingWrite{}, false
	}
	final := map[codec.UUID]*string{}
	for _, op := range c.Operations {
		switch o := op.(type) {
		case document.WriteOp:
			body := o.Patch
			final[o.DocID] = &body
		case document.DeleteOp:
			final[o.DocID] = nil
		}
	}
	w := peersync.PendingWrite{Commit: c.Hash, TxID: peersync.WriteBackTxID(c.Hash)}
	for _, id := range ids {
		d := wire.WriteBackDoc{DocID: id.String(), BaseHash: bases[id]}
		if body := final[id]; body != nil {
			d.Body = *body
		} else {
			d.Deleted = true
		}
		w.Docs = append(w.Docs, d)
	}
	return w, true
}

// resolvedCommit reads which local write a decision commit decided on.
func resolvedCommit(msg string) (codec.Hash, bool) {
	if !strings.HasPrefix(msg, resolvedPrefix) {
		return codec.Hash{}, false
	}
	for _, f := range strings.Fields(strings.TrimPrefix(msg, resolvedPrefix)) {
		if k, v, _ := strings.Cut(f, "="); k == "commit" {
			h, err := codec.HashFromHex(v)
			return h, err == nil
		}
	}
	return codec.Hash{}, false
}

// loadPendingLocked rebuilds the pending list from the DAG. Decisions are made in order, so the
// newest decision's write and everything before it are decided; the local writes above it are
// not - including any made while that decision was in flight, which sit below the decision
// commit itself. A projection's history is a single line (it takes no peer pushes), so the first
// parent is the whole of it.
func (s *KdbServerRuntime) loadPendingLocked() error {
	st := &s.writeBack
	head, err := s.dag.Head()
	if err != nil {
		return err
	}
	var pending []peersync.PendingWrite
	var stop *codec.Hash
	for h := head; ; {
		if stop != nil && h == *stop {
			break
		}
		c, ok := s.dag.GetCommit(h)
		if !ok {
			break
		}
		if w, ok := pendingWrite(c); ok {
			pending = append(pending, w)
		} else if r, ok := resolvedCommit(c.Message); ok && stop == nil {
			stop = &r
		}
		if len(c.ParentHashes) == 0 {
			break
		}
		h = c.ParentHashes[0]
	}
	for i, j := 0, len(pending)-1; i < j; i, j = i+1, j-1 {
		pending[i], pending[j] = pending[j], pending[i]
	}
	st.pending, st.loaded = pending, true
	return nil
}

// PendingWrites lists the projection's undecided local writes, oldest first.
func (p projectionTarget) PendingWrites() ([]peersync.PendingWrite, error) {
	st := &p.rt.writeBack
	st.mu.Lock()
	defer st.mu.Unlock()
	if !st.loaded {
		if err := p.rt.loadPendingLocked(); err != nil {
			return nil, err
		}
	}
	return append([]peersync.PendingWrite(nil), st.pending...), nil
}

// pendingDocsLocked is every document an undecided local write touches: a pulled page must not
// overwrite them, or the local value would be lost before the source has decided on it.
func (s *KdbServerRuntime) pendingDocsLocked() (map[codec.UUID]bool, error) {
	if !s.writeBack.loaded {
		if err := s.loadPendingLocked(); err != nil {
			return nil, err
		}
	}
	out := map[codec.UUID]bool{}
	for _, w := range s.writeBack.pending {
		for _, d := range w.Docs {
			if id, err := codec.ParseUUID(d.DocID); err == nil {
				out[id] = true
			}
		}
	}
	return out, nil
}

// ResolveWrite records the source's decision on w. Applied, the local value already is the
// source's. Otherwise the attempt goes to the conflict queue and each document goes back to the
// source's current state (or leaves the projection, if that state is gone, unreadable or outside
// the filter) - in the decision commit itself, so the decision and its effect land together.
func (p projectionTarget) ResolveWrite(w peersync.PendingWrite, res wire.ProjectWriteResultMessage) error {
	s := p.rt
	st := &s.writeBack
	st.mu.Lock()
	defer st.mu.Unlock()
	var ops []document.Op
	if res.Outcome != wire.WriteBackApplied {
		if err := s.recordRejectedWrite(w, res); err != nil {
			return err
		}
		var filter sql.Expr
		if s.ProjectionFilter != "" {
			var err error
			if filter, err = sql.ParseFilter(s.ProjectionFilter); err != nil {
				return err
			}
		}
		for _, c := range res.Current {
			id, err := codec.ParseUUID(c.DocID)
			if err != nil {
				return err
			}
			ops = append(ops, document.DeleteOp{DocID: id})
			if c.Absent {
				continue
			}
			if filter != nil && !sql.EvalPredicate(filter, document.Document{ID: id, JSON: c.Body}, schema.None(), nil) {
				continue
			}
			ops = append(ops, document.WriteOp{DocID: id, Patch: c.Body})
		}
	}
	msg := fmt.Sprintf("%scommit=%s outcome=%s", resolvedPrefix, w.Commit.Hex(), res.Outcome)
	if res.CommitHex != "" {
		msg += " source=" + res.CommitHex
	}
	if _, err := s.systemCommit(ops, msg); err != nil {
		st.loaded = false
		return err
	}
	for i, x := range st.pending {
		if x.Commit == w.Commit {
			st.pending = append(st.pending[:i:i], st.pending[i+1:]...)
			break
		}
	}
	return nil
}

func (s *KdbServerRuntime) recordRejectedWrite(w peersync.PendingWrite, res wire.ProjectWriteResultMessage) error {
	if s.Conflicts == nil {
		return nil
	}
	current := map[string]wire.WriteBackCurrent{}
	for _, c := range res.Current {
		current[c.DocID] = c
	}
	kind := kdberr.PreconditionFailed
	if res.Outcome == wire.WriteBackRefused {
		kind = kdberr.SchemaIncompatible
	}
	var items []kdberr.ConflictItem
	for _, d := range w.Docs {
		item := kdberr.ConflictItem{DocumentID: d.DocID, OperationType: kind}
		if !d.Deleted {
			body := d.Body
			item.LocalDoc = &body
		}
		if c, ok := current[d.DocID]; ok && !c.Absent {
			body := c.Body
			item.IncomingDoc = &body
		}
		items = append(items, item)
	}
	now := time.Now().UTC()
	_, err := s.Conflicts.Record(peersync.ConflictEntry{
		ID:        peersync.ConflictID(peersync.ConflictWriteBack, s.Runtime.DefaultNamespace, w.Commit.Hex()),
		Kind:      peersync.ConflictWriteBack,
		Namespace: s.Runtime.DefaultNamespace,
		LocalHex:  w.Commit.Hex(),
		Items:     items,
		Detail: fmt.Sprintf("local write %s to projection of %s was %s by the source: %s; the documents were put back to the source's state",
			w.Commit.Hex(), s.ProjectionOf, res.Outcome, res.Reason),
		FirstSeen: now, LastSeen: now,
	})
	return err
}

// ApplyWriteBack commits a projection's write on this, its source, as principal: each document
// is replaced with its final state, provided it still has the base the projection saw. The
// transaction id is derived from the projection's commit, so a resend of a write this node
// already applied answers applied again instead of applying it twice or calling it a conflict.
func (s *KdbServerRuntime) ApplyWriteBack(principal auth.Principal, m wire.ProjectWriteMessage) (wire.ProjectWriteResultMessage, error) {
	txID, err := codec.ParseUUID(m.TxID)
	if err != nil {
		return refusedWrite("bad transaction id: " + err.Error()), nil
	}
	if c, ok := s.dag.GetCommitByTransactionID(txID); ok {
		return wire.ProjectWriteResultMessage{Outcome: wire.WriteBackApplied, CommitHex: c.Hash.Hex()}, nil
	}
	head, err := s.dag.Head()
	if err != nil {
		return wire.ProjectWriteResultMessage{}, err
	}
	tx := document.Transaction{ID: txID, BaseVersion: head, Timestamp: codec.TimestampNow()}
	ids := make([]codec.UUID, 0, len(m.Docs))
	for _, d := range m.Docs {
		id, err := codec.ParseUUID(d.DocID)
		if err != nil {
			return refusedWrite("bad document id: " + err.Error()), nil
		}
		ids = append(ids, id)
		pre := document.Precondition{Kind: document.ExpectAbsent}
		if d.BaseHash != "" {
			h, err := codec.HashFromHex(d.BaseHash)
			if err != nil {
				return refusedWrite("bad base hash: " + err.Error()), nil
			}
			pre = document.Precondition{Kind: document.ExpectContentHash, ContentHash: h}
		}
		// Every op carries the precondition: a guarded op is judged by it alone, not by what
		// changed since BaseVersion, so the write lands on whatever head it reaches the gate at.
		pre.OpIndex = len(tx.Operations)
		tx.Preconditions = append(tx.Preconditions, pre)
		tx.Operations = append(tx.Operations, document.DeleteOp{DocID: id})
		if !d.Deleted {
			pre.OpIndex = len(tx.Operations)
			tx.Preconditions = append(tx.Preconditions, pre)
			tx.Operations = append(tx.Operations, document.WriteOp{DocID: id, Patch: d.Body})
		}
	}
	if err := s.AuthEngine.Authorizer().Authorize(context.Background(), principal, auth.TxCommitAction{Namespace: s.Runtime.DefaultNamespace}); err != nil {
		return s.notApplied(principal, ids, wire.WriteBackRefused, (&AuthorizationError{Cause: err}).Error()), nil
	}
	commit, err := s.Commit(s.Runtime.DefaultNamespace, tx, "", principal)
	if err == nil {
		return wire.ProjectWriteResultMessage{Outcome: wire.WriteBackApplied, CommitHex: commit.Hash.Hex()}, nil
	}
	var conflict *ConflictError
	if errors.As(err, &conflict) {
		return s.notApplied(principal, ids, wire.WriteBackConflict, "changed at the source since the projection's copy"), nil
	}
	if permanentWriteError(err) {
		return s.notApplied(principal, ids, wire.WriteBackRefused, err.Error()), nil
	}
	return wire.ProjectWriteResultMessage{}, err
}

// permanentWriteError is a refusal that a resend of the same write cannot get past.
func permanentWriteError(err error) bool {
	var (
		authz  *AuthorizationError
		sch    *SchemaError
		ro     *ProjectionReadOnlyError
		exhaus *ResourceExhaustedError
	)
	return errors.As(err, &authz) || errors.As(err, &sch) || errors.As(err, &ro) || errors.As(err, &exhaus) ||
		errors.Is(err, ErrProjectionWriteOp)
}

// notApplied answers a write the source did not apply with its current state of each document,
// as far as principal may read it.
func (s *KdbServerRuntime) notApplied(principal auth.Principal, ids []codec.UUID, outcome, reason string) wire.ProjectWriteResultMessage {
	res := wire.ProjectWriteResultMessage{Outcome: outcome, Reason: reason}
	for _, id := range ids {
		cur := wire.WriteBackCurrent{DocID: id.String(), Absent: true}
		if s.AuthEngine.Authorizer().Authorize(context.Background(), principal,
			auth.DocumentReadAction{Namespace: s.Runtime.DefaultNamespace, DocID: id.String()}) == nil {
			if body, _, found, err := s.GetDocument(s.Runtime.DefaultNamespace, id); err == nil && found {
				cur = wire.WriteBackCurrent{DocID: id.String(), Body: body}
			}
		}
		res.Current = append(res.Current, cur)
	}
	return res
}

func refusedWrite(reason string) wire.ProjectWriteResultMessage {
	return wire.ProjectWriteResultMessage{Outcome: wire.WriteBackRefused, Reason: reason}
}
