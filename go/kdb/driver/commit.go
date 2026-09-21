package driver

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// ErrConflict is matched (errors.Is) by every write refused because a document it touches changed
// since the transaction read it. Re-read and retry.
var ErrConflict = errors.New("kdb driver: conflict")

// ErrUnknownNamespace is returned for a namespace that does not exist, named by a statement that
// only reads.
var ErrUnknownNamespace = errors.New("kdb driver: unknown namespace")

// ConflictError is a refused write: in Namespace, the listed documents changed since the
// transaction's base version, or a precondition did not hold.
type ConflictError struct {
	Namespace string
	Documents []string
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("kdb driver: conflict in namespace %s on %s", e.Namespace, strings.Join(e.Documents, ", "))
}

func (e *ConflictError) Is(target error) bool { return target == ErrConflict }

// SchemaError is a write refused by schema validation or a unique constraint.
type SchemaError struct {
	Namespace string
	Detail    string
}

func (e *SchemaError) Error() string {
	return fmt.Sprintf("kdb driver: schema violation in namespace %s: %s", e.Namespace, e.Detail)
}

// commitPart is one namespace's share of a transaction.
type commitPart struct {
	ns *namespace
	tx document.Transaction
}

// commit applies parts atomically: one ordinary commit for a single namespace, a cross-namespace
// group for several.
func (db *database) commit(parts []commitPart) error {
	switch len(parts) {
	case 0:
		return nil
	case 1:
		return commitOne(parts[0].ns, parts[0].tx)
	default:
		return db.commitAcross(parts)
	}
}

// commitOne is the driver's single-namespace commit: under the namespace's write lock, run the
// engine and queue the commit on the log; then, lock released, wait for it to be durable - so
// concurrent connections share fsyncs rather than each paying one in turn.
func commitOne(ns *namespace, tx document.Transaction) error {
	if err := ns.fenceErr(); err != nil {
		return err
	}
	if tx.ID == (codec.UUID{}) {
		id, err := codec.RandomUUID()
		if err != nil {
			return err
		}
		tx.ID = id
	}
	if tx.Timestamp.EpochMicros() == 0 {
		tx.Timestamp = codec.TimestampNow()
	}
	defer ns.d.Pin(tx.BaseVersion)()

	ns.write.Lock()
	result, err := ns.engine.Commit(tx, ns.d, ns.rt.Storage, ns.schema(), nil, "")
	if err != nil {
		ns.write.Unlock()
		return err
	}
	committed, err := resultError(ns.id, result)
	if err != nil {
		ns.write.Unlock()
		return err
	}
	var wait func() error
	if ns.persister != nil {
		if wait, err = ns.persister.PersistAsync(committed); err != nil {
			ns.write.Unlock()
			return err
		}
	}
	ns.write.Unlock()
	if wait != nil {
		return wait()
	}
	return nil
}

// commitAcross commits a transaction spanning namespaces, with the protocol
// server.NamespaceSet.CommitAcross uses (docs/kdb-cross-namespace-transactions-plan.md §3.2):
// every namespace's lock in id order, every participant checked before any is written, parts
// published provisional and queued, locks released, then the group decided.
func (db *database) commitAcross(parts []commitPart) error {
	sort.Slice(parts, func(i, j int) bool { return parts[i].ns.id < parts[j].ns.id })
	for i := 1; i < len(parts); i++ {
		if parts[i].ns == parts[i-1].ns {
			return fmt.Errorf("kdb driver: namespace %s appears twice in one transaction", parts[i].ns.id)
		}
	}
	for _, p := range parts {
		if err := p.ns.fenceErr(); err != nil {
			return err
		}
		defer p.ns.d.Pin(p.tx.BaseVersion)()
	}
	locked := 0
	unlock := func() {
		for ; locked > 0; locked-- {
			parts[locked-1].ns.write.Unlock()
		}
	}
	defer unlock()
	for _, p := range parts {
		p.ns.write.Lock()
		locked++
	}

	ids := make([]string, len(parts))
	for i, p := range parts {
		ids[i] = p.ns.id
	}
	group, err := db.coord.Begin(ids)
	if err != nil {
		return err
	}
	prepared := make([]*transaction.PreparedCommit, len(parts))
	for i := range parts {
		parts[i].tx.ID = group.ID
		if parts[i].tx.Timestamp.EpochMicros() == 0 {
			parts[i].tx.Timestamp = codec.TimestampNow()
		}
		preparer, ok := parts[i].ns.engine.(transaction.Preparer)
		if !ok {
			group.Abandon()
			return fmt.Errorf("kdb driver: engine %T cannot prepare", parts[i].ns.engine)
		}
		pc, result, err := preparer.PrepareCommit(parts[i].tx, parts[i].ns.d, parts[i].ns.rt.Storage, parts[i].ns.schema())
		if err == nil && result != nil {
			_, err = resultError(parts[i].ns.id, result)
		}
		if err != nil {
			for _, done := range prepared[:i] {
				done.Discard()
			}
			group.Abandon()
			return err
		}
		prepared[i] = pc
	}

	fail := func(applied int, cause error) error {
		if applied == 0 {
			group.Abandon()
			return cause
		}
		err := group.Fail(cause)
		for _, p := range parts {
			p.ns.fence(err)
		}
		return err
	}
	for i, p := range parts {
		result, err := prepared[i].Apply(transaction.ApplyOptions{Message: group.Message(), Provisional: true})
		if err == nil {
			var c document.Commit
			if c, err = resultError(p.ns.id, result); err == nil {
				err = group.AddPart(p.ns.rt, c)
				if err != nil {
					return fail(i+1, err)
				}
				continue
			}
		}
		return fail(i, err)
	}
	unlock()

	if err := group.Finish(); err != nil {
		for _, p := range parts {
			p.ns.fence(err)
		}
		return err
	}
	return nil
}

// resultError turns an engine result into the commit it produced or the error it amounts to.
func resultError(namespace string, result transaction.TransactionResult) (document.Commit, error) {
	switch r := result.(type) {
	case transaction.ResultSuccess:
		return r.Commit, nil
	case transaction.ResultConflict:
		docs := make([]string, 0, len(r.Report.Conflicts))
		for _, c := range r.Report.Conflicts {
			docs = append(docs, c.DocumentID)
		}
		return document.Commit{}, &ConflictError{Namespace: namespace, Documents: docs}
	case transaction.ResultSchemaError:
		var parts []string
		for _, v := range r.Violations {
			for _, fv := range v.Violations {
				detail := fv.ViolationType.String()
				if fv.FieldName != "" {
					detail = fv.FieldName + ": " + detail
				}
				if fv.ViolationType == kdberr.UniqueConstraint && fv.Detail != "" {
					detail += " (" + fv.Detail + ")"
				}
				parts = append(parts, fmt.Sprintf("op %d: %s", v.OpIndex, detail))
			}
		}
		return document.Commit{}, &SchemaError{Namespace: namespace, Detail: strings.Join(parts, "; ")}
	case transaction.ResultAborted:
		return document.Commit{}, r.Cause
	default:
		return document.Commit{}, fmt.Errorf("kdb driver: unexpected result %T", result)
	}
}
