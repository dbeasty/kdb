package driver

import (
	"errors"
	"fmt"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
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

// commitAcross commits a transaction spanning namespaces through embed.CommitGroup - the protocol
// the wire server uses too (docs/kdb-cross-namespace-transactions-plan.md §3.2). The driver's
// side of it is driverParticipant: a namespace mutex where the server has a write gate.
func (db *database) commitAcross(parts []commitPart) error {
	groupParts := make([]embed.GroupPart, len(parts))
	for i, p := range parts {
		if err := p.ns.fenceErr(); err != nil {
			return err
		}
		groupParts[i] = embed.GroupPart{Participant: driverParticipant{p.ns}, Tx: p.tx}
	}
	_, wait, err := embed.CommitGroup(db.coord, groupParts)
	if err != nil {
		return err
	}
	return wait()
}

// driverParticipant is one namespace's side of embed.CommitGroup.
type driverParticipant struct{ ns *namespace }

func (p driverParticipant) Namespace() string                  { return p.ns.id }
func (p driverParticipant) Runtime() *embed.EmbeddedKdbRuntime { return p.ns.rt }
func (p driverParticipant) Fence(cause error)                  { p.ns.fence(cause) }

func (p driverParticipant) Lock() (func(), error) {
	p.ns.write.Lock()
	return p.ns.write.Unlock, nil
}

func (p driverParticipant) Prepare(tx document.Transaction) (embed.PreparedPart, error) {
	preparer, ok := p.ns.engine.(transaction.Preparer)
	if !ok {
		return nil, fmt.Errorf("kdb driver: engine %T cannot prepare", p.ns.engine)
	}
	pc, result, err := preparer.PrepareCommit(tx, p.ns.d, p.ns.rt.Storage, p.ns.schema())
	if err != nil {
		return nil, err
	}
	if result != nil {
		_, err := resultError(p.ns.id, result)
		return nil, err
	}
	return driverPrepared{ns: p.ns, pc: pc}, nil
}

type driverPrepared struct {
	ns *namespace
	pc *transaction.PreparedCommit
}

func (dp driverPrepared) Discard() { dp.pc.Discard() }

func (dp driverPrepared) Apply(message string) (document.Commit, error) {
	result, err := dp.pc.Apply(transaction.ApplyOptions{Message: message, Provisional: true})
	if err != nil {
		return document.Commit{}, err
	}
	return resultError(dp.ns.id, result)
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
