package transaction

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/storage"
)

// evaluatePreconditions checks every declared precondition against targetTreeHash - the tree the
// transaction is actually landing on - and returns the operations whose assertion failed.
//
// This is deliberately NOT the same check as detectConflicts. Conflict detection is
// content-addressed and compares the transaction's base tree against the target tree, which
// means a write whose content already equals what is there is indistinguishable from a no-op and
// passes (see the "the document is always the truth" semantics recorded in
// docs/kdb-finish-up-plan.md). A compare-and-set cannot inherit that: a client asserting
// "replace only if the content hash is still X" is asserting something about identity of state,
// not about whether its own write would change anything. So ExpectContentHash compares the
// stored hash literally and fails on mismatch even when the incoming content is identical to
// what is already stored.
//
// Evaluation runs inside the caller's write serialization (KdbServerRuntime's writeGate). A
// precondition evaluated outside it is a time-of-check-to-time-of-use bug: the state it asserted
// about could change between the check and the append.
func evaluatePreconditions(
	tx document.Transaction,
	namespaceID string,
	store storage.Adapter,
	targetTreeHash codec.Hash,
) ([]OperationConflict, []OperationViolation) {
	byIndex, err := document.PreconditionsByOpIndex(tx.Preconditions, len(tx.Operations))
	if err != nil {
		// A malformed precondition set (out-of-range or contradictory) is the client's error,
		// not a conflict: retrying it unchanged will fail identically.
		return nil, []OperationViolation{{
			OpIndex: 0,
			Violations: []kdberr.FieldViolation{{
				ViolationType: kdberr.CustomConstraint,
				Detail:        err.Error(),
			}},
		}}
	}
	if len(byIndex) == 0 {
		return nil, nil
	}

	var failures []OperationConflict
	for index, op := range tx.Operations {
		pre, declared := byIndex[index]
		if !declared || pre.Kind == document.ExpectAny {
			continue
		}
		var docID codec.UUID
		switch o := op.(type) {
		case document.WriteOp:
			docID = o.DocID
		case document.DeleteOp:
			docID = o.DocID
		default:
			// Preconditions describe a document's state; an op that names no document has none
			// to assert about. Treated as a client error for the same reason as a malformed
			// index - it cannot ever be satisfied, so failing it as a conflict would invite an
			// infinite retry loop.
			return nil, []OperationViolation{{
				OpIndex: index, Op: op,
				Violations: []kdberr.FieldViolation{{
					ViolationType: kdberr.CustomConstraint,
					Detail:        "precondition declared on an operation that names no document",
				}},
			}}
		}

		existing, _ := store.GetDocument(namespaceID, docID, targetTreeHash)
		satisfied, actual := preconditionHolds(pre, existing)
		if satisfied {
			continue
		}
		failures = append(failures, OperationConflict{
			OpIndex:     index,
			Op:          op,
			Type:        kdberr.PreconditionFailed,
			ExistingDoc: existing,
			BaseDoc:     existing,
			// Detail travels to the client through the conflict report's operation type; the
			// actual hash is what lets a CAS caller decide whether to re-read or give up.
			ActualContentHash: actual,
		})
	}
	return failures, nil
}

// preconditionHolds evaluates one assertion against the document currently at the id, returning
// whether it holds and (for reporting) the actual content hash observed.
func preconditionHolds(pre document.Precondition, existing *document.Document) (bool, *codec.Hash) {
	switch pre.Kind {
	case document.ExpectAbsent:
		return existing == nil, nil
	case document.ExpectPresent:
		if existing == nil {
			return false, nil
		}
		h, err := existing.ContentHash()
		if err != nil {
			return true, nil
		}
		return true, &h
	case document.ExpectContentHash:
		if existing == nil {
			return false, nil
		}
		h, err := existing.ContentHash()
		if err != nil {
			// A stored document whose content hash cannot be computed cannot be asserted about;
			// failing closed is the only safe answer for a compare-and-set.
			return false, nil
		}
		return h == pre.ContentHash, &h
	default:
		return true, nil
	}
}
