package transaction

import (
	"reflect"
	"sync"
	"weak"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/storage"
)

// Attachment rules (docs/kdb-attachments-plan.md):
//   - an AttachmentPutOp needs an attachment ID, a valid name unique within its document, its blob
//     already in the blob store, and its document to exist once the transaction has applied
//     (stored before, or written in it - not deleted after);
//   - a change to one attachment conflicts with a concurrent change to that same attachment (its
//     ID), and with a concurrent delete of the document; a document delete conflicts with a
//     concurrent change to any of its attachments; changes to different attachments of a document
//     do not conflict, and a JSON-only write conflicts with none - attachments are not part of the
//     document's content hash.

// BlobPresence is the optional storage capability preflight uses to ask whether a blob is stored
// without reading it - an attachment can be gigabytes. A store without it is asked with ReadBlob.
type BlobPresence interface {
	HasBlob(contentHash codec.Hash) (bool, error)
}

// AttachmentBlobSource answers whether a namespace's attachment store holds a blob. Large
// attachments live outside the storage adapter's blob store (storage/attachstore), so the runtime
// that owns an adapter registers itself here and preflight asks it first.
type AttachmentBlobSource interface {
	HasAttachmentBlob(namespaceID string, contentHash codec.Hash) (bool, error)
}

// attachmentBlobs maps an adapter's address to a weak lookup of the runtime that owns it. Neither
// is held: a runtime dropped without Close is collected as before, and its entry then answers
// nothing (and is removed).
var attachmentBlobs sync.Map // uintptr -> func() AttachmentBlobSource

// RegisterAttachmentBlobs makes preflight ask owner whether an attachment blob is stored, for
// transactions against store. owner is held weakly. A nil owner removes the registration.
func RegisterAttachmentBlobs[T any, P interface {
	*T
	AttachmentBlobSource
}](store storage.Adapter, owner P) {
	key, ok := adapterKey(store)
	if !ok {
		return
	}
	if owner == nil {
		attachmentBlobs.Delete(key)
		return
	}
	w := weak.Make((*T)(owner))
	attachmentBlobs.Store(key, func() AttachmentBlobSource {
		if p := w.Value(); p != nil {
			return P(p)
		}
		return nil
	})
}

// adapterKey is store's address, for an adapter that is a pointer.
func adapterKey(store storage.Adapter) (uintptr, bool) {
	if store == nil {
		return 0, false
	}
	v := reflect.ValueOf(store)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		return 0, false
	}
	return v.Pointer(), true
}

func hasBlob(store storage.Adapter, namespaceID string, h codec.Hash) bool {
	if key, ok := adapterKey(store); ok {
		if fn, ok := attachmentBlobs.Load(key); ok {
			if src := fn.(func() AttachmentBlobSource)(); src != nil {
				has, err := src.HasAttachmentBlob(namespaceID, h)
				return err == nil && has
			}
			attachmentBlobs.Delete(key) // its runtime is gone
		}
	}
	if bp, ok := store.(BlobPresence); ok {
		has, err := bp.HasBlob(h)
		return err == nil && has
	}
	data, err := store.ReadBlob(h)
	return err == nil && data != nil
}

// touchesAttachmentOps reports whether tx carries an attachment operation at all; a transaction
// without one skips every attachment check.
func touchesAttachmentOps(tx document.Transaction) bool {
	for _, op := range tx.Operations {
		switch op.(type) {
		case document.AttachmentPutOp, document.AttachmentDeleteOp:
			return true
		}
	}
	return false
}

func preflightAttachments(
	tx document.Transaction, namespaceID string, d *dag.InMemoryCommitDag, store storage.Adapter,
	targetTreeHash, anchorCommit codec.Hash,
) []OperationViolation {
	if !touchesAttachmentOps(tx) {
		return nil
	}
	// A document's presence after the transaction: the target tree, then each op in order.
	present := map[codec.UUID]bool{}
	presentAt := func(id codec.UUID) bool {
		if p, ok := present[id]; ok {
			return p
		}
		doc, _ := store.GetDocument(namespaceID, id, targetTreeHash)
		return doc != nil
	}
	for _, op := range tx.Operations {
		switch o := op.(type) {
		case document.WriteOp:
			present[o.DocID] = true
		case document.DeleteOp:
			present[o.DocID] = false
		}
	}
	var out []OperationViolation
	for index, op := range tx.Operations {
		put, ok := op.(document.AttachmentPutOp)
		if !ok {
			continue
		}
		detail := ""
		switch {
		case put.AttachmentID == (codec.UUID{}):
			detail = "attachment has no id"
		case document.ValidateAttachmentName(put.Name) != nil:
			detail = document.ValidateAttachmentName(put.Name).Error()
		case put.Size < 0:
			detail = "attachment size is negative"
		case !presentAt(put.DocID):
			detail = "no such document: an attachment needs its document"
		case !hasBlob(store, namespaceID, put.ContentHash):
			detail = "attachment blob not stored: " + put.ContentHash.Hex()
		}
		if detail != "" {
			out = append(out, OperationViolation{OpIndex: index, Op: op, Violations: []kdberr.FieldViolation{{
				FieldName: "_attachment", ViolationType: kdberr.CustomConstraint, Detail: detail,
			}}})
		}
	}
	if len(out) > 0 {
		return out
	}
	// The rules a table enforces itself - a name unique within its document - checked by applying
	// the operations to the table they land on.
	table, err := d.AttachmentTableAt(anchorCommit)
	if err != nil {
		return []OperationViolation{{OpIndex: 0, Op: tx.Operations[0], Violations: []kdberr.FieldViolation{{
			FieldName: "_attachment", ViolationType: kdberr.CustomConstraint, Detail: err.Error(),
		}}}}
	}
	for index, op := range tx.Operations {
		if table, err = table.Apply(tx.Operations[index : index+1]); err != nil {
			return []OperationViolation{{OpIndex: index, Op: op, Violations: []kdberr.FieldViolation{{
				FieldName: "_attachment", ViolationType: kdberr.CustomConstraint, Detail: err.Error(),
			}}}}
		}
	}
	return nil
}

// detectAttachmentConflicts compares each document the transaction's attachment operations (and
// document deletes) touch between the transaction's base and the commit it lands on.
func detectAttachmentConflicts(
	tx document.Transaction, namespaceID string, d *dag.InMemoryCommitDag, store storage.Adapter,
	baseTreeHash, targetTreeHash codec.Hash, anchorCommit codec.Hash, guarded map[int]struct{},
) ([]OperationConflict, error) {
	if !touchesAttachmentOps(tx) && !hasDelete(tx) {
		return nil, nil
	}
	if tx.BaseVersion == anchorCommit || tx.BaseVersion == (codec.Hash{}) {
		return nil, nil // nothing landed in between, or anchored on whatever the head is
	}
	base, err := d.AttachmentTableAt(tx.BaseVersion)
	if err != nil {
		return nil, err
	}
	target, err := d.AttachmentTableAt(anchorCommit)
	if err != nil {
		return nil, err
	}
	if base.Len() == 0 && target.Len() == 0 && !touchesAttachmentOps(tx) {
		return nil, nil
	}
	// One attachment moved, or (for a document delete) any of the document's.
	attachmentChanged := func(doc, att codec.UUID) bool {
		b, bok := base.Get(doc, att)
		t, tok := target.Get(doc, att)
		return bok != tok || b != t
	}
	documentChanged := func(doc codec.UUID) bool {
		return base.List(doc).Hash() != target.List(doc).Hash()
	}
	var out []OperationConflict
	for index, op := range tx.Operations {
		if _, exempt := guarded[index]; exempt {
			continue
		}
		var id codec.UUID
		var changed bool
		switch o := op.(type) {
		case document.AttachmentPutOp:
			id, changed = o.DocID, attachmentChanged(o.DocID, o.AttachmentID)
		case document.AttachmentDeleteOp:
			id, changed = o.DocID, attachmentChanged(o.DocID, o.AttachmentID)
		case document.DeleteOp:
			id, changed = o.DocID, documentChanged(o.DocID)
		default:
			continue
		}
		baseDoc, _ := store.GetDocument(namespaceID, id, baseTreeHash)
		existingDoc, _ := store.GetDocument(namespaceID, id, targetTreeHash)
		_, isDelete := op.(document.DeleteOp)
		switch {
		case changed:
			// The attachment moved under us - for an attachment op, or for a delete that would
			// take attachments the caller has not seen.
			out = append(out, OperationConflict{OpIndex: index, Op: op, Type: kdberr.ConcurrentWrite,
				ExistingDoc: existingDoc, BaseDoc: baseDoc})
		case !isDelete && baseDoc != nil && existingDoc == nil:
			// The document was deleted concurrently: its attachment cannot be set or removed.
			out = append(out, OperationConflict{OpIndex: index, Op: op, Type: kdberr.WriteDelete,
				BaseDoc: baseDoc})
		}
	}
	return out, nil
}

func hasDelete(tx document.Transaction) bool {
	for _, op := range tx.Operations {
		if _, ok := op.(document.DeleteOp); ok {
			return true
		}
	}
	return false
}
