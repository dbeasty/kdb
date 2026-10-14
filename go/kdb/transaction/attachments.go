package transaction

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/storage"
)

// Attachment rules (docs/kdb-attachments-plan.md):
//   - an AttachmentPutOp needs its blob already in the blob store, and its document to exist once
//     the transaction has applied (stored before, or written in it - not deleted after);
//   - an attachment change conflicts with a concurrent change to the same document's attachment,
//     and with a concurrent delete of the document; a document delete conflicts with a concurrent
//     attachment change; a JSON-only write conflicts with neither - the attachment is not part of
//     the document's content hash.

// BlobPresence is the optional storage capability preflight uses to ask whether a blob is stored
// without reading it - an attachment can be gigabytes. A store without it is asked with ReadBlob.
type BlobPresence interface {
	HasBlob(contentHash codec.Hash) (bool, error)
}

func hasBlob(store storage.Adapter, h codec.Hash) bool {
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
	tx document.Transaction, namespaceID string, store storage.Adapter, targetTreeHash codec.Hash,
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
		case put.Size < 0:
			detail = "attachment size is negative"
		case !presentAt(put.DocID):
			detail = "no such document: an attachment needs its document"
		case !hasBlob(store, put.ContentHash):
			detail = "attachment blob not stored: " + put.ContentHash.Hex()
		}
		if detail != "" {
			out = append(out, OperationViolation{OpIndex: index, Op: op, Violations: []kdberr.FieldViolation{{
				FieldName: "_attachment", ViolationType: kdberr.CustomConstraint, Detail: detail,
			}}})
		}
	}
	return out
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
	changed := func(id codec.UUID) bool {
		b, bok := base.Get(id)
		t, tok := target.Get(id)
		return bok != tok || b != t
	}
	var out []OperationConflict
	for index, op := range tx.Operations {
		if _, exempt := guarded[index]; exempt {
			continue
		}
		var id codec.UUID
		switch o := op.(type) {
		case document.AttachmentPutOp:
			id = o.DocID
		case document.AttachmentDeleteOp:
			id = o.DocID
		case document.DeleteOp:
			id = o.DocID
		default:
			continue
		}
		baseDoc, _ := store.GetDocument(namespaceID, id, baseTreeHash)
		existingDoc, _ := store.GetDocument(namespaceID, id, targetTreeHash)
		_, isDelete := op.(document.DeleteOp)
		switch {
		case changed(id):
			// The attachment moved under us - for an attachment op, or for a delete that would
			// take an attachment the caller has not seen.
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
