package server

import (
	"fmt"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// Expect is a precondition on a document's current value, checked at the front of the write gate
// - after every earlier local write and every peer merge ahead of it - so a read-modify-write
// cannot act on a value that has since changed.
type Expect struct {
	// ContentHash, when set, is the hex content hash the document must currently have (as
	// ReadForUpdate returned it).
	ContentHash string
	// Absent requires the document not to exist: a create that must not overwrite.
	Absent bool
}

// PreconditionFailedError is a PutJSON whose Expect did not hold. Current is the document's
// content hash now, empty when it does not exist; re-read and retry.
type PreconditionFailedError struct {
	Namespace, DocID string
	Expected         string
	Current          string
}

func (e *PreconditionFailedError) Error() string {
	want := e.Expected
	if want == "" {
		want = "absent"
	}
	have := e.Current
	if have == "" {
		have = "absent"
	}
	return fmt.Sprintf("kdb server: precondition failed for %s/%s: expected %s, document is %s", e.Namespace, e.DocID, want, have)
}

// ReadForUpdate reads a document at head together with its content hash, for a read-modify-write
// through PutJSON: pass the hash back as Expect.ContentHash. found is false (and hash empty) when
// the document does not exist.
func (s *KdbServerRuntime) ReadForUpdate(namespaceID string, docID codec.UUID) (json, contentHash string, found bool, err error) {
	_, commit, ok, err := s.Runtime.DAG.HeadCommit()
	if err != nil || !ok {
		return "", "", false, err
	}
	doc, err := s.Runtime.Storage.GetDocument(namespaceID, docID, commit.DocumentTreeHash)
	if err != nil || doc == nil {
		return "", "", false, err
	}
	h, err := doc.ContentHash()
	if err != nil {
		return "", "", false, err
	}
	return doc.JSON, h.Hex(), true, nil
}

// currentHash is docID's content hash at the live head, empty when absent. Only meaningful under
// the write gate.
func (s *KdbServerRuntime) currentHash(namespaceID string, docID codec.UUID) (string, error) {
	_, commit, ok, err := s.Runtime.DAG.HeadCommit()
	if err != nil || !ok {
		return "", err
	}
	doc, err := s.Runtime.Storage.GetDocument(namespaceID, docID, commit.DocumentTreeHash)
	if err != nil || doc == nil {
		return "", err
	}
	h, err := doc.ContentHash()
	if err != nil {
		return "", err
	}
	return h.Hex(), nil
}

// PutJSON replaces a document whole - the stored value becomes exactly jsonBody, with no field
// of the old value surviving, as embed.PutJSONDocument does - through the write gate, so it is
// serialized with every other local write and with peer ingest. expect, when non-nil, must hold
// at the front of the gate or the write fails with *PreconditionFailedError and changes nothing.
//
// An application that reads, decides and writes (db.Update) passes the hash ReadForUpdate gave it
// and retries on PreconditionFailedError: a replicated merge that landed in between is then seen
// rather than overwritten.
func (s *KdbServerRuntime) PutJSON(namespaceID string, docID codec.UUID, jsonBody string, expect *Expect, principal auth.Principal) (document.Commit, error) {
	head, err := s.Runtime.DAG.Head()
	if err != nil {
		return document.Commit{}, err
	}
	txID, err := codec.RandomUUID()
	if err != nil {
		return document.Commit{}, err
	}
	defer s.pinBaseTree(head)()
	tx := s.authored(document.Transaction{
		ID:          txID,
		BaseVersion: head,
		// Delete then write in one transaction is a replace: the write starts from nothing.
		Operations: []document.Op{document.DeleteOp{DocID: docID}, document.WriteOp{DocID: docID, Patch: jsonBody}},
		Timestamp:  codec.TimestampNow(),
	})
	return s.clientWrite(tx, func() (document.Commit, error) {
		return s.runTransaction(tx, principal, txOptions{}, func() (transactionResult, error) {
			// At the front of the gate: nothing else can move head until this commit lands.
			if expect != nil {
				current, err := s.currentHash(namespaceID, docID)
				if err != nil {
					return nil, err
				}
				want := expect.ContentHash
				if expect.Absent {
					want = ""
				}
				if current != want {
					return nil, &PreconditionFailedError{Namespace: namespaceID, DocID: docID.String(), Expected: want, Current: current}
				}
			}
			// The precondition was checked at the live head, so that is the base: a merge that
			// moved head without touching this document is not a conflict.
			live := tx
			if h, err := s.Runtime.DAG.Head(); err == nil {
				live.BaseVersion = h
			}
			return s.UpsertEngine.Commit(live, s.dag, s.Runtime.Storage, s.Schema(), nil, s.clientMessage(live))
		})
	})
}
