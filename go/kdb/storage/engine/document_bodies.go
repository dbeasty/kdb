package engine

import (
	"fmt"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
)

// Document bodies as content-addressed objects, for namespaces whose delta
// log is going to be truncated out from under them.
//
// Under HistoryModeFull the delta log is the only durable home a document
// version has, and that is fine, because nothing ever deletes a segment: a
// version evicted from docsByHash is found again through the log (see
// deltaColdLoader), and the objects strategy stores a *pointer* into the
// log rather than the bytes, precisely to avoid writing every version
// twice.
//
// HistoryModeNone breaks that arrangement, because it deletes segments. A
// pointer into a frame that no longer exists is worse than no pointer at
// all, and the live dataset would be unreadable the moment its versions
// were evicted from memory. So under that mode the bytes go somewhere the
// truncation cannot reach: the blob store the engine already has - memtable
// now, SSTable after a flush, discovered again at open
// (LsmBlobStore.DiscoverTables).
//
// Written through the memtable directly rather than WriteBlob, for the
// same reason tree objects are: WriteBlob appends to the WAL and, under
// DurabilitySync, fsyncs. Doing that per document per commit would put a
// second sync on the write path, and it buys nothing - the commit's own
// delta frame is already durable and already holds these bytes. What the
// blob copy has to survive is *truncation*, not a crash, and the ordering
// that guarantees it is a memtable flush before any segment is deleted.
// See PrepareForTruncation.

// putDocumentBody records one document version's bytes under its content
// hash, so it can be found again after the delta segment that carried it
// is deleted. A no-op under any mode that does not truncate.
func (e *ServerEngine) putDocumentBody(contentHash codec.Hash, doc document.Document) {
	if e.memTable == nil || e.HistoryMode() != storage.HistoryModeNone {
		return
	}
	e.memTable.Put(contentHash, []byte(doc.JSON))
}

// loadDocumentBody reads a version back out of the blob store.
//
// The content hash is verified rather than trusted: this store is shared
// with tree objects and document locations, all keyed by hash in the same
// space, and a lookup that collided with one of those would otherwise
// hand back a tree object decoded as a document. Verifying makes a wrong
// answer impossible and a miss the only failure mode.
func (e *ServerEngine) loadDocumentBody(docID codec.UUID, contentHash codec.Hash) (document.Document, bool, error) {
	if e.memTable == nil {
		return document.Document{}, false, nil
	}
	raw := e.memTable.Get(contentHash)
	if raw == nil {
		return document.Document{}, false, nil
	}
	doc := document.Document{ID: docID, JSON: string(raw)}
	got, err := doc.ContentHash()
	if err != nil || got != contentHash {
		return document.Document{}, false, nil
	}
	return doc, true, nil
}

// PrepareForTruncation makes every document version this namespace still
// needs durable outside the delta log, and reports whether it succeeded.
//
// This is the precondition for deleting a delta segment under
// HistoryModeNone, and the reason truncation is safe at all.
//
// It is not enough to flush whatever happens to be in the memtable.
// putDocumentBody writes a version's bytes as it is committed, but that is
// this process's memtable: a version written by an earlier session, or
// before this namespace was converted to history=none, has no blob copy at
// all, and its only home is the very segment about to be deleted. So the
// live tree is walked and every version it names is *checked* - and any
// that is missing is fetched (from memory, or from the log while the log
// is still there) and written in.
//
// The cost is proportional to the live dataset, not to history, and it is
// paid at checkpoint cadence rather than per write. That is the right
// shape: it is the same walk a backup would do, and it is what makes the
// invariant "everything the live tree names is in the blob store" true by
// construction rather than by assumption.
func (e *ServerEngine) PrepareForTruncation() error {
	if e.HistoryMode() != storage.HistoryModeNone {
		return nil
	}
	if e.memTable == nil || e.config.IOShim == nil {
		return nil
	}
	tree := e.LiveTree()
	var missing []treeEntryRef
	tree.Walk(func(id codec.UUID, h codec.Hash) bool {
		if e.memTable.Get(h) != nil {
			return true
		}
		missing = append(missing, treeEntryRef{docID: id, contentHash: h})
		return true
	})
	for _, m := range missing {
		doc, ok := e.docsByHash.Get(m.contentHash)
		if !ok {
			// Not resident either. The delta log still has it - this runs
			// before anything is deleted - so the ordinary cold path finds
			// it, and loadCold re-admits it on the way past.
			loaded, found, err := e.loadCold(m.docID, m.contentHash)
			if err != nil {
				return err
			}
			if !found {
				// A version the live tree names that nothing can produce.
				// Refuse the whole truncation: deleting segments now would
				// turn a version that is merely unreachable into one that
				// is genuinely gone.
				return fmt.Errorf(
					"kdb: namespace %s: cannot make document %s version %s durable outside the delta log; "+
						"refusing to truncate",
					e.namespaceID, m.docID.String(), m.contentHash.Hex())
			}
			doc = loaded
		}
		e.memTable.Put(m.contentHash, []byte(doc.JSON))
	}
	if _, err := e.memTable.Flush(0); err != nil {
		return err
	}
	if e.wal != nil {
		return e.wal.Sync()
	}
	return nil
}

// treeEntryRef pairs a document with the version the live tree names for
// it, which is the unit PrepareForTruncation has to make durable.
type treeEntryRef struct {
	docID       codec.UUID
	contentHash codec.Hash
}

// LiveBodiesDurable reports whether every version the live tree names is
// already in the blob store. For tests and for the inspect tooling: it is
// the invariant truncation depends on, so being able to check it directly
// is worth the small surface.
func (e *ServerEngine) LiveBodiesDurable() bool {
	if e.memTable == nil {
		return false
	}
	ok := true
	e.LiveTree().Walk(func(_ codec.UUID, h codec.Hash) bool {
		if e.memTable.Get(h) == nil {
			ok = false
			return false
		}
		return true
	})
	return ok
}
