package embed

import (
	"fmt"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
)

// Reading one document's past.
//
// The commit DAG has held every version of every document since it was
// written — under storage.HistoryModeFull nothing on that path is ever
// reclaimed — but until now nothing embedded could ask for them. SQL_EXEC
// carries AT COMMIT / AT VERSION / AT TIME and HISTORY_LIST reports a
// namespace's commits, and both of those are the *server's*: a caller with no
// wire and no SQL had no way in at all.
//
// The gap that mattered was not reading at a commit — that is four lines, and
// go/kdb/server has had them all along. It was that a caller holding a
// document id has no way to learn which commits ever touched it. The DAG is
// keyed by commit, not by document, so "what happened to this one row?" could
// only be answered by walking everything. These two functions are that
// question, made askable.

// DocumentVersion is one commit that wrote a document.
type DocumentVersion struct {
	// Commit is the commit this version was written in — the handle to hand
	// back to DocumentAt.
	Commit codec.Hash
	// Deleted marks the commit that removed the document rather than wrote it.
	Deleted bool
}

// DocumentAt returns a document's JSON as of a commit, and whether it existed
// there.
//
// The commit is a point in the namespace's whole history rather than in this
// document's, so any commit hash is a legitimate question: a document that had
// not been written yet simply reports false.
func DocumentAt(rt *EmbeddedKdbRuntime, namespaceID string, docID codec.UUID, at codec.Hash) (string, bool, error) {
	if rt == nil || rt.DAG == nil || rt.Storage == nil {
		return "", false, fmt.Errorf("kdb: runtime has no history to read")
	}
	commit, ok := rt.DAG.GetCommit(at)
	if !ok {
		return "", false, fmt.Errorf("kdb: commit %s is not in this namespace's history", at.Hex())
	}
	doc, err := rt.Storage.GetDocument(namespaceID, docID, commit.DocumentTreeHash)
	if err != nil {
		return "", false, err
	}
	if doc == nil {
		return "", false, nil
	}
	return doc.JSON, true, nil
}

// DocumentVersions lists the commits that wrote a document, oldest first.
//
// A scan of the namespace's durable commit log, because that is the only place
// the association exists: the DAG indexes commits, and a commit names the
// documents it touched, but nothing indexes the reverse. So this is O(the
// log), not O(this document's history), and a caller that needs it repeatedly
// should ask once and keep the answer — the list for a given document only
// ever grows, and only at its end.
//
// Streams the log where the reader can (storage.DeltaCommitStreamer), because
// ReadAll materialises every version of every document in a segment in order
// to hand back the handful this caller wants.
func DocumentVersions(rt *EmbeddedKdbRuntime, docID codec.UUID) ([]DocumentVersion, error) {
	if rt == nil || rt.deltaReader == nil {
		return nil, fmt.Errorf("kdb: runtime keeps no durable commit log")
	}
	segments, err := rt.deltaReader.ListSegments()
	if err != nil {
		return nil, err
	}

	var out []DocumentVersion
	streamer, streams := rt.deltaReader.(storage.DeltaCommitStreamer)

	for _, seg := range segments {
		if streams {
			// A commit names the documents it wrote, which is all this needs,
			// so streaming answers without materialising the segment.
			if err := streamer.StreamCommits(seg, func(c document.Commit, _ int64) error {
				for _, op := range c.Operations {
					switch o := op.(type) {
					case document.WriteOp:
						if o.DocID == docID {
							out = append(out, DocumentVersion{Commit: c.Hash})
						}
					case document.DeleteOp:
						if o.DocID == docID {
							out = append(out, DocumentVersion{Commit: c.Hash, Deleted: true})
						}
					}
				}
				return nil
			}); err != nil {
				return nil, err
			}
			continue
		}

		records, err := rt.deltaReader.ReadAll(seg)
		if err != nil {
			return nil, err
		}
		for _, r := range records {
			for _, p := range r.DocumentPatches {
				if p.DocID == docID {
					out = append(out, DocumentVersion{Commit: r.CommitHash, Deleted: p.After == nil})
				}
			}
		}
	}
	return out, nil
}
