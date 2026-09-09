package control

import (
	"net/http"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
)

// Every version of one document.
//
// Built by reading the document at each commit rather than by filtering commit *operations*, and
// that choice is load-bearing: operations are evictable under the DAG's retention budget
// (dag/ops_retention.go), so an operation scan would silently truncate a document's history the
// moment the budget bit - showing a short, plausible, wrong answer. Reading through the storage
// adapter goes via the tree, which is rebuilt on demand when it is not resident, so the answer is
// the same however long ago the write happened.
//
// It costs one read per commit walked, which is why it is paged and capped rather than "the whole
// history of this document". A per-document commit index would remove that cost and is the obvious
// follow-up if this ever becomes hot; it is not worth building before the feature has users.

// docVersion is one point at which this document changed.
type docVersion struct {
	Commit      string `json:"commit"`
	ShortCommit string `json:"shortCommit"`
	Timestamp   string `json:"timestamp"`
	Message     string `json:"message"`
	// Change is what happened to the document at this commit: created, modified, or deleted.
	Change string `json:"change"`
	// ContentHash identifies the version, and is what a conditional write asserts on. Empty for a
	// deletion, which has no content.
	ContentHash string `json:"contentHash,omitempty"`
	SizeBytes   int    `json:"sizeBytes,omitempty"`
}

// handleDocumentHistory lists the commits at which one document changed, newest first.
func (s *Server) handleDocumentHistory(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string, rt *serverRuntime) {
	docID, err := codec.UUIDFromString(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "document id must be a UUID")
		return
	}
	limit, err := intParam(r, "limit", 25, 1, 200)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	skip, err := intParam(r, "skip", 0, 0, maxSkip)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	from, err := s.resolveRevisionFor(rt, r.URL.Query().Get("from"))
	if err != nil {
		s.writeRevisionError(w, err)
		return
	}
	nav, err := s.navigatorFor(rt)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "no_navigator", err.Error())
		return
	}
	if rt.Runtime == nil {
		writeError(w, http.StatusInternalServerError, "no_runtime", "runtime has no embedded runtime")
		return
	}

	// One commit past the window on each side. The older one is needed to decide whether the
	// oldest commit in the page was a change or merely the state carried into it; the newer one
	// tells the caller whether more remain.
	lookBack := skip
	if lookBack > 0 {
		lookBack--
	}
	infos, err := nav.ListCommits(from, lookBack, limit+2+(skip-lookBack))
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "walk_failed", err.Error())
		return
	}

	versions, more, truncated := s.documentVersions(rt, ns, docID, infos, skip, lookBack, limit)
	body := map[string]any{
		"namespace": ns,
		"docId":     docID.String(),
		"versions":  versions,
		"hasMore":   more,
		"nextSkip":  skip + len(versions),
	}
	if truncated != "" {
		body["note"] = truncated
	}
	writeJSON(w, http.StatusOK, body)
}

// documentVersions walks the commits and keeps the ones where this document's content changed.
//
// Comparing content hashes rather than commit membership is what makes the result mean something:
// a commit that rewrote a document with identical content did not change it, and listing it as a
// version would send a reader looking for a difference that is not there.
func (s *Server) documentVersions(
	rt *serverRuntime, ns string, docID codec.UUID, infos []dag.CommitInfo, skip, lookBack, limit int,
) (out []docVersion, more bool, note string) {
	store := rt.Runtime.Storage
	out = make([]docVersion, 0, limit)

	// Resolve the document at each commit once, oldest of the window first is not needed - the walk
	// is newest-first and each entry is compared against the next (older) one.
	type state struct {
		info document.Commit
		hash string
		size int
		ok   bool
	}
	resolved := make([]state, 0, len(infos))
	for _, info := range infos {
		st := state{}
		if doc, err := store.GetDocument(ns, docID, info.TreeHash); err == nil && doc != nil {
			st.ok, st.hash, st.size = true, contentHashOf(docID, doc.JSON), len(doc.JSON)
		}
		st.info = document.Commit{
			Hash: info.Hash, Timestamp: info.Timestamp, Message: info.Message,
			ParentHashes: info.ParentHashes,
		}
		resolved = append(resolved, st)
	}

	skipped := 0
	for i := 0; i < len(resolved); i++ {
		cur := resolved[i]
		// The state this commit inherited. Past the end of the walk there is nothing older, so the
		// document either appeared here or was never here.
		var prev state
		if i+1 < len(resolved) {
			prev = resolved[i+1]
		}

		change := ""
		switch {
		case cur.ok && !prev.ok:
			change = "created"
		case cur.ok && prev.ok && cur.hash != prev.hash:
			change = "modified"
		case !cur.ok && prev.ok:
			change = "deleted"
		}
		if change == "" {
			continue
		}
		// Entries below the caller's window are counted, not returned: the window is over
		// *versions*, not over commits, and a caller paging through versions should not have to
		// know how many commits sat between them.
		if skipped < skip-lookBack {
			skipped++
			continue
		}
		if len(out) == limit {
			return out, true, note
		}
		v := docVersion{
			Commit:      cur.info.Hash.Hex(),
			ShortCommit: shortHash(cur.info.Hash.Hex()),
			Timestamp:   millisToRFC3339(cur.info.Timestamp.EpochMillis),
			Message:     cur.info.Message,
			Change:      change,
		}
		if cur.ok {
			v.ContentHash, v.SizeBytes = cur.hash, cur.size
		}
		out = append(out, v)
	}

	// The walk ran out. Whether that is the start of history or the edge of what is retained is
	// worth distinguishing, because only the second is fixed by configuring a longer window.
	if len(resolved) > 0 {
		oldest := resolved[len(resolved)-1]
		if len(oldest.info.ParentHashes) > 0 {
			note = "this is as far back as this page walked; there is more history before it"
			more = true
		}
	}
	return out, more, note
}
