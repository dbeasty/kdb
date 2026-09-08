package control

import (
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// Editing one document.
//
// The interesting part is not the write, it is the concurrency. A browser holds a document on
// screen while other clients keep writing, so a save has to be able to say "only if this is still
// what I read". The engine already has that: a Transaction carries per-operation Preconditions,
// and ExpectContentHash compares the stored hash literally - it fails even when the incoming
// content happens to be identical, because a compare-and-set is an assertion about the identity of
// the state, not about whether the write would change anything.
//
// So the contract is: read a document, get its contentHash, send it back with the edit. If someone
// else got there first the save is refused with 409 and the hash that beat it, which is what lets
// the UI show the operator what changed rather than a generic failure.

type docWriteRequest struct {
	// Body is the document, as JSON. Sent as a raw message so it reaches storage byte-for-byte:
	// decoding into a map and re-encoding would reorder keys, and this engine promises it does
	// not do that.
	Body json.RawMessage `json:"body"`
	// IfContentHash makes the write conditional: apply only if the stored document still hashes
	// to this. Empty means unconditional.
	IfContentHash string `json:"ifContentHash"`
	// IfAbsent makes the write a create: apply only if no document exists at this id.
	IfAbsent bool `json:"ifAbsent"`
}

type docDeleteRequest struct {
	IfContentHash string `json:"ifContentHash"`
}

// handlePutDocument writes one document, optionally conditionally.
func (s *Server) handlePutDocument(w http.ResponseWriter, r *http.Request, principal auth.Principal, ns string, rt *serverRuntime) {
	docID, err := codec.UUIDFromString(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "document id must be a UUID")
		return
	}
	var req docWriteRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<20)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "could not read request body: "+err.Error())
		return
	}
	if len(req.Body) == 0 {
		writeError(w, http.StatusBadRequest, "bad_request", `"body" is required`)
		return
	}
	if !json.Valid(req.Body) {
		writeError(w, http.StatusBadRequest, "bad_request", "body is not valid JSON")
		return
	}
	if req.IfContentHash != "" && req.IfAbsent {
		writeError(w, http.StatusBadRequest, "bad_request",
			`"ifContentHash" and "ifAbsent" contradict each other: one asserts the document exists `+
				`with a given content, the other that it does not exist at all`)
		return
	}

	pre, err := preconditionFor(req.IfContentHash, req.IfAbsent)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	// A write is a shallow root-level merge over the stored document - the engine's documented
	// behaviour for every write path, not a choice this endpoint makes. So a key the body omits
	// keeps its stored value, and an operator editing in a text box has to be told that rather
	// than discovering it: the response names exactly which keys were kept.
	retained := retainedKeys(rt, ns, docID, req.Body)

	s.commitDocumentOps(w, r, principal, ns, rt,
		[]document.Op{document.WriteOp{DocID: docID, Patch: string(req.Body)}}, pre,
		func(commit document.Commit) map[string]any {
			out := map[string]any{
				"namespace": ns, "docId": docID.String(), "commit": commit.Hash.Hex(),
				"merged": true,
			}
			// The hash of what is now stored, read back rather than computed from the request:
			// under merge semantics the stored document is not the body that was sent, and a
			// client using this for its next conditional write would be refused.
			if stored, _, found, err := rt.GetDocument(ns, docID); err == nil && found {
				out["contentHash"] = contentHashOf(docID, stored)
			}
			if len(retained) > 0 {
				out["retainedKeys"] = retained
				out["note"] = "a write merges over the stored document, so these keys were kept " +
					"even though the body omitted them. To remove them, delete the document and " +
					"recreate it."
			}
			return out
		})
}

// handleDeleteDocument removes one document, optionally conditionally.
func (s *Server) handleDeleteDocument(w http.ResponseWriter, r *http.Request, principal auth.Principal, ns string, rt *serverRuntime) {
	docID, err := codec.UUIDFromString(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "document id must be a UUID")
		return
	}
	var req docDeleteRequest
	// A DELETE with no body is an unconditional delete, so an empty body is not an error.
	if r.Body != nil {
		_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&req)
	}
	pre, err := preconditionFor(req.IfContentHash, false)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	s.commitDocumentOps(w, r, principal, ns, rt,
		[]document.Op{document.DeleteOp{DocID: docID}}, pre,
		func(commit document.Commit) map[string]any {
			return map[string]any{
				"namespace": ns, "docId": docID.String(), "commit": commit.Hash.Hex(), "deleted": true,
			}
		})
}

// preconditionFor builds the assertion guarding operation 0, or nil for an unconditional write.
func preconditionFor(ifContentHash string, ifAbsent bool) ([]document.Precondition, error) {
	switch {
	case ifAbsent:
		return []document.Precondition{{OpIndex: 0, Kind: document.ExpectAbsent}}, nil
	case ifContentHash != "":
		h, err := codec.HashFromHex(strings.TrimSpace(ifContentHash))
		if err != nil {
			return nil, errors.New("ifContentHash is not a content hash: " + err.Error())
		}
		return []document.Precondition{{OpIndex: 0, Kind: document.ExpectContentHash, ContentHash: h}}, nil
	default:
		return nil, nil
	}
}

// commitDocumentOps is the shared write path: build a transaction on the current head, check the
// leases, commit, and translate the failure modes into answers a client can act on.
func (s *Server) commitDocumentOps(
	w http.ResponseWriter,
	r *http.Request,
	principal auth.Principal,
	ns string,
	rt *serverRuntime,
	ops []document.Op,
	preconditions []document.Precondition,
	success func(document.Commit) map[string]any,
) {
	head, err := s.resolveRevisionFor(rt, "")
	if err != nil {
		s.writeRevisionError(w, err)
		return
	}
	builder := &transaction.Builder{NamespaceID: ns, BaseVersion: head, Schema: rt.Schema()}
	for _, op := range ops {
		switch o := op.(type) {
		case document.WriteOp:
			builder.Write(o.DocID, o.Patch)
		case document.DeleteOp:
			builder.Delete(o.DocID)
		}
	}
	tx, err := builder.Build(codec.Timestamp{})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "transaction_failed", err.Error())
		return
	}
	tx.Preconditions = preconditions

	if err := rt.DocumentLocks.AssertUnheldByOthers(ns, controlSessionID(principal), tx); err != nil {
		writeError(w, http.StatusConflict, "document_locked", err.Error())
		return
	}

	commit, err := rt.Commit(ns, tx, controlSessionID(principal), principal)
	if err != nil {
		s.writeCommitError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, success(commit))
}

// writeCommitError turns a failed commit into a response that says what to do next.
//
// The three outcomes are genuinely different and must not collapse into one status. A failed
// precondition means someone else wrote first and the caller's premise is stale - it carries the
// hash that beat them, so they can re-read and merge. A plain conflict means retry. A schema
// violation means the document is wrong and retrying unchanged will fail identically.
func (s *Server) writeCommitError(w http.ResponseWriter, err error) {
	var conflict *server.ConflictError
	if errors.As(err, &conflict) {
		items := make([]map[string]any, 0, len(conflict.Report.Conflicts))
		precondition := false
		for _, c := range conflict.Report.Conflicts {
			item := map[string]any{
				"docId": c.DocumentID,
				"kind":  c.OperationType.String(),
			}
			if c.ActualContentHash != nil {
				item["actualContentHash"] = *c.ActualContentHash
				precondition = true
			}
			items = append(items, item)
		}
		code, message := "conflict", "another writer committed first; re-read and retry"
		if precondition {
			code = "precondition_failed"
			message = "this document changed since you read it. The current content hash is " +
				"reported below; re-read the document and reapply the edit."
		}
		body := map[string]any{
			"conflicts":  items,
			"baseHash":   conflict.Report.BaseHash,
			"targetHash": conflict.Report.TargetHash,
		}
		if conflict.RetryAfterMs > 0 {
			body["retryAfterMs"] = conflict.RetryAfterMs
		}
		writeErrorWithDetail(w, http.StatusConflict, code, message, body)
		return
	}

	var schemaErr *server.SchemaError
	if errors.As(err, &schemaErr) {
		writeError(w, http.StatusBadRequest, "schema_violation", err.Error())
		return
	}
	writeError(w, http.StatusInternalServerError, "commit_failed", err.Error())
}

// contentHashOf is the hash the next conditional write should assert on. Returned with a
// successful save so a client editing repeatedly does not have to re-read between edits.
func contentHashOf(docID codec.UUID, body string) string {
	h, err := document.ComputeContentHash(document.Document{ID: docID, JSON: body})
	if err != nil {
		return ""
	}
	return h.Hex()
}

var _ = kdberr.ConflictReport{}

// retainedKeys reports the top-level keys the stored document has that the incoming body does not -
// the keys a merge will keep and an operator may have meant to remove.
//
// Read before the write, so it describes the document the save is landing on. Best-effort: a
// document that cannot be read yields no keys, and the save is unaffected.
func retainedKeys(rt *serverRuntime, ns string, docID codec.UUID, body []byte) []string {
	stored, _, found, err := rt.GetDocument(ns, docID)
	if err != nil || !found {
		return nil
	}
	var have, want map[string]json.RawMessage
	if json.Unmarshal([]byte(stored), &have) != nil || json.Unmarshal(body, &want) != nil {
		return nil
	}
	var out []string
	for k := range have {
		if _, ok := want[k]; !ok {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}
