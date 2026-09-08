package control

import (
	"encoding/json"
	"net/http"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/embed"
)

// Revert, as docs/kdb-control-ui-plan.md §6.3 specifies it: never one click, and never a rewrite
// of history.
//
// The engine's embed.RevertTo restores the state a namespace had at a past commit by writing a
// *new* commit whose tree is the target's. History moves forward. That is not a stylistic
// preference - commits carry no pre-image, so history cannot be run backwards, and moving a branch
// head backwards would leave reads and writes disagreeing about what the database is.
//
// What this adds on top is the part an operator needs and a CLI command does not have to provide:
// a preview of exactly what would change, and a guarantee that the thing applied is the thing
// previewed. Hence the plan/apply split and the mandatory expectHead.

type revertRequest struct {
	// To is the revision whose state should be restored - any revision specification.
	To string `json:"to"`
	// ExpectHead is the head the plan was computed against. Required on apply.
	ExpectHead string `json:"expectHead"`
}

func decodeRevertRequest(w http.ResponseWriter, r *http.Request) (revertRequest, bool) {
	var req revertRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "could not read request body: "+err.Error())
		return revertRequest{}, false
	}
	if req.To == "" {
		writeError(w, http.StatusBadRequest, "bad_request",
			`"to" is required: the revision whose state should be restored`)
		return revertRequest{}, false
	}
	return req, true
}

// handleRevertPlan is the dry run. It computes the diff from head to the target - which is
// precisely the set of changes applying would make - and returns the head it was computed against,
// so apply can refuse if the world has moved.
func (s *Server) handleRevertPlan(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string) {
	req, ok := decodeRevertRequest(w, r)
	if !ok {
		return
	}
	head, err := s.resolveRevision("head")
	if err != nil {
		s.writeRevisionError(w, err)
		return
	}
	target, err := s.resolveRevision(req.To)
	if err != nil {
		s.writeRevisionError(w, err)
		return
	}

	// head -> target: the entries here are the writes and deletes a revert would perform.
	_, _, entries, err := s.diffRevisions(head.Hex(), target.Hex())
	if err != nil {
		if isUnknownRevision(err) {
			s.writeRevisionError(w, err)
			return
		}
		writeError(w, http.StatusServiceUnavailable, "plan_unavailable",
			"this revert could not be planned: "+err.Error()+
				". A target whose document tree can no longer be reconstructed cannot be reverted to; "+
				"under history=none that is what a reclaimed retention window looks like.")
		return
	}
	counts := countChanges(entries)
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace":  ns,
		"target":     target.Hex(),
		"expectHead": head.Hex(),
		"entries":    entries,
		// Named for what applying would do, rather than reusing the diff's added/removed, which
		// read backwards here: a document "added" between head and target is one the revert
		// restores.
		"willRestore": counts["added"] + counts["modified"],
		"willRemove":  counts["removed"],
		"noop":        len(entries) == 0,
		"counts":      counts,
	})
}

// handleRevertApply performs the revert, refusing if head has moved since the plan.
func (s *Server) handleRevertApply(w http.ResponseWriter, r *http.Request, principal auth.Principal, ns string) {
	req, ok := decodeRevertRequest(w, r)
	if !ok {
		return
	}
	if req.ExpectHead == "" {
		writeError(w, http.StatusBadRequest, "bad_request",
			`"expectHead" is required: apply the plan you were shown, against the head it was `+
				`computed from. Call the plan endpoint first.`)
		return
	}
	expect, err := s.resolveRevision(req.ExpectHead)
	if err != nil {
		s.writeRevisionError(w, err)
		return
	}
	head, err := s.resolveRevision("head")
	if err != nil {
		s.writeRevisionError(w, err)
		return
	}
	if head != expect {
		// Someone committed between the preview and the apply. Reverting anyway would undo their
		// write too, without it ever having appeared in the preview.
		writeError(w, http.StatusConflict, "head_moved",
			"head has moved since this revert was planned (expected "+shortHash(expect.Hex())+
				", found "+shortHash(head.Hex())+"): re-plan and review the change again")
		return
	}

	rt := s.opts.Runtime.Runtime
	if rt == nil {
		writeError(w, http.StatusInternalServerError, "no_runtime", "runtime has no embedded runtime")
		return
	}
	result, err := embed.RevertTo(rt, ns, req.To)
	if err != nil {
		if isUnknownRevision(err) {
			s.writeRevisionError(w, err)
			return
		}
		writeError(w, http.StatusServiceUnavailable, "revert_failed", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"namespace": ns,
		"commit":    result.Commit.Hex(),
		"target":    result.Target.Hex(),
		"restored":  result.Restored,
		"removed":   result.Removed,
		// The revert is itself a commit, so it can be reverted in turn - which is the property
		// that makes this safe to offer at all.
		"revertedBy": principal.ID,
	})
}
