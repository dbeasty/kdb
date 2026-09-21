package control

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/replication"
)

// ReplicationSource is the replicator a control plane reports and drives. nil when the process
// has no peers configured; the peer endpoints then say so.
type ReplicationSource interface {
	Status() []replication.PeerStatus
	SyncNow(name string) (peersync.V2Result, error)
}

// GET /v1/peers - every configured peer and how far replication with it has got.
func (s *Server) handlePeers(w http.ResponseWriter, _ *http.Request, _ auth.Principal) {
	if s.opts.Replication == nil {
		writeJSON(w, http.StatusOK, map[string]any{"peers": []any{}, "configured": false})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"peers": s.opts.Replication.Status(), "configured": true})
}

// POST /v1/peers/{name}/sync - sync with one peer now and report what it did. A write: a sync
// moves heads.
func (s *Server) handlePeerSync(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	if !s.opts.AllowWrites {
		writeError(w, http.StatusForbidden, "read_only",
			"this control plane is read-only; start the service with --control-write to enable mutating endpoints")
		return
	}
	if s.opts.Replication == nil {
		writeError(w, http.StatusNotFound, "no_replication", "this process has no replication peers configured")
		return
	}
	res, err := s.opts.Replication.SyncNow(r.PathValue("name"))
	if err != nil {
		writeErrorWithDetail(w, http.StatusBadGateway, "sync_failed", err.Error(), map[string]any{"result": res})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// GET /v1/ns/{ns}/conflicts - the namespace's open replication conflicts.
func (s *Server) handleConflicts(w http.ResponseWriter, _ *http.Request, _ auth.Principal, ns string, rt *serverRuntime) {
	writeJSON(w, http.StatusOK, map[string]any{"namespace": ns, "conflicts": rt.Conflicts.List()})
}

// POST /v1/ns/{ns}/conflicts/{id}/resolve - {"choices": {"<docId>": {"take":"local"|"remote"} |
// {"body": "<json>"}}}. Merges the peer's side with those choices; the merge replicates.
func (s *Server) handleResolveConflict(w http.ResponseWriter, r *http.Request, principal auth.Principal, _ string, rt *serverRuntime) {
	var body struct {
		Choices map[string]peersync.Choice `json:"choices"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "body must be {\"choices\": {...}}: "+err.Error())
		return
	}
	choices := make(map[codec.UUID]peersync.Choice, len(body.Choices))
	for id, c := range body.Choices {
		docID, err := codec.ParseUUID(id)
		if err != nil {
			writeError(w, http.StatusBadRequest, "bad_request", "choice key "+id+" is not a document id")
			return
		}
		choices[docID] = c
	}
	commit, err := rt.ResolveConflict(r.PathValue("id"), choices, principal)
	switch {
	case errors.Is(err, peersync.ErrConflictNotFound):
		writeError(w, http.StatusNotFound, "unknown_conflict", err.Error())
	case err != nil:
		writeError(w, http.StatusConflict, "not_resolved", err.Error())
	default:
		writeJSON(w, http.StatusOK, map[string]any{"commit": commit.Hash.Hex()})
	}
}

// DELETE /v1/ns/{ns}/conflicts/{id} - dismiss a conflict without acting on it.
func (s *Server) handleDismissConflict(w http.ResponseWriter, r *http.Request, principal auth.Principal, _ string, rt *serverRuntime) {
	err := rt.DismissConflict(r.PathValue("id"), principal)
	switch {
	case errors.Is(err, peersync.ErrConflictNotFound):
		writeError(w, http.StatusNotFound, "unknown_conflict", err.Error())
	case err != nil:
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}
