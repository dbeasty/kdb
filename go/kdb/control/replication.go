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

// GET /v1/ns/{ns}/home - the namespace's single-home assignment, if any.
func (s *Server) handleHome(w http.ResponseWriter, _ *http.Request, _ auth.Principal, ns string, rt *serverRuntime) {
	h, ok := rt.HomeOf()
	writeJSON(w, http.StatusOK, map[string]any{"namespace": ns, "singleHome": ok, "home": h, "thisNode": rt.NodeID.String()})
}

// PUT /v1/ns/{ns}/home - {"node": "<node id>", "addr": "<client address>"} makes that node the
// only one that accepts the namespace's writes; {"node": ""} returns it to multi-leader. Either
// raises the fence.
func (s *Server) handleAssignHome(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string, rt *serverRuntime) {
	var body struct {
		Node string `json:"node"`
		Addr string `json:"addr"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if rt.Meta == nil {
		writeError(w, http.StatusConflict, "no_metadata", "this process has no metadata namespace, which single-home ownership is recorded in")
		return
	}
	h, err := rt.Meta.AssignHome(ns, body.Node, body.Addr)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"namespace": ns, "home": h})
}

// GET /v1/ns/{ns}/resolution - the namespace's conflict resolution chain and its hash, which
// peers compare before they merge.
func (s *Server) handleResolution(w http.ResponseWriter, _ *http.Request, _ auth.Principal, ns string, rt *serverRuntime) {
	c := rt.ResolutionChainOf()
	rules := []peersync.ResolutionRule{}
	if c != nil {
		rules = c.Rules
	}
	writeJSON(w, http.StatusOK, map[string]any{"namespace": ns, "rules": rules, "hash": c.Hash(), "thisNode": rt.NodeID.String()})
}

// PUT /v1/ns/{ns}/resolution - {"rules": [{"kind": "source-priority", "nodes": ["<node id>", ...]},
// {"kind": "field-merge"}, {"kind": "queue"}]} replaces the namespace's chain; {"rules": []}
// removes it. Recorded as a replicated definition.
func (s *Server) handleSetResolution(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string, rt *serverRuntime) {
	var body peersync.ResolutionChain
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if err := body.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_chain", err.Error())
		return
	}
	if rt.Meta == nil {
		writeError(w, http.StatusConflict, "no_metadata", "this process has no metadata namespace, which resolution chains are recorded in")
		return
	}
	if err := rt.Meta.SetResolution(ns, body); err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	c := rt.ResolutionChainOf()
	writeJSON(w, http.StatusOK, map[string]any{"namespace": ns, "rules": body.Rules, "hash": c.Hash()})
}

// GET /v1/placement - every namespace with a single-home assignment, and where its home is. A
// namespace absent from the list is multi-leader: any node holding it accepts its writes.
func (s *Server) handlePlacement(w http.ResponseWriter, _ *http.Request, _ auth.Principal) {
	rt, ok := s.namespaces().Runtime(s.defaultNamespace())
	if !ok || rt.Meta == nil {
		writeJSON(w, http.StatusOK, map[string]any{"homes": map[string]any{}})
		return
	}
	homes, err := rt.Meta.Placement()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"homes": homes})
}
