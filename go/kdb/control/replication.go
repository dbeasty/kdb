package control

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/replication"
	"github.com/limidus/kdb/go/kdb/server"
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
// GET /v1/ns/{ns}/conflicts - the namespace's queued conflicts. ?authority=true keeps only those
// handed to a resolver authority, and ?undelivered=true only those not yet acknowledged - together,
// what a polling authority has still to see (it acknowledges each with POST .../{id}/ack).
// ?doc=<id> keeps those involving one document: a client resolving its own conflicts reads every
// candidate version of the document it holds, then settles with POST .../{id}/resolve.
func (s *Server) handleConflicts(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string, rt *serverRuntime) {
	q := r.URL.Query()
	onlyAuthority, onlyUndelivered, doc := q.Get("authority") == "true", q.Get("undelivered") == "true", q.Get("doc")
	out := []peersync.ConflictEntry{}
	for _, e := range rt.Conflicts.List() {
		if (onlyAuthority && !e.Authority) || (onlyUndelivered && e.Delivered) || (doc != "" && !involves(e, doc)) {
			continue
		}
		out = append(out, e)
	}
	writeJSON(w, http.StatusOK, map[string]any{"namespace": ns, "conflicts": out})
}

func involves(e peersync.ConflictEntry, doc string) bool {
	for _, it := range e.Items {
		if it.DocumentID == doc {
			return true
		}
	}
	return false
}

// resolveError answers a failed resolution: 404 for an unknown entry, 403 for a principal
// without the right, 409 for anything the caller can fix by deciding again.
func resolveError(w http.ResponseWriter, err error) {
	var authz *server.AuthorizationError
	var stale *server.ErrResolutionStale
	switch {
	case errors.Is(err, peersync.ErrConflictNotFound):
		writeError(w, http.StatusNotFound, "unknown_conflict", err.Error())
	case errors.As(err, &authz):
		writeError(w, http.StatusForbidden, "forbidden", err.Error())
	case errors.As(err, &stale):
		writeError(w, http.StatusConflict, "stale", err.Error())
	default:
		writeError(w, http.StatusConflict, "not_resolved", err.Error())
	}
}

// POST /v1/ns/{ns}/conflicts/{id}/ack - a polling resolver authority has received the entry as
// it is now; it is not listed as undelivered again unless its report changes.
func (s *Server) handleAckConflict(w http.ResponseWriter, r *http.Request, principal auth.Principal, _ string, rt *serverRuntime) {
	if err := rt.AckConflict(r.PathValue("id"), principal); err != nil {
		resolveError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /v1/ns/{ns}/conflicts/resolve-all - {"take": "local"|"remote", "filter": {"kind", "peer",
// "origin"}, "dryRun": true} settles every matching conflict by taking one side for all its
// documents, or with dryRun lists what it would settle.
func (s *Server) handleResolveAll(w http.ResponseWriter, r *http.Request, principal auth.Principal, ns string, rt *serverRuntime) {
	var body struct {
		Take   string                `json:"take"`
		Filter server.ConflictFilter `json:"filter"`
		DryRun bool                  `json:"dryRun"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if body.Take != "local" && body.Take != "remote" {
		writeError(w, http.StatusBadRequest, "bad_request", "take must be \"local\" or \"remote\"")
		return
	}
	res, err := rt.ResolveAll(body.Filter, body.Take, body.DryRun, principal)
	if err != nil {
		resolveError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"namespace": ns, "dryRun": body.DryRun, "results": res})
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
	if err != nil {
		resolveError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"commit": commit.Hash.Hex()})
}

// DELETE /v1/ns/{ns}/conflicts/{id} - dismiss a conflict without acting on it.
func (s *Server) handleDismissConflict(w http.ResponseWriter, r *http.Request, principal auth.Principal, _ string, rt *serverRuntime) {
	err := rt.DismissConflict(r.PathValue("id"), principal)
	var authz *server.AuthorizationError
	switch {
	case errors.Is(err, peersync.ErrConflictNotFound), errors.As(err, &authz):
		resolveError(w, err)
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
// {"kind": "field-merge"}, {"kind": "queue"}], "allowUnrelated": true} replaces the namespace's
// chain; {"rules": []} removes it. allowUnrelated lets the namespace merge a peer's history that
// shares no commit with its own, grafting the peer's shallow root (peersync.Graft). Recorded as a
// replicated definition.
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

// RepairSource is what the scrub and compare endpoints need from the replicator: bodies from the
// configured peers, and a Merkle comparison with one of them. The service's replicator implements
// it; a process without peers answers these endpoints as having none.
type RepairSource interface {
	FetchBodies(ns string, wanted map[codec.UUID]codec.Hash, treeHex string) (map[codec.UUID]string, error)
	Compare(name, ns string, local document.DocumentTree) (string, []document.TreeDifference, error)
}

func (s *Server) repairSource() RepairSource {
	if s.opts.Replication == nil {
		return nil
	}
	rs, _ := s.opts.Replication.(RepairSource)
	return rs
}

// POST /v1/ns/{ns}/scrub - re-read and verify every document at the head, repairing damaged bodies
// from the configured peers ({"repair": false} only reports). Answers the ScrubReport; 200 when
// nothing is left damaged, 409 when something is.
func (s *Server) handleScrub(w http.ResponseWriter, r *http.Request, _ auth.Principal, _ string, rt *serverRuntime) {
	var body struct {
		Repair *bool `json:"repair"`
	}
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, "bad_request", err.Error())
			return
		}
	}
	var fetch server.BodyFetcher
	if rs := s.repairSource(); rs != nil && (body.Repair == nil || *body.Repair) {
		fetch = rs.FetchBodies
	}
	rep, err := rt.Scrub(fetch)
	switch {
	case server.IsScrubUnrepaired(err):
		writeJSON(w, http.StatusConflict, rep)
	case err != nil:
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
	default:
		writeJSON(w, http.StatusOK, rep)
	}
}

// GET /v1/ns/{ns}/peers/{peer}/diff - the documents this node holds differently from the named
// replication peer's head, found by comparing subtree hashes rather than documents.
func (s *Server) handlePeerDiff(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string, rt *serverRuntime) {
	rs := s.repairSource()
	if rs == nil {
		writeError(w, http.StatusConflict, "no_peers", "no replication peers are configured")
		return
	}
	peer := r.PathValue("peer")
	local, err := rt.HeadTree()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	peerTree, diff, err := rs.Compare(peer, ns, local)
	if err != nil {
		writeError(w, http.StatusBadGateway, "peer_unavailable", err.Error())
		return
	}
	type row struct {
		DocumentID string `json:"documentId"`
		Local      string `json:"local,omitempty"`
		Remote     string `json:"remote,omitempty"`
	}
	rows := make([]row, 0, len(diff))
	for _, d := range diff {
		r := row{DocumentID: d.DocID.String()}
		if d.Local != (codec.Hash{}) {
			r.Local = d.Local.Hex()
		}
		if d.Remote != (codec.Hash{}) {
			r.Remote = d.Remote.Hex()
		}
		rows = append(rows, r)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace": ns, "peer": peer, "localTree": local.TreeHash.Hex(), "peerTree": peerTree,
		"equal": len(rows) == 0, "differences": rows,
	})
}

// DeepenSource opens a repair session to a named replication peer - what deepen needs.
type DeepenSource interface {
	OpenRepairSessionTo(name, ns string) (*peersync.RepairSession, error)
}

// POST /v1/ns/{ns}/deepen - {"peer": "<name>"} fetches the history below the namespace's shallow
// roots (a namespace bootstrapped by snapshot) from that replication peer. Answers what each root
// got and the shallow roots left.
func (s *Server) handleDeepen(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string, rt *serverRuntime) {
	var body struct {
		Peer string `json:"peer"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Peer == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "body must be {\"peer\": \"<name>\"}")
		return
	}
	src, _ := s.opts.Replication.(DeepenSource)
	if s.opts.Replication == nil || src == nil {
		writeError(w, http.StatusConflict, "no_peers", "no replication peers are configured")
		return
	}
	results, err := rt.Deepen(func() (*peersync.RepairSession, error) { return src.OpenRepairSessionTo(body.Peer, ns) })
	type row struct {
		Root        string   `json:"root"`
		Fetched     int      `json:"fetched"`
		Horizon     []string `json:"horizon,omitempty"`
		Unshallowed bool     `json:"unshallowed"`
	}
	rows := []row{}
	for _, res := range results {
		rw := row{Root: res.Root.Hex(), Fetched: res.Fetched, Unshallowed: res.Unshallowed}
		for _, h := range res.Horizon {
			rw.Horizon = append(rw.Horizon, h.Hex())
		}
		rows = append(rows, rw)
	}
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"namespace": ns, "results": rows, "error": err.Error(), "shallowRoots": rt.ShallowRoots()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"namespace": ns, "results": rows, "shallowRoots": rt.ShallowRoots()})
}
