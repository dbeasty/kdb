package control

import (
	"net/http"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/config"
)

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	type body struct {
		Version    string `json:"version"`
		Namespace  string `json:"namespace"`
		UptimeSec  int64  `json:"uptimeSeconds"`
		Draining   bool   `json:"draining"`
		ReadOnly   bool   `json:"readOnly"`
		MemoryZone string `json:"memoryZone"`
		WritesOpen bool   `json:"controlWritesEnabled"`
	}
	rt := s.opts.Runtime
	out := body{
		Version:    s.opts.Version,
		Namespace:  s.opts.Namespace,
		UptimeSec:  int64(s.opts.Now().Sub(s.started) / time.Second),
		Draining:   rt.IsDraining(),
		MemoryZone: rt.MemoryZone().String(),
		WritesOpen: s.opts.AllowWrites,
	}
	if rt.Runtime != nil {
		out.ReadOnly = rt.Runtime.ReadOnly
	}
	writeJSON(w, http.StatusOK, out)
}

// handleNamespaces lists what this control plane can see.
//
// One entry today: KdbServerRuntime is per-namespace by construction, and kdb-service opens a
// single namespace. embed.Host already supports N namespaces over one data root (Layer 17
// Component 64); wiring the service to it is the milestone that turns this into a real list, and
// the response shape is already the list it will return.
func (s *Server) handleNamespaces(w http.ResponseWriter, r *http.Request, _ auth.Principal, _ string) {
	type entry struct {
		ID       string `json:"id"`
		Catalog  string `json:"catalog"`
		Head     string `json:"head"`
		ReadOnly bool   `json:"readOnly"`
	}
	e := entry{ID: s.opts.Namespace}
	if rt := s.opts.Runtime.Runtime; rt != nil {
		e.Catalog = rt.Catalog
		e.ReadOnly = rt.ReadOnly
		if head, err := rt.DAG.Head(); err == nil {
			e.Head = head.Hex()
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"namespaces": []entry{e}})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string) {
	d, err := s.commitDAG()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "no_dag", err.Error())
		return
	}
	head, headCommit, ok, err := d.HeadCommit()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "head_unreadable", err.Error())
		return
	}
	type body struct {
		Namespace   string         `json:"namespace"`
		Head        string         `json:"head"`
		HeadCommit  *commitSummary `json:"headCommit,omitempty"`
		Branches    int            `json:"branches"`
		Draining    bool           `json:"draining"`
		ReadOnly    bool           `json:"readOnly"`
		ExpiryState string         `json:"documentExpiry"`
	}
	out := body{
		Namespace:   ns,
		Head:        head.Hex(),
		Branches:    len(d.ListBranches()),
		Draining:    s.opts.Runtime.IsDraining(),
		ExpiryState: s.opts.Runtime.ExpirySummary(),
	}
	if rt := s.opts.Runtime.Runtime; rt != nil {
		out.ReadOnly = rt.ReadOnly
	}
	if ok {
		sum := summarizeCommit(headCommit)
		out.HeadCommit = &sum
	}
	writeJSON(w, http.StatusOK, out)
}

// handleSchema reports the schema as what it is: a flat, typed lens over document fields.
//
// KdbSchema has no notion of tables - Fields is a single list (schema/schema.go) - so this does
// not invent a table grouping the engine does not have.
func (s *Server) handleSchema(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string) {
	sch := s.opts.Runtime.Schema()
	type fieldView struct {
		Name     string `json:"name"`
		Type     string `json:"type"`
		Required bool   `json:"required"`
		Indexed  bool   `json:"indexed"`
		Unique   bool   `json:"unique"`
	}
	fields := make([]fieldView, 0, len(sch.Fields))
	for _, f := range sch.Fields {
		typeName := "unknown"
		if f.Type != nil {
			typeName = f.Type.SQLTypeName()
		}
		fields = append(fields, fieldView{
			Name: f.Name, Type: typeName, Required: f.Required, Indexed: f.Indexed, Unique: f.Unique,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace":   ns,
		"schemaHash":  sch.SchemaHash.Hex(),
		"version":     sch.Version,
		"description": sch.Description,
		"fields":      fields,
	})
}

func (s *Server) handleLog(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string) {
	limit, err := intParam(r, "limit", 50, 1, 500)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	skip, err := intParam(r, "skip", 0, 0, maxSkip)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	from, err := s.resolveRevision(r.URL.Query().Get("from"))
	if err != nil {
		s.writeRevisionError(w, err)
		return
	}
	commits, more, err := s.log(from, limit, skip)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "walk_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace": ns,
		"from":      from.Hex(),
		"commits":   commits,
		"hasMore":   more,
		// The next page is expressed as a skip rather than a cursor because the traversal has no
		// stable ordinal to resume from; see log's own comment.
		"nextSkip": skip + len(commits),
	})
}

func (s *Server) handleCommit(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string) {
	hash, err := s.resolveRevision(r.PathValue("hash"))
	if err != nil {
		s.writeRevisionError(w, err)
		return
	}
	d, err := s.commitDAG()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "no_dag", err.Error())
		return
	}
	commit, ok := d.GetCommit(hash)
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "no such commit in this namespace")
		return
	}
	sum := summarizeCommit(commit)
	sum.Refs = s.refsByCommit()[sum.Hash]

	// Operations are fetched for the one commit a caller actually opened - and may legitimately
	// be absent, because the DAG evicts them under its retention budget. Saying so is better than
	// rendering an empty list, which reads as "this commit wrote nothing".
	ops, opsErr := d.CommitOperations(hash)
	views := make([]opView, 0, len(ops))
	for _, op := range ops {
		views = append(views, describeOp(op))
	}
	body := map[string]any{
		"namespace":  ns,
		"commit":     sum,
		"operations": views,
	}
	if opsErr != nil {
		body["operationsAvailable"] = false
		body["operationsNote"] = "this commit's operations are not resident and could not be " +
			"loaded (they are evictable under the DAG's retention budget); the commit itself and " +
			"its document tree are unaffected"
	} else {
		body["operationsAvailable"] = true
	}
	// A commit is content-addressed and immutable, so it is exactly the thing a validator should
	// short-circuit. The operations list can change availability, which is why it is weak.
	w.Header().Set("ETag", `W/"`+sum.Hash+`"`)
	if match := r.Header.Get("If-None-Match"); match == `W/"`+sum.Hash+`"` {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	writeJSON(w, http.StatusOK, body)
}

func (s *Server) handleCommitDiff(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string) {
	toSpec := r.PathValue("hash")
	// Default to the first parent, which is what "what did this commit change" means. head~1 is
	// not that - on a merge it would follow the wrong side - so the parent is read from the commit
	// itself. A root commit has none, and a diff against nothing is a question with no answer, so
	// it is refused rather than silently reported as "no changes".
	fromSpec := r.URL.Query().Get("against")
	if fromSpec == "" {
		to, err := s.resolveRevision(toSpec)
		if err != nil {
			s.writeRevisionError(w, err)
			return
		}
		d, err := s.commitDAG()
		if err != nil {
			writeError(w, http.StatusInternalServerError, "no_dag", err.Error())
			return
		}
		commit, ok := d.GetCommit(to)
		if !ok {
			writeError(w, http.StatusNotFound, "not_found", "no such commit in this namespace")
			return
		}
		if len(commit.ParentHashes) == 0 {
			writeError(w, http.StatusBadRequest, "no_parent",
				"this is a root commit and has no parent to diff against; pass ?against=<revision>")
			return
		}
		fromSpec = commit.ParentHashes[0].Hex()
		toSpec = to.Hex()
	}

	from, to, entries, err := s.diffRevisions(fromSpec, toSpec)
	if err != nil {
		if isUnknownRevision(err) {
			s.writeRevisionError(w, err)
			return
		}
		// A tree that cannot be resolved is the other real failure here: under history=none the
		// window may simply no longer reach that far back, which is a retention answer, not a bug.
		writeError(w, http.StatusServiceUnavailable, "diff_unavailable",
			"this diff could not be computed: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace": ns,
		"from":      from.Hex(),
		"to":        to.Hex(),
		"entries":   entries,
		"counts":    countChanges(entries),
	})
}

func (s *Server) handleRefs(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string) {
	d, err := s.commitDAG()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "no_dag", err.Error())
		return
	}
	type ref struct {
		Name      string `json:"name"`
		Head      string `json:"head"`
		ShortHead string `json:"shortHead"`
		UpdatedAt string `json:"updatedAt,omitempty"`
		Message   string `json:"message,omitempty"`
	}
	branches := make([]ref, 0)
	for _, b := range d.ListBranches() {
		branches = append(branches, ref{
			Name: b.Name, Head: b.HeadHash.Hex(), ShortHead: shortHash(b.HeadHash.Hex()),
			UpdatedAt: millisToRFC3339(b.UpdatedAt.EpochMillis),
		})
	}
	tags := make([]ref, 0)
	for _, t := range d.ListTags() {
		tags = append(tags, ref{
			Name: t.Name, Head: t.CommitHash.Hex(), ShortHead: shortHash(t.CommitHash.Hex()),
			UpdatedAt: millisToRFC3339(t.CreatedAt.EpochMillis), Message: t.Message,
		})
	}
	head, _ := d.Head()
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace": ns,
		"head":      head.Hex(),
		"branches":  branches,
		"tags":      tags,
	})
}

// handleDocument reads one document, at head or at any past revision.
//
// Reading at a revision resolves it to a commit and reads that commit's document tree. The tree
// hash is what the storage adapter wants - its parameter is named atCommit but is a tree hash, and
// passing a commit hash there resolves nothing rather than erroring, which would look like a
// missing document.
func (s *Server) handleDocument(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string) {
	docID, err := codec.UUIDFromString(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "document id must be a UUID")
		return
	}
	at := r.URL.Query().Get("at")
	if at == "" {
		body, commitHex, found, err := s.opts.Runtime.GetDocument(ns, docID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "read_failed", err.Error())
			return
		}
		if !found {
			writeError(w, http.StatusNotFound, "not_found", "no such document at head")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"namespace": ns, "docId": docID.String(), "commit": commitHex,
			"body": rawJSON(body), "atHead": true,
		})
		return
	}

	commitHash, err := s.resolveRevision(at)
	if err != nil {
		s.writeRevisionError(w, err)
		return
	}
	d, err := s.commitDAG()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "no_dag", err.Error())
		return
	}
	commit, ok := d.GetCommit(commitHash)
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "no such commit in this namespace")
		return
	}
	doc, err := s.opts.Runtime.Runtime.Storage.GetDocument(ns, docID, commit.DocumentTreeHash)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "read_failed",
			"could not read at that revision: "+err.Error())
		return
	}
	if doc == nil {
		writeError(w, http.StatusNotFound, "not_found",
			"that document does not exist at "+shortHash(commitHash.Hex()))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace": ns, "docId": docID.String(), "commit": commitHash.Hex(),
		"body": rawJSON(doc.JSON), "atHead": false,
		// Stated rather than implied: a historical read is a read, and nothing written through
		// this control plane can land anywhere but head.
		"readOnly": true,
	})
}

func (s *Server) handleSettings(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	descriptors := s.settingsSnapshot()
	if descriptors == nil {
		descriptors = []config.SettingDescriptor{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"settings": descriptors,
		// Stated rather than implied: everything here is read-only in this build, and an operator
		// should not have to discover that by trying.
		"mutable": false,
		"note": "settings are reported with provenance but cannot be changed through this build; " +
			"see docs/kdb-control-ui-plan.md §7.3 for the knobs that are live-mutable in the engine",
	})
}

func (s *Server) handleOpsRuntime(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	rt := s.opts.Runtime
	body := map[string]any{
		"draining":   rt.IsDraining(),
		"memoryZone": rt.MemoryZone().String(),
		"expiry":     rt.ExpirySummary(),
	}
	if adm := rt.Admission(); adm != nil {
		body["admission"] = map[string]any{
			"enabled":            true,
			"rescueReserveBytes": adm.RescueReserveBytes(),
		}
	} else {
		body["admission"] = map[string]any{
			"enabled": false,
			"note":    "no memory budget configured; every operation is admitted",
		}
	}
	writeJSON(w, http.StatusOK, body)
}

// writeRevisionError answers a revision that names nothing with 404 rather than 500. The engine's
// RevisionNotFoundError carries a Reason distinguishing "never existed here" from "reclaimed by
// the retention window", and only the second is fixed by configuring a longer window - so the
// message is passed through rather than flattened.
func (s *Server) writeRevisionError(w http.ResponseWriter, err error) {
	if isUnknownRevision(err) {
		writeError(w, http.StatusNotFound, "unknown_revision", err.Error())
		return
	}
	writeError(w, http.StatusInternalServerError, "revision_failed", err.Error())
}

// rawJSON lets an already-encoded document body through json.Marshal untouched, so a document
// round-trips byte-exact - which is the engine's own guarantee about document bodies and would be
// broken by decoding and re-encoding it here.
type rawJSON string

func (r rawJSON) MarshalJSON() ([]byte, error) {
	if r == "" {
		return []byte("null"), nil
	}
	return []byte(r), nil
}
