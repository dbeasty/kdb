package control

import (
	"errors"
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
		sum := summarize(headCommit)
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
	skip, err := intParam(r, "skip", 0, 0, maxWalk)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	from, err := s.resolveRef(r.URL.Query().Get("from"))
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
	hash, err := s.resolveRef(r.PathValue("hash"))
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
	sum := summarize(commit)
	sum.Refs = branchesByCommit(d)[sum.Hash]

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
	to, err := s.resolveRef(r.PathValue("hash"))
	if err != nil {
		s.writeRevisionError(w, err)
		return
	}
	d, err := s.commitDAG()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "no_dag", err.Error())
		return
	}
	// Default to the first parent, which is what "what did this commit change" means. A root
	// commit has none, and diffing it against itself would report nothing changed - so it is
	// compared against the empty tree by diffing it with itself and reporting every document as
	// added is wrong too. Instead: no parent means the caller must name a base explicitly.
	against := r.URL.Query().Get("against")
	var from codec.Hash
	if against != "" {
		from, err = s.resolveRef(against)
		if err != nil {
			s.writeRevisionError(w, err)
			return
		}
	} else {
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
		from = commit.ParentHashes[0]
	}
	// Prefer the operation-based path when this is a commit against its own first parent: it is
	// the common case, and it costs one read per operation rather than materializing two whole
	// trees, which on any namespace larger than the commit is the cheaper answer by a wide margin.
	// dag.Diff is the fallback and is exact; the two are asserted to agree in the tests.
	var entries []diffEntry
	var basis string
	if commit, ok := d.GetCommit(to); ok && len(commit.ParentHashes) > 0 && commit.ParentHashes[0] == from {
		entries, err = s.diffAgainstParent(commit, from, ns)
		basis = "operations"
	} else {
		err = errNotParentDiff
	}
	if err != nil {
		// Either an arbitrary pair of commits, or a commit whose operations are no longer
		// resident. Both fall back to comparing document trees, which is exact when the trees can
		// be resolved and is the only option for two unrelated commits.
		entries, err = s.diffCommits(from, to)
		basis = "trees"
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "diff_unavailable",
			"this diff could not be computed: "+err.Error()+". A commit's operations are evictable "+
				"under the DAG's retention budget, and historical document trees are rebuilt on "+
				"demand rather than kept, so a diff far enough back may be temporarily out of reach.")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace": ns,
		"from":      from.Hex(),
		"to":        to.Hex(),
		"entries":   entries,
		"counts":    countChanges(entries),
		// Which path produced this, because the two have different failure modes and an operator
		// debugging a surprising diff should not have to guess which one ran.
		"basis": basis,
	})
}

// errNotParentDiff routes an arbitrary commit pair to the tree-based comparison.
var errNotParentDiff = errors.New("not a first-parent diff")

func countChanges(entries []diffEntry) map[string]int {
	counts := map[string]int{"added": 0, "modified": 0, "removed": 0}
	for _, e := range entries {
		counts[e.Change]++
	}
	return counts
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
	}
	branches := make([]ref, 0)
	for _, b := range d.ListBranches() {
		branches = append(branches, ref{
			Name: b.Name, Head: b.HeadHash.Hex(), ShortHead: shortHash(b.HeadHash.Hex()),
			UpdatedAt: millisToRFC3339(b.UpdatedAt.EpochMillis),
		})
	}
	head, _ := d.Head()
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace": ns,
		"head":      head.Hex(),
		"branches":  branches,
		// Tags are a stub in the DAG today - a map with no create/list/delete methods - so this is
		// honestly empty rather than absent, and stays that way until they are finished.
		"tags": []ref{},
	})
}

func (s *Server) handleDocument(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string) {
	if at := r.URL.Query().Get("at"); at != "" {
		// Reading at an arbitrary commit needs the hybrid engine wired into the server read path,
		// which is its own milestone. Refusing is better than silently answering from head, which
		// would show an operator the wrong data under a URL that says otherwise.
		writeError(w, http.StatusNotImplemented, "not_implemented",
			"reading at a past commit is not wired into the server read path yet; omit ?at= to read head")
		return
	}
	docID, err := codec.UUIDFromString(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "document id must be a UUID")
		return
	}
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
		"namespace": ns,
		"docId":     docID.String(),
		"commit":    commitHex,
		"body":      rawJSON(body),
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

func (s *Server) writeRevisionError(w http.ResponseWriter, err error) {
	var re *revisionError
	if errors.As(err, &re) {
		writeError(w, http.StatusNotFound, "unknown_revision", re.Error())
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
