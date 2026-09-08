package control

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// The data-plane reads: list the documents in a namespace, and run a SELECT over them.
//
// Both are reads and both are available at any revision, which is the point of putting them in the
// same file as each other rather than next to the history endpoints: a document browser that can
// only show you the present is a much less interesting thing in a database whose whole premise is
// that the past is still there.

// maxScan bounds how many documents one listing request may walk. Paging is a scan - the document
// tree has no ordinal addressing - so a deep page costs the walk it skips, and this is what keeps
// a single request bounded rather than proportional to the namespace.
const maxScan = 50_000

// errStopScan aborts ScanDocuments once the page is full. ScanDocuments has no early exit of its
// own; returning an error from the batch callback is how a caller stops it, so this sentinel is
// filtered back out rather than reported.
var errStopScan = errors.New("control: page complete")

type documentSummary struct {
	DocID string `json:"docId"`
	// Preview is the head of the document body, for a grid cell. The whole body is one request
	// away by id, and putting every body in a listing would make the listing cost the size of the
	// namespace it is summarizing.
	Preview   string `json:"preview"`
	SizeBytes int    `json:"sizeBytes"`
	Truncated bool   `json:"truncated,omitempty"`
}

const previewBytes = 200

// handleDocuments lists the documents in a namespace, newest revision first by document id.
//
// The cursor is the last document id of the previous page. Document ids are UUIDs and the tree
// walks them in a stable order, so "everything after this id" is a resumable position without the
// server holding any per-client state - which matters for a listing a browser will page through
// while other clients are writing.
func (s *Server) handleDocuments(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string, rt *serverRuntime) {
	limit, err := intParam(r, "limit", 50, 1, 500)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	treeHash, atHead, err := s.treeHashAt(rt, r.URL.Query().Get("at"))
	if err != nil {
		s.writeRevisionError(w, err)
		return
	}
	if rt.Runtime == nil {
		writeError(w, http.StatusInternalServerError, "no_runtime", "runtime has no embedded runtime")
		return
	}
	cursor := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("cursor")))

	page := make([]documentSummary, 0, limit)
	scanned := 0
	full := false
	scanErr := rt.Runtime.Storage.ScanDocuments(ns, treeHash, 256, func(batch []document.Document) error {
		// Sorting per batch, not globally: the scan yields documents in the tree's own order and
		// this only makes each batch's order explicit. A cursor still works because the skip below
		// compares ids rather than positions.
		sort.Slice(batch, func(i, j int) bool { return batch[i].ID.String() < batch[j].ID.String() })
		for _, doc := range batch {
			scanned++
			if scanned > maxScan {
				full = true
				return errStopScan
			}
			id := doc.ID.String()
			if cursor != "" && id <= cursor {
				continue
			}
			if len(page) == limit {
				full = true
				return errStopScan
			}
			page = append(page, summarizeDocument(id, doc.JSON))
		}
		return nil
	})
	if scanErr != nil && !errors.Is(scanErr, errStopScan) {
		writeError(w, http.StatusServiceUnavailable, "scan_failed",
			"could not read this namespace's documents: "+scanErr.Error())
		return
	}
	sort.Slice(page, func(i, j int) bool { return page[i].DocID < page[j].DocID })
	if len(page) > limit {
		page = page[:limit]
	}

	body := map[string]any{
		"namespace": ns,
		"documents": page,
		"atHead":    atHead,
		"hasMore":   full && len(page) == limit,
	}
	if len(page) > 0 {
		body["nextCursor"] = page[len(page)-1].DocID
	}
	if !atHead {
		body["readOnly"] = true
	}
	writeJSON(w, http.StatusOK, body)
}

func summarizeDocument(id, jsonBody string) documentSummary {
	out := documentSummary{DocID: id, SizeBytes: len(jsonBody)}
	if len(jsonBody) <= previewBytes {
		out.Preview = jsonBody
		return out
	}
	// Cut on a rune boundary so the preview is still valid UTF-8 in the response.
	cut := previewBytes
	for cut > 0 && !isUTF8Start(jsonBody[cut]) {
		cut--
	}
	out.Preview = jsonBody[:cut]
	out.Truncated = true
	return out
}

func isUTF8Start(b byte) bool { return b&0xC0 != 0x80 }

// treeHashAt resolves a revision to the document tree to read at, and reports whether that is head.
//
// The tree hash, not the commit hash: storage.Adapter's parameter is named atCommit but is matched
// against structures keyed by tree hash, so a commit hash there resolves nothing and reads as an
// empty namespace rather than an error.
func (s *Server) treeHashAt(rt *serverRuntime, spec string) (codec.Hash, bool, error) {
	commitHash, err := s.resolveRevisionFor(rt, spec)
	if err != nil {
		return codec.Hash{}, false, err
	}
	d, err := s.commitDAGFor(rt)
	if err != nil {
		return codec.Hash{}, false, err
	}
	commit, ok := d.GetCommit(commitHash)
	if !ok {
		return codec.Hash{}, false, &revisionMissingError{spec: spec}
	}
	head, headErr := d.Head()
	return commit.DocumentTreeHash, headErr == nil && head == commitHash, nil
}

type revisionMissingError struct{ spec string }

func (e *revisionMissingError) Error() string {
	return "no such commit in this namespace: " + e.spec
}

// sqlRequest is the body of a SQL console request.
type sqlRequest struct {
	SQL string `json:"sql"`
	// Params are positional bind values for `?` placeholders.
	Params []any `json:"params"`
	// At runs the query against a past revision. Reads only, always.
	At string `json:"at"`
	// Limit caps returned rows; the server caps it again regardless.
	Limit int `json:"limit"`
}

// handleSQL runs a statement.
//
// One endpoint, three permission levels, decided from the *parsed statement* rather than from the
// request:
//
//   - SELECT is a read. It needs only the read grant this route already checked, so the console
//     works on a control plane started read-only.
//   - CREATE TABLE / CREATE INDEX / DROP INDEX is schema change, applied immediately.
//   - Everything else is DML, planned and then committed as one transaction.
//
// The last two require both --control-write *and* a write grant for the namespace. Those are
// different questions and both are asked: the flag is what this deployment permits at all, the
// grant is what this principal may do, and neither substitutes for the other.
//
// Classifying rather than matching on text is deliberate, and unknown statement kinds count as
// writes - the same rule classifyStatement uses at the wire layer. A new mutating statement added
// to package sql must not become executable-by-anyone by default just because this switch has not
// learned its name yet.
func (s *Server) handleSQL(w http.ResponseWriter, r *http.Request, principal auth.Principal, ns string, rt *serverRuntime) {
	var req sqlRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 256<<10)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "could not read request body: "+err.Error())
		return
	}
	if strings.TrimSpace(req.SQL) == "" {
		writeError(w, http.StatusBadRequest, "bad_request", `"sql" is required`)
		return
	}
	stmt, err := sql.DefaultParser{}.Parse(req.SQL)
	if err != nil {
		writeError(w, http.StatusBadRequest, "parse_error", err.Error())
		return
	}
	params, err := bindParams(req.Params)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}

	kind := classify(stmt)
	if kind != stmtSelect {
		if req.At != "" {
			// The past is not writable, and a statement that says otherwise is a mistake worth
			// naming rather than quietly running against head.
			writeError(w, http.StatusBadRequest, "not_writable_at_revision",
				"a statement that changes data or schema cannot be run at a past revision; "+
					"history moves forward only. Drop \"at\" to run it against head.")
			return
		}
		if !s.opts.AllowWrites {
			writeError(w, http.StatusForbidden, "read_only",
				"this control plane is read-only, so it runs SELECT only; start the service with "+
					"--control-write to allow statements that change data or schema")
			return
		}
		if !s.authorizeWith(w, r, rt, principal, auth.SqlExecAction{Namespace: ns, ReadOnly: false}) {
			return
		}
	}

	switch kind {
	case stmtSelect:
		s.runSelect(w, req, ns, rt)
	case stmtDDL:
		s.runDDL(w, req, params, ns, rt)
	default:
		s.runDML(w, r, req, params, principal, ns, rt)
	}
}

type stmtKind int

const (
	stmtSelect stmtKind = iota
	stmtDDL
	stmtDML
)

// classify sorts a parsed statement the way the wire listener's classifyStatement does, including
// its safe default: anything unrecognised is DML.
func classify(stmt sql.Statement) stmtKind {
	switch stmt.(type) {
	case sql.StmtSelect:
		return stmtSelect
	case sql.StmtCreateTable, sql.StmtCreateIndex, sql.StmtDropIndex:
		return stmtDDL
	default:
		return stmtDML
	}
}

func (s *Server) runSelect(w http.ResponseWriter, req sqlRequest, ns string, rt *serverRuntime) {
	limit := req.Limit
	if limit <= 0 || limit > 2000 {
		limit = 500
	}
	commitHash, err := s.resolveRevisionFor(rt, req.At)
	if err != nil {
		s.writeRevisionError(w, err)
		return
	}
	params, err := bindParams(req.Params)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}

	stats := &sql.ExecStats{}
	ctx := sql.QueryContext{
		NamespaceID: ns,
		Schema:      rt.Schema(),
		AtCommit:    &commitHash,
		Parameters:  params,
		MaxRows:     limit,
		Stats:       stats,
	}
	if adm := rt.Admission(); adm != nil {
		ctx.RowBudget = int(adm.ScanRowBudget())
	}
	result, err := rt.SQLEngine().Execute(req.SQL, ctx)
	if err != nil {
		writeError(w, http.StatusBadRequest, "query_failed", err.Error())
		return
	}

	columns := make([]string, 0, len(result.Columns))
	for _, c := range result.Columns {
		columns = append(columns, c.Name)
	}
	rows := make([][]any, 0, len(result.Rows))
	for _, row := range result.Rows {
		rows = append(rows, cellsOf(row))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace":      ns,
		"kind":           "select",
		"columns":        columns,
		"rows":           rows,
		"rowCount":       len(rows),
		"truncated":      len(rows) >= limit,
		"resolvedCommit": commitHash.Hex(),
		"plan":           result.Plan,
		"rowsExamined":   stats.RowsExamined,
	})
}

// runDDL applies schema change immediately. DDL is not buffered into a transaction - the engine
// applies it through Execute, exactly as the wire layer does.
func (s *Server) runDDL(w http.ResponseWriter, req sqlRequest, params []sql.Parameter, ns string, rt *serverRuntime) {
	head, err := s.resolveRevisionFor(rt, "")
	if err != nil {
		s.writeRevisionError(w, err)
		return
	}
	ctx := sql.QueryContext{
		NamespaceID: ns,
		Schema:      rt.Schema(),
		AtCommit:    &head,
		Parameters:  params,
	}
	if _, err := rt.SQLEngine().Execute(req.SQL, ctx); err != nil {
		writeError(w, http.StatusBadRequest, "ddl_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace": ns,
		"kind":      "ddl",
		"applied":   true,
	})
}

// runDML plans the statement's operations and commits them as one transaction.
//
// ExecuteDML *plans*: it returns the writes and deletes the statement implies without applying
// them, which is what lets the wire layer buffer them into a session's transaction. There is no
// session here, so this is autocommit - one statement, one transaction, one commit - and it goes
// through KdbServerRuntime.Commit like every other write, which means the write gate, conflict
// detection against the base version, unique-key enforcement, index maintenance, and a per-op
// authorization check all still apply.
func (s *Server) runDML(w http.ResponseWriter, r *http.Request, req sqlRequest, params []sql.Parameter, principal auth.Principal, ns string, rt *serverRuntime) {
	head, err := s.resolveRevisionFor(rt, "")
	if err != nil {
		s.writeRevisionError(w, err)
		return
	}
	ctx := sql.QueryContext{
		NamespaceID: ns,
		Schema:      rt.Schema(),
		AtCommit:    &head,
		Parameters:  params,
	}
	if adm := rt.Admission(); adm != nil {
		// UPDATE and DELETE resolve their targets by scanning, so the same bound a SELECT gets
		// applies: an unbounded predicate is unbounded work either way.
		ctx.RowBudget = int(adm.ScanRowBudget())
	}
	planned, err := rt.SQLEngine().ExecuteDML(req.SQL, ctx)
	if err != nil {
		writeError(w, http.StatusBadRequest, "dml_failed", err.Error())
		return
	}
	if len(planned.Operations) == 0 {
		// Nothing matched. A commit here would be an empty commit in the history for a statement
		// that changed nothing, which is noise in the one place noise is expensive.
		writeJSON(w, http.StatusOK, map[string]any{
			"namespace": ns, "kind": "dml", "rowsAffected": 0, "committed": false,
			"note": "no rows matched, so nothing was committed",
		})
		return
	}

	builder := &transaction.Builder{NamespaceID: ns, BaseVersion: head, Schema: rt.Schema()}
	for _, op := range planned.Operations {
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

	// A document another client holds a lease on is not this statement's to write. The wire path
	// makes the same check before committing; skipping it here would let the console step over a
	// hold that a client explicitly took.
	if err := rt.DocumentLocks.AssertUnheldByOthers(ns, controlSessionID(principal), tx); err != nil {
		writeError(w, http.StatusConflict, "document_locked", err.Error())
		return
	}

	commit, err := rt.Commit(ns, tx, controlSessionID(principal), principal)
	if err != nil {
		// A conflict is the expected outcome of two writers racing, not a server fault, and it is
		// retryable by re-running the statement against the new head.
		status := http.StatusInternalServerError
		if strings.Contains(strings.ToLower(err.Error()), "conflict") {
			status = http.StatusConflict
		}
		writeError(w, status, "commit_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace":    ns,
		"kind":         "dml",
		"rowsAffected": planned.RowsAffected,
		"generatedIds": planned.GeneratedIDs,
		"committed":    true,
		"commit":       commit.Hash.Hex(),
	})
}

// controlSessionID names the writer for lock ownership and for the commit's attribution. It is
// per-principal rather than per-request so that two statements from the same operator do not look
// like two clients fighting over a document one of them already holds.
func controlSessionID(principal auth.Principal) string {
	id := principal.ID
	if id == "" {
		id = "anonymous"
	}
	return "control:" + id
}

// cellsOf converts one result row into JSON-friendly values, keeping a JSON cell as raw JSON so a
// `_doc` column arrives as an object rather than a string containing an object.
func cellsOf(row sql.QueryRow) []any {
	out := make([]any, 0, len(row.Values))
	for _, cell := range row.Values {
		switch v := cell.(type) {
		case sql.CellNull:
			out = append(out, nil)
		case sql.CellString:
			out = append(out, v.Value)
		case sql.CellLong:
			out = append(out, v.Value)
		case sql.CellDouble:
			out = append(out, v.Value)
		case sql.CellJSON:
			out = append(out, rawJSON(v.JSON))
		default:
			out = append(out, nil)
		}
	}
	return out
}

// bindParams turns JSON values into the engine's parameter types.
//
// JSON has one number type and the engine has two, so an integral float becomes ParamInt: binding
// 42 as a double would not match an integer column, and a console user typing 42 means 42.
func bindParams(values []any) ([]sql.Parameter, error) {
	out := make([]sql.Parameter, 0, len(values))
	for i, v := range values {
		switch t := v.(type) {
		case nil:
			out = append(out, sql.ParamNull{})
		case string:
			out = append(out, sql.ParamString{Value: t})
		case bool:
			out = append(out, sql.ParamBool{Value: t})
		case float64:
			if t == math.Trunc(t) && math.Abs(t) < 1<<53 {
				out = append(out, sql.ParamInt{Value: int64(t)})
				continue
			}
			out = append(out, sql.ParamDouble{Value: t})
		default:
			return nil, fmt.Errorf("parameter %d is a %T, which cannot be bound: use a string, "+
				"number, boolean or null", i+1, v)
		}
	}
	return out, nil
}
