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

// handleSQL runs a read-only query.
//
// Only SELECT. This is registered as a *read* endpoint on purpose - it needs no write permission
// and works on a control plane started read-only - so anything that could mutate has to be refused
// here rather than left to the engine, and refused by classifying the parsed statement rather than
// by matching on the text. `classifyStatement` in the wire listener treats unknown statement kinds
// as DML for the same reason: a new mutating statement must not become readable by default.
func (s *Server) handleSQL(w http.ResponseWriter, r *http.Request, _ auth.Principal, ns string, rt *serverRuntime) {
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
	if _, isSelect := stmt.(sql.StmtSelect); !isSelect {
		writeError(w, http.StatusForbidden, "read_only_console",
			"this endpoint runs SELECT only. Statements that change data or schema are not served "+
				"here even when the control plane allows writes, because the console is a read "+
				"surface: use the document endpoints for writes.")
		return
	}

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
		// The same bound the wire path applies: rows *examined*, which is the only thing that
		// bounds a selective query over a large namespace.
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
		"columns":        columns,
		"rows":           rows,
		"rowCount":       len(rows),
		"truncated":      len(rows) >= limit,
		"resolvedCommit": commitHash.Hex(),
		// Which access path the executor chose. Surfacing it is most of what makes a console
		// useful for anything beyond looking: it is how you find out a query is a full scan.
		"plan":         result.Plan,
		"rowsExamined": stats.RowsExamined,
	})
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
