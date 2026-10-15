package sql

import (
	"encoding/base64"
	"fmt"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	kdbjson "github.com/limidus/kdb/go/kdb/json"
	"github.com/limidus/kdb/go/kdb/schema"
)

// DMLExecutor lowers INSERT, UPDATE, and DELETE to document operations (kdb-spec-layer16 §5).
// The ops follow the same commit path as any transaction; nothing here writes storage.
type DMLExecutor struct {
	Executor *Executor
}

// ExecuteInsert returns Write ops for each VALUES row.
func (d *DMLExecutor) ExecuteInsert(insert InsertStatement, ctx QueryContext) ([]document.Op, error) {
	env := newEvalEnv(ctx.Schema, ctx.Parameters, insert.Table)
	env.attach = ctx.attach
	var writer *attachmentWriter
	if len(insert.Attachments) > 0 {
		writer = newAttachmentWriter(d.Executor, ctx.NamespaceID)
	}
	var ops []document.Op
	for _, values := range insert.Rows {
		if len(insert.Columns) != len(values) {
			return nil, NewPlanningError("column count does not match value count", "")
		}
		id, err := codec.RandomUUID()
		if err != nil {
			return nil, err
		}
		jsonText := "{}"
		for i, col := range insert.Columns {
			jsonText, err = assignPath(jsonText, stripTableAlias(col, insert.Table), env.cell(values[i], newEvalDoc(emptyDoc)))
			if err != nil {
				return nil, err
			}
		}
		if err := validateJSON(id, jsonText, ctx.Schema); err != nil {
			return nil, err
		}
		ops = append(ops, document.WriteOp{DocID: id, Patch: jsonText})
		if writer != nil {
			attachOps, err := writer.insertAttachmentOps(env, insert.Attachments, id)
			if err != nil {
				return nil, err
			}
			ops = append(ops, attachOps...)
		}
	}
	return ops, nil
}

// ExecuteUpdate returns one Write op per matching document with the SET assignments applied.
// Values are evaluated against the pre-update document; `_doc` replaces the whole body.
func (d *DMLExecutor) ExecuteUpdate(upd UpdateStatement, ctx QueryContext) ([]document.Op, error) {
	res, err := d.ExecuteUpdateAmong(upd, ctx, nil)
	return res.Operations, err
}

// ExecuteUpdateAmong is ExecuteUpdate over the documents in among only (nil: every document at
// ctx.AtCommit), with the claim clauses applied: WHERE filters, ORDER BY orders, LIMIT keeps the
// first n, and RETURNING projects each updated document as it is after the update into the
// result's Columns and Rows (in ORDER BY order).
//
// among is how the server keeps an immediate UPDATE's work under its write gate small: it names
// the candidates found before the gate (UpdateCandidates) plus whatever changed since, and this
// re-checks every one of them at the live head.
func (d *DMLExecutor) ExecuteUpdateAmong(upd UpdateStatement, ctx QueryContext, among []codec.UUID) (DMLResult, error) {
	if len(upd.Assignments) == 0 && len(upd.Attachments) == 0 {
		return DMLResult{}, NewPlanningError("UPDATE needs at least one assignment", "")
	}
	env := newEvalEnv(ctx.Schema, ctx.Parameters, upd.Table)
	env.attach = ctx.attach
	v := &validator{sch: ctx.Schema, from: upd.Table}
	for _, a := range upd.Assignments {
		path := stripTableAlias(a.Path, upd.Table)
		if path == colKdbID {
			return DMLResult{}, NewPlanningError("kdb_id cannot be assigned", "")
		}
		if path != colDoc && !ctx.Schema.IsSchemaless() && !ctx.Schema.HasField(rootSegment(path)) {
			return DMLResult{}, NewPlanningError("unknown column: "+path, "")
		}
		if err := v.check(a.Value, false, false); err != nil {
			return DMLResult{}, err
		}
	}
	if err := validateClaimClauses(upd, ctx.Schema); err != nil {
		return DMLResult{}, err
	}
	docs, err := d.updateTargets(upd, ctx, env, among)
	if err != nil {
		return DMLResult{}, err
	}
	var writer *attachmentWriter
	if len(upd.Attachments) > 0 {
		writer = newAttachmentWriter(d.Executor, ctx.NamespaceID)
	}
	var res DMLResult
	if len(upd.Returning) > 0 {
		res.Columns = columnsFor(SelectQuery{Projections: upd.Returning, From: upd.Table}, ctx.Schema)
		res.Rows = []QueryRow{}
	}
	for _, ed := range docs {
		after := ed
		if writer != nil {
			attachOps, err := writer.updateAttachmentOps(env, upd.Attachments, ed, ctx.attach.set(ed.doc.ID))
			if err != nil {
				return DMLResult{}, err
			}
			res.Operations = append(res.Operations, attachOps...)
		}
		if len(upd.Assignments) > 0 {
			jsonText := ed.doc.JSON
			for _, a := range upd.Assignments {
				jsonText, err = assignPath(jsonText, stripTableAlias(a.Path, upd.Table), env.cell(a.Value, ed))
				if err != nil {
					return DMLResult{}, err
				}
			}
			if err := validateJSON(ed.doc.ID, jsonText, ctx.Schema); err != nil {
				return DMLResult{}, err
			}
			res.Operations = append(res.Operations, document.WriteOp{DocID: ed.doc.ID, Patch: jsonText})
			after = newEvalDoc(document.Document{ID: ed.doc.ID, JSON: jsonText})
		}
		if len(upd.Returning) > 0 {
			res.Rows = append(res.Rows, QueryRow{Values: projectRow(upd.Returning, env, after)})
		}
	}
	res.RowsAffected = rowsAffected(res.Operations)
	if len(upd.Returning) > 0 || upd.Immediate() {
		// Documents updated, not operations: an attachment change is still one row.
		res.RowsAffected = len(docs)
	}
	return res, nil
}

// validateClaimClauses checks an UPDATE's ORDER BY and RETURNING the way a SELECT's are checked,
// and refuses aggregates in them: there is one returned row per updated document.
func validateClaimClauses(upd UpdateStatement, sch schema.KdbSchema) error {
	if !upd.Immediate() {
		return nil
	}
	projections := upd.Returning
	if len(projections) == 0 {
		projections = []Projection{ProjStar{}}
	}
	q := SelectQuery{Projections: projections, From: upd.Table, Where: upd.Where, OrderBy: upd.OrderBy, Limit: upd.Limit}
	if isGroupedQuery(q) {
		return NewPlanningError("RETURNING and ORDER BY of an UPDATE cannot use aggregates", "")
	}
	return validateSelect(q, sch)
}

// updateTargets resolves the documents an UPDATE writes, in the order it writes them.
func (d *DMLExecutor) updateTargets(upd UpdateStatement, ctx QueryContext, env *evalEnv, among []codec.UUID) ([]*evalDoc, error) {
	var docs []*evalDoc
	var err error
	q := SelectQuery{Projections: []Projection{ProjStar{}}, From: upd.Table, Where: upd.Where, OrderBy: upd.OrderBy, Limit: upd.Limit}
	switch {
	case among != nil:
		if err := d.Executor.resolveSearches(q, env, ctx); err != nil {
			return nil, err
		}
		docs, err = d.Executor.materialize(among, upd.Where, env, ctx)
	case upd.Limit != nil && len(upd.OrderBy) > 0 && *upd.Limit <= maxTopK:
		// Only the best LIMIT rows are kept as the candidates stream (topk.go).
		if err := d.Executor.resolveSearches(q, env, ctx); err != nil {
			return nil, err
		}
		docs, _, err = d.Executor.topKDocs(q, upd.Where, env, ctx, *upd.Limit)
	default:
		docs, _, err = d.Executor.matchingDocs(upd.Where, upd.Table, ctx)
	}
	if err != nil {
		return nil, err
	}
	if len(upd.OrderBy) > 0 {
		keys := make([][]Cell, len(docs))
		for i, ed := range docs {
			k := make([]Cell, len(upd.OrderBy))
			for j, o := range upd.OrderBy {
				k[j] = env.cell(o.Expr, ed)
			}
			keys[i] = k
		}
		docs = sortByKeys(docs, keys, upd.OrderBy)
	}
	if upd.Limit != nil && len(docs) > *upd.Limit {
		docs = docs[:*upd.Limit]
	}
	return docs, nil
}

// UpdateCandidates returns the ids of the first k documents an immediate UPDATE would pick at
// ctx.AtCommit, in its ORDER BY order (any order without one), and complete=true when fewer than
// k documents match - so the list is every match. It reads, never writes: the server calls it
// before its write gate, and re-checks the ids under the gate with ExecuteUpdateAmong.
func (d *DMLExecutor) UpdateCandidates(upd UpdateStatement, ctx QueryContext, k int) (ids []codec.UUID, complete bool, err error) {
	if err := validateClaimClauses(upd, ctx.Schema); err != nil {
		return nil, false, err
	}
	q := SelectQuery{
		Projections: []Projection{ProjColumn{Name: colKdbID}},
		From:        upd.Table, Where: upd.Where, OrderBy: upd.OrderBy, Limit: &k,
	}
	plan, residual, err := DefaultPlanner{}.PlanSelect(q, ctx.Schema)
	if err != nil {
		return nil, false, err
	}
	ctx.Page = nil
	if ctx.MaxRows <= 0 || ctx.MaxRows < k {
		ctx.MaxRows = k
	}
	res, err := d.Executor.ExecuteSelect(q, plan, residual, ctx)
	if err != nil {
		return nil, false, err
	}
	ids = make([]codec.UUID, 0, len(res.Rows))
	for _, row := range res.Rows {
		if len(row.Values) == 0 {
			continue
		}
		s, ok := row.Values[0].(CellString)
		if !ok {
			continue
		}
		id, err := codec.ParseUUID(s.Value)
		if err != nil {
			return nil, false, err
		}
		ids = append(ids, id)
	}
	return ids, len(ids) < k, nil
}

// ExecuteDelete returns one Delete op per matching document.
func (d *DMLExecutor) ExecuteDelete(del DeleteStatement, ctx QueryContext) ([]document.Op, error) {
	docs, _, err := d.Executor.matchingDocs(del.Where, del.Table, ctx)
	if err != nil {
		return nil, err
	}
	ops := make([]document.Op, 0, len(docs))
	for _, ed := range docs {
		ops = append(ops, document.DeleteOp{DocID: ed.doc.ID})
	}
	return ops, nil
}

// assignPath applies one SET target: `_doc` replaces the document with the given JSON object
// text (stored verbatim); any other target is a JSON path set.
func assignPath(jsonText, path string, value Cell) (string, error) {
	if path == colDoc {
		var text string
		switch v := value.(type) {
		case CellString:
			text = v.Value
		case CellJSON:
			text = v.JSON
		default:
			return "", NewPlanningError("_doc must be assigned a JSON object", "")
		}
		parsed, err := kdbjson.ParseValue(text)
		if err != nil {
			return "", NewPlanningError("_doc must be assigned valid JSON: "+err.Error(), "")
		}
		if _, ok := parsed.(kdbjson.ObjectValue); !ok {
			return "", NewPlanningError("_doc must be assigned a JSON object", "")
		}
		return text, nil
	}
	if value == nil {
		value = CellNull{}
	}
	jv, err := cellToJSONValue(value)
	if err != nil {
		return "", err
	}
	compiled, err := kdbjson.CompilePath("$." + path)
	if err != nil {
		return "", err
	}
	return kdbjson.Set(jsonText, compiled, jv)
}

func validateJSON(id codec.UUID, jsonText string, sch schema.KdbSchema) error {
	if sch.IsSchemaless() {
		return nil
	}
	doc := document.Document{ID: id, JSON: jsonText}
	r := schema.Validate(doc, sch)
	if r.IsFailure() {
		msg := r.Exception().Error()
		if sve, ok := r.Exception().(*kdberr.SchemaViolationError); ok && len(sve.Violations) > 0 {
			msg = sve.Violations[0].Detail
		}
		return NewPlanningError(fmt.Sprintf("schema violation: %s", msg), "")
	}
	return nil
}

// CellToJSONValue converts a result cell to a JSON value. Exported for callers outside this
// package that hand query results to something that speaks JSON - a stored procedure, say - so
// that they read a cell exactly as an INSERT writes one.
func CellToJSONValue(cell Cell) (kdbjson.Value, error) { return cellToJSONValue(cell) }

func cellToJSONValue(cell Cell) (kdbjson.Value, error) {
	switch c := cell.(type) {
	case nil, CellNull:
		return kdbjson.NullValue{}, nil
	case CellString:
		return kdbjson.StringValue{V: c.Value}, nil
	case CellLong:
		return kdbjson.IntValue{V: c.Value}, nil
	case CellDouble:
		return kdbjson.NumberValue{V: c.Value}, nil
	case CellBool:
		return kdbjson.BoolValue{V: c.Value}, nil
	case CellBytes:
		// bytes written into the JSON body are base64 text; attachments are the place for bytes
		return kdbjson.StringValue{V: base64.StdEncoding.EncodeToString(c.Value)}, nil
	case CellJSON:
		v, err := kdbjson.ParseValue(c.JSON)
		if err != nil {
			return nil, NewPlanningError("invalid JSON value: "+err.Error(), "")
		}
		return v, nil
	default:
		return kdbjson.NullValue{}, nil
	}
}
