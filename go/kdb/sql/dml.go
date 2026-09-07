package sql

import (
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
	}
	return ops, nil
}

// ExecuteUpdate returns one Write op per matching document with the SET assignments applied.
// Values are evaluated against the pre-update document; `_doc` replaces the whole body.
func (d *DMLExecutor) ExecuteUpdate(upd UpdateStatement, ctx QueryContext) ([]document.Op, error) {
	if len(upd.Assignments) == 0 {
		return nil, NewPlanningError("UPDATE needs at least one assignment", "")
	}
	env := newEvalEnv(ctx.Schema, ctx.Parameters, upd.Table)
	v := &validator{sch: ctx.Schema, from: upd.Table}
	for _, a := range upd.Assignments {
		path := stripTableAlias(a.Path, upd.Table)
		if path == colKdbID {
			return nil, NewPlanningError("kdb_id cannot be assigned", "")
		}
		if path != colDoc && !ctx.Schema.IsNone() && !ctx.Schema.HasField(rootSegment(path)) {
			return nil, NewPlanningError("unknown column: "+path, "")
		}
		if err := v.check(a.Value, false, false); err != nil {
			return nil, err
		}
	}
	docs, _, err := d.Executor.matchingDocs(upd.Where, upd.Table, ctx)
	if err != nil {
		return nil, err
	}
	var ops []document.Op
	for _, ed := range docs {
		jsonText := ed.doc.JSON
		for _, a := range upd.Assignments {
			jsonText, err = assignPath(jsonText, stripTableAlias(a.Path, upd.Table), env.cell(a.Value, ed))
			if err != nil {
				return nil, err
			}
		}
		if err := validateJSON(ed.doc.ID, jsonText, ctx.Schema); err != nil {
			return nil, err
		}
		ops = append(ops, document.WriteOp{DocID: ed.doc.ID, Patch: jsonText})
	}
	return ops, nil
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
	if sch.IsNone() {
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
