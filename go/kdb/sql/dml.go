package sql

import (
	"fmt"

	"github.com/limidus/kdb/go/kdb/codec"
	kdbjson "github.com/limidus/kdb/go/kdb/json"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
	kdberr "github.com/limidus/kdb/go/kdb/error"
)

// DMLExecutor builds document operations for INSERT.
type DMLExecutor struct {
	Executor *Executor
}

// ExecuteInsert returns Write ops for each VALUES row.
func (d *DMLExecutor) ExecuteInsert(insert InsertStatement, ctx QueryContext) ([]document.Op, error) {
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
			cell := evalValueExpr(values[i], nil, ctx)
			if cell == nil {
				cell = CellNull{}
			}
			jv, err := cellToJSONValue(cell)
			if err != nil {
				return nil, err
			}
			path, err := kdbjson.CompilePath("$." + col)
			if err != nil {
				return nil, err
			}
			jsonText, err = kdbjson.Set(jsonText, path, jv)
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

func evalValueExpr(expr Expr, doc *document.Document, ctx QueryContext) Cell {
	switch e := expr.(type) {
	case ExprLiteral:
		return e.Cell
	case ExprParameter:
		if doc == nil {
			return parameterToCell(ctx.Parameters, e.Index)
		}
		return EvalCell(e, *doc, ctx.Schema, ctx.Parameters)
	default:
		return nil
	}
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
	case CellNull:
		return kdbjson.NullValue{}, nil
	case CellString:
		return kdbjson.StringValue{V: c.Value}, nil
	case CellLong:
		return kdbjson.IntValue{V: c.Value}, nil
	case CellDouble:
		return kdbjson.NumberValue{V: c.Value}, nil
	default:
		return kdbjson.NullValue{}, nil
	}
}
