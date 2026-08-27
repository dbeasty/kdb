package sql

import (
	"strings"

	"github.com/limidus/kdb/go/kdb/document"
	kdbjson "github.com/limidus/kdb/go/kdb/json"
	"github.com/limidus/kdb/go/kdb/schema"
)

// EvalPredicate evaluates a WHERE expression against one document.
func EvalPredicate(expr Expr, doc document.Document, sch schema.KdbSchema, params []Parameter) bool {
	switch e := expr.(type) {
	case ExprBinary:
		switch e.Op {
		case BinaryOpAnd:
			return EvalPredicate(e.Left, doc, sch, params) && EvalPredicate(e.Right, doc, sch, params)
		case BinaryOpOr:
			return EvalPredicate(e.Left, doc, sch, params) || EvalPredicate(e.Right, doc, sch, params)
		default:
			l := EvalCell(e.Left, doc, sch, params)
			r := EvalCell(e.Right, doc, sch, params)
			return compareOp(e.Op, l, r)
		}
	case ExprUnary:
		switch e.Op {
		case UnaryOpNot:
			return !EvalPredicate(e.Expr, doc, sch, params)
		case UnaryOpIsNull:
			c := EvalCell(e.Expr, doc, sch, params)
			_, isNull := c.(CellNull)
			return isNull || c == nil
		}
	}
	return false
}

func compareOp(op BinaryOp, left, right Cell) bool {
	cmp := CompareCells(left, right)
	switch op {
	case BinaryOpEQ:
		return cmp == 0
	case BinaryOpNE:
		return cmp != 0
	case BinaryOpLT:
		return cmp < 0
	case BinaryOpLE:
		return cmp <= 0
	case BinaryOpGT:
		return cmp > 0
	case BinaryOpGE:
		return cmp >= 0
	default:
		return false
	}
}

// EvalCell evaluates an expression to a cell value.
func EvalCell(expr Expr, doc document.Document, sch schema.KdbSchema, params []Parameter) Cell {
	switch e := expr.(type) {
	case ExprLiteral:
		return e.Cell
	case ExprColumnRef:
		return CellForColumn(e.Name, doc, sch)
	case ExprParameter:
		return parameterToCell(params, e.Index)
	case ExprFunctionCall:
		return EvalAggregate(e, []document.Document{doc}, sch, params)
	default:
		return nil
	}
}

// CellForColumn reads a schema field from document JSON.
func CellForColumn(name string, doc document.Document, sch schema.KdbSchema) Cell {
	if name == "kdb_id" {
		return CellString{Value: doc.ID.String()}
	}
	if name == "_doc" {
		return CellJSON{JSON: doc.JSON}
	}
	field, ok := sch.Field(name)
	if !ok {
		return CellNull{}
	}
	raw, err := kdbjson.GetString(doc.JSON, "$."+name)
	if err != nil || raw == nil {
		return CellNull{}
	}
	return jsonValueToCell(raw, field.Type)
}

func parameterToCell(params []Parameter, index int) Cell {
	if index < 0 || index >= len(params) {
		return CellNull{}
	}
	switch p := params[index].(type) {
	case ParamString:
		return CellString{Value: p.Value}
	case ParamInt:
		return CellLong{Value: p.Value}
	case ParamDouble:
		return CellDouble{Value: p.Value}
	case ParamBool:
		return CellLong{Value: boolToInt64(p.Value)}
	case ParamNull:
		return CellNull{}
	default:
		return CellNull{}
	}
}

func boolToInt64(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// CompareCells compares two cells for ordering (-1, 0, 1).
func CompareCells(left, right Cell) int {
	if left == nil && right == nil {
		return 0
	}
	if _, ok := left.(CellNull); ok || left == nil {
		if _, ok2 := right.(CellNull); ok2 || right == nil {
			return 0
		}
		return -1
	}
	if _, ok := right.(CellNull); ok || right == nil {
		return 1
	}
	switch l := left.(type) {
	case CellString:
		r := right.(CellString)
		return strings.Compare(l.Value, r.Value)
	case CellLong:
		r := right.(CellLong)
		if l.Value < r.Value {
			return -1
		}
		if l.Value > r.Value {
			return 1
		}
		return 0
	case CellDouble:
		r := right.(CellDouble)
		if l.Value < r.Value {
			return -1
		}
		if l.Value > r.Value {
			return 1
		}
		return 0
	default:
		return 0
	}
}

func jsonValueToCell(v kdbjson.Value, ft schema.FieldType) Cell {
	switch val := v.(type) {
	case kdbjson.NullValue:
		return CellNull{}
	case kdbjson.StringValue:
		return CellString{Value: val.V}
	case kdbjson.IntValue:
		return CellLong{Value: val.V}
	case kdbjson.NumberValue:
		return CellDouble{Value: val.V}
	case kdbjson.BoolValue:
		return CellLong{Value: boolToInt64(val.V)}
	default:
		_ = ft
		return CellNull{}
	}
}
