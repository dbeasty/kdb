package sql

import (
	"strings"

	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
)

// QueryHasAggregates reports whether a SELECT has aggregate projections.
func QueryHasAggregates(q SelectQuery) bool {
	for _, p := range q.Projections {
		if pe, ok := p.(ProjExpression); ok {
			if fc, ok := pe.Expr.(ExprFunctionCall); ok && isAggregateFunction(fc.Name) {
				return true
			}
		}
	}
	return false
}

func isAggregateFunction(name string) bool {
	switch strings.ToLower(name) {
	case "count", "sum", "avg", "min", "max":
		return true
	default:
		return false
	}
}

// EvalAggregate evaluates COUNT(*) and COUNT(col) over docs.
func EvalAggregate(call ExprFunctionCall, docs []document.Document, sch schema.KdbSchema, params []Parameter) Cell {
	switch strings.ToLower(call.Name) {
	case "count":
		if len(call.Args) == 0 {
			return CellLong{Value: int64(len(docs))}
		}
		arg := call.Args[0]
		if cr, ok := arg.(ExprColumnRef); ok && cr.Name == "*" {
			return CellLong{Value: int64(len(docs))}
		}
		var n int64
		for _, doc := range docs {
			c := EvalCell(arg, doc, sch, params)
			if c != nil {
				if _, isNull := c.(CellNull); !isNull {
					n++
				}
			}
		}
		return CellLong{Value: n}
	default:
		return CellNull{}
	}
}
