package sql

import (
	"strings"

	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
)

// Aggregates - kdb-spec-layer16 §5: COUNT(*), COUNT(col), SUM, AVG, MIN, MAX over a group of
// documents. SUM over integers only is an integer; any double input makes it a double. AVG is
// always a double. MIN/MAX ignore NULL. Zero rows → NULL, except COUNT → 0.

// QueryHasAggregates reports whether a SELECT has aggregate projections.
func QueryHasAggregates(q SelectQuery) bool {
	for _, p := range q.Projections {
		if pe, ok := p.(ProjExpression); ok && containsAggregate(pe.Expr) {
			return true
		}
	}
	return false
}

// isGroupedQuery reports whether the SELECT is evaluated per group (aggregates or GROUP BY).
func isGroupedQuery(q SelectQuery) bool {
	return QueryHasAggregates(q) || len(q.GroupBy) > 0
}

func isAggregateFunction(name string) bool {
	switch strings.ToLower(name) {
	case "count", "sum", "avg", "min", "max":
		return true
	default:
		return false
	}
}

func isScalarFunction(name string) bool {
	switch strings.ToLower(name) {
	case "array_contains", "array_contains_any", "array_length":
		return true
	default:
		return false
	}
}

// containsAggregate reports whether an aggregate call appears anywhere in e.
func containsAggregate(e Expr) bool {
	found := false
	var walk func(Expr)
	walk = func(x Expr) {
		if found || x == nil {
			return
		}
		if fc, ok := x.(ExprFunctionCall); ok && isAggregateFunction(fc.Name) {
			found = true
			return
		}
		forEachChild(x, walk)
	}
	walk(e)
	return found
}

// EvalAggregate evaluates an aggregate call over docs.
func EvalAggregate(call ExprFunctionCall, docs []document.Document, sch schema.KdbSchema, params []Parameter) Cell {
	env := newEvalEnv(sch, params, TableRef{})
	group := make([]*evalDoc, len(docs))
	for i := range docs {
		group[i] = newEvalDoc(docs[i])
	}
	return env.aggregate(call, group)
}

func (env *evalEnv) aggregate(call ExprFunctionCall, group []*evalDoc) Cell {
	name := strings.ToLower(call.Name)
	if name == "count" {
		if len(call.Args) == 0 {
			return CellLong{Value: int64(len(group))}
		}
		if cr, ok := call.Args[0].(ExprColumnRef); ok && cr.Name == "*" {
			return CellLong{Value: int64(len(group))}
		}
		var n int64
		for _, d := range group {
			if !isNullCell(env.cell(call.Args[0], d)) {
				n++
			}
		}
		return CellLong{Value: n}
	}
	if len(call.Args) != 1 {
		return CellNull{}
	}
	arg := call.Args[0]
	switch name {
	case "sum", "avg":
		var (
			sumI   int64
			sumF   float64
			count  int64
			anyDbl bool
		)
		for _, d := range group {
			c := env.cell(arg, d)
			i, f, isInt := numericValue(c)
			if cellTypeRank(c) != 1 {
				continue // NULL and non-numeric values contribute nothing
			}
			count++
			if isInt {
				sumI += i
			} else {
				anyDbl = true
			}
			sumF += f
		}
		if count == 0 {
			return CellNull{}
		}
		if name == "avg" {
			return CellDouble{Value: sumF / float64(count)}
		}
		if anyDbl {
			return CellDouble{Value: sumF}
		}
		return CellLong{Value: sumI}
	case "min", "max":
		var best Cell
		for _, d := range group {
			c := env.cell(arg, d)
			if isNullCell(c) {
				continue
			}
			if best == nil {
				best = c
				continue
			}
			cmp := CompareCells(c, best)
			if (name == "min" && cmp < 0) || (name == "max" && cmp > 0) {
				best = c
			}
		}
		if best == nil {
			return CellNull{}
		}
		return best
	default:
		return CellNull{}
	}
}

// groupCell evaluates a projection / ORDER BY expression in group context: aggregate calls run
// over the group, everything else (group keys, literals) is evaluated on the group's first
// document - the planner guarantees a non-aggregate column reference is a group key, so every
// document in the group agrees on it.
func (env *evalEnv) groupCell(expr Expr, group []*evalDoc) Cell {
	if fc, ok := expr.(ExprFunctionCall); ok && isAggregateFunction(fc.Name) {
		return env.aggregate(fc, group)
	}
	if len(group) == 0 {
		return CellNull{}
	}
	return env.cell(expr, group[0])
}

// emptyDoc is used to evaluate document-independent expressions (search queries, vectors).
var emptyDoc = document.Document{JSON: "{}"}
