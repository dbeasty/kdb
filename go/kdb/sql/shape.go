package sql

import (
	"fmt"
	"strings"
)

// QueryShape is a literal-free structural fingerprint of a SELECT, for admission control's
// learned cost table (kdb-spec-layer13 Component 48 §5.2). Two queries share a shape when they
// differ only in literal/parameter values - `WHERE age > 30` and `WHERE age > 50` are the same
// shape, and their memory cost is governed by the same structure: what is projected, what is
// filtered on, whether the plan blocks (ORDER BY, aggregates), and whether the result is
// bounded. The shape deliberately excludes literals so the table keys on how a query computes,
// never on what data a caller asked about.
type QueryShape struct {
	// Fingerprint is the canonical skeleton string - the map key.
	Fingerprint string
	// HasOrderBy: the executor materializes every matching row before sorting, so the result
	// bound (LIMIT) cannot cap retained memory.
	HasOrderBy bool
	// HasAggregate: every matching row is materialized as aggregate input; LIMIT bounds the
	// (single-row) output, not the input.
	HasAggregate bool
	// ProjStar: the projection returns each document's entire JSON, making per-row cost
	// approximately the document size rather than a few named cells.
	ProjStar bool
	// Limit is the query's LIMIT if present, else 0 (unbounded).
	Limit int
	// HasPredicate: a WHERE clause exists, so rows examined can exceed rows retained.
	HasPredicate bool
}

// ShapeOfSelect derives the shape of a parsed SELECT.
func ShapeOfSelect(q SelectQuery) QueryShape {
	shape := QueryShape{
		HasOrderBy:   len(q.OrderBy) > 0,
		HasAggregate: QueryHasAggregates(q),
		HasPredicate: q.Where != nil,
	}
	if q.Limit != nil {
		shape.Limit = *q.Limit
	}
	var b strings.Builder
	b.WriteString("select")
	if q.Distinct {
		b.WriteString(" distinct")
	}
	b.WriteString(" [")
	for i, p := range q.Projections {
		if i > 0 {
			b.WriteByte(',')
		}
		switch proj := p.(type) {
		case ProjStar:
			shape.ProjStar = true
			b.WriteByte('*')
		case ProjColumn:
			b.WriteString(proj.Name)
		case ProjExpression:
			writeExprSkeleton(&b, proj.Expr)
		}
	}
	b.WriteString("] from ")
	b.WriteString(q.From.Name)
	if q.Where != nil {
		b.WriteString(" where ")
		writeExprSkeleton(&b, q.Where)
	}
	if len(q.GroupBy) > 0 {
		b.WriteString(" group[")
		for i, g := range q.GroupBy {
			if i > 0 {
				b.WriteByte(',')
			}
			writeExprSkeleton(&b, g)
		}
		b.WriteByte(']')
	}
	if len(q.OrderBy) > 0 {
		b.WriteString(" order[")
		for i, o := range q.OrderBy {
			if i > 0 {
				b.WriteByte(',')
			}
			writeExprSkeleton(&b, o.Expr)
			if !o.Ascending {
				b.WriteString(" desc")
			}
		}
		b.WriteByte(']')
	}
	if q.Limit != nil {
		// Presence only, never the value: LIMIT 5 and LIMIT 50 are close enough cousins that
		// splitting them would fragment the learned table, and the row-count term of the
		// estimate handles the difference numerically.
		b.WriteString(" limited")
	}
	shape.Fingerprint = b.String()
	return shape
}

// writeExprSkeleton renders an expression with literals and parameters replaced by "?" - the
// operator/column structure is what predicts cost, the constants are what varies per call.
func writeExprSkeleton(b *strings.Builder, e Expr) {
	switch ex := e.(type) {
	case ExprLiteral, ExprParameter:
		b.WriteByte('?')
	case ExprColumnRef:
		b.WriteString(ex.Name)
	case ExprBinary:
		b.WriteByte('(')
		writeExprSkeleton(b, ex.Left)
		fmt.Fprintf(b, " %s ", binaryOpToken(ex.Op))
		writeExprSkeleton(b, ex.Right)
		b.WriteByte(')')
	case ExprUnary:
		fmt.Fprintf(b, "%s(", unaryOpToken(ex.Op))
		writeExprSkeleton(b, ex.Expr)
		b.WriteByte(')')
	case ExprFunctionCall:
		b.WriteString(ex.Name)
		b.WriteByte('(')
		for i, a := range ex.Args {
			if i > 0 {
				b.WriteByte(',')
			}
			writeExprSkeleton(b, a)
		}
		b.WriteByte(')')
	case ExprIn:
		b.WriteByte('(')
		writeExprSkeleton(b, ex.Expr)
		fmt.Fprintf(b, " in[%d])", len(ex.Values))
	case ExprBetween:
		b.WriteByte('(')
		writeExprSkeleton(b, ex.Expr)
		b.WriteString(" between ? and ?)")
	case ExprMatch:
		fmt.Fprintf(b, "match(%s,?)", ex.IndexOrField)
	case ExprSimilarity:
		fmt.Fprintf(b, "similarity(%s,?)", ex.Field)
	case ExprFuse:
		fmt.Fprintf(b, "fuse[%s](", ex.Mode)
		for i, a := range ex.Arms {
			if i > 0 {
				b.WriteByte(',')
			}
			writeExprSkeleton(b, a)
		}
		b.WriteByte(')')
	default:
		b.WriteByte('?')
	}
}

func binaryOpToken(op BinaryOp) string {
	switch op {
	case BinaryOpEQ:
		return "="
	case BinaryOpNE:
		return "!="
	case BinaryOpLT:
		return "<"
	case BinaryOpLE:
		return "<="
	case BinaryOpGT:
		return ">"
	case BinaryOpGE:
		return ">="
	case BinaryOpAnd:
		return "and"
	case BinaryOpOr:
		return "or"
	case BinaryOpLike:
		return "like"
	case BinaryOpILike:
		return "ilike"
	default:
		return "?op"
	}
}

func unaryOpToken(op UnaryOp) string {
	switch op {
	case UnaryOpNot:
		return "not"
	case UnaryOpIsNull:
		return "isnull"
	default:
		return "?op"
	}
}
