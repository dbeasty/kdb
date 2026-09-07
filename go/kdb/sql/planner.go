package sql

import "github.com/limidus/kdb/go/kdb/schema"

// Planner builds physical plans for SELECT. A malformed query (e.g. an unknown column) is
// reported via the error return, not a panic - PlanSelect runs directly in the connection
// goroutine for every client request, and an unrecovered panic there would take the whole
// server down for every other connection, not just fail the one bad query.
type Planner interface {
	PlanSelect(query SelectQuery, sch schema.KdbSchema) (PhysicalPlan, Expr, error)
}

// DefaultPlanner is a minimal planner (full scan + residual filter).
type DefaultPlanner struct{}

func (DefaultPlanner) PlanSelect(query SelectQuery, sch schema.KdbSchema) (PhysicalPlan, Expr, error) {
	// A table-less SELECT has no scan to plan: the executor evaluates its projections against
	// one synthetic row. Validate what such a query is allowed to contain and return early,
	// before the schema checks below - which are about columns that, here, cannot exist.
	if !query.HasFrom() {
		if err := validateTablelessSelect(query); err != nil {
			return nil, nil, err
		}
		return PlanSingleRow{}, query.Where, nil
	}
	for _, proj := range query.Projections {
		if col, ok := proj.(ProjColumn); ok {
			name := col.Name
			if name != "kdb_id" && name != "_doc" && !sch.HasField(name) {
				return nil, nil, NewPlanningError("unknown column: "+name, "")
			}
		}
	}
	plan := PhysicalPlan(PlanFullScan{Label: "full scan"})
	limit := 1<<31 - 1
	if query.Limit != nil {
		limit = *query.Limit
	}
	plan = PlanLimit{Limit: limit, Offset: query.Offset, Input: plan}
	return plan, query.Where, nil
}

// validateTablelessSelect rejects the things a `SELECT` with no FROM cannot mean.
//
// Each is refused rather than silently answered with NULL, because a query that quietly returns
// nothing useful is harder to debug than one that says what is wrong: `SELECT name` with no
// table is a forgotten FROM clause, not a request for a null.
func validateTablelessSelect(query SelectQuery) error {
	for _, proj := range query.Projections {
		switch p := proj.(type) {
		case ProjStar:
			return NewPlanningError("SELECT * requires a FROM clause", "")
		case ProjColumn:
			return NewPlanningError("column reference "+p.Name+" requires a FROM clause", "")
		case ProjExpression:
			if err := rejectColumnRefs(p.Expr); err != nil {
				return err
			}
			// An aggregate over no rows is a different question from `SELECT 1`, and answering
			// it would mean deciding what COUNT(*) over a synthetic row means. Refused until
			// something actually needs it.
			if _, ok := p.Expr.(ExprFunctionCall); ok {
				return NewPlanningError("aggregate requires a FROM clause", "")
			}
		}
	}
	if query.Where != nil {
		if err := rejectColumnRefs(query.Where); err != nil {
			return err
		}
	}
	for _, item := range query.OrderBy {
		if err := rejectColumnRefs(item.Expr); err != nil {
			return err
		}
	}
	return nil
}

// rejectColumnRefs walks an expression for column references, which cannot be resolved without
// a table.
func rejectColumnRefs(expr Expr) error {
	switch e := expr.(type) {
	case ExprColumnRef:
		return NewPlanningError("column reference "+e.Name+" requires a FROM clause", "")
	case ExprBinary:
		if err := rejectColumnRefs(e.Left); err != nil {
			return err
		}
		return rejectColumnRefs(e.Right)
	case ExprUnary:
		return rejectColumnRefs(e.Expr)
	case ExprFunctionCall:
		for _, arg := range e.Args {
			if err := rejectColumnRefs(arg); err != nil {
				return err
			}
		}
	}
	return nil
}
