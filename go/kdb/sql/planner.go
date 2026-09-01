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
