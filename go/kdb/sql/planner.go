package sql

import "github.com/limidus/kdb/go/kdb/schema"

// Planner builds physical plans for SELECT.
type Planner interface {
	PlanSelect(query SelectQuery, sch schema.KdbSchema) (PhysicalPlan, Expr)
}

// DefaultPlanner is a minimal planner (full scan + residual filter).
type DefaultPlanner struct{}

func (DefaultPlanner) PlanSelect(query SelectQuery, sch schema.KdbSchema) (PhysicalPlan, Expr) {
	for _, proj := range query.Projections {
		if col, ok := proj.(ProjColumn); ok {
			name := col.Name
			if name != "kdb_id" && name != "_doc" && !sch.HasField(name) {
				panic(NewPlanningError("unknown column: "+name, ""))
			}
		}
	}
	plan := PhysicalPlan(PlanFullScan{Label: "full scan"})
	limit := 1<<31 - 1
	if query.Limit != nil {
		limit = *query.Limit
	}
	plan = PlanLimit{Limit: limit, Offset: query.Offset, Input: plan}
	return plan, query.Where
}
