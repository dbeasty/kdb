package sql

import (
	"strings"

	"github.com/limidus/kdb/go/kdb/schema"
)

// Planner builds physical plans for SELECT. A malformed query (e.g. an unknown column) is
// reported via the error return, not a panic - PlanSelect runs directly in the connection
// goroutine for every client request, and an unrecovered panic there would take the whole
// server down for every other connection, not just fail the one bad query.
type Planner interface {
	PlanSelect(query SelectQuery, sch schema.KdbSchema) (PhysicalPlan, Expr, error)
}

// DefaultPlanner validates column resolution (kdb-spec-layer16 §2) and produces the baseline
// full-scan plan. Index selection (§9.3) happens in the executor, which holds the
// IndexProvider; the residual returned here is the whole WHERE clause, which the executor
// re-checks on every fetched document whatever access path it chose.
type DefaultPlanner struct{}

func (DefaultPlanner) PlanSelect(query SelectQuery, sch schema.KdbSchema) (PhysicalPlan, Expr, error) {
	// A table-less SELECT has no scan to plan: the executor evaluates its projections against
	// one synthetic row. Validate what such a query is allowed to contain and return early,
	// before the column checks below - which are about columns that, here, cannot exist.
	if !query.HasFrom() {
		if err := validateTablelessSelect(query); err != nil {
			return nil, nil, err
		}
		return PlanSingleRow{}, query.Where, nil
	}
	// validateSelect subsumes the projection-only check this used to do: it applies §2 column
	// resolution to WHERE, GROUP BY, ORDER BY and function arguments as well.
	if err := validateSelect(query, sch); err != nil {
		return nil, nil, err
	}
	plan := PhysicalPlan(PlanFullScan{Label: "full scan"})
	limit := 1<<31 - 1
	if query.Limit != nil {
		limit = *query.Limit
	}
	plan = PlanLimit{Limit: limit, Offset: query.Offset, Input: plan}
	return plan, query.Where, nil
}

// projectionAliases maps each projection alias to the expression it names, so ORDER BY and
// GROUP BY can refer to `score` in `MATCH(...) AS score`.
func projectionAliases(q SelectQuery) map[string]Expr {
	out := map[string]Expr{}
	for _, p := range q.Projections {
		switch proj := p.(type) {
		case ProjColumn:
			if proj.Alias != "" {
				out[proj.Alias] = ExprColumnRef{Name: proj.Name}
			}
		case ProjExpression:
			if proj.Alias != "" {
				out[proj.Alias] = proj.Expr
			}
		}
	}
	return out
}

// resolveAlias substitutes a projection alias referenced as a bare column.
func (env *evalEnv) resolveAlias(e Expr) Expr {
	if col, ok := e.(ExprColumnRef); ok && env.aliases != nil {
		if target, ok := env.aliases[env.columnName(col.Name)]; ok {
			return target
		}
	}
	return e
}

// validateSelect enforces §2 column resolution and §5 grouping rules.
func validateSelect(q SelectQuery, sch schema.KdbSchema) error {
	v := &validator{sch: sch, from: q.From, aliases: projectionAliases(q)}
	grouped := isGroupedQuery(q)
	for _, p := range q.Projections {
		switch proj := p.(type) {
		case ProjStar:
			if grouped {
				return NewPlanningError("SELECT * cannot be combined with aggregates or GROUP BY", "")
			}
		case ProjColumn:
			if err := v.check(ExprColumnRef{Name: proj.Name}, false, false); err != nil {
				return err
			}
		case ProjExpression:
			if err := v.check(proj.Expr, false, true); err != nil {
				return err
			}
		}
	}
	if err := v.check(q.Where, false, false); err != nil {
		return err
	}
	for _, g := range q.GroupBy {
		if err := v.check(g, true, false); err != nil {
			return err
		}
	}
	for _, o := range q.OrderBy {
		if err := v.check(o.Expr, true, grouped); err != nil {
			return err
		}
	}
	if grouped {
		keys := map[string]bool{}
		for _, g := range q.GroupBy {
			if col, ok := g.(ExprColumnRef); ok {
				keys[stripTableAlias(col.Name, q.From)] = true
				if target, ok := v.aliases[stripTableAlias(col.Name, q.From)]; ok {
					if tc, ok := target.(ExprColumnRef); ok {
						keys[stripTableAlias(tc.Name, q.From)] = true
					}
				}
			}
		}
		for _, p := range q.Projections {
			var expr Expr
			switch proj := p.(type) {
			case ProjColumn:
				expr = ExprColumnRef{Name: proj.Name}
			case ProjExpression:
				expr = proj.Expr
			}
			if err := v.checkGroupKeys(expr, keys); err != nil {
				return err
			}
		}
	}
	return nil
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

type validator struct {
	sch     schema.KdbSchema
	from    TableRef
	aliases map[string]Expr
}

// check walks an expression: column references must resolve (Rule 1), function names must be
// known, aggregates may appear only where allowed and never nest.
func (v *validator) check(e Expr, allowAliases, allowAggregates bool) error {
	if e == nil {
		return nil
	}
	switch ex := e.(type) {
	case ExprColumnRef:
		name := stripTableAlias(ex.Name, v.from)
		if isReservedColumn(name) {
			return nil
		}
		if allowAliases {
			if _, ok := v.aliases[name]; ok {
				return nil
			}
		}
		if !v.sch.IsNone() && !v.sch.HasField(rootSegment(name)) {
			return NewPlanningError("unknown column: "+name, "")
		}
		return nil
	case ExprFunctionCall:
		name := strings.ToLower(ex.Name)
		switch {
		case isAggregateFunction(name):
			if !allowAggregates {
				return NewPlanningError("aggregate function "+strings.ToUpper(name)+" is not allowed here", "")
			}
			if name == "count" && len(ex.Args) == 0 {
				return nil
			}
			if len(ex.Args) != 1 {
				return NewPlanningError(strings.ToUpper(name)+" takes exactly one argument", "")
			}
			return v.check(ex.Args[0], false, false)
		case isScalarFunction(name):
			if name == "array_length" && len(ex.Args) != 1 {
				return NewPlanningError("ARRAY_LENGTH takes exactly one argument", "")
			}
			if name != "array_length" && len(ex.Args) < 2 {
				return NewPlanningError(strings.ToUpper(name)+" takes a path and at least one value", "")
			}
			for _, a := range ex.Args {
				if err := v.check(a, false, false); err != nil {
					return err
				}
			}
			return nil
		default:
			return NewPlanningError("unknown function: "+name, "")
		}
	case ExprMatch:
		return v.check(ex.Query, false, false)
	case ExprSimilarity:
		return v.check(ex.Vector, false, false)
	case ExprFuse:
		for _, a := range ex.Arms {
			if err := v.check(a, false, false); err != nil {
				return err
			}
		}
		return nil
	}
	var firstErr error
	forEachChild(e, func(c Expr) {
		if firstErr == nil {
			firstErr = v.check(c, false, allowAggregates)
		}
	})
	return firstErr
}

// checkGroupKeys requires every non-aggregated column reference in a grouped projection to be
// a GROUP BY key.
func (v *validator) checkGroupKeys(e Expr, keys map[string]bool) error {
	if e == nil {
		return nil
	}
	switch ex := e.(type) {
	case ExprColumnRef:
		name := stripTableAlias(ex.Name, v.from)
		if !keys[name] {
			return NewPlanningError("column "+name+" must appear in GROUP BY or be used in an aggregate", "")
		}
		return nil
	case ExprFunctionCall:
		if isAggregateFunction(ex.Name) {
			return nil
		}
	}
	var firstErr error
	forEachChild(e, func(c Expr) {
		if firstErr == nil {
			firstErr = v.checkGroupKeys(c, keys)
		}
	})
	return firstErr
}
