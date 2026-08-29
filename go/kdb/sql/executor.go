package sql

import (
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

// Executor runs SELECT queries against storage.
type Executor struct {
	Storage storage.Adapter
	DAG     *dag.InMemoryCommitDag
}

// ExecuteSelect runs a planned SELECT.
func (e *Executor) ExecuteSelect(query SelectQuery, plan PhysicalPlan, residual Expr, ctx QueryContext) (QueryResult, error) {
	if QueryHasAggregates(query) {
		return e.executeAggregateSelect(query, ctx)
	}
	// ORDER BY has to run before LIMIT/OFFSET: "the first three rows in sorted order", not
	// "three arbitrary rows, sorted among themselves". The planner puts PlanLimit outermost and
	// resolveDocIDs applies it while resolving ids - before any document has been read, let
	// alone sorted - so an ordered query used to answer from whichever rows the scan reached
	// first. Strip the limit here and apply it below, once the rows are in order.
	var limit, offset int
	deferLimit := len(query.OrderBy) > 0
	if deferLimit {
		plan, limit, offset, deferLimit = stripLimit(plan)
	}
	ids, err := e.resolveDocIDs(plan, residual, ctx)
	if err != nil {
		return QueryResult{}, err
	}
	atCommit, err := e.atCommit(ctx)
	if err != nil {
		return QueryResult{}, err
	}
	commit, err := e.DAG.GetCommitOrThrow(atCommit)
	if err != nil {
		return QueryResult{}, err
	}
	treeHash := commit.DocumentTreeHash
	var pairs []struct {
		id  codec.UUID
		doc document.Document
	}
	for _, id := range ids {
		doc, err := e.Storage.GetDocument(ctx.NamespaceID, id, treeHash)
		if err != nil {
			return QueryResult{}, err
		}
		if doc != nil {
			pairs = append(pairs, struct {
				id  codec.UUID
				doc document.Document
			}{id: id, doc: *doc})
		}
	}
	if len(query.OrderBy) > 0 {
		sort.SliceStable(pairs, func(i, j int) bool {
			for _, item := range query.OrderBy {
				ca := EvalCell(item.Expr, pairs[i].doc, ctx.Schema, ctx.Parameters)
				cb := EvalCell(item.Expr, pairs[j].doc, ctx.Schema, ctx.Parameters)
				cmp := CompareCells(ca, cb)
				if cmp != 0 {
					if item.Ascending {
						return cmp < 0
					}
					return cmp > 0
				}
			}
			return false
		})
	}
	if deferLimit {
		pairs = applyLimitOffset(pairs, limit, offset)
	}
	rows := make([]QueryRow, 0, len(pairs))
	for _, p := range pairs {
		rows = append(rows, QueryRow{Values: projectRow(query.Projections, p.doc, ctx)})
	}
	return QueryResult{Columns: columnsFor(query, ctx.Schema), Rows: rows}, nil
}

// stripLimit peels the planner's outermost PlanLimit off a plan, returning the inner plan along
// with the limit and offset it carried, so the caller can apply them at the right point instead.
// Reports false (and leaves the plan alone) when there is no PlanLimit to peel.
func stripLimit(plan PhysicalPlan) (inner PhysicalPlan, limit, offset int, found bool) {
	if p, ok := plan.(PlanLimit); ok {
		return p.Input, p.Limit, p.Offset, true
	}
	return plan, 0, 0, false
}

// applyLimitOffset slices an already-ordered result. Generic over the element type so the same
// bounds logic serves rows and ids rather than being written twice with one of them subtly off.
func applyLimitOffset[T any](items []T, limit, offset int) []T {
	if offset >= len(items) {
		return nil
	}
	end := offset + limit
	if end > len(items) || end < offset { // end < offset catches an overflowing limit
		end = len(items)
	}
	return items[offset:end]
}

func (e *Executor) executeAggregateSelect(query SelectQuery, ctx QueryContext) (QueryResult, error) {
	plan, residual := DefaultPlanner{}.PlanSelect(query, ctx.Schema)
	// An aggregate consumes every matching row and produces one; LIMIT bounds that output, not
	// the input. Leaving the planner's PlanLimit in place made it truncate the rows being
	// aggregated instead, so `SELECT COUNT(*) FROM t LIMIT 1` answered 1 however many rows the
	// table held.
	plan, _, _, _ = stripLimit(plan)
	ids, err := e.resolveDocIDs(plan, residual, ctx)
	if err != nil {
		return QueryResult{}, err
	}
	atCommit, err := e.atCommit(ctx)
	if err != nil {
		return QueryResult{}, err
	}
	commit, err := e.DAG.GetCommitOrThrow(atCommit)
	if err != nil {
		return QueryResult{}, err
	}
	treeHash := commit.DocumentTreeHash
	var docs []document.Document
	for _, id := range ids {
		doc, err := e.Storage.GetDocument(ctx.NamespaceID, id, treeHash)
		if err != nil {
			return QueryResult{}, err
		}
		if doc != nil {
			docs = append(docs, *doc)
		}
	}
	row := QueryRow{Values: projectAggregateRow(query.Projections, docs, ctx.Schema, ctx.Parameters)}
	return QueryResult{Columns: columnsFor(query, ctx.Schema), Rows: []QueryRow{row}}, nil
}

// ResolveDocIDsForWhere resolves document ids matching a WHERE clause.
func (e *Executor) ResolveDocIDsForWhere(where Expr, sch schema.KdbSchema, ctx QueryContext) ([]codec.UUID, error) {
	q := SelectQuery{
		Projections: []Projection{ProjStar{}},
		From:        TableRef{Name: "t"},
		Where:       where,
	}
	plan, residual := DefaultPlanner{}.PlanSelect(q, sch)
	return e.resolveDocIDs(plan, residual, ctx)
}

func (e *Executor) resolveDocIDs(plan PhysicalPlan, residual Expr, ctx QueryContext) ([]codec.UUID, error) {
	switch p := plan.(type) {
	case PlanLimit:
		inner, err := e.resolveDocIDs(p.Input, residual, ctx)
		if err != nil {
			return nil, err
		}
		return applyLimitOffset(inner, p.Limit, p.Offset), nil
	case PlanFilter:
		return e.resolveDocIDs(p.Input, p.Predicate, ctx)
	case PlanFullScan:
		ids, err := e.fullScan(ctx)
		if err != nil {
			return nil, err
		}
		if residual == nil {
			return ids, nil
		}
		return e.filterIDs(ids, residual, ctx)
	default:
		return e.fullScan(ctx)
	}
}

func (e *Executor) fullScan(ctx QueryContext) ([]codec.UUID, error) {
	atCommit, err := e.atCommit(ctx)
	if err != nil {
		return nil, err
	}
	commit, err := e.DAG.GetCommitOrThrow(atCommit)
	if err != nil {
		return nil, err
	}
	treeHash := commit.DocumentTreeHash
	max := ctx.MaxRows
	if max <= 0 {
		max = 10000
	}
	var ids []codec.UUID
	err = e.Storage.ScanDocuments(ctx.NamespaceID, treeHash, 256, func(batch []document.Document) error {
		for _, d := range batch {
			ids = append(ids, d.ID)
			if len(ids) >= max {
				return nil
			}
		}
		return nil
	})
	return ids, err
}

func (e *Executor) filterIDs(ids []codec.UUID, predicate Expr, ctx QueryContext) ([]codec.UUID, error) {
	atCommit, err := e.atCommit(ctx)
	if err != nil {
		return nil, err
	}
	commit, err := e.DAG.GetCommitOrThrow(atCommit)
	if err != nil {
		return nil, err
	}
	treeHash := commit.DocumentTreeHash
	var out []codec.UUID
	for _, id := range ids {
		doc, err := e.Storage.GetDocument(ctx.NamespaceID, id, treeHash)
		if err != nil {
			return nil, err
		}
		if doc == nil {
			continue
		}
		if EvalPredicate(predicate, *doc, ctx.Schema, ctx.Parameters) {
			out = append(out, id)
		}
	}
	return out, nil
}

func (e *Executor) atCommit(ctx QueryContext) (codec.Hash, error) {
	if ctx.AtCommit != nil {
		return *ctx.AtCommit, nil
	}
	return e.DAG.Head()
}

func projectRow(projections []Projection, doc document.Document, ctx QueryContext) []Cell {
	sch := ctx.Schema
	for _, p := range projections {
		if _, ok := p.(ProjStar); ok {
			cols := []Cell{CellString{Value: doc.ID.String()}}
			for _, f := range sch.Fields {
				c := CellForColumn(f.Name, doc, sch)
				cols = append(cols, c)
			}
			cols = append(cols, CellJSON{JSON: doc.JSON})
			return cols
		}
	}
	out := make([]Cell, 0, len(projections))
	for _, proj := range projections {
		switch p := proj.(type) {
		case ProjColumn:
			out = append(out, CellForColumn(p.Name, doc, sch))
		case ProjExpression:
			switch ex := p.Expr.(type) {
			case ExprFunctionCall:
				out = append(out, EvalAggregate(ex, []document.Document{doc}, sch, ctx.Parameters))
			default:
				c := EvalCell(ex, doc, sch, ctx.Parameters)
				if c == nil {
					c = CellNull{}
				}
				out = append(out, c)
			}
		default:
			out = append(out, CellNull{})
		}
	}
	return out
}

func projectAggregateRow(projections []Projection, docs []document.Document, sch schema.KdbSchema, params []Parameter) []Cell {
	out := make([]Cell, 0, len(projections))
	for _, proj := range projections {
		if pe, ok := proj.(ProjExpression); ok {
			if fc, ok := pe.Expr.(ExprFunctionCall); ok {
				out = append(out, EvalAggregate(fc, docs, sch, params))
				continue
			}
		}
		out = append(out, CellNull{})
	}
	return out
}

func columnsFor(query SelectQuery, sch schema.KdbSchema) []ResultColumn {
	for _, p := range query.Projections {
		if _, ok := p.(ProjStar); ok {
			cols := []ResultColumn{{Name: "kdb_id", SQLType: "VARCHAR", Source: ColumnSourceKdbID}}
			for _, f := range sch.Fields {
				cols = append(cols, ResultColumn{Name: f.Name, SQLType: f.Type.SQLTypeName(), Source: ColumnSourceSchemaField})
			}
			cols = append(cols, ResultColumn{Name: "_doc", SQLType: "JSON", Source: ColumnSourceDocJSON})
			return cols
		}
	}
	var cols []ResultColumn
	for _, proj := range query.Projections {
		switch p := proj.(type) {
		case ProjColumn:
			name := p.Name
			if p.Alias != "" {
				name = p.Alias
			}
			sqlType := "VARCHAR"
			if f, ok := sch.Field(p.Name); ok {
				sqlType = f.Type.SQLTypeName()
			}
			cols = append(cols, ResultColumn{Name: name, SQLType: sqlType, Source: ColumnSourceSchemaField})
		case ProjExpression:
			name := p.Alias
			if name == "" {
				name = "expr"
			}
			cols = append(cols, ResultColumn{Name: name, SQLType: "JSON", Source: ColumnSourceExpression})
		}
	}
	return cols
}
