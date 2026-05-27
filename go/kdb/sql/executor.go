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
	rows := make([]QueryRow, 0, len(pairs))
	for _, p := range pairs {
		rows = append(rows, QueryRow{Values: projectRow(query.Projections, p.doc, ctx)})
	}
	return QueryResult{Columns: columnsFor(query, ctx.Schema), Rows: rows}, nil
}

func (e *Executor) executeAggregateSelect(query SelectQuery, ctx QueryContext) (QueryResult, error) {
	plan, residual := DefaultPlanner{}.PlanSelect(query, ctx.Schema)
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
		if p.Offset >= len(inner) {
			return nil, nil
		}
		end := p.Offset + p.Limit
		if end > len(inner) {
			end = len(inner)
		}
		return inner[p.Offset:end], nil
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
