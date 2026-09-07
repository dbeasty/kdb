package sql

import (
	"errors"
	"fmt"
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
	// A table-less SELECT is answered before anything touches the DAG or storage. That
	// ordering is the point, not an optimization: `SELECT 1` is what a connection pool sends to
	// check liveness, and resolving a commit for it would make the probe fail on an empty
	// namespace that has no commits yet - exactly when a caller most wants a plain answer.
	if !query.HasFrom() {
		return e.executeSingleRowSelect(query, ctx)
	}
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
			ctx.Stats.addDocRead(len(doc.JSON))
			ctx.Stats.addRetained(int64(len(doc.JSON)) + retainedRowOverheadBytes)
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
		row := QueryRow{Values: projectRow(query.Projections, p.doc, ctx)}
		ctx.Stats.addRetained(rowBytes(row))
		rows = append(rows, row)
	}
	return QueryResult{Columns: columnsFor(query, ctx.Schema), Rows: rows}, nil
}

// executeSingleRowSelect answers a SELECT with no FROM clause by evaluating its projections
// once against a single synthetic row.
//
// The synthetic row is an empty document, which is safe precisely because the planner has
// already refused every construct that would need a real one - column references, `*`, and
// aggregates (see validateTablelessSelect). What remains is literals, parameters and operators
// over them, none of which read the document.
func (e *Executor) executeSingleRowSelect(query SelectQuery, ctx QueryContext) (QueryResult, error) {
	row := document.Document{}

	// WHERE still applies, over that one row: `SELECT 1 WHERE ?` is a legitimate way to ask
	// whether a parameter is true, and answering zero rows is the honest result.
	if query.Where != nil && !EvalPredicate(query.Where, row, ctx.Schema, ctx.Parameters) {
		return QueryResult{Columns: columnsFor(query, ctx.Schema), Rows: nil}, nil
	}

	rows := []QueryRow{{Values: projectRow(query.Projections, row, ctx)}}
	// ORDER BY over one row is a no-op, but LIMIT and OFFSET are not: `LIMIT 0` genuinely means
	// no rows, and an OFFSET past the single row means the same.
	limit := 1
	if query.Limit != nil {
		limit = *query.Limit
	}
	rows = applyLimitOffset(rows, limit, query.Offset)
	for _, r := range rows {
		ctx.Stats.addRetained(rowBytes(r))
	}
	return QueryResult{Columns: columnsFor(query, ctx.Schema), Rows: rows}, nil
}

// retainedRowOverheadBytes is the per-row fixed cost charged on top of document content when
// accounting materialized results: the pair struct, slice headers, and map/interface overhead
// around each held document. A deliberate round overestimate - admission's safe direction.
const retainedRowOverheadBytes = 128

// rowBytes sizes one projected row's cells - the string/JSON content plus a fixed per-cell
// overhead for the Cell interface value itself.
func rowBytes(r QueryRow) int64 {
	total := int64(0)
	for _, c := range r.Values {
		switch v := c.(type) {
		case CellString:
			total += int64(len(v.Value))
		case CellJSON:
			total += int64(len(v.JSON))
		}
		total += 32 // interface header + small-value cells
	}
	return total
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
	plan, residual, err := DefaultPlanner{}.PlanSelect(query, ctx.Schema)
	if err != nil {
		return QueryResult{}, err
	}
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
			ctx.Stats.addDocRead(len(doc.JSON))
			ctx.Stats.addRetained(int64(len(doc.JSON)) + retainedRowOverheadBytes)
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
	plan, residual, err := DefaultPlanner{}.PlanSelect(q, sch)
	if err != nil {
		return nil, err
	}
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
	examined := 0
	err = e.Storage.ScanDocuments(ctx.NamespaceID, treeHash, 256, func(batch []document.Document) error {
		for _, d := range batch {
			examined++
			ctx.Stats.addExamined(1)
			if ctx.RowBudget > 0 && examined > ctx.RowBudget {
				return &ScanRowBudgetExceededError{Budget: ctx.RowBudget}
			}
			ids = append(ids, d.ID)
			if len(ids) >= max {
				// Stop the scan outright rather than merely stopping appending. Returning nil
				// here only ended the current batch: ScanDocuments went straight on to the next
				// one, so a query that had already collected everything it could return still
				// read the rest of the namespace into memory to throw it away.
				return errScanComplete
			}
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, errScanComplete) {
			return ids, nil
		}
		return nil, err
	}
	return ids, nil
}

// errScanComplete unwinds ScanDocuments once a scan has all the rows it can return. A sentinel
// rather than a nil return because the adapter contract treats a nil error as "keep going".
var errScanComplete = errors.New("kdb sql: scan complete")

// ScanRowBudgetExceededError reports a scan aborted for examining more rows than its budget
// allowed (kdb-spec-layer13 Component 48 §5.2). Surfaced to clients as RESOURCE_EXHAUSTED: the
// query is too expensive for this node as written, so narrowing it is the fix - retrying it
// unchanged is not.
type ScanRowBudgetExceededError struct {
	Budget int
}

func (e *ScanRowBudgetExceededError) Error() string {
	return fmt.Sprintf("kdb sql: scan examined more than its budget of %d rows - narrow the query (add a more selective predicate, or an indexed one)", e.Budget)
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
	for i, id := range ids {
		if ctx.RowBudget > 0 && i >= ctx.RowBudget {
			return nil, &ScanRowBudgetExceededError{Budget: ctx.RowBudget}
		}
		doc, err := e.Storage.GetDocument(ctx.NamespaceID, id, treeHash)
		if err != nil {
			return nil, err
		}
		ctx.Stats.addExamined(1)
		if doc != nil {
			ctx.Stats.addDocRead(len(doc.JSON))
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
