package sql

import (
	"errors"
	"fmt"
	"math"
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
	// IndexProvider, when non-nil, serves index-backed access paths and the search functions
	// (kdb-spec-layer16 §9). Nil means no indexes: lookups full-scan, MATCH/SIMILARITY fail.
	IndexProvider IndexProvider
}

// ExecuteSelect runs a planned SELECT. Pipeline for a non-grouped query (§3.3): resolve ids →
// materialize (+ residual filter) → sort → project → distinct → offset/limit. The limit is
// pushed down to id resolution only when nothing before it (ORDER BY, DISTINCT, grouping)
// depends on seeing every row.
func (e *Executor) ExecuteSelect(query SelectQuery, plan PhysicalPlan, residual Expr, ctx QueryContext) (QueryResult, error) {
	// A table-less SELECT is answered before anything touches the DAG or storage. That
	// ordering is the point, not an optimization: `SELECT 1` is what a connection pool sends to
	// check liveness, and resolving a commit for it would make the probe fail on an empty
	// namespace that has no commits yet - exactly when a caller most wants a plain answer.
	if !query.HasFrom() {
		return e.executeSingleRowSelect(query, ctx)
	}
	// Aggregates are no longer dispatched to a separate path here: grouped and ungrouped
	// queries share the pipeline below, which is what lets a GROUP BY key be projected.
	env := newEvalEnv(ctx.Schema, ctx.Parameters, query.From)
	env.aliases = projectionAliases(query)
	if err := e.resolveSearches(query, env, ctx); err != nil {
		return QueryResult{}, err
	}
	limit := math.MaxInt
	if query.Limit != nil {
		limit = *query.Limit
	}
	offset := query.Offset

	grouped := isGroupedQuery(query)
	pushLimit := !grouped && len(query.OrderBy) == 0 && !query.Distinct
	// How many ids a full scan needs before it may stop. Only a query with nothing to filter
	// afterwards and nothing that needs every row can stop early; everything else must see the
	// whole namespace, because the residual WHERE runs after id resolution. Stopping a scan at
	// the result bound *before* the filter ran is what made a selective query over a large
	// namespace return a prefix of its answer with no error.
	stopAt := 0
	if pushLimit && residual == nil {
		// Rows wanted: the LIMIT, but never more than one past the result bound (so an
		// overflow is detectable without reading the rest of the namespace), after skipping
		// OFFSET rows.
		want := e.maxResultRows(ctx) + 1
		if query.Limit != nil && limit < want {
			want = limit
		}
		if offset > 0 && offset <= math.MaxInt-want {
			want += offset
		}
		stopAt = want
	}
	ids, label, err := e.chooseAccessPath(query.Where, query.OrderBy, env, ctx, stopAt)
	if err != nil {
		return QueryResult{}, err
	}
	limited := false
	if pushLimit && residual == nil {
		ids = applyLimitOffset(ids, limit, offset)
		limited = true
	}
	docs, err := e.materialize(ids, residual, env, ctx)
	if err != nil {
		return QueryResult{}, err
	}
	if pushLimit && !limited {
		docs = applyLimitOffset(docs, limit, offset)
		limited = true
	}
	var rows []QueryRow
	if grouped {
		rows = e.groupedRows(query, env, docs)
	} else {
		rows = e.plainRows(query, env, docs)
	}
	if query.Distinct {
		rows = distinctRows(rows)
	}
	// An ungrouped aggregate produces exactly one row; LIMIT/OFFSET bound its input no more
	// than its output (TestAggregateIgnoresLimitOnItsInput). Grouped rows are paged normally.
	if !limited && !(grouped && len(query.GroupBy) == 0) {
		rows = applyLimitOffset(rows, limit, offset)
	}
	if max := e.maxResultRows(ctx); len(rows) > max {
		return QueryResult{}, &ResultRowLimitError{Limit: max}
	}
	for _, r := range rows {
		ctx.Stats.addRetained(rowBytes(r))
	}
	if rows == nil {
		rows = []QueryRow{}
	}
	return QueryResult{Columns: columnsFor(query, ctx.Schema), Rows: rows, Plan: label}, nil
}

// plainRows sorts and projects a non-grouped result. Sort keys are computed once per row, not
// inside the comparator.
func (e *Executor) plainRows(query SelectQuery, env *evalEnv, docs []*evalDoc) []QueryRow {
	if len(query.OrderBy) > 0 {
		orderExprs := make([]Expr, len(query.OrderBy))
		for i, o := range query.OrderBy {
			orderExprs[i] = env.resolveAlias(o.Expr)
		}
		keys := make([][]Cell, len(docs))
		for i, d := range docs {
			k := make([]Cell, len(orderExprs))
			for j, ex := range orderExprs {
				k[j] = env.cell(ex, d)
			}
			keys[i] = k
		}
		docs = sortByKeys(docs, keys, query.OrderBy)
	}
	rows := make([]QueryRow, 0, len(docs))
	for _, d := range docs {
		rows = append(rows, QueryRow{Values: projectRow(query.Projections, env, d)})
	}
	return rows
}

// sortByKeys returns items in ORDER BY order given their precomputed keys (stable).
func sortByKeys[T any](items []T, keys [][]Cell, order []OrderItem) []T {
	idx := make([]int, len(items))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool {
		ka, kb := keys[idx[a]], keys[idx[b]]
		for k, item := range order {
			cmp := CompareCells(ka[k], kb[k])
			if cmp != 0 {
				if item.Ascending {
					return cmp < 0
				}
				return cmp > 0
			}
		}
		return false
	})
	out := make([]T, len(items))
	for i, j := range idx {
		out[i] = items[j]
	}
	return out
}

// groupedRows evaluates a GROUP BY / aggregate query (§5). Without ORDER BY, groups come out
// in ascending group-key order under the total comparator.
func (e *Executor) groupedRows(query SelectQuery, env *evalEnv, docs []*evalDoc) []QueryRow {
	keyExprs := make([]Expr, len(query.GroupBy))
	for i, g := range query.GroupBy {
		keyExprs[i] = env.resolveAlias(g)
	}
	type group struct {
		key  []Cell
		docs []*evalDoc
	}
	var groups []*group
	if len(keyExprs) == 0 {
		groups = []*group{{docs: docs}}
	} else {
		byKey := map[string]*group{}
		for _, d := range docs {
			key := make([]Cell, len(keyExprs))
			var hash string
			for i, ex := range keyExprs {
				key[i] = env.cell(ex, d)
				hash += cellKey(key[i]) + "\x00"
			}
			g, ok := byKey[hash]
			if !ok {
				g = &group{key: key}
				byKey[hash] = g
				groups = append(groups, g)
			}
			g.docs = append(g.docs, d)
		}
		sort.SliceStable(groups, func(a, b int) bool {
			for i := range keyExprs {
				if cmp := CompareCells(groups[a].key[i], groups[b].key[i]); cmp != 0 {
					return cmp < 0
				}
			}
			return false
		})
	}
	if len(query.OrderBy) > 0 {
		keys := make([][]Cell, len(groups))
		for i, g := range groups {
			k := make([]Cell, len(query.OrderBy))
			for j, o := range query.OrderBy {
				k[j] = env.groupCell(env.resolveAlias(o.Expr), g.docs)
			}
			keys[i] = k
		}
		groups = sortByKeys(groups, keys, query.OrderBy)
	}
	rows := make([]QueryRow, 0, len(groups))
	for _, g := range groups {
		vals := make([]Cell, 0, len(query.Projections))
		for _, p := range query.Projections {
			switch proj := p.(type) {
			case ProjColumn:
				vals = append(vals, env.groupCell(ExprColumnRef{Name: proj.Name}, g.docs))
			case ProjExpression:
				vals = append(vals, env.groupCell(proj.Expr, g.docs))
			default:
				vals = append(vals, CellNull{})
			}
		}
		rows = append(rows, QueryRow{Values: vals})
	}
	return rows
}

// distinctRows keeps the first occurrence of each distinct projected row.
func distinctRows(rows []QueryRow) []QueryRow {
	seen := map[string]bool{}
	out := make([]QueryRow, 0, len(rows))
	for _, r := range rows {
		var key string
		for _, c := range r.Values {
			key += cellKey(c) + "\x00"
		}
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, r)
	}
	return out
}

// executeSingleRowSelect answers a SELECT with no FROM clause by evaluating its projections
// once against a single synthetic row.
//
// The synthetic row is an empty document, which is safe precisely because the planner has
// already refused every construct that would need a real one - column references, `*`, and
// aggregates (see validateTablelessSelect). What remains is literals, parameters and operators
// over them, none of which read the document.
func (e *Executor) executeSingleRowSelect(query SelectQuery, ctx QueryContext) (QueryResult, error) {
	row := emptyDoc
	env := newEvalEnv(ctx.Schema, ctx.Parameters, query.From)
	env.aliases = projectionAliases(query)
	doc := newEvalDoc(row)

	// WHERE still applies, over that one row: `SELECT 1 WHERE ?` is a legitimate way to ask
	// whether a parameter is true, and answering zero rows is the honest result.
	if query.Where != nil && !env.predicate(query.Where, doc) {
		return QueryResult{Columns: columnsFor(query, ctx.Schema), Rows: nil}, nil
	}

	rows := []QueryRow{{Values: projectRow(query.Projections, env, doc)}}
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
// with the limit and offset it carried. Reports false when there is no PlanLimit to peel.
func stripLimit(plan PhysicalPlan) (inner PhysicalPlan, limit, offset int, found bool) {
	if p, ok := plan.(PlanLimit); ok {
		return p.Input, p.Limit, p.Offset, true
	}
	return plan, 0, 0, false
}

// applyLimitOffset slices an already-ordered result. Generic over the element type so the same
// bounds logic serves rows, docs, and ids.
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

// --- access paths ------------------------------------------------------------------------

// chooseAccessPath picks the id source (§9.3): a search ranking when ORDER BY leads with a
// score, else the first top-level AND conjunct the provider can serve (MATCH, =, range,
// BETWEEN, IN), else a full scan. The label is reported as QueryResult.Plan.
func (e *Executor) chooseAccessPath(where Expr, orderBy []OrderItem, env *evalEnv, ctx QueryContext, stopAt int) ([]codec.UUID, string, error) {
	if len(orderBy) > 0 {
		if lead := env.resolveAlias(orderBy[0].Expr); isSearchExpr(lead) {
			if hits := env.scores[searchKey(lead)]; hits != nil {
				return rankedIDs(hits), hits.label, nil
			}
		}
	}
	for _, conj := range conjuncts(where) {
		if isSearchExpr(conj) {
			if hits := env.scores[searchKey(conj)]; hits != nil {
				return rankedIDs(hits), hits.label, nil
			}
			continue
		}
		// A document id is a key, not a column to scan for: serve it directly. Needs no index
		// provider, and the residual WHERE still re-checks every candidate.
		if ids, ok := keyLookup(conj, env); ok {
			return ids, "keylookup", nil
		}
		if e.IndexProvider == nil {
			continue
		}
		ids, label, ok, err := e.indexLookup(conj, env, ctx)
		if err != nil {
			return nil, "", err
		}
		if ok {
			// Not truncated: the residual WHERE still runs over these, so cutting the list here
			// would drop matches. The result bound is enforced on the rows actually returned.
			return ids, label, nil
		}
	}
	ids, err := e.fullScan(ctx, stopAt)
	return ids, "fullscan", err
}

// rankedIDs lists a ranking's documents best-first. It is not cut: the residual WHERE still runs
// over these, so cutting here (at MaxRows, as this once did) dropped every match ranked below the
// cut. How many candidates a ranking holds is the search's depth (searchDepth), which the spec
// bounds when there is a LIMIT; the result bound is enforced on the rows actually returned.
func rankedIDs(hits *searchHits) []codec.UUID {
	ids := make([]codec.UUID, 0, len(hits.ranked))
	seen := map[codec.UUID]bool{}
	for _, r := range hits.ranked {
		if seen[r.DocID] {
			continue
		}
		seen[r.DocID] = true
		ids = append(ids, r.DocID)
	}
	return ids
}

// conjuncts flattens the top-level AND tree of a WHERE clause.
func conjuncts(e Expr) []Expr {
	if e == nil {
		return nil
	}
	if b, ok := e.(ExprBinary); ok && b.Op == BinaryOpAnd {
		return append(conjuncts(b.Left), conjuncts(b.Right)...)
	}
	return []Expr{e}
}

// indexLookup tries to serve one conjunct from the provider.
func (e *Executor) indexLookup(conj Expr, env *evalEnv, ctx QueryContext) ([]codec.UUID, string, bool, error) {
	isConst := func(x Expr) bool {
		switch x.(type) {
		case ExprLiteral, ExprParameter:
			return true
		}
		return false
	}
	indexable := func(x Expr) (string, bool) {
		col, ok := x.(ExprColumnRef)
		if !ok {
			return "", false
		}
		name := env.columnName(col.Name)
		if isReservedColumn(name) {
			return "", false
		}
		return name, true
	}
	constCell := func(x Expr) Cell { return env.cell(x, newEvalDoc(emptyDoc)) }
	switch c := conj.(type) {
	case ExprBinary:
		field, ok := indexable(c.Left)
		other := c.Right
		op := c.Op
		if !ok {
			field, ok = indexable(c.Right)
			other = c.Left
			op = flipOp(c.Op)
		}
		if !ok || !isConst(other) {
			return nil, "", false, nil
		}
		value := constCell(other)
		if isNullCell(value) {
			return nil, "", false, nil
		}
		switch op {
		case BinaryOpEQ:
			ids, ok, err := e.IndexProvider.ExactLookup(ctx, field, value)
			return ids, "index:eq(" + field + ")", ok, err
		case BinaryOpLT:
			ids, ok, err := e.IndexProvider.RangeLookup(ctx, field, nil, value, false, false)
			return ids, "index:range(" + field + ")", ok, err
		case BinaryOpLE:
			ids, ok, err := e.IndexProvider.RangeLookup(ctx, field, nil, value, false, true)
			return ids, "index:range(" + field + ")", ok, err
		case BinaryOpGT:
			ids, ok, err := e.IndexProvider.RangeLookup(ctx, field, value, nil, false, false)
			return ids, "index:range(" + field + ")", ok, err
		case BinaryOpGE:
			ids, ok, err := e.IndexProvider.RangeLookup(ctx, field, value, nil, true, false)
			return ids, "index:range(" + field + ")", ok, err
		}
	case ExprBetween:
		field, ok := indexable(c.Expr)
		if !ok || !isConst(c.Low) || !isConst(c.High) {
			return nil, "", false, nil
		}
		low, high := constCell(c.Low), constCell(c.High)
		if isNullCell(low) || isNullCell(high) {
			return nil, "", false, nil
		}
		ids, ok, err := e.IndexProvider.RangeLookup(ctx, field, low, high, true, true)
		return ids, "index:range(" + field + ")", ok, err
	case ExprIn:
		field, ok := indexable(c.Expr)
		if !ok {
			return nil, "", false, nil
		}
		var union []codec.UUID
		seen := map[codec.UUID]bool{}
		for _, v := range c.Values {
			if !isConst(v) {
				return nil, "", false, nil
			}
			value := constCell(v)
			if isNullCell(value) {
				continue
			}
			ids, ok, err := e.IndexProvider.ExactLookup(ctx, field, value)
			if err != nil {
				return nil, "", false, err
			}
			if !ok {
				return nil, "", false, nil
			}
			for _, id := range ids {
				if !seen[id] {
					seen[id] = true
					union = append(union, id)
				}
			}
		}
		return union, "index:in(" + field + ")", true, nil
	}
	return nil, "", false, nil
}

// keyLookup serves `kdb_id = <const>` and `kdb_id IN (<const>, ...)` by key. It returns the
// candidate ids, which may name documents that do not exist (materialize skips those) - a
// string that is not a UUID names nothing, so it contributes no candidate. Only the reserved
// kdb_id column qualifies: every other column is a path into the document and needs a scan or
// an index.
func keyLookup(conj Expr, env *evalEnv) ([]codec.UUID, bool) {
	isKey := func(x Expr) bool {
		col, ok := x.(ExprColumnRef)
		return ok && env.columnName(col.Name) == "kdb_id"
	}
	constString := func(x Expr) (string, bool) {
		switch x.(type) {
		case ExprLiteral, ExprParameter:
			if c, ok := env.cell(x, newEvalDoc(emptyDoc)).(CellString); ok {
				return c.Value, true
			}
		}
		return "", false
	}
	parse := func(raw string) []codec.UUID {
		if id, err := codec.ParseUUID(raw); err == nil {
			return []codec.UUID{id}
		}
		return nil
	}
	switch c := conj.(type) {
	case ExprBinary:
		if c.Op != BinaryOpEQ {
			return nil, false
		}
		other := c.Right
		if !isKey(c.Left) {
			if !isKey(c.Right) {
				return nil, false
			}
			other = c.Left
		}
		raw, ok := constString(other)
		if !ok {
			return nil, false
		}
		return parse(raw), true
	case ExprIn:
		if !isKey(c.Expr) {
			return nil, false
		}
		var out []codec.UUID
		seen := map[codec.UUID]bool{}
		for _, v := range c.Values {
			raw, ok := constString(v)
			if !ok {
				return nil, false
			}
			for _, id := range parse(raw) {
				if !seen[id] {
					seen[id] = true
					out = append(out, id)
				}
			}
		}
		return out, true
	}
	return nil, false
}

// flipOp mirrors a comparison when the column is on the right-hand side.
func flipOp(op BinaryOp) BinaryOp {
	switch op {
	case BinaryOpLT:
		return BinaryOpGT
	case BinaryOpLE:
		return BinaryOpGE
	case BinaryOpGT:
		return BinaryOpLT
	case BinaryOpGE:
		return BinaryOpLE
	}
	return op
}

// --- materialization ---------------------------------------------------------------------

// materialize fetches each id once and keeps the documents the residual accepts. Every fetch
// counts toward DocsRead; rows examined and the row budget apply to residual evaluation.
func (e *Executor) materialize(ids []codec.UUID, residual Expr, env *evalEnv, ctx QueryContext) ([]*evalDoc, error) {
	atCommit, err := e.atCommit(ctx)
	if err != nil {
		return nil, err
	}
	commit, err := e.DAG.GetCommitOrThrow(atCommit)
	if err != nil {
		return nil, err
	}
	treeHash := commit.DocumentTreeHash
	out := make([]*evalDoc, 0, len(ids))
	for i, id := range ids {
		if residual != nil && ctx.RowBudget > 0 && i >= ctx.RowBudget {
			return nil, &ScanRowBudgetExceededError{Budget: ctx.RowBudget}
		}
		doc, err := e.Storage.GetDocument(ctx.NamespaceID, id, treeHash)
		if err != nil {
			return nil, err
		}
		if residual != nil {
			ctx.Stats.addExamined(1)
		}
		if doc == nil {
			continue
		}
		ctx.Stats.addDocRead(len(doc.JSON))
		ed := newEvalDoc(*doc)
		if residual != nil && !env.predicate(residual, ed) {
			continue
		}
		ctx.Stats.addRetained(int64(len(doc.JSON)) + retainedRowOverheadBytes)
		out = append(out, ed)
	}
	return out, nil
}

// matchingDocs resolves and materializes every document a WHERE clause accepts, using the
// index-aware access path. Shared by ResolveDocIDsForWhere and the DML executor.
func (e *Executor) matchingDocs(where Expr, from TableRef, ctx QueryContext) ([]*evalDoc, string, error) {
	q := SelectQuery{Projections: []Projection{ProjStar{}}, From: from, Where: where}
	if err := validateSelect(q, ctx.Schema); err != nil {
		return nil, "", err
	}
	env := newEvalEnv(ctx.Schema, ctx.Parameters, from)
	if err := e.resolveSearches(q, env, ctx); err != nil {
		return nil, "", err
	}
	ids, label, err := e.chooseAccessPath(where, nil, env, ctx, 0)
	if err != nil {
		return nil, "", err
	}
	docs, err := e.materialize(ids, where, env, ctx)
	return docs, label, err
}

// ResolveDocIDsForWhere resolves document ids matching a WHERE clause.
func (e *Executor) ResolveDocIDsForWhere(where Expr, sch schema.KdbSchema, ctx QueryContext) ([]codec.UUID, error) {
	ctx.Schema = sch
	docs, _, err := e.matchingDocs(where, TableRef{}, ctx)
	if err != nil {
		return nil, err
	}
	ids := make([]codec.UUID, len(docs))
	for i, d := range docs {
		ids[i] = d.doc.ID
	}
	return ids, nil
}

// fullScan lists the ids of every document at the resolved commit. stopAt > 0 lets it stop once
// it has that many; 0 means read everything. Work is bounded by ctx.RowBudget, not by MaxRows:
// MaxRows bounds what a query may *return*, and applying it here, before the residual filter,
// silently dropped every document past the first MaxRows.
func (e *Executor) fullScan(ctx QueryContext, stopAt int) ([]codec.UUID, error) {
	atCommit, err := e.atCommit(ctx)
	if err != nil {
		return nil, err
	}
	commit, err := e.DAG.GetCommitOrThrow(atCommit)
	if err != nil {
		return nil, err
	}
	treeHash := commit.DocumentTreeHash
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
			if stopAt > 0 && len(ids) >= stopAt {
				// Stop the scan outright rather than merely stopping appending: a query that
				// has everything it can return must not read the rest of the namespace.
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

const defaultMaxResultRows = 10000

// maxResultRows is the most rows a query may return.
func (e *Executor) maxResultRows(ctx QueryContext) int {
	if ctx.MaxRows <= 0 {
		return defaultMaxResultRows
	}
	return ctx.MaxRows
}

// ResultRowLimitError reports a query whose result is larger than the server returns in one
// reply. It is an error rather than a truncation on purpose: a client handed the first N rows
// of an answer with no indication there were more has no way to know its answer is wrong.
// Surfaced as RESOURCE_EXHAUSTED - add a LIMIT, or a more selective predicate.
type ResultRowLimitError struct {
	Limit int
}

func (e *ResultRowLimitError) Error() string {
	return fmt.Sprintf("kdb sql: result has more than %d rows - add a LIMIT or a more selective predicate", e.Limit)
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

func (e *Executor) atCommit(ctx QueryContext) (codec.Hash, error) {
	if ctx.AtCommit != nil {
		return *ctx.AtCommit, nil
	}
	return e.DAG.Head()
}

// --- projection --------------------------------------------------------------------------

func projectRow(projections []Projection, env *evalEnv, d *evalDoc) []Cell {
	sch := env.schema
	for _, p := range projections {
		if _, ok := p.(ProjStar); ok {
			cols := []Cell{CellString{Value: d.doc.ID.String()}}
			for _, f := range sch.Fields {
				cols = append(cols, env.cell(ExprColumnRef{Name: f.Name}, d))
			}
			cols = append(cols, CellJSON{JSON: d.doc.JSON})
			return cols
		}
	}
	out := make([]Cell, 0, len(projections))
	for _, proj := range projections {
		switch p := proj.(type) {
		case ProjColumn:
			out = append(out, env.cell(ExprColumnRef{Name: p.Name}, d))
		case ProjExpression:
			c := env.cell(p.Expr, d)
			if c == nil {
				c = CellNull{}
			}
			out = append(out, c)
		default:
			out = append(out, CellNull{})
		}
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
			col := stripTableAlias(p.Name, query.From)
			sqlType := "VARCHAR"
			source := ColumnSourceSchemaField
			switch {
			case col == colKdbID:
				source = ColumnSourceKdbID
			case col == colDoc:
				sqlType, source = "JSON", ColumnSourceDocJSON
			default:
				if f, ok := sch.Field(col); ok {
					sqlType = f.Type.SQLTypeName()
				} else if sch.IsNone() {
					sqlType = "JSON"
				}
			}
			cols = append(cols, ResultColumn{Name: name, SQLType: sqlType, Source: source})
		case ProjExpression:
			name := p.Alias
			if name == "" {
				name = "expr"
			}
			cols = append(cols, ResultColumn{Name: name, SQLType: expressionSQLType(p.Expr), Source: ColumnSourceExpression})
		}
	}
	return cols
}

func expressionSQLType(e Expr) string {
	switch ex := e.(type) {
	case ExprMatch, ExprSimilarity, ExprFuse:
		return "DOUBLE"
	case ExprFunctionCall:
		switch ex.Name {
		case "count", "array_length":
			return "BIGINT"
		case "avg":
			return "DOUBLE"
		case "array_contains", "array_contains_any":
			return "BOOLEAN"
		}
	case ExprBinary, ExprUnary, ExprIn, ExprBetween:
		return "BOOLEAN"
	case ExprLiteral:
		switch ex.Cell.(type) {
		case CellString:
			return "VARCHAR"
		case CellLong:
			return "BIGINT"
		case CellDouble:
			return "DOUBLE"
		case CellBool:
			return "BOOLEAN"
		}
	}
	return "JSON"
}
