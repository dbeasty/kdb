package sql

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

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
	// Attachments, when non-nil, stores and reads attachment bytes (SetAttachmentBlobs).
	Attachments AttachmentBlobs

	// scanIDsOnly reproduces the fetch-by-id full scan this replaced (scan for ids, then fetch
	// each document again). Tests only: it is the oracle that the single-fetch scan returns
	// identical results.
	scanIDsOnly bool
}

// ExecuteSelect runs a planned SELECT. Pipeline for a non-grouped query (§3.3): resolve ids →
// materialize (+ residual filter) → sort → project → distinct → offset/limit. The limit is
// pushed down to id resolution only when nothing before it (ORDER BY, DISTINCT, grouping)
// depends on seeing every row.
func (e *Executor) ExecuteSelect(query SelectQuery, plan PhysicalPlan, residual Expr, ctx QueryContext) (QueryResult, error) {
	if ctx.attach == nil {
		ctx.attach = e.newAttachEnv(ctx)
	}
	res, err := e.executeSelect(query, plan, residual, ctx)
	if err == nil && ctx.attach.err != nil {
		return QueryResult{}, ctx.attach.err
	}
	return res, err
}

func (e *Executor) executeSelect(query SelectQuery, plan PhysicalPlan, residual Expr, ctx QueryContext) (QueryResult, error) {
	// A table-less SELECT is answered before anything touches the DAG or storage. That
	// ordering is the point, not an optimization: `SELECT 1` is what a connection pool sends to
	// check liveness, and resolving a commit for it would make the probe fail on an empty
	// namespace that has no commits yet - exactly when a caller most wants a plain answer.
	if !query.HasFrom() {
		return e.executeSingleRowSelect(query, ctx)
	}
	if ctx.Page != nil {
		return e.executePage(query, plan, residual, ctx)
	}
	// Aggregates are no longer dispatched to a separate path here: grouped and ungrouped
	// queries share the pipeline below, which is what lets a GROUP BY key be projected.
	env := newEvalEnv(ctx.Schema, ctx.Parameters, query.From)
	env.attach = ctx.attach
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
	// ORDER BY kdb_id (ascending) is the order a keyset scan already produces: read the rows in
	// id order, starting past any `kdb_id > 'x'` bound, and stop at the LIMIT, instead of
	// materializing every candidate to sort it. This is the shape of a client paging a filtered
	// read by its last id (WHERE ... AND kdb_id > ? ORDER BY kdb_id LIMIT n).
	ctxIn, keysetIn := ctx, ctx.keyset
	idOrder := !grouped && !query.Distinct && !ctx.noIDOrder && isKdbIDOrder(query.OrderBy, env)
	if idOrder && !ctx.keyset {
		if _, ok := e.Storage.(storage.OrderedScanner); ok {
			ctx.keyset = true
		} else {
			idOrder = false
		}
	}
	if ctx.keyset {
		if lo := kdbIDLowerBound(query.Where, env); lo != nil && (ctx.scanAfter == nil || bytes.Compare(lo.Bytes(), ctx.scanAfter.Bytes()) > 0) {
			ctx.scanAfter = lo
		}
	}
	pushLimit := !grouped && (len(query.OrderBy) == 0 || idOrder) && !query.Distinct
	// How many ids a full scan needs before it may stop. Only a query with nothing to filter
	// afterwards and nothing that needs every row can stop early; everything else must see the
	// whole namespace, because the residual WHERE runs after id resolution. Stopping a scan at
	// the result bound *before* the filter ran is what made a selective query over a large
	// namespace return a prefix of its answer with no error.
	stopAt := 0
	// A keyset page's scan applies the residual itself and keeps only matching documents in id
	// order, so it can stop at the page however selective the WHERE is - except when the scan
	// hands back bare ids (scanIDsOnly) and the residual runs afterwards.
	//
	// The same holds for any scan or id list that applies the residual as it reads and yields
	// rows in the order they are returned: the first `want` matches are the answer, so reading on
	// past them only burned I/O (a page of 200 over a user's 8,000 tasks read all 8,000).
	if pushLimit && (residual == nil || !e.scanIDsOnly) {
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
	if res, ok, err := e.tryCountOnly(query, env, ctx); err != nil {
		return QueryResult{}, err
	} else if ok {
		return res, nil
	}
	if k, ok := topKEligible(query, idOrder); ok && !e.scanIDsOnly && !ctx.keyset {
		docs, label, err := e.topKDocs(query, residual, env, ctx, k)
		if err != nil {
			return QueryResult{}, err
		}
		rows := applyLimitOffset(e.plainRows(query, env, docs), limit, offset)
		if max := e.maxResultRows(ctx); len(rows) > max {
			return QueryResult{}, &ResultRowLimitError{Limit: max}
		}
		for _, r := range rows {
			ctx.Stats.addRetained(rowBytes(r))
		}
		if err := ctx.overRetainedLimit(); err != nil {
			return QueryResult{}, err
		}
		if rows == nil {
			rows = []QueryRow{}
		}
		return QueryResult{Columns: columnsFor(query, ctx.Schema), Rows: rows, Plan: label}, nil
	}
	src, err := e.chooseAccessPath(query.Where, query.OrderBy, env, ctx, stopAt, residual)
	if err != nil {
		if idOrder && !keysetIn && errors.Is(err, storage.ErrOrderedScanUnsupported) {
			// The adapter under this namespace cannot scan in order after all: sort instead.
			retry := ctxIn
			retry.noIDOrder = true
			return e.executeSelect(query, plan, residual, retry)
		}
		return QueryResult{}, err
	}
	label := src.label
	limited := false
	var docs []*evalDoc
	if src.scanned {
		// The full scan already resolved every document and applied the residual to it; there
		// is nothing left to fetch.
		docs = src.docs
		if pushLimit && residual == nil {
			docs = applyLimitOffset(docs, limit, offset)
			limited = true
		}
	} else {
		ids := src.ids
		if ctx.keyset {
			ids = idsAfterInOrder(ids, ctx.scanAfter)
		}
		if pushLimit && residual == nil {
			ids = applyLimitOffset(ids, limit, offset)
			limited = true
		}
		docs, err = e.materializeUpTo(ids, residual, env, ctx, stopAt)
		if err != nil {
			return QueryResult{}, err
		}
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
	var rowDocIDs []codec.UUID
	if ctx.keyset && !grouped && (len(query.OrderBy) == 0 || idOrder) && !query.Distinct {
		rowDocIDs = make([]codec.UUID, len(docs))
		for i, d := range docs {
			rowDocIDs[i] = d.doc.ID
		}
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
	if err := ctx.overRetainedLimit(); err != nil {
		return QueryResult{}, err
	}
	if rows == nil {
		rows = []QueryRow{}
	}
	return QueryResult{Columns: columnsFor(query, ctx.Schema), Rows: rows, Plan: label, RowDocIDs: rowDocIDs}, nil
}

// idsAfterInOrder is an index access path's ids as a keyset page reads them: in document id order,
// those after after (all of them when after is nil).
func idsAfterInOrder(ids []codec.UUID, after *codec.UUID) []codec.UUID {
	out := make([]codec.UUID, 0, len(ids))
	for _, id := range ids {
		if after == nil || bytes.Compare(id.Bytes(), after.Bytes()) > 0 {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].Bytes(), out[j].Bytes()) < 0 })
	return out
}

// executePage runs one page of a SELECT by asking the ordinary pipeline for the page's rows plus
// one, which is how it knows whether more remain without reading the rest. The pipeline's
// LIMIT/OFFSET pushdown then bounds the work to the rows up to the end of the page.
func (e *Executor) executePage(query SelectQuery, plan PhysicalPlan, residual Expr, ctx QueryContext) (QueryResult, error) {
	page := *ctx.Page
	ctx.Page = nil
	if isGroupedQuery(query) && len(query.GroupBy) == 0 {
		// An ungrouped aggregate is one row: there is nothing to page.
		return e.ExecuteSelect(query, plan, residual, ctx)
	}
	maxRows := e.maxResultRows(ctx)
	size := page.Size
	if size < 1 || size > maxRows-1 {
		size = maxRows - 1
	}
	if size < 1 {
		size = 1
	}
	want := size
	exhaustedByLimit := false
	if query.Limit != nil {
		remaining := *query.Limit - page.Offset
		if remaining <= 0 {
			return QueryResult{Columns: columnsFor(query, ctx.Schema), Rows: []QueryRow{}}, nil
		}
		if remaining <= want {
			want, exhaustedByLimit = remaining, true
		}
	}
	q := query
	lim := want + 1
	q.Limit = &lim
	if e.keysetEligible(query, page) {
		kctx := ctx
		kctx.keyset, kctx.scanAfter = true, page.After
		res, err := e.ExecuteSelect(q, plan, residual, kctx)
		switch {
		case errors.Is(err, storage.ErrOrderedScanUnsupported):
			// The adapter this namespace routes to cannot scan in order after all: page by
			// offset, which the token carries too.
		case err != nil:
			return QueryResult{}, err
		default:
			if len(res.Rows) > want {
				res.Rows = res.Rows[:want]
				res.More = !exhaustedByLimit
			}
			if len(res.RowDocIDs) > len(res.Rows) {
				res.RowDocIDs = res.RowDocIDs[:len(res.Rows)]
			}
			return res, nil
		}
	}
	q.Offset = query.Offset + page.Offset
	res, err := e.ExecuteSelect(q, plan, residual, ctx)
	if err != nil {
		return QueryResult{}, err
	}
	if len(res.Rows) > want {
		res.Rows = res.Rows[:want]
		res.More = !exhaustedByLimit
	}
	return res, nil
}

// keysetEligible reports whether a page of query can be served by key (see PageRequest): its
// rows have no order but the documents', nothing needs every row, and the storage can scan in
// id order. A continuation without a key is not: its earlier pages came in another order.
func (e *Executor) keysetEligible(query SelectQuery, page PageRequest) bool {
	if page.OffsetOnly || (page.Offset > 0 && page.After == nil) {
		return false
	}
	ordered := len(query.OrderBy) > 0 && !isKdbIDOrder(query.OrderBy, newEvalEnv(schema.KdbSchema{}, nil, query.From))
	if ordered || query.Distinct || isGroupedQuery(query) || query.Offset > 0 {
		return false
	}
	_, ok := e.Storage.(storage.OrderedScanner)
	return ok
}

// tryCountOnly answers `SELECT COUNT(*) [, COUNT(*) ...] FROM t [WHERE ...]` without holding the
// documents it counts, so counting a large namespace never materializes it.
//
// It declines - leaving the ordinary path to run - unless the projections are all bare COUNT(*)
// with no grouping or DISTINCT. Then, cheapest first:
//
//   - from counters, reading nothing: the storage counts the tree without reading it
//     (storage.DocumentCounter, which subtracts what expiry hides when the expiry index can say)
//     and the WHERE is absent or an AND of equalities / IN lists on string or boolean constants
//     that indexes serve exactly (an index count and a re-check of the predicate agree for those);
//     documents the storage hides from reads but an index still holds are listed
//     (storage.HiddenDocumentLister) and taken back out;
//   - otherwise by streaming: the documents the access path yields are checked against the WHERE
//     one at a time and counted, never retained. Subject to the scan row budget like any scan.
func (e *Executor) tryCountOnly(query SelectQuery, env *evalEnv, ctx QueryContext) (QueryResult, bool, error) {
	if query.Distinct || len(query.GroupBy) > 0 || len(query.Projections) == 0 || query.Offset > 0 {
		return QueryResult{}, false, nil
	}
	if query.Limit != nil && *query.Limit < 1 {
		return QueryResult{}, false, nil
	}
	for _, p := range query.Projections {
		pe, ok := p.(ProjExpression)
		if !ok || !isCountStar(pe.Expr) {
			return QueryResult{}, false, nil
		}
	}
	atCommit, err := e.atCommit(ctx)
	if err != nil {
		return QueryResult{}, false, err
	}
	commit, err := e.DAG.GetCommitOrThrow(atCommit)
	if err != nil {
		return QueryResult{}, false, err
	}
	n, label, ok, err := e.countFromCounters(query.Where, env, ctx, commit.DocumentTreeHash)
	if err != nil {
		return QueryResult{}, false, err
	}
	if !ok {
		if n, label, err = e.countByStreaming(query.Where, env, ctx); err != nil {
			return QueryResult{}, false, err
		}
	}
	row := QueryRow{Values: make([]Cell, len(query.Projections))}
	for i := range row.Values {
		row.Values[i] = CellLong{Value: int64(n)}
	}
	return QueryResult{Columns: columnsFor(query, ctx.Schema), Rows: []QueryRow{row}, Plan: label}, true, nil
}

// countFromCounters is tryCountOnly's no-read tier; ok=false when it cannot vouch for the answer.
func (e *Executor) countFromCounters(where Expr, env *evalEnv, ctx QueryContext, treeHash codec.Hash) (int, string, bool, error) {
	counter, ok := e.Storage.(storage.DocumentCounter)
	if !ok {
		return 0, "", false, nil
	}
	total, ok, err := counter.CountDocuments(ctx.NamespaceID, treeHash)
	if err != nil || !ok {
		return 0, "", false, err
	}
	if where == nil {
		return total, "count:tree", true, nil
	}
	got, ok := e.countByIndex(where, env, ctx)
	if !ok {
		return 0, "", false, nil
	}
	if lister, isLister := e.Storage.(storage.HiddenDocumentLister); isLister {
		hidden, ok, err := lister.HiddenDocuments(ctx.NamespaceID, treeHash)
		if err != nil || !ok {
			return 0, "", false, err
		}
		for _, d := range hidden {
			if env.predicate(where, newEvalDoc(d)) {
				got--
			}
		}
	}
	return got, "count:index", true, nil
}

// countByStreaming counts the documents where accepts, reading them through the access path the
// query would use but keeping none: one document is live at a time, however many match.
func (e *Executor) countByStreaming(where Expr, env *evalEnv, ctx QueryContext) (int, string, error) {
	n := 0
	if where != nil {
		ids, label, found, err := e.chooseIndexPath(where, nil, env, ctx)
		if err != nil {
			return 0, "", err
		}
		if found {
			atCommit, err := e.atCommit(ctx)
			if err != nil {
				return 0, "", err
			}
			commit, err := e.DAG.GetCommitOrThrow(atCommit)
			if err != nil {
				return 0, "", err
			}
			for i, id := range ids {
				if ctx.RowBudget > 0 && i >= ctx.RowBudget {
					return 0, "", &ScanRowBudgetExceededError{Budget: ctx.RowBudget}
				}
				doc, err := e.Storage.GetDocument(ctx.NamespaceID, id, commit.DocumentTreeHash)
				if err != nil {
					return 0, "", err
				}
				ctx.Stats.addExamined(1)
				if doc == nil {
					continue
				}
				ctx.Stats.addDocRead(len(doc.JSON))
				if env.predicate(where, newEvalDoc(*doc)) {
					n++
				}
			}
			return n, label, nil
		}
	}
	atCommit, err := e.atCommit(ctx)
	if err != nil {
		return 0, "", err
	}
	commit, err := e.DAG.GetCommitOrThrow(atCommit)
	if err != nil {
		return 0, "", err
	}
	examined := 0
	err = e.Storage.ScanDocuments(ctx.NamespaceID, commit.DocumentTreeHash, 256, func(batch []document.Document) error {
		for _, d := range batch {
			examined++
			ctx.Stats.addExamined(1)
			if ctx.RowBudget > 0 && examined > ctx.RowBudget {
				return &ScanRowBudgetExceededError{Budget: ctx.RowBudget}
			}
			ctx.Stats.addDocRead(len(d.JSON))
			if where == nil || env.predicate(where, newEvalDoc(d)) {
				n++
			}
		}
		return nil
	})
	if err != nil {
		return 0, "", err
	}
	return n, "fullscan", nil
}

// isCountStar reports whether x is COUNT(*) (or COUNT with no argument).
func isCountStar(x Expr) bool {
	fc, ok := x.(ExprFunctionCall)
	if !ok || !strings.EqualFold(fc.Name, "count") {
		return false
	}
	if len(fc.Args) == 0 {
		return true
	}
	cr, ok := fc.Args[0].(ExprColumnRef)
	return len(fc.Args) == 1 && ok && cr.Name == "*"
}

// exactConjunct is one conjunct an index serves exactly: `col = v` or `col IN (v, ...)` on string
// or boolean constants (NULLs in an IN list match nothing and are dropped).
type exactConjunct struct {
	field  string
	values []Cell
}

// exactConjunctOf recognises an exactConjunct; ok=false for anything else.
func exactConjunctOf(conj Expr, env *evalEnv) (exactConjunct, bool) {
	constCell := func(x Expr) (Cell, bool) {
		switch x.(type) {
		case ExprLiteral, ExprParameter:
		default:
			return nil, false
		}
		v := env.cell(x, newEvalDoc(emptyDoc))
		switch v.(type) {
		case CellString, CellBool, CellNull:
			return v, true
		}
		return nil, false
	}
	column := func(x Expr) (string, bool) {
		ref, ok := x.(ExprColumnRef)
		if !ok {
			return "", false
		}
		name := env.columnName(ref.Name)
		if isReservedColumn(name) {
			return "", false
		}
		return name, true
	}
	switch c := conj.(type) {
	case ExprBinary:
		if c.Op != BinaryOpEQ {
			return exactConjunct{}, false
		}
		field, ok := column(c.Left)
		other := c.Right
		if !ok {
			if field, ok = column(c.Right); !ok {
				return exactConjunct{}, false
			}
			other = c.Left
		}
		v, ok := constCell(other)
		if !ok || isNullCell(v) {
			return exactConjunct{}, false
		}
		return exactConjunct{field: field, values: []Cell{v}}, true
	case ExprIn:
		field, ok := column(c.Expr)
		if !ok {
			return exactConjunct{}, false
		}
		out := exactConjunct{field: field}
		for _, x := range c.Values {
			v, ok := constCell(x)
			if !ok {
				return exactConjunct{}, false
			}
			if !isNullCell(v) {
				out.values = append(out.values, v)
			}
		}
		return out, true
	}
	return exactConjunct{}, false
}

// countByIndex counts the documents an AND of exactConjuncts selects, from indexes alone: a single
// equality by its index count, anything else by intersecting the conjuncts' id sets.
func (e *Executor) countByIndex(where Expr, env *evalEnv, ctx QueryContext) (int, bool) {
	counter, ok := e.IndexProvider.(ExactCounter)
	if !ok || e.IndexProvider == nil {
		return 0, false
	}
	conjs := conjuncts(where)
	parts := make([]exactConjunct, 0, len(conjs))
	for _, c := range conjs {
		p, ok := exactConjunctOf(c, env)
		if !ok {
			return 0, false
		}
		parts = append(parts, p)
	}
	if len(parts) == 1 && len(parts[0].values) == 1 {
		n, ok, err := counter.ExactCount(ctx, parts[0].field, parts[0].values[0])
		if err != nil || !ok {
			return 0, false
		}
		return n, true
	}
	// Smallest conjunct first, so the running intersection starts as small as it can.
	sizes := make([]int, len(parts))
	for i, p := range parts {
		for _, v := range p.values {
			n, ok, err := counter.ExactCount(ctx, p.field, v)
			if err != nil || !ok {
				return 0, false
			}
			sizes[i] += n
		}
	}
	order := make([]int, len(parts))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool { return sizes[order[a]] < sizes[order[b]] })
	var acc map[codec.UUID]struct{}
	for _, i := range order {
		p := parts[i]
		set := make(map[codec.UUID]struct{}, sizes[i])
		for _, v := range p.values {
			ids, ok, err := e.IndexProvider.ExactLookup(ctx, p.field, v)
			if err != nil || !ok {
				return 0, false
			}
			for _, id := range ids {
				if acc == nil {
					set[id] = struct{}{}
				} else if _, in := acc[id]; in {
					set[id] = struct{}{}
				}
			}
		}
		acc = set
		if len(acc) == 0 {
			return 0, true
		}
	}
	return len(acc), true
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
	env.attach = ctx.attach
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
	if err := ctx.overRetainedLimit(); err != nil {
		return QueryResult{}, err
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

// accessPath is what chooseAccessPath resolved: candidate ids to fetch and filter, or - for a
// full scan - the documents themselves, already filtered by the residual.
type accessPath struct {
	ids     []codec.UUID
	docs    []*evalDoc
	scanned bool
	label   string
}

// chooseAccessPath picks the id source (§9.3): a search ranking when ORDER BY leads with a
// score, else the first top-level AND conjunct the provider can serve (MATCH, =, range,
// BETWEEN, IN), else a full scan. The label is reported as QueryResult.Plan.
//
// A full scan returns documents, not ids. The scan resolves every document anyway; it used to
// keep only their ids, and materialize then fetched each one again by id - a second tree walk and
// lookup per row. residual is applied during the scan, so only matching documents are held, as
// materialize would have held them.
func (e *Executor) chooseAccessPath(where Expr, orderBy []OrderItem, env *evalEnv, ctx QueryContext, stopAt int, residual Expr) (accessPath, error) {
	ids, label, found, err := e.chooseIndexPath(where, orderBy, env, ctx)
	if err != nil || found {
		return accessPath{ids: ids, label: label}, err
	}
	if e.scanIDsOnly {
		docs, err := e.fullScan(ctx, stopAt, nil, env)
		ids := make([]codec.UUID, len(docs))
		for i, d := range docs {
			ids[i] = d.doc.ID
		}
		return accessPath{ids: ids, label: "fullscan"}, err
	}
	docs, err := e.fullScan(ctx, stopAt, residual, env)
	return accessPath{docs: docs, scanned: true, label: "fullscan"}, err
}

// chooseIndexPath is chooseAccessPath without the full-scan fallback: found=false means no
// ranking, key or index serves where.
func (e *Executor) chooseIndexPath(where Expr, orderBy []OrderItem, env *evalEnv, ctx QueryContext) ([]codec.UUID, string, bool, error) {
	if len(orderBy) > 0 {
		if lead := env.resolveAlias(orderBy[0].Expr); isSearchExpr(lead) {
			if hits := env.scores[searchKey(lead)]; hits != nil {
				return rankedIDs(hits), hits.label, true, nil
			}
		}
	}
	return e.conjunctPath(where, env, ctx, true)
}

// conjunctPath serves where from the cheapest of its top-level AND conjuncts that a ranking, key
// or index can serve; found=false when none can. allowSearch says whether a search conjunct
// (MATCH, SIMILARITY) may serve: a ranking holds only as many hits as the search's depth, which
// is fine for a conjunct the residual re-checks but not for one disjunct of an OR, where the
// hits it leaves out are matches the other disjuncts' lookups do not find either.
func (e *Executor) conjunctPath(where Expr, env *evalEnv, ctx QueryContext, allowSearch bool) ([]codec.UUID, string, bool, error) {
	var (
		best      []codec.UUID
		bestLabel string
		found     bool
	)
	type candidate struct {
		conj Expr
		est  int // -1 when unknown
	}
	var cands []candidate
	for _, conj := range conjuncts(where) {
		if isSearchExpr(conj) {
			if !allowSearch {
				continue
			}
			if hits := env.scores[searchKey(conj)]; hits != nil {
				return rankedIDs(hits), hits.label, true, nil
			}
			continue
		}
		// A document id is a key, not a column to scan for: serve it directly. Needs no index
		// provider, and the residual WHERE still re-checks every candidate.
		if ids, ok := keyLookup(conj, env); ok {
			return ids, "keylookup", true, nil
		}
		// An OR may still be served without a provider when every disjunct is a key lookup.
		if e.IndexProvider == nil && !isOr(conj) {
			continue
		}
		cands = append(cands, candidate{conj: conj, est: e.estimateConjunct(conj, env, ctx)})
	}
	// Cheapest first, unknown costs last: with a good first pick the rest are skipped unless
	// they might beat it. Every servable conjunct is a correct choice (the residual WHERE
	// re-checks whatever is fetched) so only the cost differs. The first one used to be taken,
	// so `_c = 'tasks' AND taskId = 'x'` walked every task through the shared collection tag
	// when a single id lookup would have done.
	sort.SliceStable(cands, func(i, j int) bool {
		a, b := cands[i].est, cands[j].est
		if a < 0 || b < 0 {
			return b < 0 && a >= 0
		}
		return a < b
	})
	for _, c := range cands {
		if found && c.est >= len(best) {
			continue
		}
		ids, label, ok, err := e.indexLookup(c.conj, env, ctx)
		if err != nil {
			return nil, "", false, err
		}
		if ok && (!found || len(ids) < len(best)) {
			best, bestLabel, found = ids, label, true
		}
	}
	// Not truncated: the residual WHERE still runs over these, so cutting the list here would
	// drop matches. The result bound is enforced on the rows actually returned.
	return best, bestLabel, found, nil
}

// isOr reports whether x is an OR.
func isOr(x Expr) bool {
	b, ok := x.(ExprBinary)
	return ok && b.Op == BinaryOpOr
}

// disjuncts flattens the top-level OR tree of an expression.
func disjuncts(e Expr) []Expr {
	if b, ok := e.(ExprBinary); ok && b.Op == BinaryOpOr {
		return append(disjuncts(b.Left), disjuncts(b.Right)...)
	}
	return []Expr{e}
}

// orLookup serves an OR as the union of its disjuncts' access paths, when every disjunct has one
// (a disjunct that is an AND is served by its cheapest servable conjunct, and one that holds a
// nested OR by that OR's own union). Each disjunct's ids are a superset of the documents that
// satisfy it, so their union is a superset of the documents that satisfy the OR; the residual
// WHERE re-checks every candidate, as it does for any index path. One disjunct with no path makes
// the whole OR unservable: any document might satisfy that disjunct, so only a scan finds them.
//
// The union is deduplicated and in document id order, so a document matched by several
// disjuncts is fetched once and the order does not depend on which disjunct found it first.
func (e *Executor) orLookup(or Expr, env *evalEnv, ctx QueryContext) ([]codec.UUID, string, bool, error) {
	ds := disjuncts(or)
	labels := make([]string, 0, len(ds))
	seen := map[codec.UUID]struct{}{}
	var union []codec.UUID
	for _, d := range ds {
		ids, label, ok, err := e.conjunctPath(d, env, ctx, false)
		if err != nil || !ok {
			return nil, "", false, err
		}
		labels = append(labels, strings.TrimPrefix(label, "index:"))
		for _, id := range ids {
			if _, dup := seen[id]; !dup {
				seen[id] = struct{}{}
				union = append(union, id)
			}
		}
	}
	sort.Slice(union, func(i, j int) bool { return bytes.Compare(union[i].Bytes(), union[j].Bytes()) < 0 })
	return union, "index:or(" + strings.Join(labels, "|") + ")", true, nil
}

// estimateConjunct is how many ids serving conj from an index would return, or -1 when that is
// not known without fetching them. An OR's estimate is the sum of its disjuncts' (an upper bound
// on its union), known only when every disjunct's is.
func (e *Executor) estimateConjunct(conj Expr, env *evalEnv, ctx QueryContext) int {
	if isOr(conj) {
		total := 0
		for _, d := range disjuncts(conj) {
			n := e.estimatePredicate(d, env, ctx)
			if n < 0 {
				return -1
			}
			total += n
		}
		return total
	}
	if ids, ok := keyLookup(conj, env); ok {
		return len(ids)
	}
	counter, ok := e.IndexProvider.(ExactCounter)
	if !ok {
		return -1
	}
	if in, ok := conj.(ExprIn); ok {
		return e.estimateIn(in, counter, env, ctx)
	}
	b, ok := conj.(ExprBinary)
	if !ok || b.Op != BinaryOpEQ {
		return -1
	}
	col, other := b.Left, b.Right
	if _, isCol := col.(ExprColumnRef); !isCol {
		col, other = b.Right, b.Left
	}
	ref, isCol := col.(ExprColumnRef)
	if !isCol {
		return -1
	}
	switch other.(type) {
	case ExprLiteral, ExprParameter:
	default:
		return -1
	}
	name := env.columnName(ref.Name)
	if isReservedColumn(name) {
		return -1
	}
	value := env.cell(other, newEvalDoc(emptyDoc))
	if isNullCell(value) {
		return -1
	}
	n, ok, err := counter.ExactCount(ctx, name, value)
	if err != nil || !ok {
		return -1
	}
	return n
}

// estimatePredicate is estimateConjunct for an AND of conjuncts: the smallest known estimate
// among them (conjunctPath fetches the cheapest known conjunct first, and keeps a later one only
// if it is smaller still), or -1 when none is known.
func (e *Executor) estimatePredicate(where Expr, env *evalEnv, ctx QueryContext) int {
	best := -1
	for _, c := range conjuncts(where) {
		if isSearchExpr(c) {
			continue
		}
		if n := e.estimateConjunct(c, env, ctx); n >= 0 && (best < 0 || n < best) {
			best = n
		}
	}
	return best
}

// estimateIn is the sum of the exact counts of an IN list's values (an upper bound on the union
// indexLookup serves it with), or -1 when any of them is unknown.
func (e *Executor) estimateIn(in ExprIn, counter ExactCounter, env *evalEnv, ctx QueryContext) int {
	ref, ok := in.Expr.(ExprColumnRef)
	if !ok {
		return -1
	}
	name := env.columnName(ref.Name)
	if isReservedColumn(name) {
		return -1
	}
	total := 0
	for _, v := range in.Values {
		switch v.(type) {
		case ExprLiteral, ExprParameter:
		default:
			return -1
		}
		value := env.cell(v, newEvalDoc(emptyDoc))
		if isNullCell(value) {
			continue
		}
		n, ok, err := counter.ExactCount(ctx, name, value)
		if err != nil || !ok {
			return -1
		}
		total += n
	}
	return total
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

// indexLookup tries to serve one conjunct from the provider (an OR, from the union of its
// disjuncts' paths: see orLookup).
func (e *Executor) indexLookup(conj Expr, env *evalEnv, ctx QueryContext) ([]codec.UUID, string, bool, error) {
	if isOr(conj) {
		return e.orLookup(conj, env, ctx)
	}
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
	return e.materializeUpTo(ids, residual, env, ctx, 0)
}

// materializeUpTo is materialize that stops once it holds stopAt accepted documents (0: never) -
// for ids already in the order the rows are returned in, where the first stopAt matches are the
// whole answer.
func (e *Executor) materializeUpTo(ids []codec.UUID, residual Expr, env *evalEnv, ctx QueryContext, stopAt int) ([]*evalDoc, error) {
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
		if err := ctx.overRetainedLimit(); err != nil {
			return nil, err
		}
		out = append(out, ed)
		if stopAt > 0 && len(out) >= stopAt {
			break
		}
	}
	return out, nil
}

// isKdbIDOrder reports an ORDER BY that is exactly `kdb_id` ascending.
func isKdbIDOrder(order []OrderItem, env *evalEnv) bool {
	if len(order) != 1 || !order[0].Ascending {
		return false
	}
	col, ok := order[0].Expr.(ExprColumnRef)
	return ok && env.columnName(col.Name) == colKdbID
}

// kdbIDLowerBound is the tightest `kdb_id > '<uuid>'` among where's top-level conjuncts, as the
// id a keyset scan may start after - or nil. Only a bound written as the id's canonical text
// qualifies: kdb_id compares as a string, and canonical UUID text sorts exactly as the id's
// bytes do, so skipping ids at or below it skips only rows the predicate would reject. Anything
// else (>=, an empty string, upper case, not a UUID) is left to the residual WHERE.
func kdbIDLowerBound(where Expr, env *evalEnv) *codec.UUID {
	var best *codec.UUID
	for _, c := range conjuncts(where) {
		b, ok := c.(ExprBinary)
		if !ok {
			continue
		}
		col, other, op := b.Left, b.Right, b.Op
		if _, isCol := col.(ExprColumnRef); !isCol {
			col, other, op = b.Right, b.Left, flipOp(b.Op)
		}
		ref, isCol := col.(ExprColumnRef)
		if !isCol || op != BinaryOpGT || env.columnName(ref.Name) != colKdbID {
			continue
		}
		switch other.(type) {
		case ExprLiteral, ExprParameter:
		default:
			continue
		}
		str, ok := env.cell(other, newEvalDoc(emptyDoc)).(CellString)
		if !ok {
			continue
		}
		id, err := codec.ParseUUID(str.Value)
		if err != nil || id.String() != str.Value {
			continue
		}
		if best == nil || bytes.Compare(id.Bytes(), best.Bytes()) > 0 {
			idCopy := id
			best = &idCopy
		}
	}
	return best
}

// matchingDocs resolves and materializes every document a WHERE clause accepts, using the
// index-aware access path. Shared by ResolveDocIDsForWhere and the DML executor.
func (e *Executor) matchingDocs(where Expr, from TableRef, ctx QueryContext) ([]*evalDoc, string, error) {
	q := SelectQuery{Projections: []Projection{ProjStar{}}, From: from, Where: where}
	if err := validateSelect(q, ctx.Schema); err != nil {
		return nil, "", err
	}
	env := newEvalEnv(ctx.Schema, ctx.Parameters, from)
	env.attach = ctx.attach
	if err := e.resolveSearches(q, env, ctx); err != nil {
		return nil, "", err
	}
	src, err := e.chooseAccessPath(where, nil, env, ctx, 0, where)
	if err != nil {
		return nil, "", err
	}
	if src.scanned {
		return src.docs, src.label, nil
	}
	docs, err := e.materialize(src.ids, where, env, ctx)
	return docs, src.label, err
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

// fullScan resolves every document at the resolved commit, keeping those residual accepts (all
// of them when residual is nil). stopAt > 0 lets it stop once it has that many; 0 means read
// everything. Work is bounded by ctx.RowBudget, not by MaxRows: MaxRows bounds what a query may
// *return*, and applying it here, before the residual filter, silently dropped every document
// past the first MaxRows.
//
// Each scanned document counts once toward RowsExamined and DocsRead; each kept one toward
// RetainedBytes - as the fetch-by-id path (materialize) counts the documents it fetches.
func (e *Executor) fullScan(ctx QueryContext, stopAt int, residual Expr, env *evalEnv) ([]*evalDoc, error) {
	atCommit, err := e.atCommit(ctx)
	if err != nil {
		return nil, err
	}
	commit, err := e.DAG.GetCommitOrThrow(atCommit)
	if err != nil {
		return nil, err
	}
	treeHash := commit.DocumentTreeHash
	var docs []*evalDoc
	examined := 0
	scan := e.Storage.ScanDocuments
	if ctx.keyset {
		ordered, ok := e.Storage.(storage.OrderedScanner)
		if !ok {
			return nil, storage.ErrOrderedScanUnsupported
		}
		after := ctx.scanAfter
		scan = func(ns string, at codec.Hash, batch int, onBatch func([]document.Document) error) error {
			return ordered.ScanDocumentsAfter(ns, at, after, batch, onBatch)
		}
	}
	err = scan(ctx.NamespaceID, treeHash, 256, func(batch []document.Document) error {
		for _, d := range batch {
			examined++
			ctx.Stats.addExamined(1)
			if ctx.RowBudget > 0 && examined > ctx.RowBudget {
				return &ScanRowBudgetExceededError{Budget: ctx.RowBudget}
			}
			ctx.Stats.addDocRead(len(d.JSON))
			ed := newEvalDoc(d)
			if residual != nil && !env.predicate(residual, ed) {
				continue
			}
			ctx.Stats.addRetained(int64(len(d.JSON)) + retainedRowOverheadBytes)
			if err := ctx.overRetainedLimit(); err != nil {
				return err
			}
			docs = append(docs, ed)
			if stopAt > 0 && len(docs) >= stopAt {
				// Stop the scan outright rather than merely stopping appending: a query that
				// has everything it can return must not read the rest of the namespace.
				return errScanComplete
			}
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, errScanComplete) {
			return docs, nil
		}
		return nil, err
	}
	return docs, nil
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
			out = append(out, env.projectedColumn(p.Name, d))
		case ProjExpression:
			if ref, ok := p.Expr.(ExprColumnRef); ok {
				out = append(out, env.projectedColumn(ref.Name, d))
				continue
			}
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
			case col == colAttachments:
				sqlType = "JSON"
			default:
				if f, ok := sch.Field(col); ok {
					sqlType = f.Type.SQLTypeName()
				} else if sch.IsSchemaless() {
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
		case "count", "array_length", "attachment_count", "attachment_size":
			return "BIGINT"
		case "attachment", "from_base64":
			return "VARBINARY"
		case "attachment_mime":
			return "VARCHAR"
		case "avg":
			return "DOUBLE"
		case "array_contains", "array_contains_any", "exists":
			return "BOOLEAN"
		case "all_values":
			return "JSON"
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
