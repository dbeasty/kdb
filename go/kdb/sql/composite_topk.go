package sql

import (
	"container/heap"
	"errors"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/index"
	kdbjson "github.com/limidus/kdb/go/kdb/json"
)

// Composite top-k: `WHERE a = 'x' AND b = 'y' [AND ...] ORDER BY c [ASC|DESC] LIMIT k` served by a
// composite BTREE on (a, b, c) the way Cassandra serves a clustering key: walk the keys under the
// prefix ('x', 'y') in c's order, forwards or backwards, and read documents only until the page is
// settled. A queue claim (`UPDATE ... ORDER BY c LIMIT 1 RETURNING`) reads one document instead of
// every pending one, and a "latest 50" reads about 50.
//
// Unlike the single-field ordered top-k (ordered_topk.go), the walk is itself the candidate
// source, so it is taken only where it provably yields every matching document, in SQL's order:
//
//   - every prefix value is a string equality: key bytes and SQL '=' then agree exactly, and a
//     document whose prefix field is absent, null or of another type cannot match;
//   - documents the index could not place by value - an array or object in any of its fields,
//     filed under index.IrregularCompositeKey - are read first, whatever their prefix, and the
//     path gives up when there are many;
//   - under the prefix, the ordered field holds at most one numeric key type (byte order is then
//     CompareCells order: NULL < numbers < strings; arrays and objects are among the irregular);
//   - the full WHERE is evaluated on every document read;
//   - the read is at a commit the index serves exactly (no undo), else nothing is walked.
//
// Rows tied on the first ORDER BY item are all read before the walk stops, and the heap breaks
// the remaining ties by walk order (key, then document id), deterministically.

// CompositeOrderedIndex is an optional IndexProvider capability: the namespace's composite BTREEs.
type CompositeOrderedIndex interface {
	CompositeBTrees() []index.Store
}

// compositeStore is what the composite top-k needs from a store (index.MemoryStore).
type compositeStore interface {
	index.Store
	CompositeGroups(prefix []index.Key, after index.Key, ascending bool, atCommit *codec.Hash, maxIDs int) ([]index.KeyBucket, error)
	CompositeSortTags(prefix []index.Key, atCommit *codec.Hash) (map[byte]bool, error)
	CompositeIrregular(atCommit *codec.Hash) ([]codec.UUID, error)
}

const (
	// compositeIrregularLimit is how many unplaceable documents a composite walk reads up front
	// before it gives the query back to the ordinary path.
	compositeIrregularLimit = 1024
	// compositeWalkFloor and compositeWalkPerRow bound how many index entries one walk may pass
	// before giving up (a selective residual over a long prefix).
	compositeWalkFloor  = 4096
	compositeWalkPerRow = 256
)

// encoding tags of the natural key types (index/keyenc.go)
const (
	tagNull, tagBool, tagInt32, tagInt64, tagFloat64, tagString = 0, 1, 2, 3, 4, 6
)

// compositePlan is a composite BTREE chosen for a query: its store, the prefix values and the
// ordered field.
type compositePlan struct {
	store     compositeStore
	prefix    []index.Key
	field     string
	ascending bool
}

// planComposite finds a composite BTREE whose fields are string equalities of where followed by
// the first ORDER BY column, preferring the longest prefix.
func (e *Executor) planComposite(query SelectQuery, env *evalEnv) (compositePlan, bool) {
	ci, ok := e.IndexProvider.(CompositeOrderedIndex)
	if !ok || len(query.OrderBy) == 0 || query.Where == nil {
		return compositePlan{}, false
	}
	ref, ok := env.resolveAlias(query.OrderBy[0].Expr).(ExprColumnRef)
	if !ok {
		return compositePlan{}, false
	}
	field := env.columnName(ref.Name)
	if isReservedColumn(field) || !topLevel(field) {
		return compositePlan{}, false
	}
	eq := map[string]string{}
	for _, conj := range conjuncts(query.Where) {
		if isSearchExpr(conj) {
			return compositePlan{}, false
		}
		if name, value, ok := stringEquality(conj, env); ok {
			eq[name] = value
		}
	}
	var best compositePlan
	for _, s := range ci.CompositeBTrees() {
		cs, ok := s.(compositeStore)
		if !ok {
			continue
		}
		fields := s.Descriptor().FieldPaths()
		if fields[len(fields)-1] != field || len(fields)-1 <= len(best.prefix) {
			continue
		}
		prefix := make([]index.Key, 0, len(fields)-1)
		for _, f := range fields[:len(fields)-1] {
			v, ok := eq[f]
			if !ok {
				break
			}
			prefix = append(prefix, index.StringKey{Value: v})
		}
		if len(prefix) != len(fields)-1 {
			continue
		}
		best = compositePlan{store: cs, prefix: prefix, field: field, ascending: query.OrderBy[0].Ascending}
	}
	return best, best.store != nil
}

func topLevel(name string) bool {
	segs := kdbjson.SplitDotted(name)
	return len(segs) == 1 && segs[0] == name
}

// stringEquality reads conj as `column = 'string'` (either side), with a literal or parameter.
func stringEquality(conj Expr, env *evalEnv) (string, string, bool) {
	b, ok := conj.(ExprBinary)
	if !ok || b.Op != BinaryOpEQ {
		return "", "", false
	}
	col, other := b.Left, b.Right
	if _, isCol := col.(ExprColumnRef); !isCol {
		col, other = other, col
	}
	ref, ok := col.(ExprColumnRef)
	if !ok {
		return "", "", false
	}
	switch other.(type) {
	case ExprLiteral, ExprParameter:
	default:
		return "", "", false
	}
	name := env.columnName(ref.Name)
	if isReservedColumn(name) || !topLevel(name) {
		return "", "", false
	}
	s, ok := env.cell(other, newEvalDoc(emptyDoc)).(CellString)
	if !ok {
		return "", "", false
	}
	return name, s.Value, true
}

// compositeKeyCell is the cell ORDER BY compares for a natural key (a boolean as CellLong 0/1).
func compositeKeyCell(k index.Key) (Cell, bool) {
	switch k := k.(type) {
	case index.NullKey:
		return CellNull{}, true
	case index.BoolKey:
		return CellLong{Value: boolToInt64(k.Value)}, true
	case index.Int64Key:
		return CellLong{Value: k.Value}, true
	case index.Float64Key:
		return CellDouble{Value: k.Value}, true
	case index.StringKey:
		return CellString{Value: k.Value}, true
	}
	return nil, false
}

// compositeTopK serves the top k by a composite BTREE walk (see the comment at the top of this
// file). ok=false means it does not apply or gave up; fetched then holds what it read.
func (e *Executor) compositeTopK(query SelectQuery, env *evalEnv, ctx QueryContext, k int, atCommit, treeHash codec.Hash, fetched map[codec.UUID]*document.Document) (docs []*evalDoc, label string, ok bool, err error) {
	if k <= 0 {
		return nil, "", false, nil
	}
	plan, ok := e.planComposite(query, env)
	if !ok {
		return nil, "", false, nil
	}
	at := &atCommit
	unsupported := func(err error) bool {
		return errors.Is(err, index.ErrKeyGroupsUnsupported) || errors.Is(err, index.ErrHistoryUnavailable)
	}
	tags, err := plan.store.CompositeSortTags(plan.prefix, at)
	if unsupported(err) {
		return nil, "", false, nil
	}
	if err != nil {
		return nil, "", false, err
	}
	numeric := 0
	for tag := range tags {
		switch tag {
		case tagNull, tagString:
		case tagBool, tagInt64, tagFloat64:
			numeric++
		default:
			return nil, "", false, nil // a key type whose byte order is not SQL's
		}
	}
	if numeric > 1 {
		return nil, "", false, nil
	}
	irregular, err := plan.store.CompositeIrregular(at)
	if unsupported(err) {
		return nil, "", false, nil
	}
	if err != nil {
		return nil, "", false, err
	}
	if len(irregular) > compositeIrregularLimit {
		return nil, "", false, nil
	}

	orderExprs := make([]Expr, len(query.OrderBy))
	for i, o := range query.OrderBy {
		orderExprs[i] = env.resolveAlias(o.Expr)
	}
	h := &topKHeap{order: query.OrderBy}
	done := make(map[codec.UUID]bool)
	seq, examined := 0, 0
	read := func(id codec.UUID) error {
		if done[id] {
			return nil
		}
		done[id] = true
		examined++
		if ctx.RowBudget > 0 && examined > ctx.RowBudget {
			return &ScanRowBudgetExceededError{Budget: ctx.RowBudget}
		}
		doc, seen := fetched[id]
		if !seen {
			var gerr error
			if doc, gerr = e.Storage.GetDocument(ctx.NamespaceID, id, treeHash); gerr != nil {
				return gerr
			}
			fetched[id] = doc
			ctx.Stats.addExamined(1)
			if doc != nil {
				ctx.Stats.addDocRead(len(doc.JSON))
			}
		}
		if doc == nil {
			return nil
		}
		ed := newEvalDoc(*doc)
		if !env.predicate(query.Where, ed) {
			return nil
		}
		keys := make([]Cell, len(orderExprs))
		for i, ex := range orderExprs {
			keys[i] = env.cell(ex, ed)
		}
		ent := topKEntry{keys: keys, seq: seq, doc: ed}
		seq++
		if h.Len() < k {
			heap.Push(h, ent)
		} else if h.less(ent, h.entries[0]) {
			h.entries[0] = ent
			heap.Fix(h, 0)
		}
		return nil
	}
	// settled: a group whose key sorts after the k-th row kept, in the walk's direction, cannot
	// place a row among the first k, and nor can any group after it.
	settled := func(bound Cell) bool {
		if h.Len() < k {
			return false
		}
		cmp := CompareCells(bound, h.entries[0].keys[0])
		if plan.ascending {
			return cmp > 0
		}
		return cmp < 0
	}

	for _, id := range irregular {
		if err := read(id); err != nil {
			return nil, "", false, err
		}
	}
	walked, walkCap := 0, compositeWalkFloor+compositeWalkPerRow*k
	var after index.Key
	page := 64
	for {
		groups, err := plan.store.CompositeGroups(plan.prefix, after, plan.ascending, at, page)
		if unsupported(err) {
			return nil, "", false, nil
		}
		if err != nil {
			return nil, "", false, err
		}
		if len(groups) == 0 {
			break
		}
		for _, g := range groups {
			bound, ok := compositeKeyCell(g.Key)
			if !ok {
				return nil, "", false, nil
			}
			if settled(bound) {
				docs, ok, err := keptInCandidateOrder(h, ctx)
				return docs, compositeLabel(plan), ok, err
			}
			walked += len(g.DocIDs)
			if walked > walkCap {
				return nil, "", false, nil
			}
			for _, id := range g.DocIDs {
				if err := read(id); err != nil {
					return nil, "", false, err
				}
			}
		}
		after = groups[len(groups)-1].Key
		if page < 1<<14 {
			page *= 4
		}
	}
	docs, ok, err = keptInCandidateOrder(h, ctx)
	return docs, compositeLabel(plan), ok, err
}

func compositeLabel(p compositePlan) string {
	dir := "asc"
	if !p.ascending {
		dir = "desc"
	}
	return "index:" + p.store.Descriptor().IndexName() + "+order(" + p.field + " " + dir + ")"
}
