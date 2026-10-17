package sql

import (
	"container/heap"
	"errors"
	"math"
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/index"
	kdbjson "github.com/limidus/kdb/go/kdb/json"
)

// Ordered top-k: `ORDER BY <col> LIMIT k [OFFSET o]` served by walking a BTREE on <col> in key
// order and reading documents until the page is settled, instead of reading every candidate
// (qtask production plan U7: a 200-row page of a user with 8k tasks reads ~200 documents).
//
// The BTREE does not order documents the way ORDER BY does in general, so this path is taken only
// where the two provably agree, and topKDocs (which reads every candidate) serves everything else.
// The sort key of a document is env.cell(col) - the first candidate, unexpanded - compared with
// CompareCells: NULL < numbers (bool, long and double compared numerically) < strings < JSON.
// What the index files a document under, for a single top-level field with the natural key
// mapping (index.NaturalKeyMapping): nothing when the field is absent, an object or an array with
// no scalar element; NullKey for null; BoolKey, Int64Key, Float64Key or StringKey for a scalar;
// one key per distinct scalar element for an array.
//
// So for every document filed under at least one key, the real sort key is either one of its keys
// read as a cell (a scalar), or JSON (an array), which sorts after every key. Either way it is >=
// the smallest of its keys under CompareCells. A document filed under no key has the sort key
// NULL (absent) or JSON (object, empty array).
//
// The walk, ascending, visits the candidates in groups whose lower bounds never decrease:
//
//  1. every candidate filed under no key (lower bound NULL), read up front;
//  2. the NullKey group (NULL);
//  3. the numeric keys - the Bool, Int64 and Float64 runs of the index, which the index keeps
//     apart by type, merged by CompareCells so they come out in numeric order;
//  4. the string keys.
//
// A candidate is read at its first (smallest) key; reading it computes its real sort key, which
// is >= that group's bound. It stops before a group whose bound is strictly greater than the sort
// key of the k-th best row held: every candidate not yet read has all its keys in that group or
// later ones, so its sort key is greater too, and it cannot be among the first k. Rows tied with
// the k-th on the first ORDER BY item are all read (their later items and candidate order decide
// between them), and the kept rows go back in candidate order for the caller's stable sort, so
// ties break exactly as the full sort breaks them.
//
// Not served, and why:
//   - DESC on the first item: arrays sort first (JSON), and a one-element array is filed exactly as
//     the scalar it holds, so nothing short of reading every candidate finds them.
//   - no index path for the WHERE (a full scan): the documents filed under no key cannot be listed
//     without reading the namespace, and they sort first (NULL).
//   - a dotted path, a reserved column, an expression: the key extraction differs or is unknown.
//   - an index with a field type (schema-derived; TIMESTAMP strings become TimestampKeys, whose
//     order is not the string order), or holding any key of another type.
//   - a numeric key beyond ±2^53, where long and double comparison is not exact.
//   - a read behind the head that needs the index's undo window.
// In each case nothing has been read yet, or (the last two, found mid-walk) the documents already
// read are handed to topKDocs so none is read twice.

// IndexKeyKind is one family of BTREE keys, in the order the ordered top-k walks them.
type IndexKeyKind uint8

const (
	IndexKeyNull IndexKeyKind = iota
	IndexKeyBool
	IndexKeyInt
	IndexKeyFloat
	IndexKeyString
)

// IndexKeyGroup is one BTREE key, as the cell ORDER BY compares (a boolean as CellLong 0/1), and
// the documents filed under it in UUID order.
type IndexKeyGroup struct {
	Key Cell
	IDs []codec.UUID
}

// OrderedKeyIndex is an optional IndexProvider capability: a BTREE on a field read in key order,
// key by key. Both methods report ok=false when the field has no BTREE that qualifies (single
// top-level field, natural key mapping, only keys of the five kinds) or the read cannot be served
// exactly at ctx.AtCommit.
type OrderedKeyIndex interface {
	// IndexKeyGroups returns the keys of kind after after (from the first when nil), ascending,
	// whole keys until at least maxIDs ids are listed; an empty result means none remain.
	IndexKeyGroups(ctx QueryContext, field string, kind IndexKeyKind, after Cell, maxIDs int) ([]IndexKeyGroup, bool, error)
	// UnindexedDocs returns the ids filed under no key, in the order given.
	UnindexedDocs(ctx QueryContext, field string, ids []codec.UUID) ([]codec.UUID, bool, error)
}

// keyGroupStore is what the ordered top-k needs from an index store (index.MemoryStore).
type keyGroupStore interface {
	index.Store
	KeyGroups(kind, after index.Key, atCommit *codec.Hash, maxIDs int) ([]index.KeyBucket, error)
	Unfiled(ids []codec.UUID, atCommit *codec.Hash) ([]codec.UUID, error)
	KeysOnlyOfTypes(kinds []index.Key, atCommit *codec.Hash) (bool, error)
}

var naturalKinds = []index.Key{index.NullKey{}, index.BoolKey{}, index.Int64Key{}, index.Float64Key{}, index.StringKey{}}

// orderedStore checks that store qualifies for the ordered top-k on field.
func orderedStore(store index.Store, field string) (keyGroupStore, bool) {
	s, ok := store.(keyGroupStore)
	if !ok {
		return nil, false
	}
	d := store.Descriptor()
	if d.Type != index.IndexTypeBTree || len(d.FieldPaths()) != 1 || d.FirstField() != field {
		return nil, false
	}
	if segs := index.SplitFieldPath(field); len(segs) != 1 || segs[0] != field {
		return nil, false
	}
	if !index.NaturalKeyMapping(d.StringOption(index.OptionFieldType, "")) {
		return nil, false
	}
	return s, true
}

func unsupportedRead(err error) bool {
	return errors.Is(err, index.ErrKeyGroupsUnsupported) || errors.Is(err, index.ErrHistoryUnavailable)
}

// IndexKeyGroupsFromStore serves OrderedKeyIndex.IndexKeyGroups from a BTREE store, for a
// provider that has resolved the field's store.
func IndexKeyGroupsFromStore(store index.Store, ctx QueryContext, field string, kind IndexKeyKind, after Cell, maxIDs int) ([]IndexKeyGroup, bool, error) {
	s, ok := orderedStore(store, field)
	if !ok {
		return nil, false, nil
	}
	var sample, from index.Key
	switch kind {
	case IndexKeyNull:
		sample = index.NullKey{}
		if after != nil {
			from = index.NullKey{}
		}
	case IndexKeyBool:
		sample = index.BoolKey{}
		if c, ok := after.(CellLong); ok {
			from = index.BoolKey{Value: c.Value != 0}
		}
	case IndexKeyInt:
		sample = index.Int64Key{}
		if c, ok := after.(CellLong); ok {
			from = index.Int64Key{Value: c.Value}
		}
	case IndexKeyFloat:
		sample = index.Float64Key{}
		if c, ok := after.(CellDouble); ok {
			from = index.Float64Key{Value: c.Value}
		}
	case IndexKeyString:
		sample = index.StringKey{}
		if c, ok := after.(CellString); ok {
			from = index.StringKey{Value: c.Value}
		}
	default:
		return nil, false, nil
	}
	if after != nil && from == nil {
		return nil, false, nil
	}
	buckets, err := s.KeyGroups(sample, from, ctx.AtCommit, maxIDs)
	if unsupportedRead(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
	}
	out := make([]IndexKeyGroup, 0, len(buckets))
	for _, b := range buckets {
		var c Cell
		switch k := b.Key.(type) {
		case index.NullKey:
			c = CellNull{}
		case index.BoolKey:
			c = CellLong{Value: boolToInt64(k.Value)}
		case index.Int64Key:
			c = CellLong{Value: k.Value}
		case index.Float64Key:
			c = CellDouble{Value: k.Value}
		case index.StringKey:
			c = CellString{Value: k.Value}
		default:
			return nil, false, nil
		}
		out = append(out, IndexKeyGroup{Key: c, IDs: b.DocIDs})
	}
	return out, true, nil
}

// UnindexedDocsFromStore serves OrderedKeyIndex.UnindexedDocs from a BTREE store. It also
// refuses (ok=false) an index holding a key of a type outside the five kinds: a document filed
// only there would be neither unindexed nor in any group the walk reads.
func UnindexedDocsFromStore(store index.Store, ctx QueryContext, field string, ids []codec.UUID) ([]codec.UUID, bool, error) {
	s, ok := orderedStore(store, field)
	if !ok {
		return nil, false, nil
	}
	only, err := s.KeysOnlyOfTypes(naturalKinds, ctx.AtCommit)
	if unsupportedRead(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
	}
	if !only {
		return nil, false, nil
	}
	out, err := s.Unfiled(ids, ctx.AtCommit)
	if unsupportedRead(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
	}
	return out, true, nil
}

// The walk's budget. It reads index entries in namespace-wide key order, so how many it passes
// before the page settles depends on where the candidates' keys fall among everyone else's -
// which nothing at plan time knows: the same query is 7x faster walking when the candidates sort
// early and 5x slower when they sort behind most of the namespace (#318). The bounded top-k it
// competes with costs one document read per candidate, and passing an index entry costs about a
// tenth of a read (BenchmarkOrderedWalkVsTopK: ~75 ns against ~750 ns in memory; reads from disk
// cost more). So the walk gives up once it has passed walkEntriesPerCandidate entries per
// candidate: a walk that would settle sooner is kept, and one that would not adds about half the
// top-k's own cost to it (+65% in the benchmark's worst case, in memory; less where reads come
// from disk) - the documents it read are handed over, not read again.
const (
	walkEntriesPerCandidate = 4
	walkEntriesFloor        = 256
)

// maxOrderedCandidates bounds the candidate list the ordered top-k indexes by id; past it,
// topKDocs streams them.
const maxOrderedCandidates = 1 << 21

// exactNumericBound is the magnitude past which a long and a double no longer compare exactly.
const exactNumericBound = 1 << 53

// orderedField is the BTREE field the ordered top-k can walk for query, or "".
func orderedField(query SelectQuery, env *evalEnv) string {
	if len(query.OrderBy) == 0 || !query.OrderBy[0].Ascending {
		return ""
	}
	ref, ok := env.resolveAlias(query.OrderBy[0].Expr).(ExprColumnRef)
	if !ok {
		return ""
	}
	name := env.columnName(ref.Name)
	if isReservedColumn(name) {
		return ""
	}
	if segs := kdbjson.SplitDotted(name); len(segs) != 1 || segs[0] != name {
		return ""
	}
	return name
}

// keyGroupCursor pages one kind of key out of the index.
type keyGroupCursor struct {
	kind  IndexKeyKind
	buf   []IndexKeyGroup
	i     int
	after Cell
	done  bool
	page  int
}

// head returns the cursor's next group without consuming it; nil when exhausted.
func (c *keyGroupCursor) head(oi OrderedKeyIndex, ctx QueryContext, field string) (*IndexKeyGroup, bool, error) {
	if c.i < len(c.buf) {
		return &c.buf[c.i], true, nil
	}
	if c.done {
		return nil, true, nil
	}
	if c.page == 0 {
		c.page = 256
	} else if c.page < 1<<16 {
		c.page *= 2
	}
	groups, ok, err := oi.IndexKeyGroups(ctx, field, c.kind, c.after, c.page)
	if err != nil || !ok {
		return nil, ok, err
	}
	c.buf, c.i = groups, 0
	if len(groups) == 0 {
		c.done = true
		return nil, true, nil
	}
	c.after = groups[len(groups)-1].Key
	return &c.buf[0], true, nil
}

// btreeTopK serves the top k of the candidates ids by walking the BTREE on the first ORDER BY
// column (see the comment at the top of this file). ok=false means it does not apply, or gave up
// part way; fetched then holds every document it read (nil for one that does not exist), for
// topKDocs to reuse rather than read again. abandoned reports a walk that started and was given
// up for exceeding its budget (walkEntriesPerCandidate), so the plan label can say so.
func (e *Executor) btreeTopK(query SelectQuery, residual Expr, env *evalEnv, ctx QueryContext, k int, ids []codec.UUID, atCommit codec.Hash, treeHash codec.Hash, fetched map[codec.UUID]*document.Document) (docs []*evalDoc, ok, abandoned bool, err error) {
	docs, ok, err = e.btreeTopKWalk(query, residual, env, ctx, k, ids, atCommit, treeHash, fetched, &abandoned)
	return docs, ok, abandoned, err
}

func (e *Executor) btreeTopKWalk(query SelectQuery, residual Expr, env *evalEnv, ctx QueryContext, k int, ids []codec.UUID, atCommit codec.Hash, treeHash codec.Hash, fetched map[codec.UUID]*document.Document, abandoned *bool) ([]*evalDoc, bool, error) {
	field := orderedField(query, env)
	if field == "" || k <= 0 || len(ids) <= k || len(ids) > maxOrderedCandidates {
		return nil, false, nil
	}
	oi, ok := e.IndexProvider.(OrderedKeyIndex)
	if !ok {
		return nil, false, nil
	}
	pos := make(map[codec.UUID]int, len(ids))
	for i, id := range ids {
		if _, dup := pos[id]; dup {
			return nil, false, nil // topKDocs would offer it twice; keep its answer
		}
		pos[id] = i
	}
	// One commit for the index and the documents both.
	ictx := ctx
	ictx.AtCommit = &atCommit
	unfiled, ok, err := oi.UnindexedDocs(ictx, field, ids)
	if err != nil || !ok {
		return nil, false, err
	}
	if len(unfiled) > len(ids)/2 {
		return nil, false, nil // most candidates are read whatever the index says
	}

	orderExprs := make([]Expr, len(query.OrderBy))
	for i, o := range query.OrderBy {
		orderExprs[i] = env.resolveAlias(o.Expr)
	}
	h := &topKHeap{order: query.OrderBy}
	done := make(map[codec.UUID]bool)
	examined := 0
	read := func(id codec.UUID) error {
		if done[id] {
			return nil
		}
		done[id] = true
		examined++
		if residual != nil && ctx.RowBudget > 0 && examined > ctx.RowBudget {
			return &ScanRowBudgetExceededError{Budget: ctx.RowBudget}
		}
		doc, err := e.Storage.GetDocument(ctx.NamespaceID, id, treeHash)
		if err != nil {
			return err
		}
		fetched[id] = doc
		if residual != nil {
			ctx.Stats.addExamined(1)
		}
		if doc == nil {
			return nil
		}
		ctx.Stats.addDocRead(len(doc.JSON))
		ed := newEvalDoc(*doc)
		if residual != nil && !env.predicate(residual, ed) {
			return nil
		}
		keys := make([]Cell, len(orderExprs))
		for i, ex := range orderExprs {
			keys[i] = env.cell(ex, ed)
		}
		ent := topKEntry{keys: keys, seq: pos[id], doc: ed}
		if h.Len() < k {
			heap.Push(h, ent)
		} else if h.less(ent, h.entries[0]) {
			h.entries[0] = ent
			heap.Fix(h, 0)
		}
		return nil
	}
	// settled: a group whose keys are all >= bound cannot place a row among the first k.
	settled := func(bound Cell) bool {
		return h.Len() == k && CompareCells(bound, h.entries[0].keys[0]) > 0
	}

	// 1. Candidates filed under no key: NULL (absent) or JSON, read in candidate order.
	sort.Slice(unfiled, func(i, j int) bool { return pos[unfiled[i]] < pos[unfiled[j]] })
	for _, id := range unfiled {
		if err := read(id); err != nil {
			return nil, false, err
		}
	}

	walked := 0
	walkCap := walkEntriesPerCandidate*len(ids) + walkEntriesFloor
	visit := func(g *IndexKeyGroup) (bool, error) {
		switch c := g.Key.(type) {
		case CellLong:
			if c.Value > exactNumericBound || c.Value < -exactNumericBound {
				return false, nil
			}
		case CellDouble:
			if math.Abs(c.Value) > exactNumericBound {
				return false, nil
			}
		}
		walked += len(g.IDs)
		if walked > walkCap {
			*abandoned = true
			return false, nil
		}
		for _, id := range g.IDs {
			if _, in := pos[id]; in {
				if err := read(id); err != nil {
					return false, err
				}
			}
		}
		return true, nil
	}

	// 2-4. The key runs, in CompareCells order: null, the three numeric runs merged, strings.
	phases := [][]IndexKeyKind{{IndexKeyNull}, {IndexKeyBool, IndexKeyInt, IndexKeyFloat}, {IndexKeyString}}
	for _, kinds := range phases {
		cursors := make([]*keyGroupCursor, len(kinds))
		for i, kd := range kinds {
			cursors[i] = &keyGroupCursor{kind: kd}
		}
		for {
			var best *IndexKeyGroup
			var bestCur *keyGroupCursor
			for _, c := range cursors {
				g, ok, err := c.head(oi, ictx, field)
				if err != nil {
					return nil, false, err
				}
				if !ok {
					return nil, false, nil
				}
				if g != nil && (best == nil || CompareCells(g.Key, best.Key) < 0) {
					best, bestCur = g, c
				}
			}
			if best == nil {
				break
			}
			if settled(best.Key) {
				return keptInCandidateOrder(h, ctx)
			}
			bestCur.i++
			if ok, err := visit(best); err != nil || !ok {
				return nil, false, err
			}
		}
	}
	// Every candidate has been read: the heap holds the answer.
	return keptInCandidateOrder(h, ctx)
}

// keptInCandidateOrder is the heap's rows back in candidate order, as topKDocs returns them.
func keptInCandidateOrder(h *topKHeap, ctx QueryContext) ([]*evalDoc, bool, error) {
	kept := make([]topKEntry, len(h.entries))
	copy(kept, h.entries)
	sortTopK(kept)
	out := make([]*evalDoc, len(kept))
	var held int64
	for i, ent := range kept {
		out[i] = ent.doc
		held += int64(len(ent.doc.doc.JSON)) + retainedRowOverheadBytes
	}
	ctx.Stats.addRetained(held)
	if err := ctx.overRetainedLimit(); err != nil {
		return nil, false, err
	}
	return out, true, nil
}
