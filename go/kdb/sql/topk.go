package sql

import (
	"container/heap"
	"errors"
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// maxTopK is the largest LIMIT + OFFSET served by a bounded top-k; past it the ordinary sort
// runs (which the result bound refuses anyway).
const maxTopK = 100_000

// topKEligible reports a query whose ORDER BY ... LIMIT can be served by keeping only the best
// LIMIT+OFFSET rows while the candidates stream past (qtask production plan U7): ordered, limited,
// not grouped and not DISTINCT. ORDER BY kdb_id is served by id order instead.
func topKEligible(query SelectQuery, idOrder bool) (int, bool) {
	if len(query.OrderBy) == 0 || idOrder || query.Limit == nil || query.Distinct || isGroupedQuery(query) {
		return 0, false
	}
	k := *query.Limit
	if k < 0 || query.Offset < 0 || k > maxTopK || query.Offset > maxTopK-k {
		return 0, false
	}
	return k + query.Offset, true
}

// topKEntry is one candidate row: its sort keys and its position in the access path's order,
// which breaks ties exactly as the stable sort it replaces does.
type topKEntry struct {
	keys []Cell
	seq  int
	doc  *evalDoc
}

// topKHeap is a max-heap under the ORDER BY: its root is the worst row kept, the one a better
// candidate evicts.
type topKHeap struct {
	order   []OrderItem
	entries []topKEntry
}

func (h *topKHeap) less(a, b topKEntry) bool { // a sorts before b
	for i, item := range h.order {
		cmp := CompareCells(a.keys[i], b.keys[i])
		if cmp != 0 {
			if item.Ascending {
				return cmp < 0
			}
			return cmp > 0
		}
	}
	return a.seq < b.seq
}

func (h *topKHeap) Len() int           { return len(h.entries) }
func (h *topKHeap) Less(i, j int) bool { return h.less(h.entries[j], h.entries[i]) }
func (h *topKHeap) Swap(i, j int)      { h.entries[i], h.entries[j] = h.entries[j], h.entries[i] }
func (h *topKHeap) Push(x any)         { h.entries = append(h.entries, x.(topKEntry)) }
func (h *topKHeap) Pop() any {
	n := len(h.entries)
	e := h.entries[n-1]
	h.entries = h.entries[:n-1]
	return e
}

// topKDocs streams every document the access path yields that residual accepts and returns the
// best k under the ORDER BY, in candidate order (the caller sorts them). It holds at most k
// documents at a time, however many candidates match - a sort materialized all of them.
func (e *Executor) topKDocs(query SelectQuery, residual Expr, env *evalEnv, ctx QueryContext, k int) ([]*evalDoc, string, error) {
	orderExprs := make([]Expr, len(query.OrderBy))
	for i, o := range query.OrderBy {
		orderExprs[i] = env.resolveAlias(o.Expr)
	}
	h := &topKHeap{order: query.OrderBy}
	seq := 0
	offer := func(d *evalDoc) {
		if k == 0 {
			return
		}
		keys := make([]Cell, len(orderExprs))
		for i, ex := range orderExprs {
			keys[i] = env.cell(ex, d)
		}
		ent := topKEntry{keys: keys, seq: seq, doc: d}
		seq++
		if h.Len() < k {
			heap.Push(h, ent)
			return
		}
		if h.less(ent, h.entries[0]) {
			h.entries[0] = ent
			heap.Fix(h, 0)
		}
	}
	atCommit, err := e.atCommit(ctx)
	if err != nil {
		return nil, "", err
	}
	commit, err := e.DAG.GetCommitOrThrow(atCommit)
	if err != nil {
		return nil, "", err
	}
	treeHash := commit.DocumentTreeHash
	ids, label, found, err := e.chooseIndexPath(query.Where, query.OrderBy, env, ctx)
	if err != nil {
		return nil, "", err
	}
	if found {
		// A BTREE on the first ORDER BY column may settle the page after reading about k
		// documents (ordered_topk.go). When it does not apply, or gives up part way, the
		// documents it read are reused here rather than read again.
		fetched := map[codec.UUID]*document.Document{}
		if docs, ok, err := e.btreeTopK(query, residual, env, ctx, k, ids, atCommit, treeHash, fetched); err != nil {
			return nil, "", err
		} else if ok {
			return docs, label + "+order(" + orderedField(query, env) + ")", nil
		}
		for i, id := range ids {
			if residual != nil && ctx.RowBudget > 0 && i >= ctx.RowBudget {
				return nil, "", &ScanRowBudgetExceededError{Budget: ctx.RowBudget}
			}
			doc, seen := fetched[id]
			if !seen {
				doc, err = e.Storage.GetDocument(ctx.NamespaceID, id, treeHash)
				if err != nil {
					return nil, "", err
				}
				if residual != nil {
					ctx.Stats.addExamined(1)
				}
				if doc != nil {
					ctx.Stats.addDocRead(len(doc.JSON))
				}
			}
			if doc == nil {
				continue
			}
			ed := newEvalDoc(*doc)
			if residual != nil && !env.predicate(residual, ed) {
				continue
			}
			offer(ed)
		}
	} else {
		label = "fullscan"
		examined := 0
		err = e.Storage.ScanDocuments(ctx.NamespaceID, treeHash, 256, func(batch []document.Document) error {
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
				offer(ed)
			}
			return nil
		})
		if err != nil && !errors.Is(err, errScanComplete) {
			return nil, "", err
		}
	}
	// Back to candidate order, so the caller's stable sort breaks ties as it always has.
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
		return nil, "", err
	}
	return out, label, nil
}

func sortTopK(entries []topKEntry) {
	sort.Slice(entries, func(i, j int) bool { return entries[i].seq < entries[j].seq })
}
