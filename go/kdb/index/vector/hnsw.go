package vector

import (
	"container/heap"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
)

// node is one stored vector version in the graph. Nodes are never removed: a deleted or
// superseded version stays navigable and is filtered out of results by visibility.
type node struct {
	id    int
	doc   codec.UUID
	seq   int64
	vec   []float32
	level int
	links [][]int32 // per level, neighbour node ids
}

// hnsw is a Hierarchical Navigable Small World graph over nodes with a deterministic level
// assignment (§7): level = floor(−ln(u) / ln(m)) with u ∈ (0, 1] from the first 8 bytes of
// sha256(docId), so the same corpus builds the same graph whatever the insertion order.
type hnsw struct {
	metric   Metric
	m, mMax0 int
	efC      int
	nodes    []*node
	entry    int
	maxLevel int
}

func newHNSW(metric Metric, m, efConstruction int) *hnsw {
	return &hnsw{metric: metric, m: m, mMax0: 2 * m, efC: efConstruction, entry: -1}
}

// levelFor is the deterministic level rule.
func levelFor(docID codec.UUID, m int) int {
	sum := sha256.Sum256(docID.Bytes())
	x := binary.BigEndian.Uint64(sum[:8])
	u := (float64(x) + 1) / math.Ldexp(1, 64) // (0, 1]
	if u >= 1 {
		return 0
	}
	return int(math.Floor(-math.Log(u) / math.Log(float64(m))))
}

type scored struct {
	id    int
	score float64
}

// scoredHeap orders by (score, then id) so ties resolve identically across runs. better
// reports whether a ranks before b at the top of the heap.
type scoredHeap struct {
	items  []scored
	better func(a, b scored) bool
}

func (h *scoredHeap) Len() int           { return len(h.items) }
func (h *scoredHeap) Less(i, j int) bool { return h.better(h.items[i], h.items[j]) }
func (h *scoredHeap) Swap(i, j int)      { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *scoredHeap) Push(x interface{}) { h.items = append(h.items, x.(scored)) }
func (h *scoredHeap) Pop() (x interface{}) {
	n := len(h.items)
	x = h.items[n-1]
	h.items = h.items[:n-1]
	return
}
func (h *scoredHeap) top() scored { return h.items[0] }

func bestFirst(a, b scored) bool {
	if a.score != b.score {
		return a.score > b.score
	}
	return a.id < b.id
}

func worstFirst(a, b scored) bool {
	if a.score != b.score {
		return a.score < b.score
	}
	return a.id > b.id
}

func (g *hnsw) score(q []float32, id int) float64 { return score64(g.metric, q, g.nodes[id].vec) }

// insert adds n (already appended to g.nodes) to the graph.
func (g *hnsw) insert(n *node) {
	n.level = levelFor(n.doc, g.m)
	n.links = make([][]int32, n.level+1)
	if g.entry < 0 {
		g.entry = n.id
		g.maxLevel = n.level
		return
	}
	ep := g.entry
	for l := g.maxLevel; l > n.level; l-- {
		ep = g.greedy(n.vec, ep, l)
	}
	top := n.level
	if g.maxLevel < top {
		top = g.maxLevel
	}
	for l := top; l >= 0; l-- {
		w := g.searchLayer(n.vec, []int{ep}, g.efC, l, nil)
		limit := g.m
		if len(w) < limit {
			limit = len(w)
		}
		n.links[l] = make([]int32, 0, limit)
		for _, c := range w[:limit] {
			n.links[l] = append(n.links[l], int32(c.id))
		}
		maxLinks := g.m
		if l == 0 {
			maxLinks = g.mMax0
		}
		for _, c := range w[:limit] {
			e := g.nodes[c.id]
			e.links[l] = append(e.links[l], int32(n.id))
			if len(e.links[l]) > maxLinks {
				g.prune(e, l, maxLinks)
			}
		}
		ep = w[0].id
	}
	if n.level > g.maxLevel {
		g.entry = n.id
		g.maxLevel = n.level
	}
}

// prune keeps e's best maxLinks neighbours at layer l.
func (g *hnsw) prune(e *node, l, maxLinks int) {
	cands := make([]scored, len(e.links[l]))
	for i, id := range e.links[l] {
		cands[i] = scored{id: int(id), score: g.score(e.vec, int(id))}
	}
	sort.Slice(cands, func(i, j int) bool { return bestFirst(cands[i], cands[j]) })
	e.links[l] = e.links[l][:0]
	for _, c := range cands[:maxLinks] {
		e.links[l] = append(e.links[l], int32(c.id))
	}
}

// greedy walks layer l from ep to a local best for q.
func (g *hnsw) greedy(q []float32, ep, l int) int {
	cur := ep
	curScore := g.score(q, cur)
	for {
		improved := false
		for _, id := range g.nodes[cur].links[l] {
			s := g.score(q, int(id))
			if s > curScore || (s == curScore && int(id) < cur) {
				cur, curScore = int(id), s
				improved = true
			}
		}
		if !improved {
			return cur
		}
	}
}

// searchLayer is the standard beam search at layer l returning up to ef results best-first.
// Every node is traversed, but only nodes passing accept (nil = all) enter the result set.
func (g *hnsw) searchLayer(q []float32, eps []int, ef, l int, accept func(*node) bool) []scored {
	visited := make(map[int]struct{}, ef*4)
	cands := &scoredHeap{better: bestFirst}
	results := &scoredHeap{better: worstFirst}
	for _, ep := range eps {
		visited[ep] = struct{}{}
		s := scored{id: ep, score: g.score(q, ep)}
		heap.Push(cands, s)
		if accept == nil || accept(g.nodes[ep]) {
			heap.Push(results, s)
		}
	}
	for cands.Len() > 0 {
		c := heap.Pop(cands).(scored)
		if results.Len() >= ef && worstFirst(results.top(), c) && c.score < results.top().score {
			break
		}
		for _, id := range g.nodes[c.id].links[l] {
			e := int(id)
			if _, seen := visited[e]; seen {
				continue
			}
			visited[e] = struct{}{}
			s := scored{id: e, score: g.score(q, e)}
			if results.Len() < ef || s.score > results.top().score {
				heap.Push(cands, s)
				if accept == nil || accept(g.nodes[e]) {
					heap.Push(results, s)
					if results.Len() > ef {
						heap.Pop(results)
					}
				}
			}
		}
	}
	out := append([]scored(nil), results.items...)
	sort.Slice(out, func(i, j int) bool { return bestFirst(out[i], out[j]) })
	return out
}

// search returns up to k accepted nodes nearest q, best-first.
func (g *hnsw) search(q []float32, k, efSearch int, accept func(*node) bool) []scored {
	if g.entry < 0 || k <= 0 {
		return nil
	}
	ep := g.entry
	for l := g.maxLevel; l > 0; l-- {
		ep = g.greedy(q, ep, l)
	}
	ef := efSearch
	if ef < k {
		ef = k
	}
	w := g.searchLayer(q, []int{ep}, ef, 0, accept)
	if len(w) > k {
		w = w[:k]
	}
	return w
}
