package dag

import (
	"bytes"
	"container/heap"

	"github.com/limidus/kdb/go/kdb/codec"
)

// MergeBases returns the nearest common ancestors of two sets of commits: every commit that is an
// ancestor (or member) of some commit in as and of some commit in bs, and is not itself an ancestor
// of another such commit. Sorted by hash bytes; empty when the histories share nothing this DAG
// holds.
//
// CommonAncestor answers a different question - the first common ancestor breadth-first from its
// second argument - which need not be a nearest one (an older ancestor fewer hops away wins), and
// when there are several nearest ones (a criss-cross) which one it finds depends on the argument
// order. A three-way merge needs the nearest ones, all of them.
//
// The walk is the one git uses (paint_down_to_common): commits are visited newest generation
// first, each carrying which side reaches it; a commit both sides reach is a candidate, and
// everything below a candidate is marked stale, so the walk ends as soon as only stale commits
// are left - it visits the history since the merge bases, not the whole history. Without
// generation numbers (AncestryPruning off) it falls back to comparing ancestor closures.
//
// Shallow roots and stubs end the walk the same way they end every other: a node only finds the
// bases it holds.
func (d *InMemoryCommitDag) MergeBases(as, bs []codec.Hash) []codec.Hash {
	d.mu.Lock()
	defer d.mu.Unlock()
	var cands []codec.Hash
	if d.ensureGenerationsLocked() {
		cands = d.paintDownToCommonLocked(as, bs)
	} else {
		cands = d.commonByClosureLocked(as, bs)
	}
	out := d.removeRedundantLocked(cands)
	SortHashes(out)
	return out
}

const (
	paintA     = 1
	paintB     = 2
	paintStale = 4
)

type genItem struct {
	hash codec.Hash
	gen  uint32
}

// genQueue pops the highest generation first; ties go to the higher hash, so the order is a
// function of the graph alone.
type genQueue []genItem

func (q genQueue) Len() int { return len(q) }
func (q genQueue) Less(i, j int) bool {
	if q[i].gen != q[j].gen {
		return q[i].gen > q[j].gen
	}
	return bytes.Compare(q[i].hash.Bytes[:], q[j].hash.Bytes[:]) > 0
}
func (q genQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *genQueue) Push(x any)   { *q = append(*q, x.(genItem)) }
func (q *genQueue) Pop() any {
	old := *q
	it := old[len(old)-1]
	*q = old[:len(old)-1]
	return it
}

// paintDownToCommonLocked collects commits reached from both sides, newest first, without
// descending below one. Sound because a commit is popped only after every commit of a higher
// generation - and so every descendant of it - has been, so its paint is complete when it is
// popped. The result may still hold a candidate that is an ancestor of another (reached along a
// path that did not pass through the other); removeRedundantLocked drops those.
func (d *InMemoryCommitDag) paintDownToCommonLocked(as, bs []codec.Hash) []codec.Hash {
	flags := map[codec.Hash]uint8{}
	queued := map[codec.Hash]bool{}
	q := &genQueue{}
	live := 0 // queued commits not yet marked stale: the walk ends when none are left
	paint := func(h codec.Hash, f uint8) {
		old := flags[h]
		now := old | f
		if now == old {
			return
		}
		flags[h] = now
		if queued[h] {
			if old&paintStale == 0 && now&paintStale != 0 {
				live--
			}
			return // same generation, same place in the heap
		}
		queued[h] = true
		heap.Push(q, genItem{hash: h, gen: d.generationLocked(h)})
		if now&paintStale == 0 {
			live++
		}
	}
	for _, h := range as {
		paint(h, paintA)
	}
	for _, h := range bs {
		paint(h, paintB)
	}
	var out []codec.Hash
	for q.Len() > 0 && live > 0 {
		h := heap.Pop(q).(genItem).hash
		queued[h] = false
		f := flags[h]
		if f&paintStale == 0 {
			live--
			if f&(paintA|paintB) == paintA|paintB {
				out = append(out, h)
				f |= paintStale
				flags[h] = f
			}
		}
		for _, p := range d.expandParentsLocked(h) {
			paint(p, f)
		}
	}
	return out
}

// commonByClosureLocked is the candidate search without generation numbers: walk from bs,
// stopping at the first commits as reaches on each path.
func (d *InMemoryCommitDag) commonByClosureLocked(as, bs []codec.Hash) []codec.Hash {
	setA := map[codec.Hash]struct{}{}
	for _, a := range as {
		for h := range d.ancestorClosureLocked(a) {
			setA[h] = struct{}{}
		}
	}
	var out []codec.Hash
	seen := map[codec.Hash]bool{}
	queue := append([]codec.Hash(nil), bs...)
	for len(queue) > 0 {
		h := queue[0]
		queue = queue[1:]
		if seen[h] {
			continue
		}
		seen[h] = true
		if _, ok := setA[h]; ok {
			out = append(out, h)
			continue
		}
		queue = append(queue, d.expandParentsLocked(h)...)
	}
	return out
}

// removeRedundantLocked keeps the candidates that are not an ancestor of another candidate.
func (d *InMemoryCommitDag) removeRedundantLocked(cands []codec.Hash) []codec.Hash {
	uniq := make([]codec.Hash, 0, len(cands))
	seen := map[codec.Hash]bool{}
	for _, h := range cands {
		if !seen[h] {
			seen[h] = true
			uniq = append(uniq, h)
		}
	}
	if len(uniq) < 2 {
		return uniq
	}
	pruned := d.graph.AncestryPruning && !d.genStale
	var out []codec.Hash
	for i, h := range uniq {
		below := false
		for j, o := range uniq {
			if i == j {
				continue
			}
			if pruned {
				below = d.isAncestorPrunedLocked(h, o)
			} else {
				_, below = d.ancestorClosureLocked(o)[h]
			}
			if below {
				break
			}
		}
		if !below {
			out = append(out, h)
		}
	}
	return out
}
