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
	var out []codec.Hash
	if d.ensureGenerationsLocked() {
		out = d.removeRedundantLocked(d.paintDownToCommonLocked(as, bs, d.generationLocked), d.generationLocked)
	} else {
		out = d.removeRedundantLocked(d.commonByClosureLocked(as, bs), nil)
	}
	SortHashes(out)
	return out
}

// MergeBaseSteps is how many commits the merge-base walks (MergeBases, MergeBaseFinder) have
// stepped through so far - what they cost, as a count tests can hold a bound to.
func (d *InMemoryCommitDag) MergeBaseSteps() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.mergeBaseSteps
}

// stepLocked is expandParentsLocked for the merge-base walks, counted. Must hold mu exclusively.
func (d *InMemoryCommitDag) stepLocked(h codec.Hash) []codec.Hash {
	d.mergeBaseSteps++
	return d.expandParentsLocked(h)
}

// MergeBaseFinder answers MergeBases many times over one stretch of history, which is what a
// virtual merge base does: it asks for the merge bases of the merge bases, level after level
// (peersync's virtualBase). Without AncestryPruning every one of those calls compared full ancestor
// closures - all of history, per call, and per pair of candidates - and a criss-cross-heavy mesh
// makes dozens of calls per merge, all inside the write gate: one node spent minutes there,
// failing every other sync (5-node procedure-counter load). The finder gives such a DAG the
// generation numbers it lacks, computed once on the first call (one walk of the history below the
// commits asked about) and kept, so every call is the bounded generation walk MergeBases makes
// with AncestryPruning on. With it on, the finder just uses the DAG's own.
//
// The answers are MergeBases' answers - the nearest common ancestors are a function of the graph -
// for as long as the history it has seen does not change underneath it, which is why it is meant
// for one computation and then dropped: a parent arriving after its child was numbered would
// leave the child's generation too low. Safe for concurrent use (it works under the DAG's lock).
type MergeBaseFinder struct {
	d *InMemoryCommitDag
	// gen is the generation of every commit numbered so far, when the DAG has none of its own:
	// one more than the highest of the parents the walks descend into, 1 for a commit with none.
	gen map[codec.Hash]uint32
}

// NewMergeBaseFinder returns a finder over d's current history; see MergeBaseFinder.
func (d *InMemoryCommitDag) NewMergeBaseFinder() *MergeBaseFinder {
	return &MergeBaseFinder{d: d}
}

// MergeBases is InMemoryCommitDag.MergeBases.
func (f *MergeBaseFinder) MergeBases(as, bs []codec.Hash) []codec.Hash {
	d := f.d
	d.mu.Lock()
	defer d.mu.Unlock()
	gen := f.generationLocked
	if d.ensureGenerationsLocked() {
		gen = d.generationLocked
	}
	out := d.removeRedundantLocked(d.paintDownToCommonLocked(as, bs, gen), gen)
	SortHashes(out)
	return out
}

// generationLocked numbers h, and every not yet numbered commit below it, in one iterative pass
// (a history is as deep as its commit count, too deep to recurse). Must hold d.mu.
func (f *MergeBaseFinder) generationLocked(h codec.Hash) uint32 {
	if g, ok := f.gen[h]; ok {
		return g
	}
	if f.gen == nil {
		f.gen = map[codec.Hash]uint32{}
	}
	d := f.d
	active := map[codec.Hash]bool{}
	stack := []codec.Hash{h}
	for len(stack) > 0 {
		x := stack[len(stack)-1]
		if _, ok := f.gen[x]; ok {
			stack = stack[:len(stack)-1]
			continue
		}
		parents := d.stepLocked(x)
		if active[x] {
			// Every parent is numbered: settle this one.
			var max uint32
			for _, p := range parents {
				if g := f.gen[p]; g > max {
					max = g
				}
			}
			f.gen[x] = max + 1
			delete(active, x)
			stack = stack[:len(stack)-1]
			continue
		}
		active[x] = true
		for _, p := range parents {
			// A parent still active would be a cycle, which hash links cannot form.
			if _, ok := f.gen[p]; !ok && !active[p] {
				stack = append(stack, p)
			}
		}
	}
	return f.gen[h]
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
// popped - so gen must be higher for a commit than for each of its parents. The result may still
// hold a candidate that is an ancestor of another (reached along a path that did not pass through
// the other); removeRedundantLocked drops those.
func (d *InMemoryCommitDag) paintDownToCommonLocked(as, bs []codec.Hash, gen func(codec.Hash) uint32) []codec.Hash {
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
		heap.Push(q, genItem{hash: h, gen: gen(h)})
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
		for _, p := range d.stepLocked(h) {
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
		closure := d.ancestorClosureLocked(a)
		d.mergeBaseSteps += uint64(len(closure))
		for h := range closure {
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
		queue = append(queue, d.stepLocked(h)...)
	}
	return out
}

// removeRedundantLocked keeps the candidates that are not an ancestor of another candidate, in one
// walk down from all their parents: a candidate that walk reaches is below another. With gen (a
// generation higher for a commit than for each of its parents) the walk stops at the oldest
// candidate's generation, below which no candidate can be; without it, it is one ancestor closure -
// not, as comparing pairs was, one per pair of candidates.
func (d *InMemoryCommitDag) removeRedundantLocked(cands []codec.Hash, gen func(codec.Hash) uint32) []codec.Hash {
	isCand := make(map[codec.Hash]bool, len(cands))
	uniq := make([]codec.Hash, 0, len(cands))
	for _, h := range cands {
		if !isCand[h] {
			isCand[h] = true
			uniq = append(uniq, h)
		}
	}
	if len(uniq) < 2 {
		return uniq
	}
	var floor uint32
	if gen != nil {
		floor = gen(uniq[0])
		for _, h := range uniq[1:] {
			if g := gen(h); g < floor {
				floor = g
			}
		}
	}
	below := map[codec.Hash]bool{}
	visited := map[codec.Hash]bool{}
	var stack []codec.Hash
	for _, h := range uniq {
		stack = append(stack, d.stepLocked(h)...)
	}
	for len(stack) > 0 {
		h := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if visited[h] {
			continue
		}
		visited[h] = true
		if isCand[h] {
			below[h] = true
		}
		if gen != nil && gen(h) <= floor {
			continue // everything below h is older than every candidate
		}
		stack = append(stack, d.stepLocked(h)...)
	}
	var out []codec.Hash
	for _, h := range uniq {
		if !below[h] {
			out = append(out, h)
		}
	}
	return out
}
