package dag

import (
	"container/heap"
	"crypto/sha256"
	"encoding/binary"
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
)

// CommitsAbove returns the resident commits reachable from heads but not from stops - what was
// committed since stops were the heads - newest generation first (any order without
// AncestryPruning). Stops this DAG does not hold are ignored.
//
// With generation numbers it walks down from heads and stops together, in generation order, and
// stops as soon as every commit still queued is below a stop: it visits the new commits and the
// band of history beside them, not all of history (git's paint-down). Without them it falls back
// to the stops' full ancestor closure.
func (d *InMemoryCommitDag) CommitsAbove(heads, stops []codec.Hash) []codec.Hash {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.ensureGenerationsLocked() {
		excluded := map[codec.Hash]struct{}{}
		for _, s := range stops {
			if d.isResidentLocked(s) {
				for a := range d.ancestorClosureLocked(s) {
					excluded[a] = struct{}{}
				}
			}
		}
		var out []codec.Hash
		seen := map[codec.Hash]struct{}{}
		queue := append([]codec.Hash(nil), heads...)
		for len(queue) > 0 {
			h := queue[len(queue)-1]
			queue = queue[:len(queue)-1]
			if _, ok := seen[h]; ok {
				continue
			}
			seen[h] = struct{}{}
			if _, ok := excluded[h]; ok || !d.isResidentLocked(h) {
				continue
			}
			out = append(out, h)
			queue = append(queue, d.expandParentsLocked(h)...)
		}
		return out
	}

	const fromHead, fromStop = 1, 2
	flags := map[codec.Hash]uint8{}
	q := &aboveQueue{}
	active := 0 // queued entries pushed without fromStop: while there are none, nothing new is left
	push := func(h codec.Hash, f uint8) {
		if !d.isResidentLocked(h) {
			return
		}
		old := flags[h]
		if old|f == old {
			return
		}
		flags[h] = old | f
		if old|f == fromHead {
			active++
		}
		heap.Push(q, aboveEntry{h: h, gen: d.generationLocked(h), active: old|f == fromHead})
	}
	for _, s := range stops {
		push(s, fromStop)
	}
	for _, h := range heads {
		push(h, fromHead)
	}
	done := map[codec.Hash]struct{}{}
	var out []codec.Hash
	for q.Len() > 0 && active > 0 {
		e := heap.Pop(q).(aboveEntry)
		if e.active {
			active--
		}
		if _, ok := done[e.h]; ok {
			continue
		}
		done[e.h] = struct{}{}
		// Every child of e.h has a higher generation, so was popped first: its flags are final.
		f := flags[e.h]
		if f == fromHead {
			out = append(out, e.h)
		}
		for _, p := range d.expandParentsLocked(e.h) {
			push(p, f)
		}
	}
	return out
}

type aboveEntry struct {
	h      codec.Hash
	gen    uint32
	active bool
}

type aboveQueue []aboveEntry

func (q aboveQueue) Len() int           { return len(q) }
func (q aboveQueue) Less(i, j int) bool { return q[i].gen > q[j].gen }
func (q aboveQueue) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }
func (q *aboveQueue) Push(x any)        { *q = append(*q, x.(aboveEntry)) }
func (q *aboveQueue) Pop() any {
	old := *q
	e := old[len(old)-1]
	*q = old[:len(old)-1]
	return e
}

// HistoryFloor identifies where this DAG's retained history ends: its horizon (shallow roots and
// commits whose parents are gone, see Horizon) and how many commits it has archived to stubs. It
// changes when history is truncated, archived or grafted below, and not when commits are added
// above - so an index of what history references stays valid exactly while it does not change.
func (d *InMemoryCommitDag) HistoryFloor() codec.Hash {
	horizon := d.Horizon()
	sort.Slice(horizon, func(i, j int) bool { return horizon[i].Hex() < horizon[j].Hex() })
	d.mu.RLock()
	stubs := len(d.stubs)
	d.mu.RUnlock()
	sum := sha256.New()
	sum.Write([]byte("kdb-history-floor"))
	for _, h := range horizon {
		sum.Write(h.Bytes[:])
	}
	sum.Write(binary.BigEndian.AppendUint64(nil, uint64(stubs)))
	var out codec.Hash
	copy(out.Bytes[:], sum.Sum(nil))
	return out
}
