package dag

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// commitLocked returns the resident commit for hash. Must hold mu.
func (d *InMemoryCommitDag) commitLocked(hash codec.Hash) (document.Commit, bool) {
	n, ok := d.nodes[hash]
	return n.commit, ok
}

// isResidentLocked reports whether hash names a resident commit - not a
// stub, and not merely known to some branch. Must hold mu.
func (d *InMemoryCommitDag) isResidentLocked(hash codec.Hash) bool {
	_, ok := d.nodes[hash]
	return ok
}

// storeCommitLocked admits a commit that is not already resident, giving
// it a generation number derived from whichever of its parents are here.
// Must hold mu.
func (d *InMemoryCommitDag) storeCommitLocked(c document.Commit) {
	gen := uint32(0)
	if d.graph.AncestryPruning {
		gen = d.deriveGenerationLocked(c)
		if !d.parentsResidentLocked(c) {
			// A commit admitted before one of its parents - which
			// RestoreCheckpoint does routinely, since a checkpoint's
			// commits arrive in map order. Its generation is provisional
			// and, worse, the parent arriving later will be given a
			// *higher* one, inverting the invariant the pruned walks rely
			// on. Recorded rather than repaired here: the repair is a
			// pass over the whole graph, and the caller may be part-way
			// through inserting thousands of commits.
			d.genStale = true
		}
	}
	d.nodes[c.Hash] = commitNode{commit: c, generation: gen}
}

// replaceCommitLocked swaps the commit stored under its own hash while
// keeping the graph metadata already derived for it. For the paths that
// put operations back onto a commit they were evicted from, where the
// commit's shape has not changed and recomputing its generation would be
// waste. Must hold mu.
func (d *InMemoryCommitDag) replaceCommitLocked(c document.Commit) {
	n, ok := d.nodes[c.Hash]
	if !ok {
		d.storeCommitLocked(c)
		return
	}
	n.commit = c
	d.nodes[c.Hash] = n
}

// parentsResidentLocked reports whether every one of c's parents is here,
// as a commit or as a stub. A stub counts: traversal cannot descend past
// one, so a stubbed parent bounds a generation exactly as a root does.
func (d *InMemoryCommitDag) parentsResidentLocked(c document.Commit) bool {
	for _, p := range c.ParentHashes {
		if _, ok := d.nodes[p]; ok {
			continue
		}
		if _, ok := d.stubs[p]; ok {
			continue
		}
		return false
	}
	return true
}

// deriveGenerationLocked returns one more than the highest generation
// among c's resident parents, and 1 for a commit with none.
//
// A parent that is a stub, or absent, contributes nothing. That is not an
// approximation: expandParentsLocked returns no parents for a stub, so no
// traversal can descend past one, and a generation that stopped there is
// exactly as far as any walk can go.
func (d *InMemoryCommitDag) deriveGenerationLocked(c document.Commit) uint32 {
	var max uint32
	for _, p := range c.ParentHashes {
		if n, ok := d.nodes[p]; ok && n.generation > max {
			max = n.generation
		}
	}
	return max + 1
}

// ensureGenerationsLocked makes the generation numbers safe to prune with,
// recomputing them if any commit was admitted before one of its parents.
// Must hold mu exclusively. Returns false when pruning is off, which is
// the caller's signal to take the unpruned path.
//
// Commit *removal* does not land here. Squash and StubCommit only ever
// take commits away, which can leave a generation larger than it needs to
// be but never smaller - and a generation that is too large only prunes
// less, so the answer stays right and the walk stays sound.
func (d *InMemoryCommitDag) ensureGenerationsLocked() bool {
	if !d.graph.AncestryPruning {
		return false
	}
	if d.genStale {
		d.recomputeGenerationsLocked()
	}
	return true
}

// recomputeGenerationsLocked rebuilds every commit's generation from the
// graph as it now stands. One pass, iterative rather than recursive
// because a namespace's history is as deep as its commit count and a
// recursive walk of two million commits overruns the stack.
//
// O(V+E), which is the same order as the single ancestor closure that one
// unpruned IsAncestor call already builds - so even a namespace that
// somehow made this run on every query would be no worse off than it is
// today.
func (d *InMemoryCommitDag) recomputeGenerationsLocked() {
	const (
		unvisited = 0
		active    = 1
		done      = 2
	)
	state := make(map[codec.Hash]uint8, len(d.nodes))
	gen := make(map[codec.Hash]uint32, len(d.nodes))

	for root := range d.nodes {
		if state[root] != unvisited {
			continue
		}
		stack := []codec.Hash{root}
		for len(stack) > 0 {
			h := stack[len(stack)-1]
			switch state[h] {
			case done:
				stack = stack[:len(stack)-1]
				continue
			case active:
				// Every parent has been resolved: settle this one.
				n := d.nodes[h]
				var max uint32
				for _, p := range n.commit.ParentHashes {
					if g, ok := gen[p]; ok && g > max {
						max = g
					}
				}
				gen[h] = max + 1
				state[h] = done
				stack = stack[:len(stack)-1]
				continue
			}
			n, ok := d.nodes[h]
			if !ok {
				// A stub or an absent parent. Contributes no generation,
				// and marking it done keeps it out of the walk.
				state[h] = done
				stack = stack[:len(stack)-1]
				continue
			}
			state[h] = active
			for _, p := range n.commit.ParentHashes {
				if state[p] == unvisited {
					stack = append(stack, p)
				}
				// A parent still active is a cycle, which a hash-linked
				// graph cannot form without a hash collision. Leaving it
				// alone resolves it as generation 0, which prunes nothing
				// rather than pruning wrongly.
			}
		}
	}

	for h, g := range gen {
		if n, ok := d.nodes[h]; ok {
			n.generation = g
			d.nodes[h] = n
		}
	}
	d.genStale = false
}

// generationLocked returns hash's generation, or 0 for a hash that is not
// a resident commit. Must hold mu.
func (d *InMemoryCommitDag) generationLocked(hash codec.Hash) uint32 {
	return d.nodes[hash].generation
}
