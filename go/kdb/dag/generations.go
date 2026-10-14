package dag

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// commitLocked returns the resident commit for hash. Must hold mu.
func (d *InMemoryCommitDag) commitLocked(hash codec.Hash) (document.Commit, bool) {
	return d.nodes.commit(hash)
}

// isResidentLocked reports whether hash names a resident commit - not a
// stub, and not merely known to some branch. Must hold mu.
func (d *InMemoryCommitDag) isResidentLocked(hash codec.Hash) bool {
	return d.nodes.has(hash)
}

// storeCommitLocked admits a commit that is not already resident, giving
// it a generation number derived from whichever of its parents are here.
// Must hold mu.
func (d *InMemoryCommitDag) storeCommitLocked(c document.Commit) {
	if c.AttachmentTreeHash != nil {
		d.sawAttachments = true
	}
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
	d.nodes.put(c, gen)
}

// replaceCommitLocked swaps the commit stored under its own hash while
// keeping the graph metadata already derived for it. For the paths that
// put operations back onto a commit they were evicted from, where the
// commit's shape has not changed and recomputing its generation would be
// waste. Must hold mu.
func (d *InMemoryCommitDag) replaceCommitLocked(c document.Commit) {
	if !d.nodes.replace(c) {
		d.storeCommitLocked(c)
	}
}

// parentsResidentLocked reports whether every one of c's parents is here,
// as a commit or as a stub. A stub counts: traversal cannot descend past
// one, so a stubbed parent bounds a generation exactly as a root does.
func (d *InMemoryCommitDag) parentsResidentLocked(c document.Commit) bool {
	if _, ok := d.shallow[c.Hash]; ok {
		return true // admitted without its parents on purpose; nothing to wait for
	}
	for _, p := range c.ParentHashes {
		if d.nodes.has(p) {
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
		if g := d.nodes.generation(p); d.nodes.has(p) && g > max {
			max = g
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
	// Only the resident tier is recomputed. Graph segments are written after
	// ensureGenerationsLocked and nothing below them can arrive late, so their
	// generations are sound and a walk that reaches one stops there - which is
	// what keeps this proportional to the resident tier rather than to all of
	// history once the graph is on disk.
	state := make(map[codec.Hash]uint8, d.nodes.residentLen())
	gen := make(map[codec.Hash]uint32, d.nodes.residentLen())

	roots := make([]codec.Hash, 0, d.nodes.residentLen())
	d.nodes.forEachResident(func(h codec.Hash) { roots = append(roots, h) })
	for _, root := range roots {
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
				var max uint32
				d.nodes.forEachParent(h, func(p codec.Hash) bool {
					if g, ok := gen[p]; ok && g > max {
						max = g
					}
					return true
				})
				gen[h] = max + 1
				state[h] = done
				stack = stack[:len(stack)-1]
				continue
			}
			if !d.nodes.isResident(h) && d.nodes.has(h) {
				// In a segment: its generation is settled.
				gen[h] = d.nodes.generation(h)
				state[h] = done
				stack = stack[:len(stack)-1]
				continue
			}
			if !d.nodes.has(h) {
				// A stub or an absent parent. Contributes no generation,
				// and marking it done keeps it out of the walk.
				state[h] = done
				stack = stack[:len(stack)-1]
				continue
			}
			state[h] = active
			d.nodes.forEachParent(h, func(p codec.Hash) bool {
				if state[p] == unvisited {
					stack = append(stack, p)
				}
				// A parent still active is a cycle, which a hash-linked
				// graph cannot form without a hash collision. Leaving it
				// alone resolves it as generation 0, which prunes nothing
				// rather than pruning wrongly.
				return true
			})
		}
	}

	for h, g := range gen {
		d.nodes.setGeneration(h, g)
	}
	d.genStale = false
}

// generationLocked returns hash's generation, or 0 for a hash that is not
// a resident commit. Must hold mu.
func (d *InMemoryCommitDag) generationLocked(hash codec.Hash) uint32 {
	return d.nodes.generation(hash)
}

// GenerationDistance is descendant's generation minus ancestor's: a lower bound on the commits
// between them, exact for a linear history. false when either is not a resident commit or the
// generations cannot be derived. Squash catch-up uses it to size a backlog without walking it.
func (d *InMemoryCommitDag) GenerationDistance(ancestor, descendant codec.Hash) (int64, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.ensureGenerationsLocked() {
		return 0, false
	}
	ga, gd := d.generationLocked(ancestor), d.generationLocked(descendant)
	if ga == 0 || gd == 0 {
		return 0, false
	}
	return int64(gd) - int64(ga), true
}
