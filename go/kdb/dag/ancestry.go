package dag

import (
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// CommitsSince returns commits reachable from from but not from exclude ancestors.
//
// Still builds full closures, with or without AncestryPruning. Unlike
// IsAncestor this is a set difference, so it needs the sets it is
// differencing; generation numbers would only let it skip re-descending
// shared history, which is a smaller and more delicate win. Left for when
// a measurement asks for it - see docs/kdb-commit-graph-on-disk.md.
func (d *InMemoryCommitDag) CommitsSince(from codec.Hash, exclude map[codec.Hash]struct{}) []codec.Hash {
	d.mu.Lock()
	defer d.mu.Unlock()
	reachable := d.ancestorClosureLocked(from)
	excluded := make(map[codec.Hash]struct{})
	for h := range exclude {
		for a := range d.ancestorClosureLocked(h) {
			excluded[a] = struct{}{}
		}
	}
	var out []codec.Hash
	for h := range reachable {
		if _, skip := excluded[h]; skip {
			continue
		}
		out = append(out, h)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Hex() < out[j].Hex() })
	return out
}

// CommonAncestor finds the nearest common ancestor of two commits, or nil if disjoint.
func (d *InMemoryCommitDag) CommonAncestor(hashA, hashB codec.Hash) *codec.Hash {
	d.mu.Lock()
	defer d.mu.Unlock()
	setA := d.ancestorClosureLocked(hashA)
	seen := make(map[codec.Hash]struct{})
	queue := []codec.Hash{hashB}
	for len(queue) > 0 {
		h := queue[0]
		queue = queue[1:]
		if _, ok := seen[h]; ok {
			continue
		}
		seen[h] = struct{}{}
		if _, ok := setA[h]; ok {
			cp := h
			return &cp
		}
		for _, p := range d.expandParentsLocked(h) {
			queue = append(queue, p)
		}
	}
	return nil
}

// IsAncestor reports whether ancestor is on the path to descendant.
//
// Two implementations, selected by GraphSettings.AncestryPruning, and they
// are required to agree on every input - TestIsAncestorAgreesWithAncestorSet
// and TestPrunedAncestryAgreesWithTheClosure are what hold them to it.
//
// The unpruned one materializes descendant's entire ancestor closure and
// then tests a single membership, so one call allocates a map proportional
// to the whole history and a caller testing many candidates against one
// descendant rebuilds it every time. At the sizes this codebase is now
// aiming at, that is the cost that bites first - before residency, and for
// a reason unrelated to it.
//
// The pruned one walks from descendant and stops descending any branch
// whose generation has fallen to or below the ancestor's. Sound because
// generation strictly increases from parent to child (see
// deriveGenerationLocked): every path from descendant down to ancestor has
// strictly decreasing generations, so a commit that is already at or below
// the ancestor's generation cannot have it below. It visits only what it
// has to and allocates a visited set proportional to that, not to history.
func (d *InMemoryCommitDag) IsAncestor(ancestor, descendant codec.Hash) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.ensureGenerationsLocked() {
		closure := d.ancestorClosureLocked(descendant)
		_, ok := closure[ancestor]
		return ok
	}
	return d.isAncestorPrunedLocked(ancestor, descendant)
}

// isAncestorPrunedLocked answers IsAncestor using generation numbers. Must
// hold mu exclusively, and must be called only once ensureGenerationsLocked
// has reported the generations usable.
func (d *InMemoryCommitDag) isAncestorPrunedLocked(ancestor, descendant codec.Hash) bool {
	// The closure the unpruned path builds always contains its own start,
	// whether or not that hash names a resident commit, so a hash is its
	// own ancestor even when the DAG has never heard of it. Keep that.
	if ancestor == descendant {
		return true
	}
	// A target that is not a resident commit has generation 0 - a stubbed
	// or absent parent hash that some commit still names. Nothing can be
	// pruned against 0, so this degenerates to the plain walk rather than
	// to a wrong answer.
	floor := d.generationLocked(ancestor)

	visited := make(map[codec.Hash]struct{})
	queue := []codec.Hash{descendant}
	for len(queue) > 0 {
		h := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		if _, seen := visited[h]; seen {
			continue
		}
		visited[h] = struct{}{}
		if h == ancestor {
			return true
		}
		if floor > 0 && d.generationLocked(h) <= floor {
			// Everything below h is at a strictly lower generation than h
			// is, so none of it can be the ancestor. Note this also
			// discards h when h is not resident (generation 0), which is
			// right: expandParentsLocked has nothing to descend into
			// there anyway.
			continue
		}
		queue = append(queue, d.expandParentsLocked(h)...)
	}
	return false
}

// AncestorSet returns the ancestor closure of hash (including hash itself). Callers testing
// many candidate ancestors against one descendant should walk the graph once with this rather
// than calling IsAncestor per candidate, which rebuilds the same closure every time.
func (d *InMemoryCommitDag) AncestorSet(hash codec.Hash) map[codec.Hash]struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.ancestorClosureLocked(hash)
}

// AppendMergeCommit appends a merge commit with two parents, advancing the default branch to it
// only if the branch is still at primaryParent - the same compare-and-swap AppendCommit performs,
// and for the same reason: a merge is planned against a head that is read well before this call
// (peer-sync resolves an incoming push against the local head, then appends), so an unconditional
// advance here loses whatever landed in between.
//
// Use AppendMergeCommitOnto when the branch is deliberately somewhere other than primaryParent,
// or when there is no head to swap against at all.
func (d *InMemoryCommitDag) AppendMergeCommit(
	tx document.Transaction,
	primaryParent, mergedParent codec.Hash,
	newDocumentTree document.DocumentTree,
	schemaHash *codec.Hash,
	message string,
) (document.Commit, error) {
	return d.AppendMergeCommitOnto(&primaryParent, tx, primaryParent, mergedParent, newDocumentTree, schemaHash, message)
}

// AppendMergeCommitOnto is AppendMergeCommit for a merge whose primary parent is deliberately not
// where the branch currently points, so the compare-and-swap is made against expectedHead instead.
// transaction.Engine.Merge is the case this exists for: it replays each branch commit onto the
// tip first (which walks the branch head forward through a chain of scratch commits) and then
// caps the result with a two-parent marker rooted back at the original primary head. The branch
// head it must not have lost is the tip of that replay chain, not the marker's own parent.
//
// A nil expectedHead skips the compare-and-swap entirely, for a caller assembling a commit graph
// of an explicit shape (test fixtures, a restore replaying recorded history) where "the branch is
// not where this merge starts" is the point rather than a race.
func (d *InMemoryCommitDag) AppendMergeCommitOnto(
	expectedHead *codec.Hash,
	tx document.Transaction,
	primaryParent, mergedParent codec.Hash,
	newDocumentTree document.DocumentTree,
	schemaHash *codec.Hash,
	message string,
) (document.Commit, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.appendCommitLocked(tx, []codec.Hash{primaryParent, mergedParent}, newDocumentTree, schemaHash, message, mainBranch, expectedHead)
}

func (d *InMemoryCommitDag) expandParentsLocked(hash codec.Hash) []codec.Hash {
	if _, ok := d.stubs[hash]; ok {
		return nil
	}
	c, ok := d.commitLocked(hash)
	if !ok {
		return nil
	}
	return c.ParentHashes
}

func (d *InMemoryCommitDag) ancestorClosureLocked(start codec.Hash) map[codec.Hash]struct{} {
	acc := make(map[codec.Hash]struct{})
	queue := []codec.Hash{start}
	for len(queue) > 0 {
		h := queue[0]
		queue = queue[1:]
		if _, ok := acc[h]; ok {
			continue
		}
		acc[h] = struct{}{}
		for _, p := range d.expandParentsLocked(h) {
			queue = append(queue, p)
		}
	}
	return acc
}
