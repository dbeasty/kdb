package dag

import (
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// CommitsSince returns commits reachable from from but not from exclude ancestors.
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
func (d *InMemoryCommitDag) IsAncestor(ancestor, descendant codec.Hash) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	closure := d.ancestorClosureLocked(descendant)
	_, ok := closure[ancestor]
	return ok
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
	c, ok := d.commits[hash]
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
