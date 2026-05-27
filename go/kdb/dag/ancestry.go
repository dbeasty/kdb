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

// AppendMergeCommit appends a merge commit with two parents.
func (d *InMemoryCommitDag) AppendMergeCommit(
	tx document.Transaction,
	primaryParent, mergedParent codec.Hash,
	newDocumentTree document.DocumentTree,
	schemaHash *codec.Hash,
	message string,
) (document.Commit, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.appendCommitLocked(tx, []codec.Hash{primaryParent, mergedParent}, newDocumentTree, schemaHash, message, mainBranch)
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
