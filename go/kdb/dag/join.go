package dag

import (
	"bytes"
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// Content-free merges, and the history they stand for.
//
// Peer sync joins two heads in a merge commit that is a deterministic function of the pair, so
// two nodes merging the same pair agree. Pairwise determinism does not make merging confluent:
// merge(merge(a, b), c) and merge(merge(a, c), b) are different commits holding the same
// documents, and in a mesh whose syncs overlap new heads of that kind were made faster than they
// spread, for as long as anyone was syncing (a five-node mesh never settled). The way out is to
// see such merges for what they are. A merge whose tree is its every parent's tree carries no
// content of its own - it only says "these histories were joined" - so it can be looked through:
// what it stands for is the commits that do carry content beneath it. Two heads standing for the
// same content-carrying commits are then joined by one canonical commit naming exactly those
// commits, whoever joins them and in whatever order they were learned. That is what makes the
// process converge, and these helpers are its vocabulary.

// MergeAuthorNodeID authors every merge commit peer sync creates. A merge is a pure function of
// the heads it joins, not something a particular node did, so no node's identity goes in it.
var MergeAuthorNodeID = codec.DerivedUUID("kdb:merge-author/1")

// MergeMessage is the message of a peer-sync merge that settled nothing provisionally.
const MergeMessage = "kdb:merge/1"

// IsContentFreeMerge reports whether c is a peer-sync merge that carries no content of its own:
// authored by MergeAuthorNodeID with the plain merge message, and holding exactly the documents
// and schema every one of its parents holds. Such a commit adds nothing a reader can see, so it
// can be looked through to its parents (see Frontier). A parent this DAG does not hold, or holds
// only as a stub, cannot be compared, and makes c count as carrying content - the safe answer.
//
// c need not be in the DAG yet; its parents must be.
func (d *InMemoryCommitDag) IsContentFreeMerge(c document.Commit) bool {
	if c.AuthorNodeID != MergeAuthorNodeID || c.Message != MergeMessage || len(c.ParentHashes) == 0 {
		return false
	}
	for _, p := range c.ParentHashes {
		if d.HasStub(p) {
			return false
		}
		pc, ok := d.GetCommit(p)
		if !ok || pc.DocumentTreeHash != c.DocumentTreeHash || !sameSchemaHash(pc.SchemaHash, c.SchemaHash) {
			return false
		}
	}
	return true
}

func sameSchemaHash(a, b *codec.Hash) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// Frontier is the content-carrying history head stands for: the latest commits reachable from
// head that are not content-free merges, with none of them an ancestor of another, sorted by
// hash. For a commit that carries content it is just that commit. Two heads with the same
// frontier hold the same content-carrying history, however they were joined.
func (d *InMemoryCommitDag) Frontier(head codec.Hash) []codec.Hash {
	return d.MaximalOf(d.contentTips([]codec.Hash{head}))
}

// FrontierOf is the frontier of several heads together.
func (d *InMemoryCommitDag) FrontierOf(heads ...codec.Hash) []codec.Hash {
	return d.MaximalOf(d.contentTips(heads))
}

// contentTips looks through content-free merges from heads, collecting the first commit on each
// path that carries content.
func (d *InMemoryCommitDag) contentTips(heads []codec.Hash) []codec.Hash {
	var out []codec.Hash
	seen := map[codec.Hash]bool{}
	queue := append([]codec.Hash(nil), heads...)
	for len(queue) > 0 {
		h := queue[0]
		queue = queue[1:]
		if seen[h] {
			continue
		}
		seen[h] = true
		c, ok := d.GetCommit(h)
		if ok && d.IsContentFreeMerge(c) {
			queue = append(queue, c.ParentHashes...)
			continue
		}
		out = append(out, h)
	}
	return out
}

// MaximalOf keeps the members of hs that are not an ancestor of another member, without
// duplicates, sorted by hash bytes.
func (d *InMemoryCommitDag) MaximalOf(hs []codec.Hash) []codec.Hash {
	uniq := make([]codec.Hash, 0, len(hs))
	seen := map[codec.Hash]bool{}
	for _, h := range hs {
		if !seen[h] {
			seen[h] = true
			uniq = append(uniq, h)
		}
	}
	var out []codec.Hash
	for i, h := range uniq {
		below := false
		for j, o := range uniq {
			if i != j && d.IsAncestor(h, o) {
				below = true
				break
			}
		}
		if !below {
			out = append(out, h)
		}
	}
	SortHashes(out)
	return out
}

// SortHashes orders hs by hash bytes, the order a canonical merge names its parents in.
func SortHashes(hs []codec.Hash) {
	sort.Slice(hs, func(i, j int) bool { return bytes.Compare(hs[i].Bytes[:], hs[j].Bytes[:]) < 0 })
}

// Incorporates reports whether head holds all the content-carrying history x stands for: x is
// head or an ancestor of it, or every commit in x's frontier is. It is "does main contain x" for
// a world in which content-free merges may be looked through and so not every one of them stays
// on main's path.
func (d *InMemoryCommitDag) Incorporates(x, head codec.Hash) bool {
	if x == head || d.IsAncestor(x, head) {
		return true
	}
	if !d.HasCommit(x) {
		return false
	}
	for _, f := range d.Frontier(x) {
		if f != head && !d.IsAncestor(f, head) {
			return false
		}
	}
	return true
}

// SupersedesHead reports whether moving main from head to c keeps everything main holds: c holds
// the same documents as head, and every content-carrying commit head stands for is c or one of
// c's ancestors. c need not be in the DAG yet; its parents must be. This is how delta replay
// recognises a logged head move onto a canonical merge that does not name the previous head as
// a parent.
func (d *InMemoryCommitDag) SupersedesHead(c document.Commit, head codec.Hash) bool {
	hc, ok := d.GetCommit(head)
	if !ok || hc.DocumentTreeHash != c.DocumentTreeHash || !sameSchemaHash(hc.SchemaHash, c.SchemaHash) {
		return false
	}
	for _, f := range d.Frontier(head) {
		covered := f == c.Hash
		for _, p := range c.ParentHashes {
			if covered {
				break
			}
			covered = f == p || d.IsAncestor(f, p)
		}
		if !covered {
			return false
		}
	}
	return true
}

// AppendCommitOnto appends a commit with any number of parents, advancing main to it only if main
// is still at expectedHead (nil skips the check) - AppendMergeCommitOnto without the two-parent
// limit, for peer sync's canonical merge of several content-carrying tips. A commit that is
// already present (another node made the same one and it arrived by sync) is not an error: main
// simply moves to it.
func (d *InMemoryCommitDag) AppendCommitOnto(
	expectedHead *codec.Hash,
	tx document.Transaction,
	parents []codec.Hash,
	newDocumentTree document.DocumentTree,
	schemaHash *codec.Hash,
	message string,
) (document.Commit, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.appendCommitLocked(tx, parents, newDocumentTree, schemaHash, message, mainBranch, expectedHead)
}
