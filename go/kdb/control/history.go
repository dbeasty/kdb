package control

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
)

// This file is the control plane's view of history: resolve a revision, list commits, describe
// one, diff two.
//
// It is deliberately thin and deliberately local to this package. Equivalent, richer primitives
// (dag.ListCommits, dag.ParseRevision, embed.RevertTo) are being built in parallel on main; when
// they land, this file collapses into calls to them and nothing outside this package changes.
// Putting it here rather than on KdbServerRuntime keeps that future merge to one file.

// maxWalk bounds a single traversal. Walk materializes its frontier eagerly, so an unbounded walk
// on a long history is a memory event, not just a slow request. Paging past this is what skip is
// for.
const maxWalk = 20_000

// commitDAG unwraps the runtime's DAG to the concrete type. Walk and the branch listing are on
// the CommitDAG interface, but Diff and LookupHashPrefix are not, and the control plane needs
// both.
func (s *Server) commitDAG() (*dag.InMemoryCommitDag, error) {
	rt := s.opts.Runtime.Runtime
	if rt == nil {
		return nil, errors.New("runtime has no embedded runtime")
	}
	switch d := rt.DAG.(type) {
	case *dag.InMemoryCommitDag:
		return d, nil
	case *embed.PersistingCommitDAG:
		return d.Delegate(), nil
	default:
		return nil, fmt.Errorf("unsupported commit DAG implementation %T", rt.DAG)
	}
}

// revisionError is a revision specification that names nothing. Kept distinct from a generic
// failure so a handler can answer 404 rather than 500 - "that commit is not here" is a normal
// answer to a URL a user typed.
type revisionError struct{ spec, reason string }

func (e *revisionError) Error() string {
	return fmt.Sprintf("revision %q: %s", e.spec, e.reason)
}

// resolveRef turns a revision specification into a commit hash. It accepts, in order:
//
//	""           - the current head
//	"HEAD"       - the current head
//	a branch name
//	a full 64-hex commit hash
//	an unambiguous hex prefix of at least 8 digits
//
// The 8-digit floor is LookupHashPrefix's own, and the ambiguity case is reported rather than
// resolved arbitrarily: two commits sharing a prefix is exactly when picking one silently would
// show an operator the wrong history.
func (s *Server) resolveRef(spec string) (codec.Hash, error) {
	d, err := s.commitDAG()
	if err != nil {
		return codec.Hash{}, err
	}
	spec = strings.TrimSpace(spec)
	if spec == "" || strings.EqualFold(spec, "head") {
		h, err := d.Head()
		if err != nil {
			return codec.Hash{}, err
		}
		return h, nil
	}
	if branch, ok := d.GetBranch(spec); ok {
		return branch.HeadHash, nil
	}
	lower := strings.ToLower(spec)
	if !isHex(lower) {
		return codec.Hash{}, &revisionError{spec, "not a branch name and not hexadecimal"}
	}
	if len(lower) == 64 {
		raw, err := hex.DecodeString(lower)
		if err != nil {
			return codec.Hash{}, &revisionError{spec, "not valid hexadecimal"}
		}
		var h codec.Hash
		copy(h.Bytes[:], raw)
		if !d.HasCommit(h) {
			return codec.Hash{}, &revisionError{spec, "no such commit in this namespace"}
		}
		return h, nil
	}
	if len(lower) < 8 {
		return codec.Hash{}, &revisionError{spec, "a hash prefix must be at least 8 hex digits"}
	}
	matches := d.LookupHashPrefix(lower)
	switch len(matches) {
	case 0:
		return codec.Hash{}, &revisionError{spec, "no such commit in this namespace"}
	case 1:
		return matches[0], nil
	default:
		return codec.Hash{}, &revisionError{spec, fmt.Sprintf("ambiguous: %d commits share that prefix", len(matches))}
	}
}

// commitSummary is one commit as a log listing shows it.
//
// Operations are absent on purpose: they are the full text of every document the commit wrote
// (see dag/ops_retention.go), so a listing that carried them would cost the size of the history it
// exists to summarize. OpCount is kept so a caller can tell an empty commit from one whose
// operations are simply not resident.
type commitSummary struct {
	Hash      string   `json:"hash"`
	ShortHash string   `json:"shortHash"`
	Parents   []string `json:"parents"`
	Timestamp string   `json:"timestamp"`
	Author    string   `json:"author"`
	Message   string   `json:"message"`
	OpCount   int      `json:"opCount"`
	TreeHash  string   `json:"treeHash"`
	// Stub marks a commit whose body has been archived, so a client renders it as a boundary
	// rather than as a commit with no content.
	Stub bool `json:"stub,omitempty"`
	// Refs are the branch names pointing at this commit, for badges in the graph.
	Refs []string `json:"refs,omitempty"`
}

func summarize(c document.Commit) commitSummary {
	parents := make([]string, 0, len(c.ParentHashes))
	for _, p := range c.ParentHashes {
		parents = append(parents, p.Hex())
	}
	return commitSummary{
		Hash:      c.Hash.Hex(),
		ShortHash: shortHash(c.Hash.Hex()),
		Parents:   parents,
		Timestamp: millisToRFC3339(c.Timestamp.EpochMillis),
		Author:    c.AuthorNodeID.String(),
		Message:   c.Message,
		OpCount:   len(c.Operations),
		TreeHash:  c.DocumentTreeHash.Hex(),
	}
}

// log walks back from a revision, newest first, skipping skip entries and returning at most limit.
//
// skip is applied after the walk rather than by starting elsewhere because the DAG has no ordinal
// addressing: "the 200th commit back" is only meaningful as a traversal result. That makes deep
// paging cost the traversal it skips, which is why maxWalk bounds it and the response says
// whether more remains.
func (s *Server) log(from codec.Hash, limit, skip int) ([]commitSummary, bool, error) {
	d, err := s.commitDAG()
	if err != nil {
		return nil, false, err
	}
	want := skip + limit + 1 // +1 so the caller can tell "exactly this many" from "more follow"
	if want > maxWalk {
		want = maxWalk
	}
	entries := d.Walk(from, nil, want)
	refs := branchesByCommit(d)

	out := make([]commitSummary, 0, limit)
	for i, e := range entries {
		if i < skip {
			continue
		}
		if len(out) == limit {
			return out, true, nil
		}
		var sum commitSummary
		switch entry := e.(type) {
		case dag.FullEntry:
			sum = summarize(entry.Commit)
		case dag.StubbedEntry:
			sum = commitSummary{
				Hash:      entry.Stub.OriginalHash.Hex(),
				ShortHash: shortHash(entry.Stub.OriginalHash.Hex()),
				Timestamp: millisToRFC3339(entry.Stub.StubbedAt.EpochMillis),
				Stub:      true,
			}
		default:
			continue
		}
		sum.Refs = refs[sum.Hash]
		out = append(out, sum)
	}
	return out, false, nil
}

func branchesByCommit(d *dag.InMemoryCommitDag) map[string][]string {
	out := map[string][]string{}
	for _, b := range d.ListBranches() {
		hex := b.HeadHash.Hex()
		out[hex] = append(out[hex], b.Name)
	}
	return out
}

// diffEntry is one document's difference between two commits.
type diffEntry struct {
	// Change is "added", "modified" or "removed", from the perspective of moving From -> To.
	Change          string `json:"change"`
	DocID           string `json:"docId"`
	FromContentHash string `json:"fromContentHash,omitempty"`
	ToContentHash   string `json:"toContentHash,omitempty"`
}

// diffCommits reports which documents differ between two commits, ordered so a UI renders the same
// list every time. dag.Diff builds its result by ranging over two maps, so its order is Go's map
// order - fine for a set comparison, unusable for a view that must not reshuffle on refresh.
//
// This resolves both commits' document trees, which on a file-backed namespace means rebuilding
// whichever of them is no longer resident. That works now; it did not before
// ServerEngine.GetTree was taught to reach the rebuild, which is why the operation-based path
// above exists and is still preferred for the first-parent case on cost grounds.
func (s *Server) diffCommits(from, to codec.Hash) ([]diffEntry, error) {
	d, err := s.commitDAG()
	if err != nil {
		return nil, err
	}
	cd, err := d.Diff(from, to)
	if err != nil {
		return nil, err
	}
	out := make([]diffEntry, 0, len(cd.Entries))
	for _, e := range cd.Entries {
		switch entry := e.(type) {
		case dag.DiffAdded:
			out = append(out, diffEntry{
				Change: "added", DocID: entry.DocID.String(), ToContentHash: entry.ContentHash.Hex(),
			})
		case dag.DiffModified:
			out = append(out, diffEntry{
				Change: "modified", DocID: entry.DocID.String(),
				FromContentHash: entry.FromContentHash.Hex(), ToContentHash: entry.ToContentHash.Hex(),
			})
		case dag.DiffRemoved:
			out = append(out, diffEntry{
				Change: "removed", DocID: entry.DocID.String(), FromContentHash: entry.ContentHash.Hex(),
			})
		}
	}
	sortDiff(out)
	return out, nil
}

// sortDiff orders added, then modified, then removed, and by document id within each - a stable
// order that also reads the way an operator scans a change: what appeared, what moved, what went.
func sortDiff(entries []diffEntry) {
	rank := map[string]int{"added": 0, "modified": 1, "removed": 2}
	// Insertion sort: a diff is small in the common case, and this avoids a sort.Slice closure
	// allocation on a path that runs per commit rendered.
	for i := 1; i < len(entries); i++ {
		for j := i; j > 0; j-- {
			a, b := entries[j-1], entries[j]
			if rank[a.Change] < rank[b.Change] || (rank[a.Change] == rank[b.Change] && a.DocID <= b.DocID) {
				break
			}
			entries[j-1], entries[j] = b, a
		}
	}
}

func isHex(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'f':
		default:
			return false
		}
	}
	return true
}

// diffAgainstParent computes what one commit changed, without materializing either document tree.
//
// A commit already records what it touched: its operations name every document it wrote or
// deleted. The one thing they do not say is whether a written document existed beforehand -
// "added" versus "modified" - and that is a point read at the parent. So the diff costs one read
// per operation instead of materializing two whole trees, which is strictly cheaper than dag.Diff
// on any namespace larger than the commit itself.
//
// Operations are evictable in their own right (dag/ops_retention.go), so this can legitimately be
// unavailable; the caller falls back to the tree comparison and reports which path ran.
//
// Note the hash key space. storage.Adapter's third parameter is named atCommit but is a *document
// tree* hash - ServerEngine.treeAt matches it against the live tree snapshot and the tree store,
// both keyed by tree hash. Passing a commit hash there does not error, it simply resolves nothing,
// which would silently report every modification as an addition. Hence commit.DocumentTreeHash
// throughout, and the assertion in the control tests that this path and dag.Diff agree.
func (s *Server) diffAgainstParent(commit document.Commit, parent codec.Hash, namespaceID string) ([]diffEntry, error) {
	d, err := s.commitDAG()
	if err != nil {
		return nil, err
	}
	ops, err := d.CommitOperations(commit.Hash)
	if err != nil {
		return nil, err
	}
	parentCommit, ok := d.GetCommit(parent)
	if !ok {
		return nil, fmt.Errorf("parent commit %s is not available", shortHash(parent.Hex()))
	}
	beforeTree := parentCommit.DocumentTreeHash
	afterTree := commit.DocumentTreeHash

	store := s.opts.Runtime.Runtime.Storage
	out := make([]diffEntry, 0, len(ops))
	for _, op := range ops {
		switch o := op.(type) {
		case document.WriteOp:
			entry := diffEntry{Change: "added", DocID: o.DocID.String()}
			if prior, err := store.GetDocument(namespaceID, o.DocID, beforeTree); err == nil && prior != nil {
				entry.Change = "modified"
				entry.FromContentHash = contentHashHex(*prior)
			}
			if after, err := store.GetDocument(namespaceID, o.DocID, afterTree); err == nil && after != nil {
				entry.ToContentHash = contentHashHex(*after)
			}
			// A write whose content is identical to what was already there changes nothing, and
			// listing it as modified would send a reader looking for a difference that is not
			// there. The engine allows such a write; the diff should not invent a change.
			if entry.Change == "modified" && entry.FromContentHash != "" &&
				entry.FromContentHash == entry.ToContentHash {
				continue
			}
			out = append(out, entry)
		case document.DeleteOp:
			entry := diffEntry{Change: "removed", DocID: o.DocID.String()}
			if prior, err := store.GetDocument(namespaceID, o.DocID, beforeTree); err == nil && prior != nil {
				entry.FromContentHash = contentHashHex(*prior)
			}
			out = append(out, entry)
		default:
			// FileWriteOp and anything added later touch no document identity, so they do not
			// appear in a document diff.
		}
	}
	sortDiff(out)
	return out, nil
}

// contentHashHex is the document's content hash, or empty when it cannot be computed. The hash is
// display detail in a diff listing, so a failure to derive it must not fail the diff.
func contentHashHex(doc document.Document) string {
	h, err := document.ComputeContentHash(doc)
	if err != nil {
		return ""
	}
	return h.Hex()
}
