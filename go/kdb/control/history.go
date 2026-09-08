package control

import (
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
// It delegates. The engine has a real history API - dag.HistoryNavigator (ResolveRevision,
// ListCommits, NthAncestor, CommitAtOrBefore, the tag store) plus embed.DiffCommits and
// embed.RevertTo - and this package's job is to map it onto HTTP, not to have opinions about it.
// An earlier version of this file reimplemented revision resolution and diffing locally because
// none of that existed yet; all of it has been deleted in favour of the engine's.
//
// What stays here is presentation: JSON shapes, and a stable ordering for diff entries, because
// dag.DiffTrees builds its result by ranging over two maps and a view that reshuffled on every
// refresh would be unusable.

// commitDAG unwraps the runtime's DAG to the concrete type, for the branch and tag listings that
// are not on the navigator interface.
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

// navigator is the history API, from whichever DAG this runtime has.
func (s *Server) navigator() (dag.HistoryNavigator, error) {
	rt := s.opts.Runtime.Runtime
	if rt == nil {
		return nil, errors.New("runtime has no embedded runtime")
	}
	nav, ok := rt.DAG.(dag.HistoryNavigator)
	if !ok {
		return nil, fmt.Errorf("this runtime's commit graph (%T) does not navigate", rt.DAG)
	}
	return nav, nil
}

// resolveRevision turns a revision specification into a commit hash.
//
// The accepted grammar is the engine's, not this package's: a branch, a tag (tag:v1), a full or
// abbreviated hash, head, and the ~n / ^ walks - see dag.ParseRevision. An empty spec means head,
// which is the only thing HTTP adds, because an omitted query parameter should mean "now".
func (s *Server) resolveRevision(spec string) (codec.Hash, error) {
	nav, err := s.navigator()
	if err != nil {
		return codec.Hash{}, err
	}
	if spec == "" {
		spec = "head"
	}
	hash, err := nav.ResolveRevision(spec)
	if err == nil {
		return hash, nil
	}
	// The engine's grammar takes a full 64-hex hash. A log listing shows abbreviated ones, and a
	// person reading it will paste what they see, so an unambiguous prefix is resolved here rather
	// than refused. This sits on top of the engine's resolution and never overrides it: only a
	// spec the engine has already rejected is retried, so branch names, tags and the ~/^ walks
	// keep their meanings even if one of them happens to look like hex.
	if prefix, ok := hashPrefix(spec); ok {
		if h, found := s.resolvePrefix(prefix); found {
			return h, nil
		}
	}
	return codec.Hash{}, err
}

// hashPrefix reports whether spec is an abbreviated commit hash: hexadecimal, at least 8 digits
// (LookupHashPrefix's own floor - fewer collides too readily to be worth guessing at) and short of
// a full one, with none of the grammar's walk suffixes.
func hashPrefix(spec string) (string, bool) {
	lower := strings.ToLower(strings.TrimSpace(spec))
	if len(lower) < 8 || len(lower) >= 64 {
		return "", false
	}
	for _, c := range lower {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return "", false
		}
	}
	return lower, true
}

// resolvePrefix resolves an abbreviated hash, refusing an ambiguous one. Picking arbitrarily among
// matches is the one outcome worse than refusing: it shows an operator a different commit from the
// one they named, with nothing to indicate it.
func (s *Server) resolvePrefix(prefix string) (codec.Hash, bool) {
	d, err := s.commitDAG()
	if err != nil {
		return codec.Hash{}, false
	}
	matches := d.LookupHashPrefix(prefix)
	if len(matches) != 1 {
		return codec.Hash{}, false
	}
	return matches[0], true
}

// isUnknownRevision reports whether err means "that revision names nothing here", which is a 404
// rather than a 500 - a URL someone typed, not a failure.
func isUnknownRevision(err error) bool {
	var rev *dag.RevisionNotFoundError
	return errors.As(err, &rev)
}

// commitSummary is one commit as a log listing shows it.
//
// Operations are absent on purpose: they are the full text of every document the commit wrote, so
// a listing that carried them would cost the size of the history it exists to summarize. OpCount
// is kept, and is -1 when the operations have been evicted and the count is not knowable without
// loading them - which the UI shows as unknown rather than as zero.
type commitSummary struct {
	Hash      string   `json:"hash"`
	ShortHash string   `json:"shortHash"`
	Parents   []string `json:"parents"`
	Timestamp string   `json:"timestamp"`
	Author    string   `json:"author"`
	Message   string   `json:"message"`
	OpCount   int      `json:"opCount"`
	TreeHash  string   `json:"treeHash"`
	// Stub marks a commit archived in place; its operations and tree are no longer reachable.
	Stub bool `json:"stub,omitempty"`
	// Refs are the branch and tag names pointing at this commit, for badges in the log.
	Refs []string `json:"refs,omitempty"`
}

func summarizeInfo(c dag.CommitInfo) commitSummary {
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
		OpCount:   c.OperationCount,
		TreeHash:  c.TreeHash.Hex(),
		Stub:      c.Stubbed,
	}
}

func summarizeCommit(c document.Commit) commitSummary {
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

// log lists commits back from a revision, newest first.
func (s *Server) log(from codec.Hash, limit, skip int) ([]commitSummary, bool, error) {
	nav, err := s.navigator()
	if err != nil {
		return nil, false, err
	}
	// One more than asked for, so the caller can tell "exactly this many" from "more follow"
	// without a second round trip or a count of the whole history.
	infos, err := nav.ListCommits(from, skip, limit+1)
	if err != nil {
		return nil, false, err
	}
	more := len(infos) > limit
	if more {
		infos = infos[:limit]
	}
	refs := s.refsByCommit()
	out := make([]commitSummary, 0, len(infos))
	for _, info := range infos {
		sum := summarizeInfo(info)
		sum.Refs = refs[sum.Hash]
		out = append(out, sum)
	}
	return out, more, nil
}

// refsByCommit indexes branch and tag names by the commit they point at.
func (s *Server) refsByCommit() map[string][]string {
	out := map[string][]string{}
	d, err := s.commitDAG()
	if err != nil {
		return out
	}
	for _, b := range d.ListBranches() {
		h := b.HeadHash.Hex()
		out[h] = append(out[h], b.Name)
	}
	for _, t := range d.ListTags() {
		h := t.CommitHash.Hex()
		out[h] = append(out[h], "tag:"+t.Name)
	}
	return out
}

// diffEntry is one document's difference between two commits.
type diffEntry struct {
	// Change is "added", "modified" or "removed", moving From -> To.
	Change          string `json:"change"`
	DocID           string `json:"docId"`
	FromContentHash string `json:"fromContentHash,omitempty"`
	ToContentHash   string `json:"toContentHash,omitempty"`
}

// diffRevisions reports which documents differ between two revisions.
//
// embed.DiffCommits does the work, including resolving both document trees through the storage
// adapter - which matters on a file-backed namespace, where an older tree has to be rebuilt from
// the delta log before it can be compared. Going through the adapter rather than through
// dag.DocumentTreeStore is what keeps that rebuild outside the DAG's own lock; see
// ServerEngine.TreeAt.
func (s *Server) diffRevisions(fromSpec, toSpec string) (codec.Hash, codec.Hash, []diffEntry, error) {
	rt := s.opts.Runtime.Runtime
	if rt == nil {
		return codec.Hash{}, codec.Hash{}, nil, errors.New("runtime has no embedded runtime")
	}
	cd, err := embed.DiffCommits(rt, fromSpec, toSpec)
	if err != nil {
		return codec.Hash{}, codec.Hash{}, nil, err
	}
	return cd.FromHash, cd.ToHash, presentDiff(cd), nil
}

// presentDiff maps a CommitDiff into the wire shape, in a stable order.
func presentDiff(cd dag.CommitDiff) []diffEntry {
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
	return out
}

// sortDiff orders added, then modified, then removed, and by document id within each - a stable
// order that also reads the way an operator scans a change: what appeared, what moved, what went.
func sortDiff(entries []diffEntry) {
	rank := map[string]int{"added": 0, "modified": 1, "removed": 2}
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

func countChanges(entries []diffEntry) map[string]int {
	counts := map[string]int{"added": 0, "modified": 0, "removed": 0}
	for _, e := range entries {
		counts[e.Change]++
	}
	return counts
}
