package dag

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// CommitInfo is one commit as a listing describes it: everything needed to
// show it in a log, and nothing that costs bulk.
//
// Operations are deliberately absent, for the same reason a checkpoint
// leaves them out: they are the full text of every document the commit
// wrote (see ops_retention.go), so a listing that carried them would cost
// the size of the history it exists to summarize. OperationCount is kept
// so a caller can tell an empty commit from one whose operations are
// simply not loaded, and CommitOperations fetches them for the one commit
// a caller actually opens.
type CommitInfo struct {
	Hash          codec.Hash
	ParentHashes  []codec.Hash
	TransactionID codec.UUID
	Timestamp     codec.Timestamp
	AuthorNodeID  codec.UUID
	TreeHash      codec.Hash
	Message       string
	// OperationCount is how many operations the commit carries, or -1
	// when they have been evicted and the count is not known without a
	// load. Never 0 for a commit that wrote something.
	OperationCount int
	// Stubbed marks a commit that has been archived in place; its
	// operations and tree are no longer reachable. See StubCommit.
	Stubbed bool
}

// RevisionNotFoundError reports a revision specification that names
// nothing this namespace still has. Detect with errors.As.
//
// The distinction that matters to a caller is in Reason: a hash that was
// never here is a different problem from one that was reclaimed by a
// retention window, and only the second is fixed by configuring a longer
// window.
type RevisionNotFoundError struct {
	NamespaceID string
	Spec        string
	Reason      string
}

func (e *RevisionNotFoundError) Error() string {
	if e.Reason == "" {
		return fmt.Sprintf("kdb: namespace %q has no revision %q", e.NamespaceID, e.Spec)
	}
	return fmt.Sprintf("kdb: namespace %q has no revision %q: %s", e.NamespaceID, e.Spec, e.Reason)
}

// ListCommits walks back from a commit and describes what it finds, newest
// first, skipping the first skip entries and returning at most limit.
//
// The traversal is Walk's, so it is timestamp-ordered across merges and
// prunes at stubs, and it stops at whatever the namespace still retains
// rather than pretending a reclaimed ancestor is simply the end of
// history - a caller that needs to tell those apart compares the last
// entry's parents against what came back.
func (d *InMemoryCommitDag) ListCommits(from codec.Hash, skip, limit int) ([]CommitInfo, error) {
	if limit <= 0 {
		return nil, nil
	}
	if skip < 0 {
		skip = 0
	}
	entries := d.Walk(from, nil, skip+limit)
	if len(entries) <= skip {
		return nil, nil
	}
	entries = entries[skip:]

	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]CommitInfo, 0, len(entries))
	for _, e := range entries {
		switch t := e.(type) {
		case FullEntry:
			count := len(t.Commit.Operations)
			if count == 0 && d.opsEvictedFor(t.Commit.Hash) {
				// Evicted, so len() understates it and the true count is
				// not knowable without a load this listing will not do.
				count = -1
			}
			out = append(out, CommitInfo{
				Hash:           t.Commit.Hash,
				ParentHashes:   append([]codec.Hash(nil), t.Commit.ParentHashes...),
				TransactionID:  t.Commit.TransactionID,
				Timestamp:      t.Commit.Timestamp,
				AuthorNodeID:   t.Commit.AuthorNodeID,
				TreeHash:       t.Commit.DocumentTreeHash,
				Message:        t.Commit.Message,
				OperationCount: count,
			})
		case StubbedEntry:
			out = append(out, CommitInfo{
				Hash:           t.Stub.OriginalHash,
				Timestamp:      t.Stub.StubbedAt,
				Message:        "(archived)",
				OperationCount: -1,
				Stubbed:        true,
			})
		}
	}
	return out, nil
}

// NthAncestor returns the commit n steps back from a commit along first
// parents - what "head~10" names.
//
// First parent rather than any parent, because that is the only choice
// that makes the answer a single commit on a graph with merges, and it is
// the same choice git makes for the same reason. n == 0 is the commit
// itself.
func (d *InMemoryCommitDag) NthAncestor(from codec.Hash, n int) (codec.Hash, error) {
	if n < 0 {
		return codec.Hash{}, fmt.Errorf("kdb: negative ancestor distance %d", n)
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	current := from
	for i := 0; i < n; i++ {
		c, ok := d.commitLocked(current)
		if !ok {
			if _, stubbed := d.stubs[current]; stubbed {
				return codec.Hash{}, &RevisionNotFoundError{
					NamespaceID: d.NamespaceID, Spec: current.Hex(),
					Reason: "the commit is archived and its ancestry cannot be walked",
				}
			}
			return codec.Hash{}, &RevisionNotFoundError{
				NamespaceID: d.NamespaceID, Spec: current.Hex(),
				Reason: fmt.Sprintf("history ends %d steps back; the rest is not retained", i),
			}
		}
		if len(c.ParentHashes) == 0 {
			return codec.Hash{}, &RevisionNotFoundError{
				NamespaceID: d.NamespaceID, Spec: from.Hex(),
				Reason: fmt.Sprintf("only %d ancestors exist", i),
			}
		}
		current = c.ParentHashes[0]
	}
	return current, nil
}

// CommitAtOrBefore returns the newest ancestor of from whose timestamp is
// at or before ts - what "AT TIME" names.
//
// Walked back along first parents and stopped at the first commit old
// enough, which is what git's --before does and for the same reason: it
// answers "what did this branch look like then" rather than "which commit
// anywhere in the graph happens to carry that timestamp".
//
// Timestamps come from whichever node authored each commit, so a skewed
// clock can put a commit out of order with its own parent. That makes this
// answer approximate in exactly the way the recorded data is approximate;
// it is not made more accurate by scanning further.
func (d *InMemoryCommitDag) CommitAtOrBefore(from codec.Hash, ts codec.Timestamp) (codec.Hash, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	current := from
	for {
		c, ok := d.commitLocked(current)
		if !ok {
			return codec.Hash{}, &RevisionNotFoundError{
				NamespaceID: d.NamespaceID, Spec: fmt.Sprintf("at time %d", ts.EpochMicros()),
				Reason: "no retained commit is that old",
			}
		}
		if c.Timestamp.EpochMicros() <= ts.EpochMicros() {
			return current, nil
		}
		if len(c.ParentHashes) == 0 {
			return codec.Hash{}, &RevisionNotFoundError{
				NamespaceID: d.NamespaceID, Spec: fmt.Sprintf("at time %d", ts.EpochMicros()),
				Reason: "the namespace has no commit that old",
			}
		}
		current = c.ParentHashes[0]
	}
}

// ResolveRef resolves a commit reference to a hash.
//
// Every ref kind resolves for real or fails; none of them falls back to
// head. A resolver that answers head for a reference it cannot resolve
// returns the wrong data with no indication that anything went wrong,
// which is worse than any error.
func (d *InMemoryCommitDag) ResolveRef(ref CommitRef) (codec.Hash, error) {
	switch r := ref.(type) {
	case nil:
		return d.Head()
	case RefByHash:
		h, err := codec.HashFromHex(strings.TrimSpace(r.Hex))
		if err != nil {
			return codec.Hash{}, &RevisionNotFoundError{
				NamespaceID: d.NamespaceID, Spec: r.Hex, Reason: "not a commit hash",
			}
		}
		d.mu.RLock()
		_, resident := d.commitLocked(h)
		_, stubbed := d.stubs[h]
		d.mu.RUnlock()
		if !resident && !stubbed {
			return codec.Hash{}, &RevisionNotFoundError{
				NamespaceID: d.NamespaceID, Spec: r.Hex,
				Reason: "no such commit is retained",
			}
		}
		return h, nil
	case RefByBranch:
		b, ok := d.GetBranch(r.Name)
		if !ok {
			return codec.Hash{}, NewBranchNotFoundError("branch not found", d.NamespaceID, r.Name)
		}
		return b.HeadHash, nil
	case RefByTag:
		t, ok := d.GetTag(r.Name)
		if !ok {
			return codec.Hash{}, NewTagNotFoundError("tag not found", d.NamespaceID, r.Name)
		}
		return t.CommitHash, nil
	case RefByTime:
		head, err := d.Head()
		if err != nil {
			return codec.Hash{}, err
		}
		return d.CommitAtOrBefore(head, r.Timestamp)
	default:
		return codec.Hash{}, fmt.Errorf("kdb: unsupported commit reference %T", ref)
	}
}

// ParseRevision splits a revision specification into the reference it
// starts from and how many first-parent steps to walk back.
//
//	head          → (branch main, 0)
//	head~10       → (branch main, 10)
//	head^         → (branch main, 1)
//	<hex>~3       → (that commit, 3)
//	tag:v1        → (tag v1, 0)
//	branch:dev~2  → (branch dev, 2)
//
// A bare token that is neither "head" nor a prefixed name is read as a
// commit hash, which is what a caller pasting one from a log gets.
func ParseRevision(spec string) (CommitRef, int, error) {
	s := strings.TrimSpace(spec)
	if s == "" {
		return nil, 0, fmt.Errorf("kdb: empty revision")
	}
	back := 0
	for strings.HasSuffix(s, "^") {
		back++
		s = strings.TrimSuffix(s, "^")
	}
	if i := strings.LastIndex(s, "~"); i >= 0 {
		digits := s[i+1:]
		n := 1
		if digits != "" {
			parsed, err := strconv.Atoi(digits)
			if err != nil || parsed < 0 {
				return nil, 0, fmt.Errorf("kdb: unparseable revision %q: %q is not a step count", spec, digits)
			}
			n = parsed
		}
		back += n
		s = s[:i]
	}
	s = strings.TrimSpace(s)
	switch {
	case s == "":
		return nil, 0, fmt.Errorf("kdb: unparseable revision %q", spec)
	case strings.EqualFold(s, "head"):
		return RefByBranch{Name: mainBranch}, back, nil
	case strings.HasPrefix(s, "tag:"):
		return RefByTag{Name: strings.TrimPrefix(s, "tag:")}, back, nil
	case strings.HasPrefix(s, "branch:"):
		return RefByBranch{Name: strings.TrimPrefix(s, "branch:")}, back, nil
	default:
		return RefByHash{Hex: s}, back, nil
	}
}

// ResolveRevision resolves a revision specification - see ParseRevision -
// to the commit it names.
func (d *InMemoryCommitDag) ResolveRevision(spec string) (codec.Hash, error) {
	ref, back, err := ParseRevision(spec)
	if err != nil {
		return codec.Hash{}, err
	}
	base, err := d.ResolveRef(ref)
	if err != nil {
		return codec.Hash{}, err
	}
	if back == 0 {
		return base, nil
	}
	return d.NthAncestor(base, back)
}

// CreateTag names a commit so a reference can reach it by that name later.
//
// Tags are retention roots for writers, alongside branch heads: a tagged
// commit is one compaction refuses to squash. Creating one on a commit
// that is not retained is refused rather than recorded, so a tag never
// names a hole.
func (d *InMemoryCommitDag) CreateTag(name string, hash codec.Hash, message string) (document.Tag, error) {
	if strings.TrimSpace(name) == "" {
		return document.Tag{}, fmt.Errorf("kdb: tag name must not be empty")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.requireCommitPresentLocked(hash); err != nil {
		return document.Tag{}, err
	}
	if existing, ok := d.tags[name]; ok {
		if existing.CommitHash == hash {
			return existing, nil
		}
		return document.Tag{}, fmt.Errorf(
			"kdb: tag %q already names commit %s", name, existing.CommitHash.Hex())
	}
	t := document.Tag{
		Name: name, NamespaceID: d.NamespaceID, CommitHash: hash,
		CreatedAt: codec.TimestampNow(), Message: message,
	}
	d.tags[name] = t
	return t, nil
}

// GetTag returns the tag by that name.
func (d *InMemoryCommitDag) GetTag(name string) (document.Tag, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	t, ok := d.tags[name]
	return t, ok
}

// ListTags returns every tag, in no particular order.
func (d *InMemoryCommitDag) ListTags() []document.Tag {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]document.Tag, 0, len(d.tags))
	for _, t := range d.tags {
		out = append(out, t)
	}
	return out
}

// DeleteTag removes a tag, and with it the retention root it was.
func (d *InMemoryCommitDag) DeleteTag(name string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.tags[name]; !ok {
		return false
	}
	delete(d.tags, name)
	return true
}

// HistoryNavigator is the optional capability of a CommitDAG that can
// resolve references and describe history.
//
// A capability interface rather than an addition to CommitDAG, following
// DeltaCommitStreamer and DeltaSegmentSequencer: navigation is what a
// history *browser* needs, and every hot path in the engine - commit,
// conflict detection, point read - needs none of it. Callers type-assert
// for it and degrade rather than requiring every DAG to grow the surface.
type HistoryNavigator interface {
	ResolveRef(ref CommitRef) (codec.Hash, error)
	ResolveRevision(spec string) (codec.Hash, error)
	NthAncestor(from codec.Hash, n int) (codec.Hash, error)
	CommitAtOrBefore(from codec.Hash, ts codec.Timestamp) (codec.Hash, error)
	ListCommits(from codec.Hash, skip, limit int) ([]CommitInfo, error)
	CreateTag(name string, hash codec.Hash, message string) (document.Tag, error)
	GetTag(name string) (document.Tag, bool)
	ListTags() []document.Tag
	DeleteTag(name string) bool
}

var _ HistoryNavigator = (*InMemoryCommitDag)(nil)
