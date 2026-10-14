package peersync

import (
	"fmt"
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/wire"
)

// RefsOf describes a namespace's refs as a v2 node advertises them.
func RefsOf(ns string, d *dag.InMemoryCommitDag) wire.NamespaceRefs {
	refs := wire.NamespaceRefs{Namespace: ns, Branches: map[string]string{}, Tags: map[string]string{}}
	for _, b := range d.ListBranches() {
		if IsTrackingBranch(b.Name) {
			continue // local bookkeeping about another peer, never replicated
		}
		refs.Branches[b.Name] = b.HeadHash.Hex()
	}
	for _, t := range d.ListTags() {
		refs.Tags[t.Name] = t.CommitHash.Hex()
	}
	for _, h := range d.Horizon() {
		refs.Shallow = append(refs.Shallow, h.Hex())
	}
	sort.Strings(refs.Shallow)
	return refs
}

// RefUpdateResult is what ApplyRef did.
type RefUpdateResult struct {
	Outcome wire.RefUpdateOutcome
	// Head is the ref's value after the decision.
	Head codec.Hash
	// Conflict is set when Outcome is RefConflict.
	Conflict *kdberr.ConflictReport
	// Main carries the full ingest result when the ref was main.
	Main *IngestResult
}

// ApplyRef decides what a proposed ref value does to this node's ref of the same name, and does
// it. The proposed commit must already be stored.
//
//   - main goes through Adopt: fast-forward, deterministic merge, or a conflict report - the same
//     decision a v1 push or pull makes, including everything Adopt does to storage and indexes.
//   - any other branch moves only forward: created if absent, fast-forwarded if the proposal
//     descends from it, left alone if it is already ahead, and reported as a conflict if the
//     two diverged. Only main has live documents to merge; a side branch is history, and
//     merging it is a decision for whoever owns it.
//   - a tag never moves: created if absent, and a proposal naming a different commit for an
//     existing tag is a conflict. Tags are the names people restore to, so silently re-pointing
//     one would be the worst kind of replication.
func ApplyRef(env IngestEnv, kind wire.RefKind, name string, proposed codec.Hash) (RefUpdateResult, error) {
	if !env.DAG.HasCommit(proposed) {
		return RefUpdateResult{}, fmt.Errorf("peer sync: %s %q names commit %s, which was never sent", kind, name, proposed.Hex())
	}
	switch kind {
	case wire.RefTag:
		return applyTag(env, name, proposed)
	case wire.RefBranch, "":
		if name == mainBranch {
			return applyMain(env, proposed)
		}
		return applySideBranch(env, name, proposed)
	default:
		return RefUpdateResult{}, fmt.Errorf("peer sync: unknown ref kind %q", kind)
	}
}

func applyMain(env IngestEnv, proposed codec.Hash) (RefUpdateResult, error) {
	res, err := Adopt(env, proposed)
	if err != nil {
		return RefUpdateResult{}, err
	}
	out := RefUpdateResult{Head: res.Head, Main: &res}
	switch res.Outcome.Kind {
	case OutcomeFastForwarded:
		out.Outcome = wire.RefFastForwarded
	case OutcomeMerged:
		out.Outcome = wire.RefMerged
	case OutcomeConflict:
		out.Outcome = wire.RefConflict
		out.Conflict = res.Outcome.Report
	default:
		out.Outcome = wire.RefNoOp
	}
	return out, nil
}

func applySideBranch(env IngestEnv, name string, proposed codec.Hash) (RefUpdateResult, error) {
	lock := divergenceLockFor(env.NamespaceID)
	lock.Lock()
	defer lock.Unlock()
	b, ok := env.DAG.GetBranch(name)
	if !ok {
		if _, err := env.DAG.CreateBranch(name, proposed); err != nil {
			return RefUpdateResult{}, err
		}
		return RefUpdateResult{Outcome: wire.RefCreated, Head: proposed}, nil
	}
	ref := "branch:" + name
	switch ResolveHeadUpdate(env.DAG, b.HeadHash, proposed) {
	case HeadFastForward:
		if err := env.DAG.SetHead(name, proposed); err != nil {
			return RefUpdateResult{}, err
		}
		return RefUpdateResult{Outcome: wire.RefFastForwarded, Head: proposed}, env.clearConflict(ref)
	case HeadAlreadyAncestor:
		return RefUpdateResult{Outcome: wire.RefNoOp, Head: b.HeadHash}, env.clearConflict(ref)
	default:
		report := &kdberr.ConflictReport{TransactionID: proposed.Hex(), BaseHash: b.HeadHash.Hex(), TargetHash: proposed.Hex()}
		return RefUpdateResult{Outcome: wire.RefConflict, Head: b.HeadHash, Conflict: report},
			env.noteConflict(ref, b.HeadHash, proposed, report)
	}
}

func applyTag(env IngestEnv, name string, proposed codec.Hash) (RefUpdateResult, error) {
	lock := divergenceLockFor(env.NamespaceID)
	lock.Lock()
	defer lock.Unlock()
	if t, ok := env.DAG.GetTag(name); ok {
		ref := "tag:" + name
		if t.CommitHash == proposed {
			return RefUpdateResult{Outcome: wire.RefNoOp, Head: proposed}, env.clearConflict(ref)
		}
		report := &kdberr.ConflictReport{TransactionID: proposed.Hex(), BaseHash: t.CommitHash.Hex(), TargetHash: proposed.Hex()}
		return RefUpdateResult{Outcome: wire.RefConflict, Head: t.CommitHash, Conflict: report},
			env.noteConflict(ref, t.CommitHash, proposed, report)
	}
	if _, err := env.DAG.CreateTag(name, proposed, "replicated"); err != nil {
		return RefUpdateResult{}, err
	}
	return RefUpdateResult{Outcome: wire.RefCreated, Head: proposed}, nil
}

// refTargets lists every commit a namespace's refs name, deduplicated and sorted: the wants of a
// fetch that wants everything.
func refTargets(refs wire.NamespaceRefs) ([]codec.Hash, error) {
	seen := map[codec.Hash]struct{}{}
	add := func(hex string) error {
		h, err := codec.HashFromHex(hex)
		if err != nil {
			return err
		}
		seen[h] = struct{}{}
		return nil
	}
	for _, hex := range refs.Branches {
		if err := add(hex); err != nil {
			return nil, err
		}
	}
	for _, hex := range refs.Tags {
		if err := add(hex); err != nil {
			return nil, err
		}
	}
	out := make([]codec.Hash, 0, len(seen))
	for h := range seen {
		out = append(out, h)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Hex() < out[j].Hex() })
	return out, nil
}

// localRefHeads is every commit this node's refs name - what a fetch can name as haves beyond
// main's spread ancestors.
func localRefHeads(d *dag.InMemoryCommitDag) []codec.Hash {
	var out []codec.Hash
	for _, b := range d.ListBranches() {
		out = append(out, b.HeadHash)
	}
	for _, t := range d.ListTags() {
		out = append(out, t.CommitHash)
	}
	return out
}

// DefaultPageBytes caps one v2 page's estimated commit bytes when the requester names no cap.
const DefaultPageBytes = 4 << 20

// commitSize estimates a commit's encoded size: its operations' bodies plus a fixed allowance
// for hashes, ids and framing. An estimate is enough - it only has to keep a page near its cap,
// not at it - and encoding every commit twice to measure it exactly would double the cost of
// every fetch.
func commitSize(c document.Commit) int {
	n := 256 + len(c.Message) + 32*len(c.ParentHashes)
	for _, op := range c.Operations {
		switch o := op.(type) {
		case document.WriteOp:
			n += 48 + len(o.Patch)
		default:
			n += 48
		}
	}
	return n
}

// MissingCommitsFrom is MissingCommits for several wanted heads, paged by estimated bytes
// instead of count. done reports whether the page holds everything that was missing.
func MissingCommitsFrom(d *dag.InMemoryCommitDag, wants, haves []codec.Hash, maxBytes int) (commits []document.Commit, stubs []document.CommitStub, done bool, err error) {
	return newMissingCursor(d, wants, haves).page(d, maxBytes)
}

// missingCursor pages through what a peer is missing without working it out again for every
// page. Working it out walks the whole history the peer's haves reach (AncestorSetOf) and sorts
// the whole missing set, and MissingCommitsFrom did both for every page: with the haves grown
// by the last page's tips, each next page was the same walk over the same history less one
// page. A transfer of M commits in pages of P cost O((H+M)*M/P) - most of what a 200,000-commit
// sync allocated. The cursor walks once and keeps the order, 32 bytes per missing commit.
//
// The pages are those MissingCommitsFrom would have served: a page is a prefix of the
// topological order (ancestors first, ties broken by hash), and ordering what remains once a
// prefix is held reproduces the same order's suffix, so serving the suffix is exact.
//
// page builds the next page and does not move past it; advance does, once the peer has stored
// it - so a page refused and sent again, or a request repeated, gets the same page.
type missingCursor struct {
	wants []codec.Hash
	order []codec.Hash                       // missing commits not yet served, in topological order
	stubs map[codec.Hash]document.CommitStub // archived parents not yet sent

	pageLen   int                   // commits the last page took from order
	pageTips  []codec.Hash          // its tips: a peer that names them as haves holds the page
	pageStubs []document.CommitStub // the stubs it carried

	held []codec.Hash // tips of the latest pages advanced past (maxCursorHeld): haves for working the set out again
}

func newMissingCursor(d *dag.InMemoryCommitDag, wants, haves []codec.Hash) *missingCursor {
	have := d.AncestorSetOf(haves)
	set := map[codec.Hash]document.Commit{}
	allStubs := map[codec.Hash]document.CommitStub{}
	queue := append([]codec.Hash(nil), wants...)
	for len(queue) > 0 {
		h := queue[0]
		queue = queue[1:]
		if _, ok := have[h]; ok {
			continue
		}
		if _, ok := set[h]; ok {
			continue
		}
		if s, ok := d.GetStub(h); ok {
			allStubs[h] = s
			continue
		}
		c, ok := d.GetCommit(h)
		if !ok {
			continue
		}
		set[h] = c
		queue = append(queue, c.ParentHashes...)
	}
	ordered := topoSort(set)
	order := make([]codec.Hash, len(ordered))
	for i, c := range ordered {
		order[i] = c.Hash
	}
	return &missingCursor{wants: append([]codec.Hash(nil), wants...), order: order, stubs: allStubs}
}

// errCursorStale is page's report that a commit the cursor ordered is gone from the DAG -
// squashed or archived since - so the missing set must be worked out again.
var errCursorStale = fmt.Errorf("peer sync: the missing commits changed while paging")

// page returns the next page - commits with operations, up to maxBytes estimated (at least one)
// - and the stubs of archived parents it names. done reports whether it holds all that is left.
func (c *missingCursor) page(d *dag.InMemoryCommitDag, maxBytes int) (commits []document.Commit, stubs []document.CommitStub, done bool, err error) {
	commits, _, stubs, done, err = c.pageFrom(d, maxBytes, false)
	return commits, stubs, done, err
}

// pagePayloads is page for a host filling a pack (C4 in docs/kdb-memory-cassandra-plan.md): a
// commit the log holds comes back as its logged payload - the exact bytes the pack carries, whose
// SHA-256 is the commit's hash - and its commit without operations, which nothing but the
// payload needs. That spares the host a frame decode, a rehash and a re-encode per commit; a
// commit the log cannot supply (not logged yet, or no log) is loaded and encoded as before,
// with a nil payload.
func (c *missingCursor) pagePayloads(d *dag.InMemoryCommitDag, maxBytes int) (commits []document.Commit, payloads [][]byte, stubs []document.CommitStub, done bool, err error) {
	return c.pageFrom(d, maxBytes, true)
}

func (c *missingCursor) pageFrom(d *dag.InMemoryCommitDag, maxBytes int, wantPayloads bool) (commits []document.Commit, payloads [][]byte, stubs []document.CommitStub, done bool, err error) {
	if maxBytes <= 0 {
		maxBytes = DefaultPageBytes
	}
	c.pageLen, c.pageTips, c.pageStubs = 0, nil, nil
	size := 0
	for _, h := range c.order {
		cm, ok := d.GetCommit(h)
		if !ok {
			return nil, nil, nil, false, errCursorStale
		}
		var payload []byte
		n := 0
		if wantPayloads {
			p, logged, err := d.LoggedPayload(h)
			if err != nil {
				return nil, nil, nil, false, err
			}
			if logged {
				payload, cm.Operations, n = p, nil, len(p)
			}
		}
		if payload == nil {
			ops, err := d.CommitOperations(h)
			if err != nil {
				return nil, nil, nil, false, err
			}
			cm.Operations = ops
			n = commitSize(cm)
		}
		if len(commits) > 0 && size+n > maxBytes {
			break
		}
		size += n
		commits = append(commits, cm)
		if wantPayloads {
			payloads = append(payloads, payload)
		}
		for _, p := range cm.ParentHashes {
			if s, ok := c.stubs[p]; ok && !containsStub(stubs, p) {
				stubs = append(stubs, s)
			}
		}
	}
	c.pageLen, c.pageTips, c.pageStubs = len(commits), pageTips(commits), stubs
	return commits, payloads, stubs, c.pageLen == len(c.order), nil
}

func containsStub(stubs []document.CommitStub, h codec.Hash) bool {
	for _, s := range stubs {
		if s.OriginalHash == h {
			return true
		}
	}
	return false
}

// maxCursorHeld bounds missingCursor.held.
const maxCursorHeld = 64

// advance moves past the page last returned: the peer has stored it.
func (c *missingCursor) advance() {
	c.held = append(c.held, c.pageTips...)
	// Only a recompute reads held, and any tip it lacks costs a resend, never a wrong answer: keep
	// the newest, so a transfer of many pages does not grow it by one page's tips each.
	if len(c.held) > maxCursorHeld {
		c.held = append(c.held[:0:0], c.held[len(c.held)-maxCursorHeld:]...)
	}
	c.order = c.order[c.pageLen:]
	for _, s := range c.pageStubs {
		delete(c.stubs, s.OriginalHash)
	}
	c.pageLen, c.pageTips, c.pageStubs = 0, nil, nil
}

// continues reports whether a request for wants with haves is the next page of this cursor's
// transfer: the same wants, from a peer that now names the last page's tips as held.
func (c *missingCursor) continues(wants, haves []codec.Hash) bool {
	if c.pageLen == 0 || len(wants) != len(c.wants) {
		return false
	}
	for i := range wants {
		if wants[i] != c.wants[i] {
			return false
		}
	}
	held := make(map[codec.Hash]struct{}, len(haves))
	for _, h := range haves {
		held[h] = struct{}{}
	}
	for _, t := range c.pageTips {
		if _, ok := held[t]; !ok {
			return false
		}
	}
	return true
}
