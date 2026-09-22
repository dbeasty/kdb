package peersync

import (
	"fmt"
	"sort"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/storage"
)

// LocalNode is what peer sync needs from the runtime it feeds. A peer is just another writer:
// anything that moves a namespace's head on a peer's behalf must be serialized against the
// runtime's own writers and must run the same post-commit hooks they do (indexes, unique keys,
// commit listeners). Without this, sync raced local commits and left every derived structure
// behind the documents it describes (docs/kdb-distributed-plan.md, defects D2/D3/D8).
type LocalNode interface {
	// Exclusive runs fn under the runtime's write serialization - for the server, its
	// writeGate. It may refuse (draining, fenced, queue full) without running fn.
	Exclusive(fn func() error) error
	// Advanced is called inside Exclusive, once the head has moved, describing what moved it.
	// An error is returned to the ingest caller but does not undo the head move: the commits are
	// already durable and reachable, and derived state is rebuildable.
	Advanced(step AdvanceStep) error
}

// AdvanceStep describes one head move performed by Ingest.
type AdvanceStep struct {
	// Commits are the commits that became reachable from main, parents first. The last one is
	// the new head.
	Commits []document.Commit
	// Applied is the net change written to live document storage for this move: one final
	// operation per document, the delta from the previous head's tree to the new one.
	Applied []document.Op
}

// IngestEnv is everything Ingest needs about the namespace it feeds.
type IngestEnv struct {
	DAG         *dag.InMemoryCommitDag
	Storage     storage.Adapter
	NamespaceID string
	// Node serializes the head decision against local writers and receives post-commit
	// notifications. nil serializes only against other ingests of the same namespace in this
	// process (the embedded / test default).
	Node LocalNode
	// Persist durably logs a commit that has just become reachable from main - adopted from a
	// peer, or a merge created here. nil means peer sync has no durability of its own (memory
	// runtimes). PersistAsync, when set, is preferred: it queues under the node's serialization
	// and is waited for after it, so a large fast-forward shares fsyncs instead of paying one per
	// commit.
	//
	// Only adopted commits are logged, and in the order main moved to them. Delta replay applies
	// every logged commit to the live tree in log order and moves main to each - so a commit
	// stored but not adopted (a conflicting side, a page of a push still in flight) would, on
	// restart, be replayed onto main as if it had been.
	Persist      func(document.Commit) error
	PersistAsync func(document.Commit) (wait func() error, err error)
	// ApplyToStorage writes the documents a head move introduces into Storage. false leaves
	// storage untouched on fast-forward - DAG-only callers (tests of the decision logic). The
	// auto-merge path always writes storage, since the merge tree is built there.
	ApplyToStorage bool
	Resolution     ResolutionOptions
	// Conflicts, when set, records every refused ref update durably and clears it once a later
	// sync with the same peer succeeds - see ConflictQueue.
	Conflicts *ConflictQueue
	// Peer names the node the incoming history came from, for the conflict queue and tracking
	// branches. Empty when unknown.
	Peer string
	// Self is this node's id, for deciding whether it is the one that notifies a resolver
	// authority. Empty when unknown.
	Self string
	// CheckCommit, when set, is asked about every commit a head move would adopt; an error refuses
	// the move - the commits stay stored, the head stays put, and the refusal is queued as a
	// conflict. How a single-home namespace refuses writes from a superseded home.
	CheckCommit func(document.Commit) error
	// CanInstallSnapshot, when set, is asked before a snapshot changes anything - a namespace that
	// could not make one durable refuses it up front rather than after installing it.
	CanInstallSnapshot func() error
	// SnapshotInstalled makes an installed snapshot durable - see InstallSnapshot. nil leaves it
	// in memory, which is all a memory runtime has.
	SnapshotInstalled func(document.Commit) error
	// MaxClockSkew refuses a commit timestamped further than this past local wall time. Commits
	// are timestamped at least one microsecond after their parents, so a single commit from a node
	// whose clock runs far ahead would drag every later commit that descends from it into that
	// future. 0 means DefaultMaxClockSkew; negative disables the check.
	MaxClockSkew time.Duration
}

// DefaultMaxClockSkew is how far ahead of this node's clock a replicated commit may be.
const DefaultMaxClockSkew = 5 * time.Minute

// ClockSkewError refuses a commit timestamped too far in the future.
type ClockSkewError struct {
	CommitHash codec.Hash
	Ahead      time.Duration
	Max        time.Duration
}

func (e *ClockSkewError) Error() string {
	return fmt.Sprintf("peer sync: commit %s is timestamped %s ahead of this node's clock (limit %s) - "+
		"the node that made it has a clock running fast", e.CommitHash.Hex(), e.Ahead.Round(time.Second), e.Max)
}

func (env IngestEnv) checkSkew(c document.Commit) error {
	max := env.MaxClockSkew
	if max < 0 {
		return nil
	}
	if max == 0 {
		max = DefaultMaxClockSkew
	}
	ahead := time.Duration(c.Timestamp.EpochMicros()-time.Now().UnixMicro()) * time.Microsecond
	if ahead > max {
		return &ClockSkewError{CommitHash: c.Hash, Ahead: ahead, Max: max}
	}
	return nil
}

// IngestResult reports what Ingest did.
type IngestResult struct {
	// Stored counts commits that were new to this node's DAG.
	Stored  int
	Outcome CommitPushOutcome
	// Head is main's head after the decision.
	Head codec.Hash
}

// Ingest stores commits received from a peer and decides what main should point at, performing
// that decision - the only function that moves a namespace's head on a peer's behalf. Both the
// host's CommitPush handler and the client's pull loop call it, so push and pull cannot drift.
//
// Storing happens first and outside the node's serialization: history is never lost, and adding
// a commit to the DAG changes nothing any reader can see. Everything that can - the head, live
// storage, indexes - happens inside it.
//
// On a fast-forward the net effect of every newly reachable commit is applied to storage in one
// step and verified against the incoming head's declared tree, rather than replaying commit by
// commit: replay can only follow first parents, and a fast-forward across a merge whose first
// parent is not on this node's side would check side-branch commits against a tree that already
// contains the other side, failing a perfectly valid history.
func Ingest(env IngestEnv, commits []document.Commit, incomingHead codec.Hash) (IngestResult, error) {
	stored, err := StoreCommits(env, commits, nil)
	if err != nil {
		return IngestResult{Stored: stored}, err
	}
	res, err := Adopt(env, incomingHead)
	res.Stored = stored
	return res, err
}

// StoreCommits adds commits (parents first) and stubs to the DAG and decides nothing: no head
// moves, no storage changes, nothing is logged - see IngestEnv.Persist for why logging waits for
// adoption. It is the first half of Ingest, used alone for every page of a multi-page transfer
// but the last. Returns how many commits were new; on error, the commits before the failing one
// stay stored and the count says how many.
func StoreCommits(env IngestEnv, commits []document.Commit, stubs []document.CommitStub) (int, error) {
	for _, s := range stubs {
		env.DAG.PutStub(s)
	}
	stored := 0
	for _, c := range commits {
		if env.DAG.HasCommit(c.Hash) {
			continue
		}
		if err := env.checkSkew(c); err != nil {
			return stored, err
		}
		if err := env.DAG.PutCommit(c, true); err != nil {
			return stored, fmt.Errorf("peer sync: storing commit %s: %w", c.Hash.Hex(), err)
		}
		stored++
	}
	return stored, nil
}

// Adopt decides what main should point at given incomingHead, a commit already stored, and
// performs that decision under the node's serialization - the second half of Ingest.
func Adopt(env IngestEnv, incomingHead codec.Hash) (IngestResult, error) {
	var res IngestResult
	if !env.DAG.HasCommit(incomingHead) {
		head, err := env.DAG.Head()
		res.Head = head
		return res, err
	}
	node := env.Node
	if node == nil {
		node = lockOnlyNode{}
	}
	var durable []func() error
	var adopted []document.Commit
	err := node.Exclusive(func() error {
		lock := divergenceLockFor(env.NamespaceID)
		lock.Lock()
		defer lock.Unlock()
		// Read inside the serialization: a head read before it is a hint at best, and planning
		// against it is exactly how a concurrent local commit used to be dropped from main.
		localHead, err := env.DAG.Head()
		if err != nil {
			return err
		}
		if env.CheckCommit != nil && ResolveHeadUpdate(env.DAG, localHead, incomingHead) != HeadAlreadyAncestor {
			adopting, err := commitsBetween(env.DAG, incomingHead, localHead)
			if err != nil {
				return err
			}
			for _, c := range adopting {
				if cerr := env.CheckCommit(c); cerr != nil {
					res.Outcome = CommitPushOutcome{Kind: OutcomeConflict}
					_ = env.noteConflict("branch:"+mainBranch, localHead, incomingHead, &kdberr.ConflictReport{
						TransactionID: c.Hash.Hex(), BaseHash: localHead.Hex(), TargetHash: incomingHead.Hex(),
					})
					return cerr
				}
			}
		}
		switch ResolveHeadUpdate(env.DAG, localHead, incomingHead) {
		case HeadAlreadyAncestor:
			res.Outcome = CommitPushOutcome{Kind: OutcomeNoOp}
			return nil
		case HeadFastForward:
			step, err := fastForward(env, localHead, incomingHead)
			if err != nil {
				return err
			}
			res.Outcome = CommitPushOutcome{Kind: OutcomeFastForwarded}
			if durable, err = env.persistAll(step.Commits); err != nil {
				return err
			}
			adopted = step.Commits
			return advance(node, step)
		default:
			outcome, step, err := resolveDivergedLocked(env.DAG, env.Storage, env.NamespaceID, localHead, incomingHead, env.Resolution)
			if err != nil {
				return err
			}
			res.Outcome = outcome
			if outcome.Kind == OutcomeConflict {
				return env.noteConflict("branch:"+mainBranch, localHead, incomingHead, outcome.Report, outcome.Details...)
			}
			if outcome.MergeCommit == nil {
				return nil
			}
			// The remote side's commits, then the merge: the order main came to reach them.
			if durable, err = env.persistAll(step.Commits); err != nil {
				return err
			}
			adopted = step.Commits
			return advance(node, step)
		}
	})
	// Waited for outside the serialization, like a local commit's: the log position is fixed,
	// so the next writer can proceed while this one's fsync is in flight.
	for _, wait := range durable {
		if werr := wait(); werr != nil && err == nil {
			err = fmt.Errorf("peer sync: persisting adopted commits: %w", werr)
		}
	}
	head, headErr := env.DAG.Head()
	res.Head = head
	if err != nil {
		return res, err
	}
	if res.Outcome.Kind != OutcomeConflict {
		if cerr := env.clearConflict("branch:" + mainBranch); cerr != nil {
			return res, cerr
		}
	}
	if headErr == nil && len(adopted) > 0 {
		if cerr := env.afterAdvance(adopted, head); cerr != nil {
			return res, cerr
		}
	}
	return res, headErr
}

// afterAdvance keeps the conflict queue in step with commits main has just adopted: it records
// the provisional decisions their merges made, closes the entries they resolve, and drops
// divergences main now contains - a conflict some other node settled, whose resolution arrived
// here as an ordinary fast-forward.
func (env IngestEnv) afterAdvance(adopted []document.Commit, head codec.Hash) error {
	if env.Conflicts == nil {
		return nil
	}
	for _, c := range adopted {
		if id, ok := ResolvedEntry(c); ok {
			if err := env.Conflicts.Remove(id); err != nil {
				return err
			}
		}
		if ids := ProvisionalDocuments(c); len(ids) > 0 {
			if err := env.noteProvisional(c, ids); err != nil {
				return err
			}
		}
	}
	return SweepIncorporated(env.DAG, env.Conflicts, head)
}

// SweepIncorporated drops every divergence of main whose incoming side head already contains,
// with its tracking branch: nothing about it is left to decide.
func SweepIncorporated(d *dag.InMemoryCommitDag, q *ConflictQueue, head codec.Hash) error {
	for _, e := range q.List() {
		if e.Kind != ConflictDivergence || e.Ref != "branch:"+mainBranch {
			continue
		}
		in, err := codec.HashFromHex(e.IncomingHex)
		if err != nil || !d.HasCommit(in) || (in != head && !d.IsAncestor(in, head)) {
			continue
		}
		if e.TrackingRef != "" {
			_ = d.DeleteBranch(e.TrackingRef)
		}
		if err := q.Remove(e.ID); err != nil {
			return err
		}
	}
	return nil
}

// noteProvisional records merge m's provisional decisions for the authority: each document's
// merged value as the local side, the value it displaced as the incoming one.
func (env IngestEnv) noteProvisional(m document.Commit, ids []codec.UUID) error {
	if len(m.ParentHashes) < 2 {
		return nil
	}
	vi := valueIndex{d: env.DAG, store: env.Storage, ns: env.NamespaceID}
	p0, p1 := m.ParentHashes[0], m.ParentHashes[1]
	v0, err := vi.valuesAt(p0, ids)
	if err != nil {
		return err
	}
	v1, err := vi.valuesAt(p1, ids)
	if err != nil {
		return err
	}
	var base map[codec.UUID]docVal
	if anc := env.DAG.CommonAncestor(p0, p1); anc != nil {
		if base, err = vi.valuesAt(*anc, ids); err != nil {
			return err
		}
	}
	merged := map[codec.UUID]*string{}
	for _, op := range m.Operations {
		merged[opDocID(op)] = opBody(op)
	}
	e := ConflictEntry{
		ID: ConflictID(ConflictProvisional, env.NamespaceID, m.Hash.Hex()), Kind: ConflictProvisional,
		Namespace: env.NamespaceID, MergeHex: m.Hash.Hex(), Peer: env.Peer,
		Detail: "settled provisionally by last write; the authority may overrule it",
	}
	if a := env.Resolution.Chain.Authority(); a != nil {
		e.Authority, e.AuthorityNode = true, a.Node
	}
	for _, id := range ids {
		kept, keptAt, lost, lostAt := v0[id].body, p0, v1[id].body, p1
		if !sameBody(merged[id], kept) {
			kept, keptAt, lost, lostAt = lost, lostAt, kept, keptAt
		}
		oKept, err := vi.origin(keptAt, id)
		if err != nil {
			return err
		}
		oLost, err := vi.origin(lostAt, id)
		if err != nil {
			return err
		}
		e.Items = append(e.Items, kdberr.ConflictItem{
			DocumentID:    id.String(),
			OperationType: classifyConflictOp(bodyOp(id, kept), bodyOp(id, lost)),
			LocalDoc:      kept,
			IncomingDoc:   lost,
		})
		e.Details = append(e.Details, ConflictDetail{
			DocumentID: id.String(), Base: base[id].body,
			LocalOrigin: conflictOrigin(env.DAG, oKept), IncomingOrigin: conflictOrigin(env.DAG, oLost),
		})
	}
	if prev, ok := env.Conflicts.Get(e.ID); ok && prev.Kind == ConflictProvisional {
		return nil // already known; re-recording would only bump its sighting count
	}
	_, err = env.Conflicts.Record(e)
	return err
}

// noteConflict records a refused update of ref to incoming, and points a tracking branch at the
// incoming side so it stays reachable - retention keeps what a branch names - and resolution can
// find it after the peer has moved on.
func (env IngestEnv) noteConflict(ref string, local, incoming codec.Hash, report *kdberr.ConflictReport, details ...ConflictDetail) error {
	if env.Conflicts == nil {
		return nil
	}
	peer := env.Peer
	if peer == "" {
		peer = "unknown"
	}
	tracking := TrackingBranch(peer, ref)
	if _, ok := env.DAG.GetBranch(tracking); ok {
		if err := env.DAG.SetHead(tracking, incoming); err != nil {
			return err
		}
	} else if _, err := env.DAG.CreateBranch(tracking, incoming); err != nil {
		return err
	}
	e := ConflictEntry{
		ID: ConflictID(ConflictDivergence, env.NamespaceID, ref, peer), Kind: ConflictDivergence,
		Namespace: env.NamespaceID, Ref: ref, Peer: env.Peer,
		LocalHex: local.Hex(), IncomingHex: incoming.Hex(), TrackingRef: tracking,
	}
	if report != nil {
		e.Items = report.Conflicts
	}
	e.Details = details
	if a := env.Resolution.Chain.Authority(); a != nil && len(details) > 0 {
		e.Authority, e.AuthorityNode = true, a.Node
	}
	_, err := env.Conflicts.Record(e)
	return err
}

// clearConflict drops a recorded divergence of ref with this peer once an update of it has gone
// through, along with its tracking branch.
func (env IngestEnv) clearConflict(ref string) error {
	if env.Conflicts == nil {
		return nil
	}
	peer := env.Peer
	if peer == "" {
		peer = "unknown"
	}
	id := ConflictID(ConflictDivergence, env.NamespaceID, ref, peer)
	if _, ok := env.Conflicts.Get(id); !ok {
		return nil
	}
	_ = env.DAG.DeleteBranch(TrackingBranch(peer, ref))
	return env.Conflicts.Remove(id)
}

// persistAll queues commits for the log in order, returning what to wait on.
func (env IngestEnv) persistAll(commits []document.Commit) ([]func() error, error) {
	var waits []func() error
	for _, c := range commits {
		switch {
		case env.PersistAsync != nil:
			wait, err := env.PersistAsync(c)
			if err != nil {
				return waits, fmt.Errorf("peer sync: persisting commit %s: %w", c.Hash.Hex(), err)
			}
			waits = append(waits, wait)
		case env.Persist != nil:
			if err := env.Persist(c); err != nil {
				return waits, fmt.Errorf("peer sync: persisting commit %s: %w", c.Hash.Hex(), err)
			}
		}
	}
	return waits, nil
}

func advance(node LocalNode, step AdvanceStep) error {
	if len(step.Commits) == 0 {
		return nil
	}
	return node.Advanced(step)
}

// fastForward moves main from localHead to incomingHead, a descendant of it.
func fastForward(env IngestEnv, localHead, incomingHead codec.Hash) (AdvanceStep, error) {
	commits, err := commitsBetween(env.DAG, incomingHead, localHead)
	if err != nil {
		return AdvanceStep{}, err
	}
	applied := netEffect(commits)
	if env.ApplyToStorage {
		if err := applyAndVerify(env, localHead, incomingHead, applied); err != nil {
			return AdvanceStep{}, err
		}
	}
	if err := env.DAG.SetHead(mainBranch, incomingHead); err != nil {
		return AdvanceStep{}, err
	}
	return AdvanceStep{Commits: commits, Applied: applied}, nil
}

// applyAndVerify writes ops on top of fromHead's tree and requires the result to be exactly
// toHead's declared tree. Any failure discards the staged writes, so storage never runs ahead
// of (or disagrees with) the head.
func applyAndVerify(env IngestEnv, fromHead, toHead codec.Hash, ops []document.Op) error {
	from, err := env.DAG.GetCommitOrThrow(fromHead)
	if err != nil {
		return err
	}
	to, err := env.DAG.GetCommitOrThrow(toHead)
	if err != nil {
		return err
	}
	fail := func(err error) error {
		_ = env.Storage.DiscardPending(env.NamespaceID)
		return err
	}
	// Pre-images, read before anything is staged: if the built tree turns out not to be the one
	// the peer declared, they are what puts storage back on fromHead's tree. Ops carry no
	// pre-image of their own (kdb-history-not-invertible), so this is the only place to get one.
	ids := make([]codec.UUID, len(ops))
	for i, op := range ops {
		ids[i] = opDocID(op)
	}
	before, err := env.Storage.GetDocuments(env.NamespaceID, ids, from.DocumentTreeHash)
	if err != nil {
		return err
	}
	for _, op := range ops {
		switch o := op.(type) {
		case document.WriteOp:
			if err := env.Storage.PutDocument(env.NamespaceID, document.Document{ID: o.DocID, JSON: o.Patch}); err != nil {
				return fail(err)
			}
		case document.DeleteOp:
			if err := env.Storage.DeleteDocument(env.NamespaceID, o.DocID); err != nil {
				return fail(err)
			}
		}
	}
	built, err := env.Storage.CommitTree(env.NamespaceID, from.DocumentTreeHash)
	if err != nil {
		return fail(err)
	}
	if built.TreeHash != to.DocumentTreeHash {
		mismatch := &TreeMismatchError{CommitHash: toHead, Declared: to.DocumentTreeHash, Built: built.TreeHash}
		if err := restore(env, built.TreeHash, ids, before); err != nil {
			return fmt.Errorf("%w (and restoring the previous tree failed: %v)", mismatch, err)
		}
		return mismatch
	}
	env.DAG.PutDocumentTree(built)
	return nil
}

// restore writes pre-images back on top of the tree a failed apply left behind, returning live
// storage to the tree it started from.
func restore(env IngestEnv, onTree codec.Hash, ids []codec.UUID, before []*document.Document) error {
	for i, id := range ids {
		var err error
		if before[i] == nil {
			err = env.Storage.DeleteDocument(env.NamespaceID, id)
		} else {
			err = env.Storage.PutDocument(env.NamespaceID, *before[i])
		}
		if err != nil {
			_ = env.Storage.DiscardPending(env.NamespaceID)
			return err
		}
	}
	_, err := env.Storage.CommitTree(env.NamespaceID, onTree)
	return err
}

func opDocID(op document.Op) codec.UUID {
	switch o := op.(type) {
	case document.WriteOp:
		return o.DocID
	case document.DeleteOp:
		return o.DocID
	}
	return codec.UUID{}
}

// TreeMismatchError reports that applying a peer's commits produced a different tree than the
// one the incoming head declares - the peer's history and its claimed state disagree.
type TreeMismatchError struct {
	CommitHash codec.Hash
	Declared   codec.Hash
	Built      codec.Hash
}

func (e *TreeMismatchError) Error() string {
	return fmt.Sprintf("peer sync: commit %s declares tree %s but its history builds %s",
		e.CommitHash.Hex(), e.Declared.Hex(), e.Built.Hex())
}

// commitsBetween returns every commit reachable from head that is not base and not an ancestor
// of base, parents first, with operations loaded. Unlike Walk(head, &base), which prunes only at
// base's exact hash, this excludes all of base's history - including history reachable around
// base through a merge's other parent, which Walk would revisit.
func commitsBetween(d *dag.InMemoryCommitDag, head, base codec.Hash) ([]document.Commit, error) {
	if head == base {
		return nil, nil
	}
	inRange := map[codec.Hash]document.Commit{}
	queue := []codec.Hash{head}
	for len(queue) > 0 {
		h := queue[0]
		queue = queue[1:]
		if _, seen := inRange[h]; seen {
			continue
		}
		if h == base || d.IsAncestor(h, base) {
			continue
		}
		if d.HasStub(h) || !d.HasCommit(h) {
			continue
		}
		ops, err := d.CommitOperations(h)
		if err != nil {
			return nil, err
		}
		c, err := d.GetCommitOrThrow(h)
		if err != nil {
			return nil, err
		}
		c.Operations = ops
		inRange[h] = c
		queue = append(queue, c.ParentHashes...)
	}
	return topoSort(inRange), nil
}

// topoSort orders commits parents first (Kahn's algorithm over edges inside the set), breaking
// ties by hash so the order is identical on every node for the same set.
func topoSort(set map[codec.Hash]document.Commit) []document.Commit {
	pending := make(map[codec.Hash]int, len(set))
	children := make(map[codec.Hash][]codec.Hash, len(set))
	for h, c := range set {
		n := 0
		for _, p := range c.ParentHashes {
			if _, ok := set[p]; ok {
				n++
				children[p] = append(children[p], h)
			}
		}
		pending[h] = n
	}
	var ready []codec.Hash
	for h, n := range pending {
		if n == 0 {
			ready = append(ready, h)
		}
	}
	out := make([]document.Commit, 0, len(set))
	for len(ready) > 0 {
		sort.Slice(ready, func(i, j int) bool { return ready[i].Hex() < ready[j].Hex() })
		h := ready[0]
		ready = ready[1:]
		out = append(out, set[h])
		for _, ch := range children[h] {
			pending[ch]--
			if pending[ch] == 0 {
				ready = append(ready, ch)
			}
		}
	}
	return out
}

// netEffect folds commits (parents first) into one final operation per document, sorted by id.
// Topological order is what makes "last write" well defined: a merge commit comes after both of
// its sides, so its resolution of a document both sides touched overrides either side's write.
func netEffect(commits []document.Commit) []document.Op {
	final := map[codec.UUID]document.Op{}
	for _, c := range commits {
		for _, op := range c.Operations {
			switch o := op.(type) {
			case document.WriteOp:
				final[o.DocID] = o
			case document.DeleteOp:
				final[o.DocID] = o
			}
		}
	}
	out := make([]document.Op, 0, len(final))
	for _, id := range sortedDocIDs(final) {
		out = append(out, final[id])
	}
	return out
}

// lockOnlyNode is the LocalNode used when the caller supplies none: Ingest's own per-namespace
// divergence lock is the only serialization, and there are no hooks to run.
type lockOnlyNode struct{}

func (lockOnlyNode) Exclusive(fn func() error) error { return fn() }
func (lockOnlyNode) Advanced(AdvanceStep) error      { return nil }

// unsharedRoot reports the commit in page that failed to store because it is one of the peer's
// shallow roots whose parents this node does not hold: the page reached the bottom of a history
// the peer itself received as a snapshot.
func unsharedRoot(env IngestEnv, page []document.Commit, shallow []string) (codec.Hash, bool) {
	roots := map[string]bool{}
	for _, h := range shallow {
		roots[h] = true
	}
	for _, c := range page {
		if !roots[c.Hash.Hex()] || env.DAG.HasCommit(c.Hash) {
			continue
		}
		for _, p := range c.ParentHashes {
			if !env.DAG.HasCommit(p) {
				return c.Hash, true
			}
		}
	}
	return codec.Hash{}, false
}

// noteUnrelated records that this namespace cannot sync with the peer because of root, and
// returns the typed error for the sync result.
func (env IngestEnv) noteUnrelated(root codec.Hash, cause error) error {
	e := &UnrelatedHistoryError{Namespace: env.NamespaceID, Root: root, Cause: cause}
	if env.Conflicts != nil {
		peer := env.Peer
		if peer == "" {
			peer = "unknown"
		}
		_, _ = env.Conflicts.Record(ConflictEntry{
			ID: ConflictID(ConflictUnrelatedHistory, env.NamespaceID, peer, root.Hex()), Kind: ConflictUnrelatedHistory,
			Namespace: env.NamespaceID, Peer: env.Peer, IncomingHex: root.Hex(), Detail: e.Error(),
		})
	}
	return e
}
