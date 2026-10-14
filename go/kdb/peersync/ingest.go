package peersync

import (
	"bytes"
	"errors"
	"fmt"
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

// BootstrapNode is a LocalNode that can run a snapshot install's exclusive section as a long
// hold: the install keeps the write serialization for as long as the whole namespace takes to
// apply, so the node refuses writers to the namespace at once with a retryable busy error rather
// than letting each queue behind it until its deadline. A LocalNode without it gets Exclusive.
type BootstrapNode interface {
	ExclusiveBootstrap(fn func() error) error
}

// exclusiveBootstrap runs fn as a bootstrap's exclusive section on node.
func exclusiveBootstrap(node LocalNode, fn func() error) error {
	if b, ok := node.(BootstrapNode); ok {
		return b.ExclusiveBootstrap(fn)
	}
	return node.Exclusive(fn)
}

// AdvanceStep describes one head move performed by Ingest.
type AdvanceStep struct {
	// Commits are the commits that became reachable from main, parents first. The last one is
	// the new head.
	Commits []document.Commit
	// Applied is the net change written to live document storage for this move: one final
	// operation per document, the delta from the previous head's tree to the new one.
	Applied []document.Op
	// AppliedPages, when set, is more of Applied, too large to hold at once: it calls fn with
	// successive batches of operations, read back from disk. A snapshot install sets it rather
	// than Applied, which would be every document of the namespace in memory at once. Valid only
	// during the Advanced call it arrives in; a batch is valid only during fn. Read both through
	// EachApplied.
	AppliedPages func(fn func(ops []document.Op) error) error
}

// EachApplied calls fn with the step's applied operations in batches: Applied, then every batch
// AppliedPages yields. A step that applied nothing never calls fn.
func (s AdvanceStep) EachApplied(fn func(ops []document.Op) error) error {
	if len(s.Applied) > 0 {
		if err := fn(s.Applied); err != nil {
			return err
		}
	}
	if s.AppliedPages != nil {
		return s.AppliedPages(fn)
	}
	return nil
}

// HasApplied reports whether the step applied anything to live storage (or may have: a paged
// step is not counted up front).
func (s AdvanceStep) HasApplied() bool { return len(s.Applied) > 0 || s.AppliedPages != nil }

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
	// ShallowRootsChanged records the namespace's shallow roots durably when Deepen changes them -
	// see Deepen for the order it is called in. nil: nothing to record (memory runtimes).
	ShallowRootsChanged func([]codec.Hash) error
	// GraftRecorded records durably that root is grafted - a peer's shallow root taken in without
	// its parents (see Graft), which a replay must admit as such. nil: nothing to record (memory
	// runtimes).
	GraftRecorded func(root codec.Hash) error
	// SquashInstalled makes a squash catch-up durable (see squash.go): root, now main's head and a
	// shallow root, is recorded as one, changed (content hash to body) are the bodies it brought,
	// which no log segment holds, and a checkpoint is taken through the current log position with
	// main at root. Called inside the node's serialization. nil leaves the squash in memory, which
	// is all a memory runtime has.
	SquashInstalled func(root document.Commit, changed map[codec.Hash]string) error
	// SquashRefused, when set, makes this namespace refuse to serve squash catch-ups, saying why -
	// for one that must keep full history on every replica. Peers then replay. A pull into it
	// does not squash either.
	SquashRefused string
	// MaxClockSkew refuses a commit timestamped further than this past local wall time. Commits
	// are timestamped at least one microsecond after their parents, so a single commit from a node
	// whose clock runs far ahead would drag every later commit that descends from it into that
	// future. 0 means DefaultMaxClockSkew; negative disables the check.
	MaxClockSkew time.Duration
	// StagingDir is where a session stages what it must take in whole before it can act on it -
	// a snapshot's pages before they are installed, a pushed graft's before it is verified - so
	// memory holds a page, not the namespace. Empty uses the system temporary directory.
	StagingDir string
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
//
// Commits are stored marked unlogged (dag.PutUnloggedCommit) until persistAll logs them, so that
// a checkpoint taken meanwhile - a clean close while a push is refused by the drain, a
// maintenance pass while a conflict waits - does not describe history only memory holds.
func StoreCommits(env IngestEnv, commits []document.Commit, stubs []document.CommitStub) (int, error) {
	for _, s := range stubs {
		env.DAG.PutStub(s)
	}
	stored := 0
	for _, c := range commits {
		if env.DAG.HasCommit(c.Hash) {
			if err := env.refillUnlogged(c); err != nil {
				return stored, fmt.Errorf("peer sync: repairing commit %s: %w", c.Hash.Hex(), err)
			}
			continue
		}
		if err := env.checkSkew(c); err != nil {
			return stored, err
		}
		if err := env.DAG.PutUnloggedCommit(c); err != nil {
			return stored, fmt.Errorf("peer sync: storing commit %s: %w", c.Hash.Hex(), err)
		}
		stored++
	}
	return stored, nil
}

// refillUnlogged repairs a commit this node holds without operations the delta log can supply,
// from the copy a peer just sent. Earlier versions checkpointed commits received from a peer and
// never logged; such a commit reopens with its operations gone and no frame to load them from,
// and since the DAG holds it, "already have it" made every later sync skip the copy that could
// fix it and then fail when adoption needed its operations. The copy is checked against the
// commit's hash, which covers its operations.
//
// Only a commit whose operations are evicted is checked, so a resident one costs a map lookup;
// one the log does hold is left alone.
func (env IngestEnv) refillUnlogged(c document.Commit) error {
	if len(c.Operations) == 0 || !env.DAG.OperationsEvicted(c.Hash) {
		return nil
	}
	if _, err := env.DAG.CommitOperations(c.Hash); err == nil || !errors.Is(err, dag.ErrOperationsNotLogged) {
		return nil
	}
	return env.DAG.RefillUnloggedCommit(c)
}

// Adopt decides what main should point at given incomingHead, a commit already stored, and
// performs that decision under the node's serialization - the second half of Ingest.
func Adopt(env IngestEnv, incomingHead codec.Hash) (IngestResult, error) {
	return adopt(env, incomingHead, false)
}

// AdoptFastForward is Adopt when incomingHead fast-forwards main, and nothing at all otherwise:
// no merge, no conflict recorded or cleared, outcome OutcomeNoOp. A pull uses it to adopt a long
// range a prefix at a time as the pages arrive (V2ClientConfig.AdoptEveryBytes): main moves only
// to commits that extend it, so the result is the history a single adoption at the end would
// have produced, but the received commits are logged - and their operations become evictable -
// as the transfer goes, instead of all staying resident until its last page. Whether main can
// fast-forward is decided under the node's serialization; a local commit that diverged main in
// between makes this a no-op and leaves the decision to the adoption at the end.
func AdoptFastForward(env IngestEnv, incomingHead codec.Hash) (IngestResult, error) {
	return adopt(env, incomingHead, true)
}

func adopt(env IngestEnv, incomingHead codec.Hash, fastForwardOnly bool) (IngestResult, error) {
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
	// made is the commit this node created to adopt incomingHead - a merge or a join - once it
	// has been queued for the log.
	var made *document.Commit
	// skipped: fastForwardOnly, and main could not fast-forward - nothing was decided.
	skipped := false
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
		if fastForwardOnly && ResolveHeadUpdate(env.DAG, localHead, incomingHead) != HeadFastForward {
			skipped = true
			return nil
		}
		if env.CheckCommit != nil && ResolveHeadUpdate(env.DAG, localHead, incomingHead) != HeadAlreadyAncestor {
			adopting, err := commitsBetween(env.DAG, incomingHead, localHead)
			if err != nil {
				return err
			}
			for _, c := range adoptable(env.DAG, adopting) {
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
				return env.undoAdoption(err, localHead, incomingHead, step, nil, env.ApplyToStorage)
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
				// A merge writes its documents to storage whatever ApplyToStorage says; a join
				// writes none (step.Applied is empty).
				return env.undoAdoption(err, localHead, outcome.MergeCommit.Hash, step, outcome.MergeCommit, true)
			}
			adopted = step.Commits
			made = outcome.MergeCommit
			return advance(node, step)
		}
	})
	// Waited for outside the serialization, like a local commit's: the log position is fixed,
	// so the next writer can proceed while this one's fsync is in flight.
	waitFailed := false
	for _, wait := range durable {
		if werr := wait(); werr != nil {
			waitFailed = true
			if err == nil {
				err = fmt.Errorf("peer sync: persisting adopted commits: %w", werr)
			}
		}
	}
	// A failed wait is past undoing here - main has been released to other writers, and a log
	// that failed a write refuses every later one (embed's commitLogWriter latches it) - so it is
	// left to the checkpoint: the commits whose waits failed got their marks back (persistAll),
	// and the one this node made gets one too. While main names any of them, or a commit built
	// on one, no checkpoint is written (dag.CheckpointSnapshotLogged), and the next open replays
	// what the log does hold.
	if waitFailed && made != nil {
		env.DAG.MarkUnlogged(made.Hash)
	}
	head, headErr := env.DAG.Head()
	res.Head = head
	if err != nil {
		return res, err
	}
	if skipped {
		res.Outcome = CommitPushOutcome{Kind: OutcomeNoOp}
		return res, headErr
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
		if err != nil || !d.HasCommit(in) || !d.Incorporates(in, head) {
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
//
// Each commit's unlogged mark is cleared before its frame is queued, not after: a checkpoint
// claiming the segment the frame lands in must describe the commit, or the next open would skip
// the frame and lose it - see dag.MarkLogged. A commit that was marked gets its mark back if
// queueing it fails, and if waiting for it does: a failed wait means the log may not hold the
// frame, and a checkpoint that leaves out a commit the log does hold costs only a refetch, where
// one that describes a commit the log lacks breaks the namespace. Commits after a refused one are
// never queued and keep the marks they had.
//
// A commit that was not marked - one this node made, a merge or a join, or history already
// logged - is left unmarked either way. The caller knows which of those is new and marks it
// (undoAdoption); history already logged is durable whatever this attempt did.
func (env IngestEnv) persistAll(commits []document.Commit) ([]func() error, error) {
	var waits []func() error
	for _, c := range commits {
		h := c.Hash
		was := env.DAG.IsUnlogged(h)
		env.DAG.MarkLogged(h)
		failed := func() {
			if was {
				env.DAG.MarkUnlogged(h)
			}
		}
		switch {
		case env.PersistAsync != nil:
			wait, err := env.PersistAsync(c)
			if err != nil {
				failed()
				return waits, fmt.Errorf("peer sync: persisting commit %s: %w", h.Hex(), err)
			}
			waits = append(waits, func() error {
				err := wait()
				if err != nil {
					failed()
				}
				return err
			})
		case env.Persist != nil:
			if err := env.Persist(c); err != nil {
				failed()
				return waits, fmt.Errorf("peer sync: persisting commit %s: %w", h.Hex(), err)
			}
		}
	}
	return waits, nil
}

// undoAdoption takes back an adoption whose log write was refused under the serialization: main
// moved to newHead, and (when storageMoved) live storage to newHead's tree, before anything was
// logged, and nothing durable stands behind either. Left in place, every later local write would
// build on - and be logged on top of - a head whose history the log does not have, and replay
// cannot admit a commit whose parent it never stored. made is the commit this node created for
// the adoption (a merge or a join), marked unlogged so no checkpoint describes it; the peer's
// commits behind it kept their marks (persistAll).
//
// Returns cause, with the undo's own failure if it had one. Frames queued before the refusal stay
// queued; the next open's replay adopts them, as it would after a crash.
func (env IngestEnv) undoAdoption(cause error, localHead, newHead codec.Hash, step AdvanceStep, made *document.Commit, storageMoved bool) error {
	if made != nil {
		env.DAG.MarkUnlogged(made.Hash)
	}
	if err := env.DAG.SetHead(mainBranch, localHead); err != nil {
		return fmt.Errorf("%w (and main could not be put back: %v)", cause, err)
	}
	if !storageMoved || len(step.Applied) == 0 {
		return cause
	}
	if err := rewindStorage(env, newHead, localHead, step.Applied); err != nil {
		return fmt.Errorf("%w (and storage could not be put back on the previous head: %v)", cause, err)
	}
	return cause
}

// rewindStorage puts live storage, standing on from's tree after ops took it there, back on to's
// tree: every document ops touched gets its value at to's tree back.
func rewindStorage(env IngestEnv, from, to codec.Hash, ops []document.Op) error {
	fromCommit, err := env.DAG.GetCommitOrThrow(from)
	if err != nil {
		return err
	}
	toCommit, err := env.DAG.GetCommitOrThrow(to)
	if err != nil {
		return err
	}
	ids := make([]codec.UUID, len(ops))
	for i, op := range ops {
		ids[i] = opDocID(op)
	}
	before, err := env.Storage.GetDocuments(env.NamespaceID, ids, toCommit.DocumentTreeHash)
	if err != nil {
		return err
	}
	built, err := restoreTree(env, fromCommit.DocumentTreeHash, ids, before)
	if err != nil {
		return err
	}
	if built.TreeHash != toCommit.DocumentTreeHash {
		return &TreeMismatchError{CommitHash: to, Declared: toCommit.DocumentTreeHash, Built: built.TreeHash}
	}
	return nil
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
	commits = adoptable(env.DAG, commits)
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
	_, err := restoreTree(env, onTree, ids, before)
	return err
}

// restoreTree is restore, returning the tree it built.
func restoreTree(env IngestEnv, onTree codec.Hash, ids []codec.UUID, before []*document.Document) (document.DocumentTree, error) {
	for i, id := range ids {
		var err error
		if before[i] == nil {
			err = env.Storage.DeleteDocument(env.NamespaceID, id)
		} else {
			err = env.Storage.PutDocument(env.NamespaceID, *before[i])
		}
		if err != nil {
			_ = env.Storage.DiscardPending(env.NamespaceID)
			return document.DocumentTree{}, err
		}
	}
	return env.Storage.CommitTree(env.NamespaceID, onTree)
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
	// The range is collected as a slice and an index into it, not a map of commits: a catch-up's
	// range is every commit it replays, and a map entry per commit (plus topoSort's own maps
	// over it) was much of what a replay allocated per commit.
	var inRange []document.Commit
	index := map[codec.Hash]int32{}
	belowBase := d.AncestorTest(base)
	queue := []codec.Hash{head}
	for next := 0; next < len(queue); next++ {
		h := queue[next]
		if _, seen := index[h]; seen {
			continue
		}
		if h == base || belowBase(h) {
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
		index[h] = int32(len(inRange))
		inRange = append(inRange, c)
		queue = append(queue, c.ParentHashes...)
	}
	return topoSortIndexed(inRange, index), nil
}

// adoptable drops shallow roots from commits a head move is about to adopt. A shallow root - a
// snapshot's root, a graft, or a commit behind this node's root whose state a pull took in
// (boundary.go) - was admitted and logged on its own, and is never newly reachable history: its
// operations are the change from parents this node does not hold, so applying them, logging them
// again, checking them against a home's fence or announcing them to commit listeners as if main had
// just taken them would all be wrong. The merge above such a root already carries every document
// its parents disagree on.
//
// The result never shares spare capacity with commits, so appending to it cannot write into the
// caller's slice; when nothing is dropped - every catch-up but one crossing a graft - it is commits
// itself, capped, rather than a copy of every commit in the range.
func adoptable(d *dag.InMemoryCommitDag, commits []document.Commit) []document.Commit {
	first := -1
	for i := range commits {
		if d.IsShallow(commits[i].Hash) {
			first = i
			break
		}
	}
	if first < 0 {
		return commits[:len(commits):len(commits)]
	}
	out := make([]document.Commit, first, len(commits)-1)
	copy(out, commits[:first])
	for _, c := range commits[first+1:] {
		if !d.IsShallow(c.Hash) {
			out = append(out, c)
		}
	}
	return out
}

// topoSort orders commits parents first (Kahn's algorithm over edges inside the set), breaking
// ties by hash so the order is identical on every node for the same set.
func topoSort(set map[codec.Hash]document.Commit) []document.Commit {
	list := make([]document.Commit, 0, len(set))
	index := make(map[codec.Hash]int32, len(set))
	for h, c := range set {
		index[h] = int32(len(list))
		list = append(list, c)
	}
	return topoSortIndexed(list, index)
}

// topoSortIndexed is topoSort over list, with index mapping each commit's hash to its position.
// The order of list does not matter: ties are broken by hash. It sorts list in place and returns
// it.
func topoSortIndexed(list []document.Commit, index map[codec.Hash]int32) []document.Commit {
	n := len(list)
	if n == 0 {
		return []document.Commit{}
	}
	// pending[i] counts list[i]'s parents inside the set (a parent named twice counts twice, and
	// is released twice); children are laid out flat, list[i]'s at edges[start[i]:start[i+1]].
	pending := make([]int32, n)
	start := make([]int32, n+1)
	for i := range list {
		for _, p := range list[i].ParentHashes {
			if j, ok := index[p]; ok {
				pending[i]++
				start[j+1]++
			}
		}
	}
	for i := 0; i < n; i++ {
		start[i+1] += start[i]
	}
	edges := make([]int32, start[n])
	fill := append([]int32(nil), start[:n]...)
	for i := range list {
		for _, p := range list[i].ParentHashes {
			if j, ok := index[p]; ok {
				edges[fill[j]] = int32(i)
				fill[j]++
			}
		}
	}
	ready := commitHeap{list: list}
	for i := range pending {
		if pending[i] == 0 {
			ready.push(int32(i))
		}
	}
	order := make([]int32, 0, n) // order[k] is the position in list of the k-th commit out
	for len(ready.idx) > 0 {
		i := ready.pop()
		order = append(order, i)
		for _, ch := range edges[start[i]:start[i+1]] {
			pending[ch]--
			if pending[ch] == 0 {
				ready.push(ch)
			}
		}
	}
	if len(order) < n {
		// Commits left pending - only a cycle could do it, and content addressing rules cycles
		// out - are dropped, as they always were. Copied out, since order is no permutation.
		out := make([]document.Commit, len(order))
		for k, i := range order {
			out[k] = list[i]
		}
		return out
	}
	// The order is applied to list in place, cycle by cycle, rather than copied out: the commits
	// are the bulk of what a long range holds.
	moved := make([]bool, n)
	for k := range order {
		if moved[k] {
			continue
		}
		held := list[k]
		j := k
		for {
			moved[j] = true
			src := int(order[j])
			if src == k {
				list[j] = held
				break
			}
			list[j] = list[src]
			j = src
		}
	}
	return list
}

// commitHeap is a min-heap of positions in list, by their commits' hash bytes - the order of
// their hex strings, which is what topoSort breaks ties by. Typed, rather than container/heap's,
// so a push does not box its element.
type commitHeap struct {
	list []document.Commit
	idx  []int32
}

func (h *commitHeap) less(a, b int) bool {
	return bytes.Compare(h.list[h.idx[a]].Hash.Bytes[:], h.list[h.idx[b]].Hash.Bytes[:]) < 0
}

func (h *commitHeap) push(i int32) {
	h.idx = append(h.idx, i)
	for c := len(h.idx) - 1; c > 0; {
		p := (c - 1) / 2
		if !h.less(c, p) {
			break
		}
		h.idx[c], h.idx[p] = h.idx[p], h.idx[c]
		c = p
	}
}

func (h *commitHeap) pop() int32 {
	top := h.idx[0]
	last := len(h.idx) - 1
	h.idx[0] = h.idx[last]
	h.idx = h.idx[:last]
	for p := 0; ; {
		c := 2*p + 1
		if c >= last {
			break
		}
		if r := c + 1; r < last && h.less(r, c) {
			c = r
		}
		if !h.less(c, p) {
			break
		}
		h.idx[c], h.idx[p] = h.idx[p], h.idx[c]
		p = c
	}
	return top
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
