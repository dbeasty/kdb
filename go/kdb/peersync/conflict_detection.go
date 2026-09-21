package peersync

import (
	"bytes"
	"sort"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/transaction"
)

const mainBranch = "main"

// HeadUpdate classifies how an incoming peer head relates to the local head - git's
// fast-forward-or-diverged model, not "always replace it" (the bug this file exists to fix; see
// kdb-peer-sync's PeerSyncConflictDetection.kt, Component 39, which this file ports field-for-
// field for the Go client/host - the Kotlin side already had the identical bug fixed here).
type HeadUpdate int

const (
	// HeadFastForward: incomingHead is a descendant of localHead - safe to move the branch pointer.
	HeadFastForward HeadUpdate = iota
	// HeadAlreadyAncestor: localHead is already at or ahead of incomingHead - nothing to do.
	HeadAlreadyAncestor
	// HeadDiverged: neither is an ancestor of the other - a real divergence, resolved by ResolveDivergence.
	HeadDiverged
)

// ResolveHeadUpdate decides whether incomingHead can simply replace localHead.
func ResolveHeadUpdate(d *dag.InMemoryCommitDag, localHead, incomingHead codec.Hash) HeadUpdate {
	if localHead == incomingHead {
		return HeadAlreadyAncestor
	}
	if d.IsAncestor(localHead, incomingHead) {
		return HeadFastForward
	}
	if d.IsAncestor(incomingHead, localHead) {
		return HeadAlreadyAncestor
	}
	return HeadDiverged
}

// CommitPushOutcomeKind classifies the result of ResolveDivergence.
type CommitPushOutcomeKind int

const (
	OutcomeNoOp CommitPushOutcomeKind = iota
	OutcomeFastForwarded
	// OutcomeMerged: divergence with no real per-document conflict - both sides' commits are now
	// reachable from main via a real two-parent merge commit (git-style; see AppendMergeCommit).
	OutcomeMerged
	// OutcomeConflict: genuine divergence, the same document was changed differently on each
	// side. main is left untouched - the caller decides what happens next.
	OutcomeConflict
)

// CommitPushOutcome is the result of resolving one incoming head against the local one - shared
// by both the host's CommitPush handler and the client's PullMissing, per the same symmetry
// contract Component 39 established in Kotlin: one decision function, not two independently
// maintained copies (that's exactly how the original blind-head-move bug went unnoticed on one
// side while looking "fine" on the other).
type CommitPushOutcome struct {
	Kind        CommitPushOutcomeKind
	MergeCommit *document.Commit
	Report      *kdberr.ConflictReport
}

// ResolutionOptions controls how resolveDivergedLocked handles a genuine same-document conflict
// during divergence resolution. The zero value preserves the original behavior - always report,
// never resolve - so any existing caller that doesn't set this sees no change.
type ResolutionOptions struct {
	// Policy selects how an overlapping (same-document) conflict is resolved.
	// ConflictPolicyStrict (or the zero value, ConflictPolicyAppendOnly - divergence resolution
	// only reaches this switch for a genuine overlap, which an APPEND_ONLY namespace never
	// produces by construction) means "report, don't resolve", unchanged from before this option
	// existed. ConflictPolicyLastWrite means the incoming/remote side's write always wins for a
	// conflicting document - this is transaction.ConflictPolicyLastWrite's existing "the
	// transaction being applied always overwrites what's there" semantics
	// (default_engine.go's finalizeTransaction), read in peer sync's own replay direction: spec
	// kdb-spec.md §8.3 step 3 is "replay B's [the remote/incoming side's] transactions onto A's
	// [local's] HEAD", so "the transaction being applied" is the remote side.
	// ConflictPolicyCustom consults Resolver once per conflicting document.
	Policy transaction.ConflictPolicy
	// Choose, when set, decides every same-document conflict before Policy is consulted: given a
	// document's final operation on each side, it returns the operation the merge applies, or
	// false to leave the conflict reported. It is how an operator's resolution of a queued
	// conflict is applied (see ResolveConflict) - an explicit local/remote choice, not a policy,
	// so unlike Resolver it is told which side is which.
	Choose func(docID codec.UUID, local, remote document.Op) (document.Op, bool)
	// Resolver is consulted when Policy is ConflictPolicyCustom. A nil Resolver, or a
	// resolution failure/nil result for any document, falls back to reporting the conflict
	// rather than guessing - matching transaction.Engine's own CUSTOM fallback.
	Resolver transaction.ConflictResolver
}

var (
	divergenceLocksGuard sync.Mutex
	divergenceLocks      = map[string]*sync.Mutex{}
)

func divergenceLockFor(namespaceID string) *sync.Mutex {
	divergenceLocksGuard.Lock()
	defer divergenceLocksGuard.Unlock()
	m, ok := divergenceLocks[namespaceID]
	if !ok {
		m = &sync.Mutex{}
		divergenceLocks[namespaceID] = m
	}
	return m
}

// ResolveDivergence decides what "main" should point at after an incoming head is seen, and
// performs that decision (SetHead, or AppendMergeCommit). Callers must always store every commit
// from either side into d before calling this ("putCommit always stores" - history is never
// lost, only the branch-pointer decision is gated).
//
// localHead must be where "main" actually points - both real callers pass d.Head(). The
// auto-merge path compare-and-swaps against it (AppendMergeCommit), so a stale localHead is
// refused with a *dag.HeadConflictError rather than quietly re-pointing main off whatever landed
// in the meantime.
//
// Serialized per namespace: this function reads the head, decides, and only then mutates the DAG
// across several non-atomic calls. Two concurrent callers sharing one dag (two connections
// pushing to the same host, or a push racing a pull) could otherwise both read the same stale
// head, both decide independently, and both mutate - reopening exactly the class of fork/lost-
// update bug this function exists to fix, just one level up from the original unconditional
// SetHead. Mirrors KdbServerRuntime.commitMu (go/kdb/server/server_runtime.go) - same shape of
// bug, same fix shape, different layer.
func ResolveDivergence(
	d *dag.InMemoryCommitDag,
	store storage.Adapter,
	namespaceID string,
	localHead, incomingHead codec.Hash,
	opts ResolutionOptions,
) (CommitPushOutcome, error) {
	lock := divergenceLockFor(namespaceID)
	lock.Lock()
	defer lock.Unlock()
	return resolveDivergenceLocked(d, store, namespaceID, localHead, incomingHead, opts)
}

func resolveDivergenceLocked(
	d *dag.InMemoryCommitDag,
	store storage.Adapter,
	namespaceID string,
	localHead, incomingHead codec.Hash,
	opts ResolutionOptions,
) (CommitPushOutcome, error) {
	switch ResolveHeadUpdate(d, localHead, incomingHead) {
	case HeadFastForward:
		if err := d.SetHead(mainBranch, incomingHead); err != nil {
			return CommitPushOutcome{}, err
		}
		return CommitPushOutcome{Kind: OutcomeFastForwarded}, nil
	case HeadAlreadyAncestor:
		return CommitPushOutcome{Kind: OutcomeNoOp}, nil
	default: // HeadDiverged
		outcome, _, err := resolveDivergedLocked(d, store, namespaceID, localHead, incomingHead, opts)
		return outcome, err
	}
}

// MergeAuthorNodeID authors every merge commit peer sync creates. A merge is a pure function of
// the two heads it joins, not something a particular node did, so no node's identity goes in it
// - otherwise two nodes merging the same pair would disagree about the author and so about the
// hash.
var MergeAuthorNodeID = codec.DerivedUUID("kdb:merge-author/1")

// mergeMessage is every peer-sync merge commit's message.
const mergeMessage = "kdb:merge/1"

// mergeCommitParents orders a merge's parents canonically - by hash, not by which side is local -
// so both nodes joining the same two heads name the same first parent.
func mergeCommitParents(a, b codec.Hash) (codec.Hash, codec.Hash) {
	if bytes.Compare(a.Bytes[:], b.Bytes[:]) <= 0 {
		return a, b
	}
	return b, a
}

// mergeNonConflicting joins localHead and incomingHead in a merge commit that is identical on
// every node that makes it - the same parents, operations, timestamp, author and message, and so
// the same hash - so two nodes resolving the same divergence independently converge on one
// commit instead of each producing its own and then merging those forever (D4).
//
// storageWrites is what changes in this node's live storage: the final state of every document
// the remote side touched, with same-document conflicts already resolved. It is applied on top
// of the local head's tree to build the merged tree.
//
// commitOps is the merge's own operations: every document on which the two parents differ, with
// its merged value. Applied on top of either parent's tree they build the merged tree, so the
// merge replays correctly from whichever parent a reader holds.
func mergeNonConflicting(
	d *dag.InMemoryCommitDag,
	store storage.Adapter,
	namespaceID string,
	localHead, incomingHead, ancestor codec.Hash,
	storageWrites map[codec.UUID]document.Op,
	commitOps map[codec.UUID]document.Op,
) (document.Commit, error) {
	localHeadCommit, err := d.GetCommitOrThrow(localHead)
	if err != nil {
		return document.Commit{}, err
	}
	incomingCommit, err := d.GetCommitOrThrow(incomingHead)
	if err != nil {
		return document.Commit{}, err
	}
	fail := func(err error) (document.Commit, error) {
		_ = store.DiscardPending(namespaceID)
		return document.Commit{}, err
	}
	for _, docID := range sortedDocIDs(storageWrites) {
		switch o := storageWrites[docID].(type) {
		case document.WriteOp:
			if err := store.PutDocument(namespaceID, document.Document{ID: o.DocID, JSON: o.Patch}); err != nil {
				return fail(err)
			}
		case document.DeleteOp:
			if err := store.DeleteDocument(namespaceID, o.DocID); err != nil {
				return fail(err)
			}
		}
	}
	mergedTree, err := store.CommitTree(namespaceID, localHeadCommit.DocumentTreeHash)
	if err != nil {
		return fail(err)
	}
	p0, p1 := mergeCommitParents(localHead, incomingHead)
	ops := make([]document.Op, 0, len(commitOps))
	for _, docID := range sortedDocIDs(commitOps) {
		ops = append(ops, commitOps[docID])
	}
	// The later of the two heads' timestamps: deterministic, unlike this node's clock. The DAG
	// then stamps the commit one microsecond past it, as it does every commit.
	ts := localHeadCommit.Timestamp
	if incomingCommit.Timestamp.EpochMicros() > ts.EpochMicros() {
		ts = incomingCommit.Timestamp
	}
	var schemaHash *codec.Hash
	if localHeadCommit.SchemaHash != nil && incomingCommit.SchemaHash != nil && *localHeadCommit.SchemaHash == *incomingCommit.SchemaHash {
		sh := *localHeadCommit.SchemaHash
		schemaHash = &sh
	}
	mergeTx := document.Transaction{
		ID:           codec.DerivedUUID("kdb:merge/1:" + p0.Hex() + ":" + p1.Hex()),
		BaseVersion:  ancestor,
		Operations:   ops,
		Timestamp:    ts,
		AuthorNodeID: MergeAuthorNodeID,
	}
	return d.AppendMergeCommitOnto(&localHead, mergeTx, p0, p1, mergedTree, schemaHash, mergeMessage)
}

// touchedDocsForRange replays every commit strictly between ancestor and head, oldest first, to
// determine what each document touched in that range ended up as on this side - last-write-wins
// per document within the range, same as applying them for real would produce. Two sides landing
// different Op for the same docID (including one writing while the other deletes) is always
// treated as a genuine conflict: this does not attempt to prove two independently-produced writes
// are "actually" identical, and does not perform field-level 3-way merging.
//
// Walks the DAG directly from head back to ancestor (same shape as CommitsToPush) rather than
// consuming a pre-fetched, already-unordered commit list: ComputeSyncPlan's LocalOnly/RemoteOnly
// come from CommitsSince, which sorts by hash purely to make its own output deterministic and
// carries no causal meaning, and this function used to additionally re-sort that by wall-clock
// Timestamp - vulnerable to cross-node clock skew, and to sort.Slice's lack of a stability
// guarantee for the exact-tie case (routine for a batch pushed together, since
// codec.TimestampNow() is microsecond-granularity). Two peers resolving the same divergence, or
// one peer resolving it twice on retry, could disagree on which write was "last" and converge on
// different merge content. d.Walk's traversal only ever dequeues a commit's parents after the
// commit itself has been dequeued, so the reversed result below is a genuine topological order
// (every commit strictly after all of its own descendants in the range) - deterministic
// regardless of timestamps, immune to clock skew entirely.
// touchedDoc is one document's final state within a divergence range, plus the provenance
// (originating commit's timestamp and hash) localWriteWins needs to compare it against the other
// side's candidate for the same document.
type touchedDoc struct {
	Op         document.Op
	Timestamp  codec.Timestamp
	CommitHash codec.Hash
}

func touchedDocsForRange(d *dag.InMemoryCommitDag, head, ancestor codec.Hash) (map[codec.UUID]touchedDoc, []document.Commit, error) {
	if head == ancestor {
		return map[codec.UUID]touchedDoc{}, nil, nil
	}
	// commitsBetween, not Walk(head, &ancestor): Walk prunes only at ancestor's exact hash and so
	// revisits shared history reachable around it through a merge's other parent, which would
	// count writes both sides already share as one side's divergent edits.
	commits, err := commitsBetween(d, head, ancestor)
	if err != nil {
		return nil, nil, err
	}
	out := make(map[codec.UUID]touchedDoc)
	for _, c := range commits {
		for _, op := range c.Operations {
			switch o := op.(type) {
			case document.WriteOp:
				out[o.DocID] = touchedDoc{Op: o, Timestamp: c.Timestamp, CommitHash: c.Hash}
			case document.DeleteOp:
				out[o.DocID] = touchedDoc{Op: o, Timestamp: c.Timestamp, CommitHash: c.Hash}
			}
		}
	}
	return out, commits, nil
}

// opsOnly discards touchedDoc's provenance, keeping just each document's resulting Op - the
// shape mergeNonConflicting (and, by extension, storage.CommitTree) actually needs.
func opsOnly(m map[codec.UUID]touchedDoc) map[codec.UUID]document.Op {
	out := make(map[codec.UUID]document.Op, len(m))
	for k, v := range m {
		out[k] = v.Op
	}
	return out
}

func intersectDocIDs[T any](a, b map[codec.UUID]T) []codec.UUID {
	var out []codec.UUID
	for k := range a {
		if _, ok := b[k]; ok {
			out = append(out, k)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}

func sortedDocIDs(m map[codec.UUID]document.Op) []codec.UUID {
	out := make([]codec.UUID, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}

// classifyConflictOp derives the kdberr.ConflictOperationType for one overlapping document from
// what each side did to it - shared by buildConflictReport and resolveCustomConflicts so both
// describe the same conflict the same way.
func classifyConflictOp(localOp, remoteOp document.Op) kdberr.ConflictOperationType {
	_, localIsDelete := localOp.(document.DeleteOp)
	_, remoteIsWrite := remoteOp.(document.WriteOp)
	_, localIsWrite := localOp.(document.WriteOp)
	_, remoteIsDelete := remoteOp.(document.DeleteOp)
	switch {
	case localIsDelete && remoteIsWrite:
		return kdberr.DeleteWrite
	case localIsWrite && remoteIsDelete:
		return kdberr.WriteDelete
	default:
		return kdberr.ConcurrentWrite
	}
}

// documentFromOp reconstructs the document.Document a WriteOp produced, straight from the
// commit-carried patch JSON (no storage lookup needed) - nil for a DeleteOp or a nil op.
func documentFromOp(docID codec.UUID, op document.Op) *document.Document {
	if w, ok := op.(document.WriteOp); ok {
		return &document.Document{ID: docID, JSON: w.Patch}
	}
	return nil
}
