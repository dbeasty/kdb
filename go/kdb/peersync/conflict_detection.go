package peersync

import (
	"math"
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
		return resolveDivergedLocked(d, store, namespaceID, localHead, incomingHead, opts)
	}
}

// resolveDivergedLocked handles the HeadDiverged case: reuses ComputeSyncPlan (sync_plan.go)
// rather than reimplementing ancestor/reachability walking. Deliberately works from each
// commit's Operations list (always present - part of the Commit record itself, transmitted over
// the wire) rather than the DocumentTree object, which needs incomingHead's tree already
// registered in this node's dag - putCommit alone does not guarantee that, only the commit
// record; tree reconstruction is left to the optional MaterializeCommit callback.
func resolveDivergedLocked(
	d *dag.InMemoryCommitDag,
	store storage.Adapter,
	namespaceID string,
	localHead, incomingHead codec.Hash,
	opts ResolutionOptions,
) (CommitPushOutcome, error) {
	plan, err := ComputeSyncPlan(d, localHead, incomingHead)
	if err != nil {
		return CommitPushOutcome{}, err
	}
	if plan.CommonAncestor == nil {
		return CommitPushOutcome{}, kdberr.NewVersionNotFoundError(
			"no common ancestor between local "+localHead.Hex()+" and incoming "+incomingHead.Hex()+
				" - a commit references a parent this node never received",
			namespaceID, incomingHead.Hex(),
		)
	}
	ancestor := *plan.CommonAncestor

	localTouched, err := touchedDocsForRange(d, localHead, ancestor)
	if err != nil {
		return CommitPushOutcome{}, err
	}
	remoteTouched, err := touchedDocsForRange(d, incomingHead, ancestor)
	if err != nil {
		return CommitPushOutcome{}, err
	}
	overlapping := intersectDocIDs(localTouched, remoteTouched)

	if len(overlapping) == 0 {
		mergeCommit, err := mergeNonConflicting(d, store, namespaceID, localHead, incomingHead, ancestor, opsOnly(remoteTouched))
		if err != nil {
			return CommitPushOutcome{}, err
		}
		return CommitPushOutcome{Kind: OutcomeMerged, MergeCommit: &mergeCommit}, nil
	}

	// A genuine same-document conflict. Real "replay per conflict policy" (kdb-spec.md §8.3 step
	// 3): STRICT/unset always reports (unchanged); LAST_WRITE and CUSTOM resolve it into the
	// same single-merge-commit auto-merge path used for the disjoint case above, by first
	// deciding what the overlapping documents' *effective* remote-side write should be.
	switch opts.Policy {
	case transaction.ConflictPolicyLastWrite:
		// Whichever write actually happened later wins - NOT "whichever side is being applied"
		// (the previous behavior: unconditionally remote, i.e. direction-dependent). That made
		// LAST_WRITE non-commutative: node A pulling from B and node B receiving A's push both
		// resolve the *same* conflicting document, but "remote" means B on A and A on B, so they
		// picked opposite winners and permanently diverged on exactly the document LAST_WRITE
		// exists to reconcile (kdb-finish-up-plan.md's 1-G11). resolveLastWriteWinners compares
		// each overlapping document's two candidate writes directly, so every node computing this
		// same resolution reaches the same answer regardless of which side it's running on.
		effectiveRemoteWrites := opsOnly(remoteTouched)
		for _, docID := range overlapping {
			if localWriteWins(localTouched[docID], remoteTouched[docID]) {
				// The winning write must ride IN the merge commit's own operations, not be
				// omitted. Deleting the entry (the previous fix's shape) was right for the node
				// creating the merge - its storage already holds local's write - but the merge
				// commit travels: a peer whose own state is the LOSING side materializes the
				// pushed commits oldest-first, so the losing raw commit lands after the winning
				// one, and a merge commit carrying no op for the document leaves that peer on
				// the loser's content forever (observed live in the e2e
				// direction-reversed-relay scenario: A converged to the winner, B to the
				// loser). Substituting local's own op makes the merge self-contained: applying
				// it is a no-op where local already won, and imposes the winner everywhere
				// else.
				effectiveRemoteWrites[docID] = localTouched[docID].Op
			}
		}
		mergeCommit, err := mergeNonConflicting(d, store, namespaceID, localHead, incomingHead, ancestor, effectiveRemoteWrites)
		if err != nil {
			return CommitPushOutcome{}, err
		}
		return CommitPushOutcome{Kind: OutcomeMerged, MergeCommit: &mergeCommit}, nil
	case transaction.ConflictPolicyCustom:
		ancestorCommit, err := d.GetCommitOrThrow(ancestor)
		if err != nil {
			return CommitPushOutcome{}, err
		}
		resolvedWrites, resolved, err := resolveCustomConflicts(
			store, namespaceID, ancestorCommit.DocumentTreeHash, overlapping, localTouched, remoteTouched, opts.Resolver,
		)
		if err != nil {
			return CommitPushOutcome{}, err
		}
		if resolved {
			mergeCommit, err := mergeNonConflicting(d, store, namespaceID, localHead, incomingHead, ancestor, resolvedWrites)
			if err != nil {
				return CommitPushOutcome{}, err
			}
			return CommitPushOutcome{Kind: OutcomeMerged, MergeCommit: &mergeCommit}, nil
		}
		// Falls through to reporting below - matches finalizeTransaction's own CUSTOM fallback
		// when there is no resolver, or it declines/fails on any document.
	}

	report := buildConflictReport(localHead, incomingHead, overlapping, localTouched, remoteTouched)
	return CommitPushOutcome{Kind: OutcomeConflict, Report: &report}, nil
}

// localWriteWins compares two candidate writes for the same document - local's own final write
// in this divergence range, and remote's - and reports whether local's should be kept instead of
// remote's, for ConflictPolicyLastWrite. Later Timestamp wins; an exact tie (routine within one
// batch, since codec.TimestampNow() is microsecond-granularity) is broken by comparing commit
// hash lexicographically - arbitrary, but identical on every node computing this same
// resolution, so both sides still converge to the same document instead of each keeping "their
// own" arbitrarily.
func localWriteWins(local, remote touchedDoc) bool {
	lt, rt := local.Timestamp.EpochMicros(), remote.Timestamp.EpochMicros()
	if lt != rt {
		return lt > rt
	}
	return local.CommitHash.Hex() > remote.CommitHash.Hex()
}

// mergeNonConflicting stages the remote side's writes/deletes into storage, then lets
// storage.CommitTree build the resulting tree on top of local's own parent tree - untouched
// documents carry over unchanged, since overlapping is empty there's nothing to reconcile. The
// merge commit's own Operations must be the delta it introduces relative to its *primary* parent
// (localHead) - i.e. exactly the remote side's writes/deletes - not empty: replay-based
// materialization walks history and reapplies each commit's own Operations against
// ParentHashes[0]'s tree, and an empty Operations list here would silently drop the remote side's
// documents for any consumer that materializes via replay instead of reading the tree directly.
func mergeNonConflicting(
	d *dag.InMemoryCommitDag,
	store storage.Adapter,
	namespaceID string,
	localHead, incomingHead, ancestor codec.Hash,
	remoteTouched map[codec.UUID]document.Op,
) (document.Commit, error) {
	localHeadCommit, err := d.GetCommitOrThrow(localHead)
	if err != nil {
		return document.Commit{}, err
	}
	for _, op := range remoteTouched {
		switch o := op.(type) {
		case document.WriteOp:
			if err := store.PutDocument(namespaceID, document.Document{ID: o.DocID, JSON: o.Patch}); err != nil {
				return document.Commit{}, err
			}
		case document.DeleteOp:
			if err := store.DeleteDocument(namespaceID, o.DocID); err != nil {
				return document.Commit{}, err
			}
		}
	}
	mergedTree, err := store.CommitTree(namespaceID, localHeadCommit.DocumentTreeHash)
	if err != nil {
		return document.Commit{}, err
	}
	txID, err := codec.RandomUUID()
	if err != nil {
		return document.Commit{}, err
	}
	authorID, err := codec.RandomUUID()
	if err != nil {
		return document.Commit{}, err
	}
	ops := make([]document.Op, 0, len(remoteTouched))
	for _, docID := range sortedDocIDs(remoteTouched) {
		ops = append(ops, remoteTouched[docID])
	}
	mergeTx := document.Transaction{
		ID:           txID,
		BaseVersion:  ancestor,
		Operations:   ops,
		Timestamp:    codec.TimestampNow(),
		AuthorNodeID: authorID,
	}
	return d.AppendMergeCommit(mergeTx, localHead, incomingHead, mergedTree, nil, "peer-sync auto-merge (non-conflicting)")
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

func touchedDocsForRange(d *dag.InMemoryCommitDag, head, ancestor codec.Hash) (map[codec.UUID]touchedDoc, error) {
	if head == ancestor {
		return map[codec.UUID]touchedDoc{}, nil
	}
	walked := d.Walk(head, &ancestor, math.MaxInt)
	out := make(map[codec.UUID]touchedDoc)
	for i := len(walked) - 1; i >= 0; i-- {
		full, ok := walked[i].(dag.FullEntry)
		if !ok {
			continue
		}
		for _, op := range full.Commit.Operations {
			switch o := op.(type) {
			case document.WriteOp:
				out[o.DocID] = touchedDoc{Op: o, Timestamp: full.Commit.Timestamp, CommitHash: full.Commit.Hash}
			case document.DeleteOp:
				out[o.DocID] = touchedDoc{Op: o, Timestamp: full.Commit.Timestamp, CommitHash: full.Commit.Hash}
			}
		}
	}
	return out, nil
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

// resolveCustomConflicts consults resolver once per overlapping document (ConflictPolicyCustom),
// mirroring finalizeTransaction's own CUSTOM handling: ExistingDoc is local's side, IncomingDoc
// is remote's, BaseDoc is the common ancestor's (a storage lookup, since - unlike the touched-doc
// operations themselves - the ancestor's content isn't carried on either divergent commit).
// Returns resolved=false (not an error) if resolver is nil, or it fails/declines (returns a nil
// document) for any single document - the caller falls back to reporting the conflict rather
// than guessing, exactly like finalizeTransaction does.
func resolveCustomConflicts(
	store storage.Adapter,
	namespaceID string,
	ancestorTreeHash codec.Hash,
	overlapping []codec.UUID,
	localTouched, remoteTouched map[codec.UUID]touchedDoc,
	resolver transaction.ConflictResolver,
) (map[codec.UUID]document.Op, bool, error) {
	if resolver == nil {
		return nil, false, nil
	}
	resolved := opsOnly(remoteTouched)
	for _, docID := range overlapping {
		localOp := localTouched[docID].Op
		remoteOp := remoteTouched[docID].Op
		baseDoc, err := store.GetDocument(namespaceID, docID, ancestorTreeHash)
		if err != nil {
			return nil, false, err
		}
		outcome, err := resolver.Resolve(transaction.DocumentConflict{
			DocID:         docID,
			OperationType: classifyConflictOp(localOp, remoteOp),
			ExistingDoc:   documentFromOp(docID, localOp),
			IncomingDoc:   documentFromOp(docID, remoteOp),
			BaseDoc:       baseDoc,
		})
		if err != nil || outcome == nil {
			return nil, false, nil
		}
		resolved[docID] = document.WriteOp{DocID: docID, Patch: outcome.JSON}
	}
	return resolved, true, nil
}

func buildConflictReport(
	localHead, incomingHead codec.Hash,
	overlapping []codec.UUID,
	localTouched, remoteTouched map[codec.UUID]touchedDoc,
) kdberr.ConflictReport {
	items := make([]kdberr.ConflictItem, 0, len(overlapping))
	for _, docID := range overlapping {
		localOp := localTouched[docID].Op
		remoteOp := remoteTouched[docID].Op
		opType := classifyConflictOp(localOp, remoteOp)
		var localDoc, incomingDoc *string
		if w, ok := localOp.(document.WriteOp); ok {
			p := w.Patch
			localDoc = &p
		}
		if w, ok := remoteOp.(document.WriteOp); ok {
			p := w.Patch
			incomingDoc = &p
		}
		items = append(items, kdberr.ConflictItem{
			DocumentID:    docID.String(),
			OperationType: opType,
			LocalDoc:      localDoc,
			IncomingDoc:   incomingDoc,
		})
	}
	// No single "transactionId" applies to a multi-commit peer-sync push (reuses
	// kdberr.ConflictReport rather than inventing a peer-sync-specific shape); the incoming head
	// hex is the closest equivalent identifier for "what was being applied".
	return kdberr.ConflictReport{
		TransactionID: incomingHead.Hex(),
		BaseHash:      localHead.Hex(),
		TargetHash:    incomingHead.Hex(),
		Conflicts:     items,
	}
}
