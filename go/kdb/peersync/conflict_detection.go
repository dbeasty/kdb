package peersync

import (
	"sort"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/storage"
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
) (CommitPushOutcome, error) {
	lock := divergenceLockFor(namespaceID)
	lock.Lock()
	defer lock.Unlock()
	return resolveDivergenceLocked(d, store, namespaceID, localHead, incomingHead)
}

func resolveDivergenceLocked(
	d *dag.InMemoryCommitDag,
	store storage.Adapter,
	namespaceID string,
	localHead, incomingHead codec.Hash,
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
		return resolveDivergedLocked(d, store, namespaceID, localHead, incomingHead)
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

	localTouched, err := touchedDocsForRange(d, plan.LocalOnly)
	if err != nil {
		return CommitPushOutcome{}, err
	}
	remoteTouched, err := touchedDocsForRange(d, plan.RemoteOnly)
	if err != nil {
		return CommitPushOutcome{}, err
	}
	overlapping := intersectDocIDs(localTouched, remoteTouched)

	if len(overlapping) == 0 {
		mergeCommit, err := mergeNonConflicting(d, store, namespaceID, localHead, incomingHead, ancestor, remoteTouched)
		if err != nil {
			return CommitPushOutcome{}, err
		}
		return CommitPushOutcome{Kind: OutcomeMerged, MergeCommit: &mergeCommit}, nil
	}

	report := buildConflictReport(localHead, incomingHead, overlapping, localTouched, remoteTouched)
	return CommitPushOutcome{Kind: OutcomeConflict, Report: &report}, nil
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

// touchedDocsForRange replays each commit's operations, oldest first, to determine what each
// document in this commit range ended up as on this side - last-write-wins per document within
// the range, same as applying them for real would produce. Two sides landing different Op for
// the same docID (including one writing while the other deletes) is always treated as a genuine
// conflict: this does not attempt to prove two independently-produced writes are "actually"
// identical, and does not perform field-level 3-way merging.
func touchedDocsForRange(d *dag.InMemoryCommitDag, hashes []codec.Hash) (map[codec.UUID]document.Op, error) {
	commits := make([]document.Commit, 0, len(hashes))
	for _, h := range hashes {
		c, err := d.GetCommitOrThrow(h)
		if err != nil {
			return nil, err
		}
		commits = append(commits, c)
	}
	sort.Slice(commits, func(i, j int) bool {
		return commits[i].Timestamp.EpochMicros() < commits[j].Timestamp.EpochMicros()
	})
	out := make(map[codec.UUID]document.Op)
	for _, c := range commits {
		for _, op := range c.Operations {
			switch o := op.(type) {
			case document.WriteOp:
				out[o.DocID] = o
			case document.DeleteOp:
				out[o.DocID] = o
			}
		}
	}
	return out, nil
}

func intersectDocIDs(a, b map[codec.UUID]document.Op) []codec.UUID {
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

func buildConflictReport(
	localHead, incomingHead codec.Hash,
	overlapping []codec.UUID,
	localTouched, remoteTouched map[codec.UUID]document.Op,
) kdberr.ConflictReport {
	items := make([]kdberr.ConflictItem, 0, len(overlapping))
	for _, docID := range overlapping {
		localOp := localTouched[docID]
		remoteOp := remoteTouched[docID]
		_, localIsDelete := localOp.(document.DeleteOp)
		_, remoteIsWrite := remoteOp.(document.WriteOp)
		_, localIsWrite := localOp.(document.WriteOp)
		_, remoteIsDelete := remoteOp.(document.DeleteOp)
		opType := kdberr.ConcurrentWrite
		switch {
		case localIsDelete && remoteIsWrite:
			opType = kdberr.DeleteWrite
		case localIsWrite && remoteIsDelete:
			opType = kdberr.WriteDelete
		}
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
