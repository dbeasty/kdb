package dev.kdb.peersync

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.error.ConflictItem
import dev.kdb.error.ConflictOperationType
import dev.kdb.error.ConflictReport
import dev.kdb.error.KdbErrorCode
import dev.kdb.error.KdbException
import dev.kdb.storage.StorageAdapter
import dev.kdb.transaction.ConflictPolicy
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * Component 39 fix. Whether an incoming peer head can simply replace the local one, matching
 * git's fast-forward-or-diverged model - not, as the pre-fix code assumed, "always replace it".
 */
public sealed class HeadUpdate {
    /** [incomingHead] is a descendant of [localHead] - safe to move the branch pointer. */
    public object FastForward : HeadUpdate()

    /** [localHead] is already at or ahead of [incomingHead] - nothing to do. */
    public object AlreadyAncestor : HeadUpdate()

    /** Neither is an ancestor of the other - a real divergence, resolved by [resolveDivergence]. */
    public object Diverged : HeadUpdate()
}

public suspend fun resolveHeadUpdate(
    dag: CommitDag,
    localHead: KdbHash,
    incomingHead: KdbHash,
): HeadUpdate {
    if (localHead == incomingHead) return HeadUpdate.AlreadyAncestor
    if (dag.isAncestor(localHead, incomingHead)) return HeadUpdate.FastForward
    if (dag.isAncestor(incomingHead, localHead)) return HeadUpdate.AlreadyAncestor
    return HeadUpdate.Diverged
}

/**
 * Outcome of resolving one incoming head against the local one - shared by both
 * [PeerSyncFrameHandler.handleCommitPush] (receiving a push) and [PeerSession.pullMissing]
 * (having just pulled), per §5's symmetry contract: one decision function, not two independently
 * maintained copies (that's exactly how the original blind-head-move bug went unnoticed on one
 * side while looking "fine" on the other).
 */
public sealed class CommitPushOutcome {
    public object NoOp : CommitPushOutcome()

    public object FastForwarded : CommitPushOutcome()

    /** Divergence with no real per-document conflict - both sides' commits are now reachable
     * from main via a real two-parent merge commit (git-style; see [CommitDag.appendMergeCommit]). */
    public data class Merged(val mergeCommit: KdbCommit) : CommitPushOutcome()

    /** Genuine divergence: the same document was changed differently on each side. main is left
     * untouched - the caller decides what happens next (retry, prefer a side, ask a human). */
    public data class Conflict(val report: ConflictReport) : CommitPushOutcome()
}

public class AncestryLookupException(
    message: String,
    public val namespaceId: String,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.VERSION_NOT_FOUND
}

public class PeerSyncConflictException(
    message: String,
    public val report: ConflictReport,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.CONFLICT
}

/**
 * The shared divergence-resolution decision (§5's symmetry contract). Always stores every commit
 * from either side into [dag] before this is called (§5: "putCommit always stores" - history is
 * never lost, only the branch-pointer decision is gated) - this function only decides what "main"
 * should point at afterward.
 *
 * Behavior (see component 39 spec §5, §7 tests 2-4):
 *  - No divergence in the *documents* touched by each side since their common ancestor (either
 *    because history is linear, or because the two sides touched disjoint documents) -> merge
 *    automatically, no report. This is what makes APPEND_ONLY "just work" (test 4): an
 *    append-only namespace never has two sides touching the same document, so it always takes
 *    this path - the policy isn't special-cased, the data shape makes it fall out for free.
 *  - The same document was actually left in different states by each side -> real conflict.
 *    [conflictPolicy] STRICT (and, conservatively, LAST_WRITE/CUSTOM until they get a real
 *    document-level auto-resolution implementation - not part of this component's test list)
 *    reports it rather than picking a winner; main does not move.
 */
public suspend fun resolveDivergence(
    dag: CommitDag,
    storage: StorageAdapter,
    namespaceId: String,
    localHead: KdbHash,
    incomingHead: KdbHash,
    conflictPolicy: ConflictPolicy = ConflictPolicy.STRICT,
): CommitPushOutcome =
    // Serialized per namespace: this function reads the head, decides, and only then mutates
    // the DAG (setHead, or appendMergeCommit) across several non-atomic calls. Two concurrent
    // callers sharing one `dag` (e.g. two connections pushing to the same host, or a push
    // racing a pull) could otherwise both read the same stale head, both decide independently,
    // and both mutate - reopening exactly the class of fork/lost-update bug this component
    // exists to fix, just one level up from the original bug's unconditional dag.setHead. This
    // mirrors KdbServerRuntime.commitMu on the Go side (go/kdb/server/server_runtime.go) - same
    // shape of bug, found independently while implementing this component's concurrent test
    // (§7 test 9).
    divergenceLockFor(namespaceId).withLock {
        resolveDivergenceLocked(dag, storage, namespaceId, localHead, incomingHead, conflictPolicy)
    }

private val divergenceLocksGuard = Mutex()
private val divergenceLocks = mutableMapOf<String, Mutex>()

private suspend fun divergenceLockFor(namespaceId: String): Mutex =
    divergenceLocksGuard.withLock { divergenceLocks.getOrPut(namespaceId) { Mutex() } }

private suspend fun resolveDivergenceLocked(
    dag: CommitDag,
    storage: StorageAdapter,
    namespaceId: String,
    localHead: KdbHash,
    incomingHead: KdbHash,
    conflictPolicy: ConflictPolicy,
): CommitPushOutcome {
    when (resolveHeadUpdate(dag, localHead, incomingHead)) {
        HeadUpdate.FastForward -> {
            dag.setHead(MAIN_BRANCH, incomingHead)
            return CommitPushOutcome.FastForwarded
        }
        HeadUpdate.AlreadyAncestor -> return CommitPushOutcome.NoOp
        HeadUpdate.Diverged -> {
            // Reuses computeSyncPlan (PeerSyncTypes.kt) rather than reimplementing ancestor/
            // reachability walking, per the component spec's own dependency note. Deliberately
            // works from each commit's `operations` list (always present - it's part of the
            // KdbCommit record itself, transmitted over the wire) rather than dag.diff/
            // getDocumentTreeOrThrow: those need the DocumentTree *object* for incomingHead to
            // already be registered in this node's dag.trees, which putCommit alone does not
            // guarantee (only the commit record; tree reconstruction is left to the caller's
            // optional materializeCommit callback in real deployments). Working from operations
            // directly needs nothing beyond what putCommit already stored.
            val plan = computeSyncPlan(dag, localHead, incomingHead)
            val ancestor =
                plan.commonAncestor
                    ?: throw AncestryLookupException(
                        "no common ancestor between local $localHead and incoming $incomingHead " +
                            "- a commit references a parent this node never received",
                        namespaceId,
                    )
            val localTouched = touchedDocsForRange(dag, plan.localOnly)
            val remoteTouched = touchedDocsForRange(dag, plan.remoteOnly)
            val overlapping = localTouched.keys intersect remoteTouched.keys

            if (overlapping.isEmpty()) {
                val localHeadCommit = dag.getCommitOrThrow(localHead)
                // Same mechanism kdb-embed's materializeCommit already uses to apply a foreign
                // commit's operations (EmbedOperations.kt materializeSingleCommit): stage the
                // remote side's writes/deletes into storage, then let storage.commitTree build
                // the resulting tree on top of local's own parent tree - untouched documents
                // carry over unchanged, since overlapping is empty there's nothing to reconcile.
                for ((docId, op) in remoteTouched) {
                    when (op) {
                        is KdbOp.Write -> storage.putDocument(namespaceId, KdbDocument(docId, op.patch))
                        is KdbOp.Delete -> storage.deleteDocument(namespaceId, docId)
                        else -> Unit
                    }
                }
                val mergedTree = storage.commitTree(namespaceId, localHeadCommit.documentTreeHash)
                // The merge commit's own operations must be the delta it introduces relative to
                // its *primary* parent (localHead) - i.e. exactly the remote side's writes/
                // deletes being layered on top - not empty. Replay-based materialization
                // (kdb-embed's materializeCommitHistory) walks history and reapplies each
                // commit's own `operations` against `parentHashes.first()`'s tree; an empty
                // operations list here would silently drop the remote side's documents for any
                // consumer that materializes via replay instead of reading newDocumentTree
                // directly (found via the genuinely-concurrent integration test, §7 test 9).
                val mergeTx =
                    KdbTransaction(
                        id = KdbUuid.random(),
                        baseVersion = ancestor,
                        operations = remoteTouched.values.toList(),
                        timestamp = KdbTimestamp.now(),
                        authorNodeId = KdbUuid.random(),
                    )
                val mergeCommit =
                    dag.appendMergeCommit(
                        mergeTx,
                        primaryParent = localHead,
                        mergedParent = incomingHead,
                        newDocumentTree = mergedTree,
                        schemaHash = null,
                        message = "peer-sync auto-merge (non-conflicting)",
                    )
                return CommitPushOutcome.Merged(mergeCommit)
            }

            val report =
                buildConflictReport(
                    localHead = localHead,
                    incomingHead = incomingHead,
                    conflictingDocIds = overlapping.toList(),
                    localTouched = localTouched,
                    remoteTouched = remoteTouched,
                )
            return CommitPushOutcome.Conflict(report)
        }
    }
}

/**
 * Replays each commit's operations, oldest first, to determine what each document in this
 * commit range ended up as *on this side* - last-write-wins per document within the range, same
 * as applying them for real would produce. Two sides landing different [KdbOp] for the same
 * docId (including one writing while the other deletes) is always treated as a genuine conflict:
 * this component does not attempt to prove two independently-produced writes are "actually"
 * identical (a narrow, rarely-hit case) and does not perform field-level 3-way merging - both
 * are explicitly out of scope (§8 Non-Goals).
 */
private suspend fun touchedDocsForRange(
    dag: CommitDag,
    commitHashes: List<KdbHash>,
): Map<KdbUuid, KdbOp> {
    val commits = commitHashes.map { dag.getCommitOrThrow(it) }.sortedBy { it.timestamp.toEpochMicros() }
    val out = mutableMapOf<KdbUuid, KdbOp>()
    for (commit in commits) {
        for (op in commit.operations) {
            when (op) {
                is KdbOp.Write -> out[op.docId] = op
                is KdbOp.Delete -> out[op.docId] = op
                else -> Unit
            }
        }
    }
    return out
}

private fun buildConflictReport(
    localHead: KdbHash,
    incomingHead: KdbHash,
    conflictingDocIds: List<KdbUuid>,
    localTouched: Map<KdbUuid, KdbOp>,
    remoteTouched: Map<KdbUuid, KdbOp>,
): ConflictReport {
    val items =
        conflictingDocIds.map { docId ->
            val localOp = localTouched[docId]
            val remoteOp = remoteTouched[docId]
            val operationType =
                when {
                    localOp is KdbOp.Delete && remoteOp is KdbOp.Write -> ConflictOperationType.DELETE_WRITE
                    localOp is KdbOp.Write && remoteOp is KdbOp.Delete -> ConflictOperationType.WRITE_DELETE
                    else -> ConflictOperationType.CONCURRENT_WRITE
                }
            ConflictItem(
                documentId = docId.toString(),
                operationType = operationType,
                localDoc = (localOp as? KdbOp.Write)?.patch,
                incomingDoc = (remoteOp as? KdbOp.Write)?.patch,
            )
        }
    // No single "transactionId" applies to a multi-commit peer-sync push (see kdb-transaction's
    // ConflictReport, reused here per §5 rather than inventing a peer-sync-specific shape); the
    // incoming head hex is the closest equivalent identifier for "what was being applied".
    return ConflictReport(
        transactionId = incomingHead.toHex(),
        baseHash = localHead.toHex(),
        targetHash = incomingHead.toHex(),
        conflicts = items,
    )
}

internal const val MAIN_BRANCH = "main"
