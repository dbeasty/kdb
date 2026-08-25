package dev.kdb.peersync

import dev.kdb.codec.KdbHash
import dev.kdb.transport.core.TransportTlsSettings
import dev.kdb.dag.CommitDag
import dev.kdb.dag.TraversalEntry
import dev.kdb.document.KdbCommit
import dev.kdb.error.ConflictReport
import dev.kdb.transaction.ConflictPolicy

public typealias CommitMaterializer = suspend (KdbCommit) -> Unit

public data class PeerHostConfig(
    val namespaceId: String,
    val nodeId: String,
    val transportHub: String,
    val materializeCommit: CommitMaterializer? = null,
    /** How a genuine divergence (component 39) resolves for this namespace on receiving a push.
     * Disjoint-document divergence always auto-merges regardless of this value (see
     * [resolveDivergence]'s doc comment); this only affects the same-document-conflict case. */
    val conflictPolicy: ConflictPolicy = ConflictPolicy.STRICT,
)

public data class PeerClientConfig(
    val namespaceId: String,
    val nodeId: String,
    val peerUri: String,
    val connectionContext: dev.kdb.auth.ConnectionContext = dev.kdb.auth.ConnectionContext.EMPTY,
    val tls: TransportTlsSettings? = null,
    /** Same as [PeerHostConfig.conflictPolicy], applied when this side is the one pulling. */
    val conflictPolicy: ConflictPolicy = ConflictPolicy.STRICT,
)

public data class DagSyncPlan(
    val commonAncestor: KdbHash?,
    val localOnly: List<KdbHash>,
    val remoteOnly: List<KdbHash>,
)

public data class PeerSyncResult(
    val appliedCommits: Int,
    val pushedCommits: Int,
    val finalHead: KdbHash,
    val plan: DagSyncPlan?,
    /** Non-null when pullMissing/syncBidirectional hit a genuine same-document divergence
     * (component 39, §7 test 2/3): finalHead was deliberately left unmoved from what it was
     * before the pull - the caller must resolve this before retrying, not just ignore it. */
    val conflict: ConflictReport? = null,
)

public suspend fun computeSyncPlan(
    dag: CommitDag,
    localHead: KdbHash,
    remoteHead: KdbHash,
): DagSyncPlan {
    if (localHead == remoteHead) {
        return DagSyncPlan(localHead, emptyList(), emptyList())
    }
    val ancestor = dag.commonAncestor(localHead, remoteHead)
    val exclude = setOfNotNull(ancestor)
    val localOnly = dag.commitsSince(localHead, exclude)
    val remoteOnly =
        if (dag.hasCommit(remoteHead)) {
            dag.commitsSince(remoteHead, exclude)
        } else {
            emptyList()
        }
    return DagSyncPlan(ancestor, localOnly, remoteOnly)
}

public suspend fun commitsToPush(
    dag: CommitDag,
    localHead: KdbHash,
    remoteHead: KdbHash,
    limit: Int = 100,
): List<KdbCommit> {
    if (localHead == remoteHead) return emptyList()
    if (!dag.hasCommit(remoteHead)) return emptyList()
    val walked = dag.walk(from = localHead, until = remoteHead, limit = limit)
    return walked.filterIsInstance<TraversalEntry.Full>().map { it.commit }.reversed()
}
