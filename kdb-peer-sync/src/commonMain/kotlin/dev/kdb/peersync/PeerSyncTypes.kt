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
    /** The namespace's attachment blobs, served to v2 peers (BLOB_FETCH) and taken from them
     * (ATTACHMENT_UPLOAD). Null: this node advertises no attachment support, and a peer whose
     * commits carry attachments is refused them. */
    val blobs: PeerBlobStore? = null,
    /** Serves this node's state at its head to a peer bootstrapping from a snapshot, and
     * advertises the roots of snapshots this node was bootstrapped from. */
    val snapshots: PeerSnapshots? = null,
)

public data class PeerClientConfig(
    val namespaceId: String,
    val nodeId: String,
    val peerUri: String,
    val connectionContext: dev.kdb.auth.ConnectionContext = dev.kdb.auth.ConnectionContext.EMPTY,
    val tls: TransportTlsSettings? = null,
    /** Same as [PeerHostConfig.conflictPolicy], applied when this side is the one pulling. */
    val conflictPolicy: ConflictPolicy = ConflictPolicy.STRICT,
    /** The namespace's attachment blobs: a v2 pull fetches what its commits attach into it, a v2
     * push uploads from it. Null: no attachment support is advertised. */
    val blobs: PeerBlobStore? = null,
    /** Applies each commit a pull moves main onto to live state; null leaves that to the caller. */
    val materializeCommit: CommitMaterializer? = null,
    /** Speak protocol v2 (falling back to v1 against a v1-only peer). False: v1 only. */
    val protocolV2: Boolean = true,
    /** How long a request waits for its reply. */
    val requestTimeoutMillis: Long = 30_000,
    /** Snapshot bootstrap for a fresh namespace; null: always replay history. */
    val snapshots: PeerSnapshots? = null,
    /** Bootstrap a fresh namespace from a snapshot even when the peer could send all its history. */
    val preferSnapshot: Boolean = false,
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
