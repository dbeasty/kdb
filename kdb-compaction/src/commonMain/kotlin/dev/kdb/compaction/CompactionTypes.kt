package dev.kdb.compaction

import dev.kdb.codec.KdbHash
import dev.kdb.policy.RetainStrategy

public data class CompactionRequest(
    val namespaceId: String,
    val force: Boolean = false,
    val maxSquashCommits: Int = 10_000,
)

public data class CompactionResult(
    val squashedCount: Int,
    val syntheticRoot: KdbHash?,
    val gcReclaimedBytes: Long,
    val storageJobsEnqueued: Int = 0,
)

public data class CompactionPlan(
    val boundaries: List<PlannedSquash>,
    val peerSafe: Boolean,
    val blockers: List<CompactionBlocker>,
)

public data class PlannedSquash(
    val boundary: KdbHash,
    val squashHashes: List<KdbHash>,
    val strategy: RetainStrategy,
)

public sealed class CompactionBlocker {
    public data class ProtectedTag(val tag: String, val hash: KdbHash) : CompactionBlocker()
    public data class ProtectedBranch(val branch: String, val hash: KdbHash) : CompactionBlocker()
    public data class PeerBelowBoundary(val peerId: String, val head: KdbHash) : CompactionBlocker()
    public data class PolicyDisabled(val reason: String) : CompactionBlocker()
}

public data class CompactionIntent(
    val namespaceId: String,
    val boundary: KdbHash,
    val issuedAtMillis: Long,
)

public data class CompactionAckSet(
    val ackedPeers: Set<String>,
    val rejected: Map<String, KdbHash>,
)
