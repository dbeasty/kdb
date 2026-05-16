package dev.kdb.compaction

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.dag.CommitDag
import dev.kdb.dag.TraversalEntry
import dev.kdb.policy.CompactionPolicyEvaluator
import dev.kdb.policy.DefaultCompactionPolicyEvaluator
import dev.kdb.policy.NamespacePolicy
import dev.kdb.policy.NamespacePolicyRegistry
import dev.kdb.policy.SquashMode
import dev.kdb.storage.StorageAdapter

public interface CompactionEngine {
    public suspend fun runCycle(request: CompactionRequest): CompactionResult
    public suspend fun plan(request: CompactionRequest): CompactionPlan
    public fun updatePeerHeads(
        namespaceId: String,
        heads: Map<String, KdbHash>,
    )
}

public fun compactionEngine(
    dag: CommitDag,
    storage: StorageAdapter,
    policyRegistry: NamespacePolicyRegistry,
    coordinator: InProcessCompactionCoordinator = InProcessCompactionCoordinator(),
    materializer: SnapshotMaterializer? = null,
    gc: OrphanBlobGc = DefaultOrphanBlobGc(storage),
    evaluator: CompactionPolicyEvaluator = DefaultCompactionPolicyEvaluator,
): CompactionEngine =
    DefaultCompactionEngine(
        dag,
        storage,
        policyRegistry,
        coordinator,
        materializer ?: DefaultSnapshotMaterializer(storage, dag, dag.namespaceId),
        gc,
        evaluator,
    )

internal class DefaultCompactionEngine(
    private val dag: CommitDag,
    private val storage: StorageAdapter,
    private val policyRegistry: NamespacePolicyRegistry,
    private val coordinator: InProcessCompactionCoordinator,
    private val materializer: SnapshotMaterializer,
    private val gc: OrphanBlobGc,
    private val evaluator: CompactionPolicyEvaluator,
) : CompactionEngine {
    private val peerRegistry = CompactionPeerRegistry(coordinator)

    override fun updatePeerHeads(
        namespaceId: String,
        heads: Map<String, KdbHash>,
    ) {
        require(namespaceId == dag.namespaceId)
        peerRegistry.updatePeerHeads(namespaceId, heads)
    }

    override suspend fun plan(request: CompactionRequest): CompactionPlan {
        require(request.namespaceId == dag.namespaceId)
        val policy = policyRegistry.get(request.namespaceId)
        if (policy.compaction.squashAfter == SquashMode.NEVER) {
            return CompactionPlan(
                emptyList(),
                peerSafe = true,
                blockers = listOf(CompactionBlocker.PolicyDisabled("squashAfter=NEVER")),
            )
        }
        val head = dag.head()
        val timestamps = collectTimestamps(head)
        val tagged = dag.listTags().map { it.commitHash }.toSet()
        val branchHeads = dag.listBranches().map { it.headHash }.toSet()
        val parentOf = buildParentMap(timestamps.keys)
        val candidates =
            evaluator.boundaryCandidates(
                policy.compaction,
                timestamps,
                tagged,
                branchHeads,
                head,
                parentOf,
            )
        val peerHeads = coordinator.peerHeads()
        val blockers = mutableListOf<CompactionBlocker>()
        val planned = mutableListOf<PlannedSquash>()
        for (candidate in candidates) {
            val squashable =
                dag.compactableBefore(candidate.boundary, peerHeads).take(request.maxSquashCommits)
            if (squashable.isEmpty()) continue
            planned +=
                PlannedSquash(
                    boundary = candidate.boundary,
                    squashHashes = squashable,
                    strategy = candidate.strategy,
                )
        }
        val ack =
            if (planned.isNotEmpty()) {
                coordinator.broadcastIntent(
                    CompactionIntent(
                        request.namespaceId,
                        planned.first().boundary,
                        KdbTimestamp.now().epochMillis,
                    ),
                )
            } else {
                CompactionAckSet(emptySet(), emptyMap())
            }
        for ((peer, head) in ack.rejected) {
            blockers += CompactionBlocker.PeerBelowBoundary(peer, head)
        }
        for (tag in dag.listTags()) {
            if (tag.commitHash in planned.flatMap { it.squashHashes }.toSet()) {
                blockers += CompactionBlocker.ProtectedTag(tag.name, tag.commitHash)
            }
        }
        return CompactionPlan(
            planned,
            peerSafe = ack.rejected.isEmpty(),
            blockers = blockers,
        )
    }

    override suspend fun runCycle(request: CompactionRequest): CompactionResult {
        val plan = plan(request)
        if (plan.boundaries.isEmpty()) {
            return CompactionResult(0, null, 0L, 0)
        }
        if (!plan.peerSafe && !request.force) {
            throw PeerCompactionRejectedException(
                request.namespaceId,
                plan.boundaries.first().boundary,
                plan.blockers.filterIsInstance<CompactionBlocker.PeerBelowBoundary>()
                    .associate { it.peerId to it.head },
            )
        }
        var totalSquashed = 0
        var synthetic: KdbHash? = null
        for (boundary in plan.boundaries) {
            if (boundary.squashHashes.size < 2) continue
            val tree = materializer.materializeAt(boundary.boundary)
            val commit =
                dag.squash(
                    squashHashes = boundary.squashHashes,
                    boundary = boundary.boundary,
                    syntheticTree = tree,
                    syntheticSchemaHash = dag.getCommit(boundary.boundary)?.schemaHash,
                )
            totalSquashed += boundary.squashHashes.size
            synthetic = commit.hash
        }
        val reclaimed = gc.sweep(request.namespaceId, reachableHashesForGc())
        return CompactionResult(totalSquashed, synthetic, reclaimed, 0)
    }

    private suspend fun buildParentMap(commits: Set<KdbHash>): (KdbHash) -> KdbHash? {
        val parents = hashMapOf<KdbHash, KdbHash?>()
        for (h in commits) {
            val c = dag.getCommit(h)
            parents[h] =
                if (c != null && c.parentHashes.size == 1) {
                    c.parentHashes.single()
                } else {
                    null
                }
        }
        return { h -> parents[h] }
    }

    private suspend fun collectTimestamps(head: KdbHash): Map<KdbHash, KdbTimestamp> {
        val out = linkedMapOf<KdbHash, KdbTimestamp>()
        val walked = dag.walk(from = head, limit = 10_000)
        for (entry in walked) {
            if (entry is TraversalEntry.Full) {
                out[entry.commit.hash] = entry.commit.timestamp
            }
        }
        return out
    }

    private suspend fun reachableHashesForGc(): Set<KdbHash> {
        val hashes = mutableSetOf<KdbHash>()
        val head = dag.head()
        for (entry in dag.walk(from = head, limit = 10_000)) {
            if (entry is TraversalEntry.Full) {
                hashes += entry.commit.hash
                hashes += entry.commit.documentTreeHash
            }
        }
        return hashes
    }
}
