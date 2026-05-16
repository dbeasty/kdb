package dev.kdb.policy

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp

public data class CompactionBoundaryPlan(
    val boundary: KdbHash,
    val squashThrough: KdbHash,
    val strategy: RetainStrategy,
)

public interface CompactionPolicyEvaluator {
    public fun boundaryCandidates(
        policy: CompactionPolicy,
        commitTimestamps: Map<KdbHash, KdbTimestamp>,
        tagged: Set<KdbHash>,
        branchHeads: Set<KdbHash>,
        head: KdbHash,
        parentOf: (KdbHash) -> KdbHash?,
    ): List<CompactionBoundaryPlan>
}

public object DefaultCompactionPolicyEvaluator : CompactionPolicyEvaluator {
    override fun boundaryCandidates(
        policy: CompactionPolicy,
        commitTimestamps: Map<KdbHash, KdbTimestamp>,
        tagged: Set<KdbHash>,
        branchHeads: Set<KdbHash>,
        head: KdbHash,
        parentOf: (KdbHash) -> KdbHash?,
    ): List<CompactionBoundaryPlan> {
        if (policy.squashAfter == SquashMode.NEVER) return emptyList()
        if (commitTimestamps.isEmpty()) return emptyList()

        val protected = tagged + branchHeads
        val now = commitTimestamps[head] ?: return emptyList()
        val ordered = linearAncestors(head, parentOf).filter { it in commitTimestamps }
        if (ordered.size < 2) return emptyList()

        val plans = mutableListOf<CompactionBoundaryPlan>()
        val rules = policy.retainGranularity.sortedBy { it.olderThanMillis }
        if (rules.isEmpty()) return emptyList()

        for (rule in rules) {
            val cutoffMicros = now.toEpochMicros() - rule.olderThanMillis * 1000L
            val candidates =
                ordered.filter { h ->
                    val ts = commitTimestamps[h] ?: return@filter false
                    ts.toEpochMicros() <= cutoffMicros
                }
            if (candidates.isEmpty()) continue

            val squashable =
                when (rule.strategy) {
                    RetainStrategy.FULL_HISTORY -> emptyList()
                    RetainStrategy.TAGGED_ONLY ->
                        candidates.filter { it !in protected }
                    RetainStrategy.DAILY_SNAPSHOTS ->
                        dailySnapshotSquashCandidates(candidates, commitTimestamps, protected)
                }
            if (squashable.size < 2) continue

            val boundary = squashable.first()
            val squashThrough = squashable.last()
            plans +=
                CompactionBoundaryPlan(
                    boundary = boundary,
                    squashThrough = squashThrough,
                    strategy = rule.strategy,
                )
        }
        return plans.distinctBy { it.boundary }
    }

    private fun linearAncestors(
        head: KdbHash,
        parentOf: (KdbHash) -> KdbHash?,
    ): List<KdbHash> {
        val out = mutableListOf<KdbHash>()
        var cur: KdbHash? = head
        while (cur != null) {
            out += cur
            cur = parentOf(cur)
        }
        return out
    }

    private fun dailySnapshotSquashCandidates(
        candidates: List<KdbHash>,
        timestamps: Map<KdbHash, KdbTimestamp>,
        protected: Set<KdbHash>,
    ): List<KdbHash> {
        val byDay = linkedMapOf<Long, KdbHash>()
        for (h in candidates) {
            if (h in protected) continue
            val ts = timestamps[h] ?: continue
            val day = ts.toEpochMicros() / (24L * 3600 * 1_000_000L)
            byDay[day] = h
        }
        val keep = byDay.values.toSet()
        return candidates.filter { it !in keep && it !in protected }
    }
}
