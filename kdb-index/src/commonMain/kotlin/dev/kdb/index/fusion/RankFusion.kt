package dev.kdb.index.fusion

import dev.kdb.codec.KdbUuid
import dev.kdb.index.RankedResult

/**
 * Rank fusion (Layer 16, Component 65). Both modes read only positions or per-arm normalised
 * scores, never raw cross-arm scores, and the output order is deterministic (fused score
 * descending, then document id ascending) so Kotlin and Go agree on the same corpus.
 */
public enum class FusionMode { RRF, WEIGHTED_SUM }

public const val DEFAULT_RRF_K: Int = 60

/**
 * One ranked input list plus its parameters. [results] must already be sorted by score descending,
 * document id ascending. [depth] 0 means unlimited; [minScore] null means no floor.
 */
public data class FusionArm(
    val results: List<RankedResult>,
    val weight: Double = 1.0,
    val depth: Int = 0,
    val minScore: Float? = null,
)

public fun fuseRankings(
    arms: List<FusionArm>,
    mode: FusionMode = FusionMode.RRF,
    limit: Int = Int.MAX_VALUE,
    rrfK: Int = DEFAULT_RRF_K,
): List<RankedResult> {
    val scores = LinkedHashMap<KdbUuid, Double>()
    fun bump(id: KdbUuid, delta: Double) {
        scores[id] = (scores[id] ?: 0.0) + delta
    }
    for (arm in arms) {
        val w = if (arm.weight == 0.0) 1.0 else arm.weight
        var list = arm.results
        arm.minScore?.let { floor -> list = list.filter { it.score >= floor } }
        if (arm.depth > 0 && list.size > arm.depth) list = list.take(arm.depth)
        when (mode) {
            FusionMode.RRF ->
                list.forEachIndexed { i, r -> bump(r.docId, w / (rrfK + i + 1).toDouble()) }

            FusionMode.WEIGHTED_SUM -> {
                if (list.isEmpty()) continue
                val lo = list.minOf { it.score }
                val hi = list.maxOf { it.score }
                for (r in list) {
                    val norm = if (hi > lo) (r.score - lo).toDouble() / (hi - lo).toDouble() else 1.0
                    bump(r.docId, w * norm)
                }
            }
        }
    }
    return scores.entries
        .map { RankedResult(it.key, it.value.toFloat()) }
        .sortedWith(compareByDescending<RankedResult> { it.score }.thenBy { it.docId.toString() })
        .let { if (it.size > limit) it.take(limit) else it }
}
