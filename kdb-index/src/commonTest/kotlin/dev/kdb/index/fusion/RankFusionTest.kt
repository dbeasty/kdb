package dev.kdb.index.fusion

import dev.kdb.codec.KdbUuid
import dev.kdb.index.RankedResult
import kotlin.math.abs
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RankFusionTest {

    private val d1 = KdbUuid.fromString("00000000-0000-4000-8000-000000000001")
    private val d2 = KdbUuid.fromString("00000000-0000-4000-8000-000000000002")
    private val d3 = KdbUuid.fromString("00000000-0000-4000-8000-000000000003")

    /**
     * [RankedResult.score] is a Float, so a hand-computed Double is only comparable to Float
     * precision. (The golden fixtures assert to 1e-9 because their expected values are
     * themselves the Float the implementation produces.)
     */
    private fun assertClose(
        expected: Double,
        actual: Float,
        tolerance: Double = 1e-7,
    ) {
        assertTrue(abs(actual.toDouble() - expected) <= tolerance, "expected $expected, got $actual")
    }

    /**
     * Guards §8 RRF against a hand-computed case. Two arms, weight 1, k = 60:
     *   arm A ranks d1, d2 → d1 gets 1/61, d2 gets 1/62
     *   arm B ranks d2, d3 → d2 gets 1/61, d3 gets 1/62
     * so d2 = 1/61 + 1/62 = 0.032522..., d1 = 1/61 = 0.016393..., d3 = 1/62 = 0.016129...
     */
    @Test
    fun rrfSumsReciprocalRanksAcrossArms() {
        val fused =
            fuseRankings(
                listOf(
                    FusionArm(listOf(RankedResult(d1, 9f), RankedResult(d2, 5f))),
                    FusionArm(listOf(RankedResult(d2, 0.9f), RankedResult(d3, 0.5f))),
                ),
                FusionMode.RRF,
            )
        assertEquals(listOf(d2, d1, d3), fused.map { it.docId })
        assertClose(1.0 / 61.0 + 1.0 / 62.0, fused[0].score)
        assertClose(1.0 / 61.0, fused[1].score)
        assertClose(1.0 / 62.0, fused[2].score)
    }

    /**
     * Guards §8 weighted sum: each arm is min-max normalised over its own filtered list, so
     * the arm's best is 1.0 and its worst 0.0, then weighted and summed.
     *   arm A (weight 2): scores 10, 5, 0 → 1.0, 0.5, 0.0 → contributes 2.0, 1.0, 0.0
     *   arm B (weight 1): only d3 at 4.0 → all-equal list normalises to 1.0 → contributes 1.0
     * so d1 = 2.0, d3 = 0.0 + 1.0 = 1.0, d2 = 1.0 — d2 and d3 tie and order by document id.
     */
    @Test
    fun weightedSumNormalisesEachArmBeforeWeighting() {
        val fused =
            fuseRankings(
                listOf(
                    FusionArm(
                        listOf(RankedResult(d1, 10f), RankedResult(d2, 5f), RankedResult(d3, 0f)),
                        weight = 2.0,
                    ),
                    FusionArm(listOf(RankedResult(d3, 4f))),
                ),
                FusionMode.WEIGHTED_SUM,
            )
        assertEquals(listOf(d1, d2, d3), fused.map { it.docId })
        assertClose(2.0, fused[0].score)
        assertClose(1.0, fused[1].score)
        assertClose(1.0, fused[2].score)
    }

    /** Guards §8 step 1 ordering: minScore filters first, then depth truncates what remains. */
    @Test
    fun minScoreFiltersBeforeDepthTruncates() {
        val arm =
            FusionArm(
                listOf(RankedResult(d1, 9f), RankedResult(d2, 1f), RankedResult(d3, 8f)),
                depth = 2,
                minScore = 5f,
            )
        // d2 is dropped by the floor, so depth 2 keeps d1 and d3 rather than d1 and d2.
        val fused = fuseRankings(listOf(arm), FusionMode.RRF)
        assertEquals(listOf(d1, d3), fused.map { it.docId })
    }

    /** Guards §8: a document absent from an arm contributes 0 from that arm, never a penalty. */
    @Test
    fun absentArmsContributeZero() {
        val fused =
            fuseRankings(
                listOf(
                    FusionArm(listOf(RankedResult(d1, 1f))),
                    FusionArm(emptyList()),
                ),
                FusionMode.WEIGHTED_SUM,
            )
        assertEquals(listOf(d1), fused.map { it.docId })
        assertClose(1.0, fused[0].score)
    }

    /** Guards the tie rule: equal fused scores order by ascending document id string. */
    @Test
    fun tiesResolveByDocumentId() {
        val fused =
            fuseRankings(
                listOf(FusionArm(listOf(RankedResult(d3, 1f), RankedResult(d1, 1f)))),
                FusionMode.WEIGHTED_SUM,
            )
        // An all-equal arm normalises to 1.0 for both, so only the id ordering separates them.
        assertEquals(listOf(d1, d3), fused.map { it.docId })
    }

    /** Guards `limit`: positive truncates the fused list, 0 or less means every result. */
    @Test
    fun limitTruncatesAndZeroMeansUnlimited() {
        val arms = listOf(FusionArm(listOf(RankedResult(d1, 3f), RankedResult(d2, 2f), RankedResult(d3, 1f))))
        assertEquals(2, fuseRankings(arms, FusionMode.RRF, limit = 2).size)
        assertEquals(3, fuseRankings(arms, FusionMode.RRF, limit = 0).size)
    }
}
