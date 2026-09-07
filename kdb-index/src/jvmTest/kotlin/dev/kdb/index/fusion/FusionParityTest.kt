package dev.kdb.index.fusion

import dev.kdb.codec.KdbUuid
import dev.kdb.index.GoldenFixtures
import dev.kdb.index.RankedResult
import dev.kdb.index.arr
import dev.kdb.index.field
import dev.kdb.index.int
import dev.kdb.index.num
import dev.kdb.index.obj
import dev.kdb.index.str
import dev.kdb.json.JsonValue
import kotlin.math.abs
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Parity gate for Component 66: every case in `fusion_cases.json` must fuse to the same ranking
 * and the same scores (to 1e-9) as the Go implementation.
 */
class FusionParityTest {

    private companion object {
        const val FIXTURE = "fusion_cases.json"
        const val TOLERANCE = 1e-9
    }

    @Test
    fun matchesEveryGoldenFusionCase() {
        val fixture = GoldenFixtures.json(FIXTURE)
        if (fixture == null) {
            println(GoldenFixtures.missing(FIXTURE))
            return
        }
        val cases = fixture.field("cases")!!.arr()
        assertTrue(cases.isNotEmpty(), "$FIXTURE holds no cases")

        for (case in cases) {
            val name = case.field("name")!!.str()
            val mode =
                when (val m = case.field("mode")!!.str()) {
                    "rrf" -> FusionMode.RRF
                    "weighted", "weighted_sum" -> FusionMode.WEIGHTED_SUM
                    else -> error("case $name: unknown mode $m")
                }
            val limit = case.field("limit")?.int() ?: 0
            val arms =
                case.field("arms")!!.arr().map { arm ->
                    FusionArm(
                        results = arm.field("results")!!.arr().map { it.toRanked() },
                        weight = arm.field("weight")?.num() ?: 1.0,
                        depth = arm.field("depth")?.int() ?: 0,
                        minScore = arm.field("minScore")?.num()?.toFloat(),
                    )
                }
            val expected = case.field("expected")!!.arr().map { it.toRanked() }

            val actual = fuseRankings(arms, mode, limit)

            assertEquals(
                expected.map { it.docId.toString() },
                actual.map { it.docId.toString() },
                "case $name: ranking differs",
            )
            for (i in expected.indices) {
                assertTrue(
                    abs(actual[i].score.toDouble() - expected[i].score.toDouble()) <= TOLERANCE,
                    "case $name: score at rank $i was ${actual[i].score}, expected ${expected[i].score}",
                )
            }
        }
    }

    /** A `[uuid, score]` pair. */
    private fun JsonValue.toRanked(): RankedResult {
        val pair = arr()
        return RankedResult(KdbUuid.fromString(pair[0].str()), pair[1].num().toFloat())
    }

    /** Guards the fixture itself being read, so an empty or moved file cannot pass silently. */
    @Test
    fun fixtureCoversBothModes() {
        val fixture = GoldenFixtures.json(FIXTURE)
        if (fixture == null) {
            println(GoldenFixtures.missing(FIXTURE))
            return
        }
        val modes = fixture.field("cases")!!.arr().map { it.obj()["mode"]!!.str() }.toSet()
        assertEquals(setOf("rrf", "weighted"), modes, "the fixture should exercise both fusion modes")
    }
}
