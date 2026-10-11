package dev.kdb.storage.delta

import dev.kdb.golden.PhysicalGoldenFixtures
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.fail

/**
 * Roadmap R1: every target replays the ZSTD delta segments Go and the JVM write. Until R1,
 * Kotlin/JS and Native could not decode a single page of either - the scanner reported the first
 * frame corrupt.
 */
class DeltaZstdGoldenReadTest {
    @Test
    fun goAndJvmZstdSegmentsScan() {
        val want = listOf(DeltaGoldenCommits.first(), DeltaGoldenCommits.second())
        for (side in listOf("go", "kotlin")) {
            val segment = PhysicalGoldenFixtures.read(side, "delta_segment_zstd.hex") ?: fail("$side/delta_segment_zstd.hex missing")
            val scanned = DeltaSegmentScanner.scanSegmentBytes(segment)
            assertEquals(want.map { it.hash }, scanned.map { it.commitHash }, "$side: both commits, in order")
            assertEquals(want.map { it.message }, scanned.map { it.commit.message }, "$side: decoded commit bodies")
            for (s in scanned) assertEquals(DeltaPageCodec.CODEC_ZSTD, segment[s.frameOffset + 5], "$side: frame at ${s.frameOffset} is zstd")
        }
    }
}
