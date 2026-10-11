package dev.kdb.compression

import kotlin.test.Test

/** Kotlin/JS writes exactly the pure-Kotlin frames Go is shown to decode (roadmap R1). */
class ZstdJsEncoderTest {
    @Test
    fun jsWritesTheFramesGoReads() = ZstdGoldenCorpus.assertPlatformCompressWritesCommittedFixtures()
}
