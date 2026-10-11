package dev.kdb.compression

import kotlin.test.Test

/** Kotlin/Native writes exactly the pure-Kotlin frames Go is shown to decode (roadmap R1). */
class ZstdNativeEncoderTest {
    @Test
    fun nativeWritesTheFramesGoReads() = ZstdGoldenCorpus.assertPlatformCompressWritesCommittedFixtures()
}
