package dev.kdb.storage.delta

import kotlinx.coroutines.test.runTest
import kotlin.test.Test

/** Kotlin/JS has no zstd (roadmap R1): its delta pages must be tagged CODEC_NONE. */
class DeltaJsCodecTest {
    @Test
    fun jsWritesCodecNonePages() = runTest { assertTargetWithoutZstdWritesNonePages() }
}
