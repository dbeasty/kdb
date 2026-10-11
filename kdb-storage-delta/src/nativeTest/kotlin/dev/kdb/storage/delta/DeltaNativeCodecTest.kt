package dev.kdb.storage.delta

import kotlinx.coroutines.test.runTest
import kotlin.test.Test

/** Kotlin/Native has no zstd (roadmap R1): its delta pages must be tagged CODEC_NONE. */
class DeltaNativeCodecTest {
    @Test
    fun nativeWritesCodecNonePages() = runTest { assertTargetWithoutZstdWritesNonePages() }
}
