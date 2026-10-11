package dev.kdb.storage.delta

import kotlinx.coroutines.test.runTest
import kotlin.test.Test

/** Kotlin/Native compresses delta pages with the pure-Kotlin zstd (roadmap R1). */
class DeltaNativeCodecTest {
    @Test
    fun nativeWritesZstdPages() = runTest { assertTargetWritesZstdPages() }
}
