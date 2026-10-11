package dev.kdb.storage.delta

import kotlinx.coroutines.test.runTest
import kotlin.test.Test

/** Kotlin/JS compresses delta pages with the pure-Kotlin zstd (roadmap R1). */
class DeltaJsCodecTest {
    @Test
    fun jsWritesZstdPages() = runTest { assertTargetWritesZstdPages() }
}
