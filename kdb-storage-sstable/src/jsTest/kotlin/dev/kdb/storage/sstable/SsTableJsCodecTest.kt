package dev.kdb.storage.sstable

import kotlinx.coroutines.test.runTest
import kotlin.test.Test

/** Kotlin/JS compresses SSTable blocks with the pure-Kotlin zstd (roadmap R1). */
class SsTableJsCodecTest {
    @Test
    fun jsWritesZstdBlocks() = runTest { assertTargetWritesZstdBlocks() }
}
