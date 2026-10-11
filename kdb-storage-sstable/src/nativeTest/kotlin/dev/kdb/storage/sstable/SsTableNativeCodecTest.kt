package dev.kdb.storage.sstable

import kotlinx.coroutines.test.runTest
import kotlin.test.Test

/** Kotlin/Native compresses SSTable blocks with the pure-Kotlin zstd (roadmap R1). */
class SsTableNativeCodecTest {
    @Test
    fun nativeWritesZstdBlocks() = runTest { assertTargetWritesZstdBlocks() }
}
