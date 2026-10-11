package dev.kdb.storage.sstable

import kotlinx.coroutines.test.runTest
import kotlin.test.Test

/** Kotlin/JS has no zstd (roadmap R1): its SSTable blocks must be tagged CODEC_NONE. */
class SsTableJsCodecTest {
    @Test
    fun jsWritesCodecNoneBlocks() = runTest { assertTargetWithoutZstdWritesNone() }
}
