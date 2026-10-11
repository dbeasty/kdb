package dev.kdb.storage.sstable

import dev.kdb.codec.KdbHash
import dev.kdb.compression.ZstdCompression
import dev.kdb.storage.CompressionCodec
import dev.kdb.storage.mem.InMemoryPlatformIoShim
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals

/**
 * Which codec an SSTable block is written with, and that the block reads back on the same target.
 * docs/kdb-open-issues-plan.md U5: on JS and Native, ZstdCompression was a length-prefixed copy
 * of the payload, but the writer tagged every block ZSTD - so a JS- or Native-written table
 * claimed to be zstd and Go and the JVM could not read it.
 */
class SsTableBlockCodecTest {
    @Test
    fun defaultCodecRoundTripsAndRecordsTheCodecActuallyUsed() =
        runTest {
            val (blockCodec, _) = writeOneBlock(codec = null)
            val want = if (ZstdCompression.isAvailable) SsTableCodec.BLOCK_CODEC_ZSTD else SsTableCodec.BLOCK_CODEC_NONE
            assertEquals(want, blockCodec, "a block must record the codec its body was actually written with")
        }

    @Test
    fun noneCodecRoundTripsOnEveryTarget() =
        runTest {
            val (blockCodec, _) = writeOneBlock(codec = CompressionCodec.NONE)
            assertEquals(SsTableCodec.BLOCK_CODEC_NONE, blockCodec)
        }
}

private val FIXTURE_KEY: KdbHash = KdbHash.fromBytes(ByteArray(32) { (7 + it).toByte() })
private val FIXTURE_VALUE: ByteArray =
    "block codec fixture value, compressible compressible compressible".encodeToByteArray()

/**
 * Writes one key through [DefaultSsTableWriter] (with [codec], or the writer's default when null),
 * reads it back through [DefaultSsTableReader], and returns the codec byte of the first block's
 * header together with the value read back.
 */
internal suspend fun writeOneBlock(codec: CompressionCodec?): Pair<Byte, ByteArray> {
    val shim = InMemoryPlatformIoShim()
    val writer =
        if (codec == null) DefaultSsTableWriter(shim, "ns", 0) else DefaultSsTableWriter(shim, "ns", 0, codec)
    writer.put(FIXTURE_KEY, FIXTURE_VALUE)
    val handle = writer.finish()
    val header = shim.readFromSegment(handle.segmentName, 0, SsTableCodec.BLOCK_HEADER_SIZE)
    val got = DefaultSsTableReader(shim, handle).get(FIXTURE_KEY)
    assertContentEquals(FIXTURE_VALUE, got, "the block must read back on the target that wrote it")
    return header[1] to got!!
}

/**
 * Shared by jsTest and nativeTest: on a target with no real zstd, the default writer must emit
 * CODEC_NONE (which Go and the JVM read), and the stub must refuse rather than misread.
 */
internal suspend fun assertTargetWithoutZstdWritesNone() {
    assertEquals(false, ZstdCompression.isAvailable, "this target has no zstd codec linked (roadmap R1)")
    val (blockCodec, _) = writeOneBlock(codec = null)
    assertEquals(SsTableCodec.BLOCK_CODEC_NONE, blockCodec, "a target without zstd must not tag blocks ZSTD")
}
