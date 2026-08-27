package dev.kdb.storage.sstable

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.document.kdbSha256
import dev.kdb.storage.io.FileBackedPlatformIoShimFactory
import dev.kdb.storage.io.PlatformIoConfig
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertNull

private fun randomKey(): KdbHash = KdbHash.fromBytes(kdbSha256(KdbUuid.random().toString().encodeToByteArray()))

/**
 * Regression tests for the finding recorded in docs/kdb-finish-up-plan.md as 1-K1:
 * DefaultSsTableReader.get could not locate a real footer at all. buildFooter wrote indexLen
 * only inside the footer itself (at an offset that depends on already knowing where the footer
 * starts), with no way for a reader to find it from just the end of the file; readFooterIndexLen
 * read the last 8 bytes and took bytes [4:8] as indexLen, which was actually the tail of the
 * 32-byte fileHash, never a real length. A second, independent bug (DefaultSsTableWriter.finish
 * storing each BlockHandle's compressedSize as the block's *full* encoded length instead of the
 * body alone, causing get() to over-read 12 bytes into whatever followed) was fixed in the same
 * pass. This module had no round-trip test before this fix, mirroring go/kdb/storage/sstable,
 * which had the identical defect (verified there first by a failing round trip).
 */
class SsTableCodecTest {
    private fun shim() = FileBackedPlatformIoShimFactory.open(PlatformIoConfig(fsyncOnFlush = true))

    @Test
    fun roundTrip() =
        runTest {
            val io = shim()
            val writer = DefaultSsTableWriter(io, "ns", 0)
            val key = randomKey()
            val value = """{"hello":"world"}""".encodeToByteArray()
            writer.put(key, value)
            val handle = writer.finish()

            val reader = DefaultSsTableReader(io, handle)
            val got = reader.get(key)
            assertEquals(value.decodeToString(), got?.decodeToString())
        }

    @Test
    fun multiBlockRoundTrip() =
        runTest {
            val io = shim()
            val writer = DefaultSsTableWriter(io, "ns", 0)
            val n = 20
            val keys = List(n) { randomKey() }
            val values = List(n) { i -> """{"i":$i,"payload":"${"0".repeat(40 - i.toString().length)}$i"}""".encodeToByteArray() }
            for (i in 0 until n) writer.put(keys[i], values[i])
            val handle = writer.finish()

            val reader = DefaultSsTableReader(io, handle)
            for (i in 0 until n) {
                val got = reader.get(keys[i])
                assertEquals(
                    values[i].decodeToString(),
                    got?.decodeToString(),
                    "block $i resolved to the wrong value - a misaligned read would land in a neighboring block",
                )
            }
        }

    @Test
    fun getMissingKeyReturnsNull() =
        runTest {
            val io = shim()
            val writer = DefaultSsTableWriter(io, "ns", 0)
            writer.put(randomKey(), """{"present":true}""".encodeToByteArray())
            val handle = writer.finish()

            val reader = DefaultSsTableReader(io, handle)
            assertNull(reader.get(randomKey()))
        }

    @Test
    fun decodeBlockRejectsCorruptCrc() {
        val block = SsTableCodec.encodeBlock("""{"v":"original"}""".encodeToByteArray(), compress = true)
        val corrupt = block.copyOf()
        corrupt[corrupt.size - 1] = (corrupt[corrupt.size - 1].toInt() xor 0xFF).toByte()
        assertFails { SsTableCodec.decodeBlock(corrupt) }
        // The original, unmodified block must still decode cleanly.
        SsTableCodec.decodeBlock(block)
    }
}
