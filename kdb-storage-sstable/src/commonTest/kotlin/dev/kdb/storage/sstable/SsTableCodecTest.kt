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
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

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

    /**
     * A deleted key must come back as "present, deleted" - distinguishable from a key the table
     * never held, which is the distinction [SsTableReader.get] alone cannot make and the reason a
     * flushed delete used to fall through to an older table. Mirrors Go's TestTombstoneRoundTrips.
     */
    @Test
    fun tombstoneRoundTrips() =
        runTest {
            val io = shim()
            val writer = DefaultSsTableWriter(io, "ns", 0)
            val kept = randomKey()
            val gone = randomKey()
            writer.put(kept, "still here".encodeToByteArray())
            writer.delete(gone)
            val handle = writer.finish()

            val reader = DefaultSsTableReader(io, handle)

            val keptEntry = reader.lookup(kept)
            assertNotNull(keptEntry)
            assertFalse(keptEntry.deleted)
            assertEquals("still here", keptEntry.value?.decodeToString())

            val goneEntry = reader.lookup(gone)
            assertNotNull(goneEntry, "the tombstone was not recorded at all")
            assertTrue(goneEntry.deleted, "the deleted key came back as a live value")
            assertNull(reader.get(gone))

            assertNull(reader.lookup(randomKey()), "a key never written must be absent, not tombstoned")
        }

    /**
     * Pins the compatibility claim in [SsTableCodec.TOMBSTONE_FLAG]'s comment: the fourth field is
     * written only for tombstones, so a table without any produces exactly the bytes the
     * three-field format did. The golden fixtures - and Go's identical encoder - depend on this.
     */
    @Test
    fun footerWithNoTombstonesIsUnchanged() {
        val key = randomKey()
        val footer = SsTableCodec.buildFooter(mapOf(key to BlockHandle(7L, 11, 0)), randomKey())
        val indexLen =
            ((footer[4].toInt() and 0xFF) shl 24) or ((footer[5].toInt() and 0xFF) shl 16) or
                ((footer[6].toInt() and 0xFF) shl 8) or (footer[7].toInt() and 0xFF)
        assertEquals("${key.toHex()}:7:11", footer.copyOfRange(8, 8 + indexLen).decodeToString())
    }

    /**
     * Segments written before tombstones existed have no fourth field, and must still parse as
     * ordinary (non-deleted) entries.
     */
    @Test
    fun parseFooterAcceptsThreeFieldLines() {
        val key = randomKey()
        val index = "${key.toHex()}:7:11".encodeToByteArray()
        val footer = ByteArray(8 + index.size + 32 + SsTableCodec.FOOTER_TRAILER_SIZE)
        footer[4] = (index.size ushr 24).toByte()
        footer[5] = (index.size ushr 16).toByte()
        footer[6] = (index.size ushr 8).toByte()
        footer[7] = index.size.toByte()
        index.copyInto(footer, 8)

        val bh = SsTableCodec.parseFooter(footer)[key]
        assertNotNull(bh, "legacy three-field line did not parse")
        assertFalse(bh.deleted)
        assertEquals(7L, bh.offset)
        assertEquals(11, bh.compressedSize)
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
