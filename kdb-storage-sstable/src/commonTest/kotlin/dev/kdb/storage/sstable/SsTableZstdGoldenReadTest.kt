package dev.kdb.storage.sstable

import dev.kdb.codec.KdbHash
import dev.kdb.golden.PhysicalGoldenFixtures
import dev.kdb.storage.mem.InMemoryPlatformIoShim
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * Roadmap R1: every target reads the zstd-compressed SSTables Go and the JVM write. Until R1,
 * Kotlin/JS and Native threw ZstdUnavailableException on the first block of either fixture.
 * The fixtures and their key/value sequence are SsTablePhysicalGoldenTest's (S4/S8) and
 * go/kdb/interop/sstable_physical_golden_test.go's.
 */
class SsTableZstdGoldenReadTest {
    private fun hash(seed: Int): KdbHash = KdbHash.fromBytes(ByteArray(32) { (seed + it).toByte() })

    private val payload: ByteArray = byteArrayOf(0, 1, 2, 3, 0x7F, -128, -1, 0x2A, 0x2A, 0x2A)

    @Test
    fun goAndJvmZstdSegmentsRead() =
        runTest {
            for (side in listOf("go", "kotlin")) {
                val segment = PhysicalGoldenFixtures.read(side, "sstable_segment.hex") ?: fail("$side/sstable_segment.hex missing")
                assertEquals(SsTableCodec.BLOCK_CODEC_ZSTD, segment[1], "$side: the first block is zstd-compressed")
                val shim = InMemoryPlatformIoShim()
                val name = "ns/fixture-ns/sstable/L0/$side-fixture"
                shim.appendToSegment(name, segment)
                val reader = DefaultSsTableReader(shim, SsTableHandle(hash(0), 0, name))
                assertEquals("alpha-rewritten", reader.get(hash(1))?.decodeToString(), "$side: key 1")
                assertContentEquals(payload, reader.get(hash(2)), "$side: key 2")
                assertContentEquals(ByteArray(0), reader.get(hash(4)), "$side: key 4 (empty value)")
                assertTrue(assertNotNull(reader.lookup(hash(3))).deleted, "$side: key 3 is a tombstone")
            }
        }
}
