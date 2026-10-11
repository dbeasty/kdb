package dev.kdb.storage.engine

import dev.kdb.codec.KdbHash
import dev.kdb.storage.PlatformIoShim
import dev.kdb.storage.StorageEngineConfig
import dev.kdb.storage.mem.InMemoryPlatformIoShim
import dev.kdb.storage.wal.DefaultWriteAheadLogFactory
import dev.kdb.storage.wal.WalRecordKind
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * U11: the engine's [StorageEngineConfig.walMaxSegmentBytes] has to reach the WAL. Before this,
 * [DefaultStorageEngineFactory] built `DefaultWriteAheadLogFactory()` with no arguments, so every
 * namespace rotated at the 64 MiB default whatever the config said.
 */
class WalSegmentSizeTest {
    private val ns = "wal-cap"

    private suspend fun walSegments(shim: PlatformIoShim): List<String> =
        shim.listSegments(ns).filter { it.startsWith("ns/$ns/wal/") }

    private fun payload(i: Int): ByteArray = ByteArray(100) { (i * 31 + it).toByte() }

    @Test
    fun smallCapRotatesAndTheChainReplaysInOrder() =
        runTest {
            val shim = InMemoryPlatformIoShim()
            val config = StorageEngineConfig(globalMemoryBudgetBytes = 8_000_000, ioShim = shim, walMaxSegmentBytes = 512)
            val handle = DefaultStorageEngineFactory(StorageEngineTarget.SERVER).open(ns, config)
            val hashes = mutableListOf<KdbHash>()
            // 40 records of ~100+ bytes each is ~5 KiB: about ten segments at a 512-byte cap, one
            // at the default.
            repeat(40) { hashes += handle.adapter.writeBlob(payload(it)) }
            handle.close()

            val segments = walSegments(shim)
            assertTrue(segments.size >= 5, "expected the 512-byte cap to rotate the WAL, got segments $segments")

            // A fresh engine on the same files recovers every blob from the rotated chain.
            val reopened = DefaultStorageEngineFactory(StorageEngineTarget.SERVER).open(ns, config)
            val engine = reopened.adapter as ServerStorageEngine
            engine.recoverBlobsFromWal()
            hashes.forEachIndexed { i, h -> assertContentEquals(payload(i), engine.readBlob(h), "blob $i") }
            reopened.close()

            // And the replay walks the segments in write order.
            val wal = DefaultWriteAheadLogFactory().openOrCreate(ns, config, shim)
            val replayed = mutableListOf<Pair<Long, KdbHash>>()
            wal.recover { rec ->
                if (rec.kind == WalRecordKind.PutBlob) {
                    replayed += rec.sequence to KdbHash.fromBytes(rec.payload.copyOfRange(0, 32))
                }
            }
            assertEquals(hashes, replayed.map { it.second }, "records must replay in the order written")
            assertEquals(replayed.map { it.first }.sorted(), replayed.map { it.first }, "sequences must increase")
        }

    @Test
    fun defaultCapIsSixtyFourMebibytesAndDoesNotRotateSmallWrites() =
        runTest {
            val shim = InMemoryPlatformIoShim()
            val config = StorageEngineConfig(globalMemoryBudgetBytes = 8_000_000, ioShim = shim)
            assertEquals(64L * 1024 * 1024, config.walMaxSegmentBytes)
            val handle = DefaultStorageEngineFactory(StorageEngineTarget.SERVER).open(ns, config)
            repeat(40) { handle.adapter.writeBlob(payload(it)) }
            handle.close()
            assertEquals(1, walSegments(shim).size)
        }

    @Test
    fun zeroOrNegativeCapIsRejected() {
        for (bad in listOf(0L, -1L, Long.MIN_VALUE)) {
            assertFailsWith<IllegalArgumentException>("walMaxSegmentBytes=$bad") {
                StorageEngineConfig(ioShim = InMemoryPlatformIoShim(), walMaxSegmentBytes = bad)
            }
        }
    }
}
