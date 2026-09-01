package dev.kdb.storage.memtable

import dev.kdb.codec.KdbHash
import dev.kdb.storage.PlatformIoShim
import dev.kdb.storage.mem.InMemoryPlatformIoShim
import dev.kdb.storage.sstable.BlockCache
import dev.kdb.storage.sstable.LsmBlobStore
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertNull

/** A deterministic 32-byte key for tests - no real hashing needed, just something KdbHash accepts. */
private fun randomKey(seed: String): KdbHash {
    val seedBytes = seed.encodeToByteArray()
    val out = ByteArray(32)
    for (i in out.indices) out[i] = seedBytes[i % seedBytes.size]
    return KdbHash.fromBytes(out)
}

/** Delegates every call to [inner] except sealSegment, which throws once configured to via [failNextSeal]. */
private class SealFailingShim(private val inner: PlatformIoShim) : PlatformIoShim by inner {
    var failNextSeal: Boolean = false

    override suspend fun sealSegment(segmentName: String) {
        if (failNextSeal) {
            failNextSeal = false
            throw IllegalStateException("simulated seal failure")
        }
        inner.sealSegment(segmentName)
    }
}

/**
 * Regression tests for the finding recorded in docs/kdb-finish-up-plan.md as 1-K2:
 * MemTableManager.flush cleared pendingFlush immediately after staging the writer, *before*
 * writer.finish() (the call that can actually fail - I/O errors, a full disk) ever ran. A throw
 * there used to lose the whole generation silently: active had already been swapped to a fresh
 * empty table, so the flushed-but-not-yet-durable writes became reachable from neither active,
 * pendingFlush (already cleared), nor the blob store (finish() never completed) - get() reported
 * every one of them as simply absent, even though the data was never actually lost anywhere else
 * (no WAL layer under this component to recover it from).
 */
class MemTableTest {
    @Test
    fun flushFailureStillLeavesDataVisibleViaPendingFlush() =
        runTest {
            val shim = SealFailingShim(InMemoryPlatformIoShim())
            val blobStore = LsmBlobStore(shim, "ns", BlockCache(1024 * 1024))
            val manager = MemTableManager("ns", shim, blobStore)

            val key = randomKey("k1")
            val value = """{"v":"should not be lost"}""".encodeToByteArray()
            manager.put(key, value)

            shim.failNextSeal = true
            assertFails { manager.flush() }

            // The write must still be visible - it was never actually lost, just not yet durable.
            val got = manager.get(key)
            assertEquals(value.decodeToString(), got?.decodeToString())
        }

    @Test
    fun flushSucceedsNormallyAndDataRemainsReadableFromBlobStore() =
        runTest {
            val shim = InMemoryPlatformIoShim()
            val blobStore = LsmBlobStore(shim, "ns", BlockCache(1024 * 1024))
            val manager = MemTableManager("ns", shim, blobStore)

            val key = randomKey("k2")
            val value = """{"v":"durable"}""".encodeToByteArray()
            manager.put(key, value)
            val handle = manager.flush()
            requireNotNull(handle)

            val got = manager.get(key)
            assertEquals(value.decodeToString(), got?.decodeToString())
        }

    /**
     * Regression test for the lost-tombstone hazard: a key written and flushed, then deleted and
     * flushed again, must stay deleted. Tombstones used to be skipped at flush time (the SSTable
     * format had no delete marker), so the second flush erased the only record of the delete and
     * the first flush's value came straight back. Mirrors Go's TestDeleteSurvivesAFlush.
     */
    @Test
    fun deleteSurvivesAFlush() =
        runTest {
            val shim = InMemoryPlatformIoShim()
            val blobStore = LsmBlobStore(shim, "ns", BlockCache(1024 * 1024))
            val manager = MemTableManager("ns", shim, blobStore)

            val key = randomKey("resurrect")
            manager.put(key, """{"v":"original"}""".encodeToByteArray())
            requireNotNull(manager.flush())
            assertEquals("""{"v":"original"}""", manager.get(key)?.decodeToString())

            manager.delete(key)
            assertNull(manager.get(key), "the tombstone must hide the flushed value while it is in memory")
            requireNotNull(manager.flush()) { "a memtable holding only tombstones must still produce a table" }

            assertNull(manager.get(key), "the delete did not survive the flush")
        }

    /** A tombstone must shadow older tables, but must not shadow a value written after it. */
    @Test
    fun deleteThenRewriteSurvivesAFlush() =
        runTest {
            val shim = InMemoryPlatformIoShim()
            val blobStore = LsmBlobStore(shim, "ns", BlockCache(1024 * 1024))
            val manager = MemTableManager("ns", shim, blobStore)

            val key = randomKey("revived")
            manager.put(key, "v1".encodeToByteArray())
            requireNotNull(manager.flush())
            manager.delete(key)
            requireNotNull(manager.flush())
            manager.put(key, "v2".encodeToByteArray())
            requireNotNull(manager.flush())

            assertEquals("v2", manager.get(key)?.decodeToString(), "the tombstone shadowed a newer write")
        }

    /** An genuinely empty memtable still writes nothing - only tombstones changed. */
    @Test
    fun flushingAnEmptyMemTableWritesNothing() =
        runTest {
            val shim = InMemoryPlatformIoShim()
            val blobStore = LsmBlobStore(shim, "ns", BlockCache(1024 * 1024))
            assertNull(MemTableManager("ns", shim, blobStore).flush())
        }
}

/** Regression tests for SortedMemTable's sizeBytes accounting, also part of 1-K2. */
class SortedMemTableSizeTest {
    @Test
    fun overwriteNetsOutThePreviousValuesSize() =
        runTest {
            val table = SortedMemTable()
            val key = randomKey("k")
            table.put(key, ByteArray(100))
            assertEquals(100L, table.sizeBytes)
            table.put(key, ByteArray(40)) // overwrite with a smaller value
            assertEquals(40L, table.sizeBytes, "expected sizeBytes to net out the replaced value, not just add the new one")
        }

    @Test
    fun deleteSubtractsTheDeletedValuesSize() =
        runTest {
            val table = SortedMemTable()
            val key = randomKey("k")
            table.put(key, ByteArray(100))
            table.delete(key)
            assertEquals(0L, table.sizeBytes, "expected sizeBytes to return to 0 after deleting the only entry")
        }
}
