package dev.kdb.storage.engine

import dev.kdb.codec.KdbUuid
import dev.kdb.storage.Durability
import dev.kdb.storage.StorageEngineConfig
import dev.kdb.storage.mem.InMemoryPlatformIoShim
import dev.kdb.storage.wal.WalAppendResult
import dev.kdb.storage.wal.WalRecord
import dev.kdb.storage.wal.WalRecoverySummary
import dev.kdb.storage.wal.WriteAheadLog
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Counts sync() calls without doing real I/O, so durability-mode tests assert on behavior
 * directly. internal, not private: shared with AsyncDurabilityJvmTest (jvmTest) - see that
 * file's doc comment for why the ASYNC-durability cases live there instead of here.
 */
internal class FakeWal : WriteAheadLog {
    override val walId: KdbUuid = KdbUuid.random()
    override val partitionKey: String = "fake"
    override val lastSequence: Long get() = seq
    override val activeSegmentSizeBytes: Long = 0

    // Plain vars are fine here: exercised only from a single test
    // dispatcher (kotlinx.coroutines.test.runTest / runBlocking), never
    // from real concurrent threads.
    private var seq = 0L
    var syncCalls = 0L
        private set

    override suspend fun append(record: WalRecord): WalAppendResult {
        seq += 1
        return WalAppendResult(seq, 0, 0)
    }

    override suspend fun appendBatch(records: List<WalRecord>): WalAppendResult = WalAppendResult(0, 0, 0)

    override suspend fun sync() {
        syncCalls += 1
    }

    override suspend fun recover(handler: suspend (WalRecord) -> Unit): WalRecoverySummary = WalRecoverySummary(0, 0, 0, 0)

    override suspend fun truncate(truncateThroughSequence: Long) {}

    override suspend fun close() {}
}

class DurabilityTest {
    private fun config(durability: Durability, asyncIntervalMs: Long? = null) =
        StorageEngineConfig(
            globalMemoryBudgetBytes = 1_000_000,
            ioShim = InMemoryPlatformIoShim(),
            durability = durability,
            asyncSyncIntervalMillis = asyncIntervalMs,
        )

    @Test
    fun syncDurability_syncsEveryWrite() =
        runTest {
            val wal = FakeWal()
            val engine = ServerStorageEngine("ns", config(Durability.SYNC), wal)
            repeat(10) { engine.writeBlob(byteArrayOf(it.toByte())) }
            assertTrue(wal.syncCalls > 0, "expected sync calls under SYNC durability")
        }

    @Test
    fun memoryOnlyDurability_neverSyncs() =
        runTest {
            val wal = FakeWal()
            val engine = ServerStorageEngine("ns", config(Durability.MEMORY_ONLY), wal)
            repeat(10) { engine.writeBlob(byteArrayOf(it.toByte())) }
            assertEquals(0, wal.syncCalls, "MEMORY_ONLY must never sync")

            val hash = engine.writeBlob("check".encodeToByteArray())
            assertEquals("check", engine.readBlob(hash)?.decodeToString())
        }

    // The ASYNC-durability cases (background timer actually firing, stopAsyncSync's final
    // flush) moved to AsyncDurabilityJvmTest in jvmTest: they need real wall-clock delay() on a
    // real Dispatchers.Default thread to observe the background sync loop fire, which runTest's
    // virtual clock can't drive (that loop runs on its own independent CoroutineScope, not
    // runTest's), and runBlocking - the natural fit here - has no JS implementation at all.
}
