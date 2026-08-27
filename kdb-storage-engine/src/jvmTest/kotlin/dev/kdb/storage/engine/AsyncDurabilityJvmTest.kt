package dev.kdb.storage.engine

import dev.kdb.storage.Durability
import dev.kdb.storage.StorageEngineConfig
import dev.kdb.storage.mem.InMemoryPlatformIoShim
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * ASYNC-durability cases split out of commonTest's DurabilityTest (which covers SYNC and
 * MEMORY_ONLY - both instant, portable, and runTest-compatible). These two need real wall-clock
 * delay() on a real Dispatchers.Default thread to observe ServerStorageEngine's background sync
 * loop actually fire - runTest's virtual clock only advances coroutines under its own scope, not
 * an independent CoroutineScope like the one that loop runs on - and runBlocking (the natural
 * tool for that) has no JS implementation, so this file can only live in jvmTest.
 */
class AsyncDurabilityJvmTest {
    private fun config(durability: Durability, asyncIntervalMs: Long? = null) =
        StorageEngineConfig(
            globalMemoryBudgetBytes = 1_000_000,
            ioShim = InMemoryPlatformIoShim(),
            durability = durability,
            asyncSyncIntervalMillis = asyncIntervalMs,
        )

    @Test
    fun asyncDurability_syncsOnTimerNotPerWrite() =
        runBlocking {
            val wal = FakeWal()
            val engine = ServerStorageEngine("ns", config(Durability.ASYNC, asyncIntervalMs = 20), wal)
            repeat(50) { engine.writeBlob(byteArrayOf(it.toByte())) }
            assertEquals(0, wal.syncCalls, "async writes must not sync inline")

            delay(80)
            assertTrue(wal.syncCalls > 0, "expected at least one background sync after waiting")
            assertTrue(wal.syncCalls < 50, "expected batching, not one sync per write")
            engine.stopAsyncSync()
        }

    @Test
    fun stopAsyncSync_flushesOnShutdown() =
        runBlocking {
            val wal = FakeWal()
            val engine =
                ServerStorageEngine(
                    "ns",
                    config(Durability.ASYNC, asyncIntervalMs = 60_000), // effectively never fires on its own
                    wal,
                )
            engine.writeBlob("x".encodeToByteArray())
            assertEquals(0, wal.syncCalls)
            engine.stopAsyncSync()
            assertTrue(wal.syncCalls > 0, "expected a final flush on stopAsyncSync")
        }
}
