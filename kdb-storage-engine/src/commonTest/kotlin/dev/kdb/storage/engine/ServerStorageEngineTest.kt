package dev.kdb.storage.engine

import dev.kdb.storage.StorageEngineConfig
import dev.kdb.storage.mem.InMemoryPlatformIoShim
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertNotNull

class ServerStorageEngineTest {
    @Test
    fun writeBlob_roundTrip() = runTest {
        val shim = InMemoryPlatformIoShim()
        val config = StorageEngineConfig(globalMemoryBudgetBytes = 8_000_000, ioShim = shim)
        val handle = DefaultStorageEngineFactory(StorageEngineTarget.IN_MEMORY).open("test", config)
        val bytes = byteArrayOf(9, 8, 7)
        val hash = handle.adapter.writeBlob(bytes)
        assertContentEquals(bytes, handle.adapter.readBlob(hash))
        assertNotNull(hash)
    }
}
