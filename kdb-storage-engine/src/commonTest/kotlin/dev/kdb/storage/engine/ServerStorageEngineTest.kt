package dev.kdb.storage.engine

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbDocument
import dev.kdb.storage.StorageEngineConfig
import dev.kdb.storage.mem.InMemoryPlatformIoShim
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertEquals

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

    @Test
    fun putDocument_notVisibleUntilCommitTree() = runTest {
        val shim = InMemoryPlatformIoShim()
        val config = StorageEngineConfig(globalMemoryBudgetBytes = 8_000_000, ioShim = shim)
        val handle = DefaultStorageEngineFactory(StorageEngineTarget.IN_MEMORY).open("test", config)
        val adapter = handle.adapter
        val doc = KdbDocument(KdbUuid.random(), """{"v":1}""")

        adapter.putDocument("test", doc)
        assertNull(adapter.getDocument("test", doc.id, KdbHash.fromBytes(ByteArray(32))))

        val tree = adapter.commitTree("test", KdbHash.fromBytes(ByteArray(32)))
        assertEquals(doc, adapter.getDocument("test", doc.id, KdbHash.fromBytes(ByteArray(32))))
        assertEquals(doc.contentHash, tree.hashFor(doc.id))
    }

    @Test
    fun discardPending_rollsBackStagedWrites() = runTest {
        val shim = InMemoryPlatformIoShim()
        val config = StorageEngineConfig(globalMemoryBudgetBytes = 8_000_000, ioShim = shim)
        val handle = DefaultStorageEngineFactory(StorageEngineTarget.IN_MEMORY).open("test", config)
        val adapter = handle.adapter
        val committed = KdbDocument(KdbUuid.random(), """{"v":"committed"}""")
        adapter.putDocument("test", committed)
        adapter.commitTree("test", KdbHash.fromBytes(ByteArray(32)))

        val staged = KdbDocument(KdbUuid.random(), """{"v":"staged"}""")
        adapter.putDocument("test", staged)
        adapter.deleteDocument("test", committed.id)
        adapter.discardPending("test")

        assertNull(adapter.getDocument("test", staged.id, KdbHash.fromBytes(ByteArray(32))))
        assertEquals(committed, adapter.getDocument("test", committed.id, KdbHash.fromBytes(ByteArray(32))))

        val tree = adapter.commitTree("test", KdbHash.fromBytes(ByteArray(32)))
        assertNull(tree.hashFor(staged.id))
        assertEquals(committed.contentHash, tree.hashFor(committed.id))
    }
}
