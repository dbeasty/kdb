package dev.kdb.storage.engine

import dev.kdb.codec.KdbUuid
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbDocument
import dev.kdb.storage.StorageEngineConfig
import dev.kdb.storage.mem.InMemoryPlatformIoShim
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

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
        val ns = "test"
        val shim = InMemoryPlatformIoShim()
        val config = StorageEngineConfig(globalMemoryBudgetBytes = 8_000_000, ioShim = shim)
        val handle = DefaultStorageEngineFactory(StorageEngineTarget.IN_MEMORY).open(ns, config)
        val doc = KdbDocument(KdbUuid.random(), """{"v":"a"}""")

        handle.adapter.putDocument(ns, doc)
        assertNull(handle.adapter.getDocument(ns, doc.id, DocumentTree.EMPTY.treeHash))

        handle.adapter.commitTree(ns, DocumentTree.EMPTY.treeHash)
        assertEquals(doc.json, handle.adapter.getDocument(ns, doc.id, DocumentTree.EMPTY.treeHash)?.json)
    }

    @Test
    fun discardPending_dropsStagedWritesWithoutAffectingCommittedState() = runTest {
        val ns = "test"
        val shim = InMemoryPlatformIoShim()
        val config = StorageEngineConfig(globalMemoryBudgetBytes = 8_000_000, ioShim = shim)
        val handle = DefaultStorageEngineFactory(StorageEngineTarget.IN_MEMORY).open(ns, config)
        val committed = KdbDocument(KdbUuid.random(), """{"v":"committed"}""")
        val abandoned = KdbDocument(KdbUuid.random(), """{"v":"abandoned"}""")

        handle.adapter.putDocument(ns, committed)
        handle.adapter.commitTree(ns, DocumentTree.EMPTY.treeHash)

        handle.adapter.putDocument(ns, abandoned)
        handle.adapter.discardPending(ns)

        assertEquals(committed.json, handle.adapter.getDocument(ns, committed.id, DocumentTree.EMPTY.treeHash)?.json)
        assertNull(handle.adapter.getDocument(ns, abandoned.id, DocumentTree.EMPTY.treeHash))

        // A subsequent commit produces a tree containing only the previously-committed doc.
        val tree = handle.adapter.commitTree(ns, DocumentTree.EMPTY.treeHash)
        assertEquals(setOf(committed.id), tree.entries.keys)
    }
}
