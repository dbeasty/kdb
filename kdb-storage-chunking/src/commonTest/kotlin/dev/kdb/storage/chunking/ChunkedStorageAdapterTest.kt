package dev.kdb.storage.chunking

import dev.kdb.codec.KdbUuid
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbDocument
import dev.kdb.storage.mem.InMemoryStorageAdapter
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.test.runTest

/**
 * Proves the decorator actually plugs chunk-level dedup into the real [dev.kdb.storage.StorageAdapter]
 * contract: blob calls go through [ChunkedBlobStore], everything else (documents, trees) is untouched
 * pass-through to the wrapped adapter — so any caller that only knows about [dev.kdb.storage.StorageAdapter]
 * gets dedup for free.
 */
class ChunkedStorageAdapterTest {

    @Test
    fun writeReadBlob_routesThroughChunking_dedupsNearDuplicates() =
        runTest {
            val adapter =
                ChunkedStorageAdapter(
                    InMemoryStorageAdapter(),
                    ChunkedBlobStore(
                        chunkThresholdBytes = 64 * 1024,
                        chunkerConfig = ChunkerConfig(minSize = 16 * 1024, avgSize = 64 * 1024, maxSize = 256 * 1024),
                    ),
                )
            val v1 = randomBytes(2 * 1024 * 1024, seed = 50)
            val v2 = withInsertion(v1, atOffset = 1024 * 1024, insertedLength = 250, seed = 51)

            val hashV1 = adapter.writeBlob(v1)
            val bytesAfterV1 = (adapter.blobStore.chunks() as InMemoryChunkStore).totalBytes
            val hashV2 = adapter.writeBlob(v2)
            val bytesAfterV2 = (adapter.blobStore.chunks() as InMemoryChunkStore).totalBytes

            assertContentEquals(v1, adapter.readBlob(hashV1))
            assertContentEquals(v2, adapter.readBlob(hashV2))
            assertTrue(
                bytesAfterV2 - bytesAfterV1 < v1.size / 4,
                "near-duplicate blob should mostly reuse chunks through the adapter",
            )
        }

    @Test
    fun documentAndTreeCalls_passThroughUnchanged() =
        runTest {
            val namespaceId = "app/docs"
            val adapter = ChunkedStorageAdapter(InMemoryStorageAdapter())
            val docId = KdbUuid.random()
            val doc = KdbDocument.fromJson(docId, """{"hello":"world"}""")

            adapter.putDocument(namespaceId, doc)
            val tree = adapter.commitTree(namespaceId, DocumentTree.EMPTY.treeHash)

            val back = adapter.getDocument(namespaceId, docId, tree.treeHash)
            assertEquals(doc.json, back?.json)
        }

    @Test
    fun chunkAwareGc_sweepsUnreachableRevisionThroughOrphanBlobGcInterface() =
        runTest {
            val adapter = ChunkedStorageAdapter(InMemoryStorageAdapter())
            val gc: dev.kdb.compaction.OrphanBlobGc = ChunkAwareOrphanBlobGc(adapter)

            val v1 = randomBytes(1024 * 1024, seed = 60)
            val v2 = withInsertion(v1, atOffset = 512 * 1024, insertedLength = 400, seed = 61)
            val hashV1 = adapter.writeBlob(v1)
            val hashV2 = adapter.writeBlob(v2)

            val bytesReclaimed = gc.sweep("app/files", reachableHashes = setOf(hashV2))

            assertTrue(bytesReclaimed > 0)
            assertNull(adapter.readBlob(hashV1))
            assertContentEquals(v2, adapter.readBlob(hashV2))
        }
}
