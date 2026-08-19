package dev.kdb.storage.chunking

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ChunkGcTest {

    private fun newStore(): ChunkedBlobStore =
        ChunkedBlobStore(
            chunkThresholdBytes = 64 * 1024,
            chunkerConfig = ChunkerConfig(minSize = 4 * 1024, avgSize = 16 * 1024, maxSize = 64 * 1024),
        )

    @Test
    fun sweep_dropsUnreferencedManifestAndItsUniqueChunks_keepsSharedChunks() {
        val store = newStore()
        val v1 = randomBytes(1024 * 1024, seed = 30)
        val v2 = withInsertion(v1, atOffset = 512 * 1024, insertedLength = 300, seed = 31)

        val hashV1 = store.writeBlob(v1)
        val hashV2 = store.writeBlob(v2)
        val sharedChunks = store.chunkRefs(hashV1) intersect store.chunkRefs(hashV2)
        assertTrue(sharedChunks.isNotEmpty(), "v1 and v2 should share chunks for this test to be meaningful")

        // Simulate v1's commit being pruned from the DAG: only v2 remains reachable.
        val result = ChunkGc(store).sweep(reachableContentHashes = setOf(hashV2))

        assertEquals(1, result.manifestsRemoved)
        assertTrue(result.chunksRemoved > 0, "chunks unique to v1 should be swept")
        assertTrue(result.bytesReclaimed > 0, "bytes-reclaimed estimate should be positive")

        assertNull(store.readBlob(hashV1))
        assertContentEquals(v2, store.readBlob(hashV2))

        // Shared chunks must have survived because v2's manifest still references them.
        for (chunk in sharedChunks) {
            assertTrue(store.chunks().has(chunk), "shared chunk $chunk should survive while v2 is reachable")
        }
    }

    @Test
    fun sweep_reachableEverything_removesNothing() {
        val store = newStore()
        val a = randomBytes(300 * 1024, seed = 40)
        val hashA = store.writeBlob(a)

        val result = ChunkGc(store).sweep(reachableContentHashes = setOf(hashA))

        assertEquals(0, result.manifestsRemoved)
        assertEquals(0, result.chunksRemoved)
        assertEquals(0L, result.bytesReclaimed)
        assertContentEquals(a, store.readBlob(hashA))
    }

    @Test
    fun sweep_nothingReachable_removesEverything() {
        val store = newStore()
        val a = randomBytes(300 * 1024, seed = 41)
        store.writeBlob(a)

        val result = ChunkGc(store).sweep(reachableContentHashes = emptySet())

        assertEquals(1, result.manifestsRemoved)
        assertTrue(result.chunksRemoved > 0)
        assertEquals(0, (store.chunks() as InMemoryChunkStore).chunkCount)
    }
}
