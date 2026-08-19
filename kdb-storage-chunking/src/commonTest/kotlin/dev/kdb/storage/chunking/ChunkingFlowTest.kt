package dev.kdb.storage.chunking

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * End-to-end walk through the flow this module exists for:
 *  1. ingest a file
 *  2. ingest a near-identical revision of it (small edit) -> mostly-shared storage
 *  3. ingest an unrelated file -> no wasted attempt at delta, just stored
 *  4. prune the oldest revision from "history" and GC -> unique chunks reclaimed,
 *     shared/still-reachable chunks and blobs survive intact
 */
class ChunkingFlowTest {

    @Test
    fun ingestRevisions_dedupNearDuplicates_thenGcAfterHistoryPrune() {
        val store =
            ChunkedBlobStore(
                chunkThresholdBytes = 64 * 1024,
                chunkerConfig = ChunkerConfig(minSize = 16 * 1024, avgSize = 64 * 1024, maxSize = 256 * 1024),
            )
        val chunkStore = store.chunks() as InMemoryChunkStore

        // 1. Base revision of a "document" file.
        val v1 = randomBytes(4 * 1024 * 1024, seed = 100)
        val hashV1 = store.writeBlob(v1)
        assertContentEquals(v1, store.readBlob(hashV1))
        val bytesStoredAfterV1 = chunkStore.totalBytes

        // 2. A small edit near the middle -> new revision, same fileId in a real ingest.
        val v2 = withInsertion(v1, atOffset = 2 * 1024 * 1024, insertedLength = 500, seed = 101)
        val hashV2 = store.writeBlob(v2)
        assertContentEquals(v2, store.readBlob(hashV2))
        val bytesStoredAfterV2 = chunkStore.totalBytes
        val growthFromV2 = bytesStoredAfterV2 - bytesStoredAfterV1
        assertTrue(
            growthFromV2 < v1.size / 4,
            "near-duplicate revision should cost far less than a full copy; grew by $growthFromV2 bytes",
        )

        // 3. A completely unrelated file -> stored in full, no delta attempted, no penalty either.
        val unrelated = randomBytes(4 * 1024 * 1024, seed = 200)
        val hashUnrelated = store.writeBlob(unrelated)
        assertContentEquals(unrelated, store.readBlob(hashUnrelated))
        assertTrue(
            (store.chunkRefs(hashUnrelated) intersect store.chunkRefs(hashV1)).isEmpty(),
            "unrelated file must not fabricate false chunk sharing",
        )

        // 4. History prunes v1 (superseded by v2); v2 and the unrelated file remain live.
        val gcResult = ChunkGc(store).sweep(reachableContentHashes = setOf(hashV2, hashUnrelated))
        assertTrue(gcResult.manifestsRemoved == 1)
        assertTrue(gcResult.chunksRemoved > 0)

        assertNull(store.readBlob(hashV1), "pruned revision should no longer be readable")
        assertContentEquals(v2, store.readBlob(hashV2), "retained revision must survive GC intact")
        assertContentEquals(unrelated, store.readBlob(hashUnrelated), "unrelated retained blob must survive GC intact")
    }
}
