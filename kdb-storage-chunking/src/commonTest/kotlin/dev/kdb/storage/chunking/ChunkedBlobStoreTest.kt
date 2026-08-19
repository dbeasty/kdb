package dev.kdb.storage.chunking

import dev.kdb.codec.KdbHash
import dev.kdb.document.kdbSha256
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ChunkedBlobStoreTest {

    private fun newStore(chunkThresholdBytes: Int = 64 * 1024): ChunkedBlobStore =
        ChunkedBlobStore(
            chunkThresholdBytes = chunkThresholdBytes,
            chunkerConfig = ChunkerConfig(minSize = 4 * 1024, avgSize = 16 * 1024, maxSize = 64 * 1024),
        )

    @Test
    fun roundTrip_smallBlob_storedRaw_notChunked() {
        val store = newStore()
        val bytes = randomBytes(1024, seed = 10)
        val hash = store.writeBlob(bytes)

        assertFalse(store.isChunked(hash))
        assertContentEquals(bytes, store.readBlob(hash))
    }

    @Test
    fun roundTrip_largeBlob_isChunkedAndReassembles() {
        val store = newStore()
        val bytes = randomBytes(1024 * 1024, seed = 11)
        val hash = store.writeBlob(bytes)

        assertTrue(store.isChunked(hash))
        assertContentEquals(bytes, store.readBlob(hash))
        assertTrue(store.chunkRefs(hash).isNotEmpty())
    }

    @Test
    fun readBlob_unknownHash_returnsNull() {
        val store = newStore()
        val bogus = KdbHash.fromBytes(kdbSha256("nope".encodeToByteArray()))
        assertEquals(null, store.readBlob(bogus))
    }

    @Test
    fun writingSameBlobTwice_isIdempotent() {
        val store = newStore()
        val bytes = randomBytes(512 * 1024, seed = 12)
        val h1 = store.writeBlob(bytes)
        val chunkCountAfterFirst = (store.chunks() as InMemoryChunkStore).chunkCount
        val h2 = store.writeBlob(bytes)
        val chunkCountAfterSecond = (store.chunks() as InMemoryChunkStore).chunkCount

        assertEquals(h1, h2)
        assertEquals(chunkCountAfterFirst, chunkCountAfterSecond)
    }

    @Test
    fun dedup_nearIdenticalFiles_shareMostChunks() {
        val store = newStore()
        val v1 = randomBytes(2 * 1024 * 1024, seed = 13)
        val v2 = withInsertion(v1, atOffset = 1024 * 1024, insertedLength = 200, seed = 14)

        store.writeBlob(v1)
        val chunkStore = store.chunks() as InMemoryChunkStore
        val bytesAfterV1 = chunkStore.totalBytes
        val chunksAfterV1 = chunkStore.chunkCount

        store.writeBlob(v2)
        val bytesAfterV2 = chunkStore.totalBytes
        val chunksAfterV2 = chunkStore.chunkCount

        val newChunksFromV2 = chunksAfterV2 - chunksAfterV1
        val newBytesFromV2 = bytesAfterV2 - bytesAfterV1

        // A ~200-byte edit in a 2MB file should add a handful of new chunks, not a second
        // copy of the file.
        assertTrue(
            newBytesFromV2 < v1.size / 4,
            "expected mostly-shared storage for a near-identical file, but v2 added $newBytesFromV2 bytes " +
                "(v1 was ${v1.size} bytes) across $newChunksFromV2 new chunks",
        )
    }

    @Test
    fun dedup_unrelatedFiles_shareNoChunks_butStillRoundTrip() {
        val store = newStore()
        val a = randomBytes(512 * 1024, seed = 20)
        val b = randomBytes(512 * 1024, seed = 21)

        val hashA = store.writeBlob(a)
        val hashB = store.writeBlob(b)

        val refsA = store.chunkRefs(hashA)
        val refsB = store.chunkRefs(hashB)
        assertTrue((refsA intersect refsB).isEmpty(), "unrelated random buffers should not share chunks")

        assertContentEquals(a, store.readBlob(hashA))
        assertContentEquals(b, store.readBlob(hashB))
    }
}
