package dev.kdb.storage.chunking

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ContentDefinedChunkerTest {

    private val config = ChunkerConfig(minSize = 256, avgSize = 1024, maxSize = 4096)

    @Test
    fun chunk_isDeterministic() {
        val data = randomBytes(64 * 1024, seed = 1)
        val a = ContentDefinedChunker.chunk(data, config)
        val b = ContentDefinedChunker.chunk(data, config)
        assertEquals(a, b)
    }

    @Test
    fun chunk_slicesCoverWholeBufferContiguously() {
        val data = randomBytes(64 * 1024, seed = 2)
        val slices = ContentDefinedChunker.chunk(data, config)
        var expectedOffset = 0
        for (slice in slices) {
            assertEquals(expectedOffset, slice.offset)
            expectedOffset += slice.length
        }
        assertEquals(data.size, expectedOffset)
    }

    @Test
    fun chunk_neverExceedsMaxSize() {
        val data = randomBytes(64 * 1024, seed = 3)
        val slices = ContentDefinedChunker.chunk(data, config)
        for (slice in slices) {
            assertTrue(slice.length <= config.maxSize, "slice length ${slice.length} exceeds maxSize")
        }
    }

    @Test
    fun chunk_emptyInput_producesNoSlices() {
        assertEquals(emptyList(), ContentDefinedChunker.chunk(ByteArray(0), config))
    }

    @Test
    fun chunk_smallInputBelowMinSize_isOneSlice() {
        val data = randomBytes(10, seed = 4)
        val slices = ContentDefinedChunker.chunk(data, config)
        assertEquals(1, slices.size)
        assertEquals(0, slices[0].offset)
        assertEquals(10, slices[0].length)
    }

    /**
     * A small edit in the middle of a large buffer should only disturb chunk boundaries
     * near the edit; most of the buffer, before and after, should re-chunk identically.
     * This is the property that makes chunk-level dedup work without any explicit diff.
     */
    @Test
    fun chunk_localEdit_leavesMostChunkBoundariesUnchanged() {
        val original = randomBytes(1024 * 1024, seed = 5)
        val edited = withInsertion(original, atOffset = 512 * 1024, insertedLength = 137, seed = 6)

        val originalHashes = chunkContentHashes(original)
        val editedHashes = chunkContentHashes(edited)

        val shared = originalHashes.intersect(editedHashes)
        val sharedFraction = shared.size.toDouble() / originalHashes.size

        assertTrue(
            sharedFraction > 0.8,
            "expected >80% of chunks to survive a small local edit, got ${sharedFraction * 100}%",
        )
    }

    private fun chunkContentHashes(data: ByteArray): Set<String> =
        ContentDefinedChunker.chunk(data, config)
            .map { data.copyOfRange(it.offset, it.offset + it.length).contentHashCode().toString() }
            .toSet()
}
