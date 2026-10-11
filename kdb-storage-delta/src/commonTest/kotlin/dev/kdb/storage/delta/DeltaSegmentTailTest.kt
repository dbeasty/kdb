package dev.kdb.storage.delta

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbOp
import dev.kdb.storage.CompressionCodec
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * N5: how a segment's frames end, by the rule Go applies too (go/kdb/storage/delta/tail.go). A torn
 * tail is bytes at the very end of a segment with nothing intact after them; damage that an intact
 * frame follows is not one, wherever it is; zero bytes after the last frame are the end.
 */
class DeltaSegmentTailTest {
    private fun chain(n: Int): List<KdbCommit> {
        val out = mutableListOf<KdbCommit>()
        var parent: KdbHash? = null
        repeat(n) { i ->
            val c =
                KdbCommit.build(
                    parentHashes = listOfNotNull(parent),
                    namespaceId = "ns1",
                    transactionId = KdbUuid.random(),
                    timestamp = KdbTimestamp.now(),
                    authorNodeId = KdbUuid.random(),
                    operations = listOf(KdbOp.Write(KdbUuid.random(), """{"n":$i}""")),
                    documentTreeHash = KdbHash.fromHex("cc".repeat(32)),
                    schemaHash = null,
                )
            out += c
            parent = c.hash
        }
        return out
    }

    private fun frames(commits: List<KdbCommit>): List<ByteArray> = commits.map { DeltaPageCodec.frame(it.toPayloadBytes(), CompressionCodec.NONE) }

    private fun offsets(frames: List<ByteArray>): List<Int> = frames.runningFold(0) { acc, f -> acc + f.size }.dropLast(1)

    private fun concat(frames: List<ByteArray>): ByteArray = frames.fold(ByteArray(0)) { acc, f -> acc + f }

    private fun flip(
        b: ByteArray,
        at: Int,
        mask: Int = 0xFF,
    ) {
        b[at] = (b[at].toInt() xor mask).toByte()
    }

    @Test
    fun aGarbledHeaderWithIntactFramesAfterItIsDamageNotATornTail() {
        val fs = frames(chain(4))
        val off = offsets(fs)
        val bytes = concat(fs)
        flip(bytes, off[1])
        val e = assertFailsWith<DeltaSegmentScanner.CorruptFrameException> { DeltaSegmentScanner.scanSegmentBytes(bytes) }
        assertEquals(off[1], e.offset)
        assertEquals(2, e.intactFramesAfter)
        assertEquals(off[2], e.nextIntactFrame)
        assertFalse(e.torn)
        assertEquals(1, e.partialCommits.size)
        val tail = DeltaSegmentScanner.scanSegment(bytes).tail
        assertEquals(DeltaSegmentScanner.TailKind.DAMAGED, tail.kind)
        assertEquals(off[1], tail.offset)
    }

    @Test
    fun aCrcMismatchFollowedByAGarbledHeaderStillCountsWhatComesAfter() {
        val fs = frames(chain(5))
        val off = offsets(fs)
        val bytes = concat(fs)
        flip(bytes, off[1] + DeltaPageCodec.FRAME_HEADER_SIZE + 2, 0x01)
        flip(bytes, off[2])
        val e = assertFailsWith<DeltaSegmentScanner.CorruptFrameException> { DeltaSegmentScanner.scanSegmentBytes(bytes) }
        assertEquals(off[1], e.offset)
        assertEquals(2, e.intactFramesAfter)
        assertEquals(off[3], e.nextIntactFrame)
        assertEquals(listOf(off[3], off[4]), DeltaSegmentScanner.intactCommitsAfter(bytes, off[1]).map { it.frameOffset })
    }

    @Test
    fun tornTailsAndPaddingEndTheSegment() {
        val fs = frames(chain(3))
        val off = offsets(fs)
        val whole = concat(fs)

        // Cut short, as a kill during the append leaves it.
        assertEquals(2, DeltaSegmentScanner.scanSegmentBytes(whole.copyOfRange(0, whole.size - 5)).size)
        assertEquals(DeltaSegmentScanner.TailKind.TORN, DeltaSegmentScanner.scanSegment(whole.copyOfRange(0, whole.size - 5)).tail.kind)

        // Zero padding after the last frame: a preallocated segment's unused space.
        val padded = whole + ByteArray(4096)
        assertEquals(3, DeltaSegmentScanner.scanSegmentBytes(padded).size)
        val clean = DeltaSegmentScanner.scanSegment(padded).tail
        assertEquals(DeltaSegmentScanner.TailKind.CLEAN, clean.kind)
        assertEquals(whole.size, clean.offset)

        // Only the magic of the last frame written, the rest still zero: a torn header, not a
        // complete frame that will not decode.
        val headerOnly = whole.copyOf()
        for (i in off[2] + 4 until headerOnly.size) headerOnly[i] = 0
        assertEquals(2, DeltaSegmentScanner.scanSegmentBytes(headerOnly + ByteArray(4096)).size)
        assertEquals(DeltaSegmentScanner.TailKind.TORN, DeltaSegmentScanner.scanSegment(headerOnly).tail.kind)

        // The second half of the last frame's body never written, then padding.
        val half = whole.copyOf()
        for (i in (off[2] + whole.size) / 2 until half.size) half[i] = 0
        val e = assertFailsWith<DeltaSegmentScanner.CorruptFrameException> { DeltaSegmentScanner.scanSegmentBytes(half + ByteArray(4096)) }
        assertTrue(e.torn, "a frame whose body never fully landed, with nothing after it, is a torn tail")
    }

    @Test
    fun anUndecodableFrameIsNeverATornTail() {
        val fs = frames(chain(2))
        val off = offsets(fs)
        val bytes = concat(fs)
        bytes[off[1] + 5] = 0x7F // the codec byte, outside the CRC
        val e = assertFailsWith<DeltaSegmentScanner.CorruptFrameException> { DeltaSegmentScanner.scanSegmentBytes(bytes) }
        assertTrue(e.undecodable)
        assertFalse(e.torn)
    }
}
