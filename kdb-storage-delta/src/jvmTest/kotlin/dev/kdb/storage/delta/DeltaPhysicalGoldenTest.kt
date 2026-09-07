package dev.kdb.storage.delta

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.compression.Crc32
import dev.kdb.compression.ZstdCompression
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbOp
import dev.kdb.storage.CompressionCodec
import dev.kdb.storage.io.SegmentNameBuilder
import dev.kdb.storage.io.SnapshotKeyBuilder
import java.io.File
import java.nio.file.Paths
import kotlin.io.path.isDirectory
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Delta-segment half of the physical-layer conformance suite -
 * docs/kdb-physical-layer-compat-test-plan.md §4.5 (D1-D9), §4.6 (L1, L5) and §4.7 (P1, P4).
 * Go counterpart: go/kdb/interop/delta_physical_golden_test.go.
 */
class DeltaPhysicalGoldenTest {

    @Test
    fun exportDeltaGolden() {
        val commit = fixtureCommit()
        // D8/D9: the commit payload and the hash it determines.
        writeHex("commit_payload.hex", commit.toPayloadBytes())
        writeHex("commit_hash.hex", commit.hash.bytes)
        // D2: CODEC_NONE carries no compressor output, so this frame is byte-comparable.
        writeHex("delta_frame_none.hex", DeltaPageCodec.frame(commit.toPayloadBytes(), CompressionCodec.NONE))
        // P4: shapes Go must be able to consume - an ordinary zstd-jni frame, and an empty one.
        writeHex("zstd_body.hex", ZstdCompression.compress(ZSTD_FIXTURE.encodeToByteArray()))
        writeHex("zstd_empty_body.hex", ZstdCompression.compress(ByteArray(0)))
    }

    /**
     * D8/D9 - the strongest single claim in the suite. The commit hash is the commit's identity
     * across the whole DAG, so if the two encoders disagreed by even one byte, a Go-written
     * commit would be a different commit on the JVM. The fixture deliberately leaves
     * [KdbCommit.schemaHash] null: that is the field where the encoders could most easily
     * diverge, Kotlin supplying an explicit null that the schema default then omits while Go
     * omits the field outright.
     */
    @Test
    fun goCommitPayloadAndHashMatch() {
        val goPayload = readGoHex("commit_payload.hex") ?: return
        val commit = fixtureCommit()
        assertContentEquals(commit.toPayloadBytes(), goPayload, "commit payload bytes")
        readGoHex("commit_hash.hex")?.let { assertContentEquals(commit.hash.bytes, it, "commit hash") }
        // And Go's own bytes must decode here to the same commit, hash included.
        assertEquals(commit.hash, KdbCommit.fromPayloadBytes(goPayload).hash)
    }

    // D2: an uncompressed KDBP frame is byte-comparable end to end.
    @Test
    fun goUncompressedFrameMatches() {
        val goFrame = readGoHex("delta_frame_none.hex") ?: return
        assertContentEquals(DeltaPageCodec.frame(fixtureCommit().toPayloadBytes(), CompressionCodec.NONE), goFrame)
    }

    // D1: the header layout - magic, version, codec id, both lengths, CRC - at fixed offsets.
    @Test
    fun frameHeaderLayout() {
        val payload = "kdbp frame body".encodeToByteArray()
        val frame = DeltaPageCodec.frame(payload, CompressionCodec.NONE)
        assertContentEquals(byteArrayOf(0x4B, 0x44, 0x42, 0x50), frame.copyOfRange(0, 4), "KDBP magic")
        assertEquals(DeltaPageCodec.PAGE_FORMAT_VERSION, frame[4])
        assertEquals(DeltaPageCodec.CODEC_NONE, frame[5])
        assertEquals(0, frame[6].toInt())
        assertEquals(0, frame[7].toInt())
        assertEquals(payload.size, readIntBe(frame, 8), "compressed length")
        assertEquals(payload.size, readIntBe(frame, 12), "uncompressed length")
        assertEquals(Crc32.of(payload), readIntBe(frame, 16), "body crc")
        assertEquals(DeltaPageCodec.FRAME_HEADER_SIZE + payload.size, frame.size)
    }

    // D4: a codec id neither side knows must be an error, never a guess.
    @Test
    fun unknownCodecIsRejected() {
        val frame = DeltaPageCodec.frame("x".encodeToByteArray(), CompressionCodec.NONE)
        frame[5] = 0x7F
        assertFailsWith<IllegalArgumentException> { DeltaPageCodec.parse(frame) }
    }

    // D6: a truncated trailing frame ends the scan cleanly - the ordinary shape of an unclean
    // shutdown, not corruption.
    @Test
    fun tornTailStopsCleanly() {
        val bytes = fixtureSegment()
        val scanned = DeltaSegmentScanner.scanSegmentBytes(bytes.copyOfRange(0, bytes.size - 4))
        assertEquals(1, scanned.size, "the frame before the torn one must still scan")
    }

    // D7: a CRC mismatch on an otherwise complete frame is corruption, reported at its offset,
    // with the commits that scanned cleanly before it still available.
    @Test
    fun crcMismatchReportsOffsetAndPartials() {
        val one = DeltaPageCodec.frame(fixtureCommit().toPayloadBytes(), CompressionCodec.NONE)
        val corrupt = fixtureSegment()
        corrupt[one.size + DeltaPageCodec.FRAME_HEADER_SIZE] =
            (corrupt[one.size + DeltaPageCodec.FRAME_HEADER_SIZE].toInt() xor 0xFF).toByte()
        val e =
            assertFailsWith<DeltaSegmentScanner.CorruptFrameException> {
                DeltaSegmentScanner.scanSegmentBytes(corrupt)
            }
        assertEquals(one.size, e.offset)
        assertEquals(1, e.partialCommits.size, "commits scanned before the damage must survive")
    }

    // D5/L1: the segment path builders must agree with Go's string-for-string, and the parser
    // must reject pre-Layer-13 random-UUID names rather than guessing at their order.
    @Test
    fun segmentNamesMatchGo() {
        assertEquals("ns/ns1/delta/00000000000000000042.seg", SegmentNameBuilder.deltaSequenced("ns1", 42))
        assertEquals("ns/ns1/delta/abc", SegmentNameBuilder.delta("ns1", "abc"))
        assertEquals("ns/ns1/wal/abc", SegmentNameBuilder.wal("ns1", "abc"))
        assertEquals("ns/ns1/sstable/L3/abc", SegmentNameBuilder.sstable("ns1", 3, "abc"))
        assertEquals("ns/ns1/", SegmentNameBuilder.namespacePrefix("ns1"))
        assertEquals("kdb:snap:e1", SnapshotKeyBuilder.enlistment("e1"))
        assertEquals("00000000000000000007.seg", SegmentNameBuilder.deltaSequencedFileName(7))
        assertNull(
            SegmentNameBuilder.parseDeltaSequencedFileName("f81d4fae-7dec-11d0-a765-00a0c91e6bf6.seg"),
            "a legacy random-UUID delta name must not parse as a sequence",
        )
        assertEquals(42L, SegmentNameBuilder.parseDeltaSequencedFileName("00000000000000000042.seg"))
    }

    // P1: CRC-32 must agree bit-for-bit, including the canonical check value.
    @Test
    fun crc32MatchesGo() {
        assertEquals(0, Crc32.of(ByteArray(0)))
        assertEquals(
            0xCBF43926.toInt(),
            Crc32.of("123456789".encodeToByteArray()),
            "the canonical CRC-32/ISO-HDLC check value",
        )
    }

    /**
     * P4 - the case that actually broke. Go's klauspost encoder emits frames without a
     * content-size field, and a *zero-byte* body for a zero-length input where libzstd emits a
     * 9-byte empty frame. Both must decompress here; the empty one used to throw
     * ArrayIndexOutOfBoundsException out of zstd-jni, which surfaced as the JVM crashing while
     * reading an ordinary Go-written SSTable.
     */
    @Test
    fun goZstdBodiesCrossDecode() {
        val body = readGoHex("zstd_body.hex") ?: return
        assertEquals(ZSTD_FIXTURE, ZstdCompression.decompress(body, 1 shl 20).decodeToString())
        readGoHex("zstd_empty_body.hex")?.let {
            assertTrue(ZstdCompression.decompress(it, 1 shl 20).isEmpty(), "an empty body decodes to nothing")
        }
    }

    // --- fixtures -------------------------------------------------------------------------

    private companion object {
        const val NAMESPACE_ID = "fixture-ns"
        const val ZSTD_FIXTURE = "kdb zstd interop fixture payload, long enough to actually compress"
        val TIMESTAMP: KdbTimestamp = KdbTimestamp.fromEpochMicros(1_700_000_000_123_456L)

        fun hash(seed: Int): KdbHash = KdbHash.fromBytes(ByteArray(32) { (seed + it).toByte() })

        fun readIntBe(b: ByteArray, off: Int): Int =
            ((b[off].toInt() and 0xFF) shl 24) or ((b[off + 1].toInt() and 0xFF) shl 16) or
                ((b[off + 2].toInt() and 0xFF) shl 8) or (b[off + 3].toInt() and 0xFF)
    }

    private fun fixtureCommit(): KdbCommit =
        KdbCommit.build(
            parentHashes = listOf(hash(1), hash(2)),
            namespaceId = NAMESPACE_ID,
            transactionId = KdbUuid.fromString("11111111-2222-3333-4444-555555555555"),
            timestamp = TIMESTAMP,
            authorNodeId = KdbUuid.fromString("66666666-7777-8888-9999-aaaaaaaaaaaa"),
            operations = emptyList<KdbOp>(),
            documentTreeHash = hash(3),
            schemaHash = null,
            message = "fixture commit 〰",
        )

    /** Two identical CODEC_NONE frames, so a test can damage the second and still expect the first. */
    private fun fixtureSegment(): ByteArray {
        val frame = DeltaPageCodec.frame(fixtureCommit().toPayloadBytes(), CompressionCodec.NONE)
        return frame + frame
    }

    private fun goldenDir(side: String): File {
        val root = Paths.get(System.getProperty("user.dir"))
        val repo = generateSequence(root) { it.parent }.firstOrNull { it.resolve("go").isDirectory() } ?: root
        return repo.resolve("go/testdata/golden/physical/$side").toFile().also { it.mkdirs() }
    }

    private fun writeHex(name: String, bytes: ByteArray) {
        goldenDir("kotlin").resolve(name).writeText(bytes.joinToString("") { "%02x".format(it) } + "\n")
    }

    /** Null when Go's exporter has not been run - a setup gap, not a compatibility failure. */
    private fun readGoHex(name: String): ByteArray? {
        val f = goldenDir("go").resolve(name)
        if (!f.exists()) return null
        val hex = f.readText().trim()
        return ByteArray(hex.length / 2) { hex.substring(it * 2, it * 2 + 2).toInt(16).toByte() }
    }
}
