package dev.kdb.storage.wal

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import java.io.File
import java.nio.file.Paths
import kotlin.io.path.isDirectory
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * WAL half of the physical-layer conformance suite - docs/kdb-physical-layer-compat-test-plan.md
 * §4.1-§4.3, cases W1-W16.
 *
 * Runs in both directions: it writes Kotlin's frame bytes to
 * `go/testdata/golden/physical/kotlin/` for Go to check, and checks Go's own fixtures under
 * `physical/go/` against what this decoder makes of them. Go's fixtures are only *absent* before
 * the Go exporter has been run, so those cases skip rather than fail - a missing fixture is a
 * setup gap, not a compatibility failure, and failing on it would make the suite unrunnable from
 * a fresh clone of either language alone.
 */
class WalPhysicalGoldenTest {

    // W1: the two runtimes must agree on the frame magic, or neither can read the other's log at
    // all. This constant was 0x4B444257 on the Go side (the pre-timestamp v1 format) while Kotlin
    // had already moved to v2.
    @Test
    fun magicMatchesCrossLanguageContract() {
        assertEquals(0x4B444358, WalCodec.MAGIC, "WAL frame magic (Go: wal.Magic)")
        assertEquals(0x4B444242, WalCodec.BATCH_MAGIC, "WAL batch magic (Go: wal.BatchMagic)")
    }

    // W2/W6: header is sequence(8) + epochMicros(8) + kind(1) + payloadCrc(4) = 21 bytes, framed
    // by magic(4) + bodyLen(4) ahead and crc(4) behind. An empty payload therefore costs 33 bytes.
    @Test
    fun emptyPayloadFrameIsExactlyThirtyThreeBytes() {
        val frame = WalCodec.encodeRecord(WalRecord(1, TIMESTAMP, WalRecordKind.PutBlob, ByteArray(0)))
        assertEquals(33, frame.size)
    }

    // W5: the ordinals are on the wire, so reordering the sealed class would silently reinterpret
    // every existing record.
    @Test
    fun kindOrdinalsAreStable() {
        assertEquals(0, WalCodec.kindOrdinal(WalRecordKind.PutBlob))
        assertEquals(1, WalCodec.kindOrdinal(WalRecordKind.DeleteBlob))
        assertEquals(2, WalCodec.kindOrdinal(WalRecordKind.FlushCheckpoint))
        assertEquals(3, WalCodec.kindOrdinal(WalRecordKind.Marker))
    }

    // W3: export Kotlin's bytes for Go to decode and re-encode.
    @Test
    fun exportWalGolden() {
        writeHex("wal_records.hex", records().fold(ByteArray(0)) { acc, r -> acc + WalCodec.encodeRecord(r) })
        writeHex("wal_put_blob_payload.hex", HASH_7.bytes + PAYLOAD)
    }

    // W3 in the other direction: Go's encoder must produce exactly these frames, and this decoder
    // must recover the original records from them.
    @Test
    fun goWalGoldenDecodesToTheSameRecords() {
        val raw = readGoHex("wal_records.hex") ?: return
        val decoded = WalCodec.decodeRecords(raw, "ns", "seg", skipCorrupt = false)
        assertEquals(0L, decoded.skippedCorrupt)
        val want = records()
        assertEquals(want.size, decoded.records.size)
        for ((i, r) in decoded.records.withIndex()) {
            assertEquals(want[i].sequence, r.sequence, "record $i sequence")
            // W4: the timestamp is the one that was written, not the time of replay.
            assertEquals(want[i].timestamp, r.timestamp, "record $i timestamp")
            assertEquals(want[i].kind, r.kind, "record $i kind")
            assertContentEquals(want[i].payload, r.payload, "record $i payload")
        }
        // And Kotlin re-encodes to the identical bytes - the byte-identity half of the claim.
        val reencoded = decoded.records.fold(ByteArray(0)) { acc, r -> acc + WalCodec.encodeRecord(r) }
        assertContentEquals(raw, reencoded, "Kotlin re-encode of Go's frames must be byte-identical")
    }

    // W7: the PutBlob payload is hash(32) || bytes on both sides, with no length prefix.
    @Test
    fun goPutBlobPayloadLayoutMatches() {
        val raw = readGoHex("wal_put_blob_payload.hex") ?: return
        assertContentEquals(HASH_7.bytes + PAYLOAD, raw)
    }

    // W8: a junk prefix must be resynced past, not treated as the end of the segment. Go used to
    // `break` here and throw away every intact record that followed.
    @Test
    fun corruptPrefixResyncsToTheNextFrame() {
        val good = WalCodec.encodeRecord(WalRecord(7, TIMESTAMP, WalRecordKind.PutBlob, PAYLOAD))
        val stream = ByteArray(12) { 0x5A } + good
        val decoded = WalCodec.decodeRecords(stream, "ns", "seg", skipCorrupt = true)
        assertEquals(1, decoded.records.size, "record after the junk prefix must survive")
        assertEquals(7L, decoded.records[0].sequence)
        assertEquals(1L, decoded.skippedCorrupt, "the junk must be counted, not silently dropped")
    }

    // W9: recordLen below the 21-byte header means the payload length would go negative. Go
    // lacked this bound and panicked with an out-of-range slice on any segment that hit it.
    @Test
    fun recordLengthBelowHeaderIsRejectedWithoutCrashing() {
        val recordLen = 5
        val total = 4 + 4 + recordLen + 4
        val arr = ByteArray(total)
        writeIntBe(arr, 0, WalCodec.MAGIC)
        writeIntBe(arr, 4, recordLen)
        writeIntBe(arr, total - 4, dev.kdb.compression.Crc32.of(arr, 0, total - 4))
        val decoded = WalCodec.decodeRecords(arr, "ns", "seg", skipCorrupt = true)
        assertEquals(0, decoded.records.size)
    }

    // W10: a flipped payload byte must cost exactly one record, and be counted.
    @Test
    fun flippedPayloadByteSkipsExactlyOneRecord() {
        val a = WalCodec.encodeRecord(WalRecord(1, TIMESTAMP, WalRecordKind.PutBlob, PAYLOAD))
        val b = WalCodec.encodeRecord(WalRecord(2, TIMESTAMP, WalRecordKind.PutBlob, PAYLOAD))
        val stream = a + b
        // Flip a payload byte in the first record, then repair the outer frame CRC so the payload
        // CRC - not the frame CRC - is what fails.
        stream[a.size - 6] = (stream[a.size - 6].toInt() xor 0xFF).toByte()
        writeIntBe(stream, a.size - 4, dev.kdb.compression.Crc32.of(stream, 0, a.size - 4))
        val decoded = WalCodec.decodeRecords(stream, "ns", "seg", skipCorrupt = true)
        assertEquals(1, decoded.records.size)
        assertEquals(2L, decoded.records[0].sequence)
        assertEquals(1L, decoded.skippedCorrupt)
    }

    // W12: a truncated final frame is the ordinary shape of an unclean shutdown - it ends the
    // scan, and is not corruption.
    @Test
    fun tornTailEndsTheScanWithoutCountingCorruption() {
        val a = WalCodec.encodeRecord(WalRecord(1, TIMESTAMP, WalRecordKind.PutBlob, PAYLOAD))
        val b = WalCodec.encodeRecord(WalRecord(2, TIMESTAMP, WalRecordKind.PutBlob, PAYLOAD))
        val stream = a + b.copyOfRange(0, b.size - 4)
        val decoded = WalCodec.decodeRecords(stream, "ns", "seg", skipCorrupt = true)
        assertEquals(1, decoded.records.size)
        assertEquals(0L, decoded.skippedCorrupt, "a torn tail is not a corrupt record")
    }

    // W14: with skipCorrupt off, both runtimes raise at the same byte offset.
    @Test
    fun strictModeReportsTheOffsetOfTheBadFrame() {
        val good = WalCodec.encodeRecord(WalRecord(1, TIMESTAMP, WalRecordKind.PutBlob, PAYLOAD))
        val stream = good + ByteArray(12) { 0x5A }
        val e =
            try {
                WalCodec.decodeRecords(stream, "ns", "seg", skipCorrupt = false)
                null
            } catch (e: WalCorruptionException) {
                e
            }
        assertTrue(e != null, "strict decode must raise on bad magic")
        assertEquals(good.size.toLong(), e.offset)
    }

    // W15/W16: the two name builders must agree string-for-string, or one runtime's chain is
    // invisible (or unparseable) to the other.
    @Test
    fun segmentChainNamesMatchGo() {
        assertEquals(
            "ns/p1/wal/00112233-4455-6677-8899-aabbccddeeff",
            DefaultWriteAheadLogFactory().activeSegmentName("p1", WAL_ID),
        )
        assertEquals(
            "ns/p1/wal/00112233-4455-6677-8899-aabbccddeeff.00000000000000000042",
            rotatedSegmentName("p1", WAL_ID, 42),
        )
    }

    // W17: the parser must accept a rotated name Go wrote. Feeding the whole file name to
    // KdbUuid.fromString - what the factory used to do - threw on exactly these.
    @Test
    fun rotatedNamesFromGoParseIntoTheRightChain() {
        val id = WAL_ID.toString()
        val names =
            listOf(
                "ns/p1/wal/$id",
                "ns/p1/wal/$id.00000000000000000042",
                "ns/p1/wal/$id.00000000000000000100",
            )
        val (chain, walId) = latestWalChain(names)
        assertEquals(id, walId)
        assertEquals(listOf(1L, 42L, 100L), chain.map { it.firstSequence })
        assertEquals(names, chain.map { it.name }, "chain must be in sequence order, active last")
    }

    // W19: two walIds in one directory resolve to the newer one's chain only, on both sides.
    @Test
    fun onlyTheNewestWalIdsChainIsOpened() {
        val older = "00000000-0000-0000-0000-000000000000"
        val newer = "ffffffff-ffff-ffff-ffff-ffffffffffff"
        val (chain, walId) =
            latestWalChain(listOf("ns/p1/wal/$older", "ns/p1/wal/$newer", "ns/p1/wal/$newer.00000000000000000009"))
        assertEquals(newer, walId)
        assertEquals(2, chain.size)
    }

    // --- fixtures -------------------------------------------------------------------------

    private companion object {
        val WAL_ID: KdbUuid = KdbUuid.fromString("00112233-4455-6677-8899-aabbccddeeff")
        val TIMESTAMP: KdbTimestamp = KdbTimestamp.fromEpochMicros(1_700_000_000_123_456L)
        val PAYLOAD: ByteArray = byteArrayOf(0, 1, 2, 3, 0x7F, -128, -1, 0x2A, 0x2A, 0x2A)
        val HASH_7: KdbHash = KdbHash.fromBytes(ByteArray(32) { (7 + it).toByte() })

        /**
         * One record per kind, an empty payload, and a pre-epoch timestamp - the last pins sign
         * handling in the 8-byte epochMicros field, where a Kotlin `Long` and a Go `int64` could
         * still disagree if either side reassembled the bytes unsigned.
         */
        fun records(): List<WalRecord> =
            listOf(
                WalRecord(1, TIMESTAMP, WalRecordKind.PutBlob, PAYLOAD),
                WalRecord(2, TIMESTAMP, WalRecordKind.DeleteBlob, HASH_7.bytes),
                WalRecord(3, TIMESTAMP, WalRecordKind.FlushCheckpoint, ByteArray(0)),
                WalRecord(4, KdbTimestamp.fromEpochMicros(-987_654_321L), WalRecordKind.Marker, byteArrayOf(0x4B, 0x44, 0x42, 0x50)),
            )

        fun writeIntBe(arr: ByteArray, off: Int, v: Int) {
            arr[off] = (v ushr 24).toByte()
            arr[off + 1] = (v ushr 16).toByte()
            arr[off + 2] = (v ushr 8).toByte()
            arr[off + 3] = v.toByte()
        }
    }

    private fun goldenDir(side: String): File {
        val root = Paths.get(System.getProperty("user.dir"))
        val repo =
            generateSequence(root) { it.parent }.firstOrNull { it.resolve("go").isDirectory() } ?: root
        return repo.resolve("go/testdata/golden/physical/$side").toFile().also { it.mkdirs() }
    }

    private fun writeHex(name: String, bytes: ByteArray) {
        goldenDir("kotlin").resolve(name).writeText(bytes.joinToString("") { "%02x".format(it) } + "\n")
    }

    /** Null when Go's exporter has not been run - see this class's doc comment. */
    private fun readGoHex(name: String): ByteArray? {
        val f = goldenDir("go").resolve(name)
        if (!f.exists()) return null
        val hex = f.readText().trim()
        return ByteArray(hex.length / 2) { hex.substring(it * 2, it * 2 + 2).toInt(16).toByte() }
    }
}
