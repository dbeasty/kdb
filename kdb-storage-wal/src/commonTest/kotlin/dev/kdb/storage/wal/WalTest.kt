package dev.kdb.storage.wal

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.kdbSha256
import dev.kdb.storage.StorageEngineConfig
import dev.kdb.storage.io.SegmentNameBuilder
import dev.kdb.storage.mem.InMemoryPlatformIoShim
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class WalTest {
  @Test
  fun appendAndRecover_roundTrip() = runTest {
        val shim = InMemoryPlatformIoShim()
        val config = StorageEngineConfig(globalMemoryBudgetBytes = 1_000_000, ioShim = shim)
        val wal = DefaultWriteAheadLogFactory().openOrCreate("ns1", config, shim)
        val hash = KdbHash.fromBytes(kdbSha256(byteArrayOf(1, 2, 3)))
        wal.append(
            WalRecord(0, KdbTimestamp.now(), WalRecordKind.PutBlob, WalPutBlob(hash, byteArrayOf(1, 2, 3)).encode()),
        )
        wal.sync()
        var count = 0
        val summary = wal.recover { count++ }
        assertEquals(1, summary.recordsReplayed)
        assertEquals(1, count)
    }

    /**
     * Regression test for the finding recorded in docs/kdb-finish-up-plan.md as 1-K3: replay used
     * to fabricate KdbTimestamp.now() for every record because WalCodec never wrote the timestamp
     * to disk at all. A distinctive, non-"now" timestamp must survive an append -> recover round
     * trip unchanged.
     */
    @Test
    fun timestampSurvivesRecoveryInsteadOfBeingFabricatedAsNow() =
        runTest {
            val shim = InMemoryPlatformIoShim()
            val config = StorageEngineConfig(globalMemoryBudgetBytes = 1_000_000, ioShim = shim)
            val wal = DefaultWriteAheadLogFactory().openOrCreate("ns-ts", config, shim)
            val originalTs = KdbTimestamp.fromEpochMicros(1_700_000_123_456L)
            wal.append(WalRecord(0, originalTs, WalRecordKind.PutBlob, byteArrayOf(1, 2, 3)))
            wal.sync()

            val replayed = mutableListOf<KdbTimestamp>()
            wal.recover { replayed += it.timestamp }

            assertEquals(listOf(originalTs), replayed)
        }

    /**
     * Regression test for 1-K3: truncate() silently did nothing unless the requested sequence
     * covered the *entire* segment - any partial truncateThroughSequence was a no-op, so the WAL
     * never shrank until every record was superseded.
     */
    @Test
    fun truncatePartiallyThroughSequenceDropsOnlyEarlierRecords() =
        runTest {
            val shim = InMemoryPlatformIoShim()
            val config = StorageEngineConfig(globalMemoryBudgetBytes = 1_000_000, ioShim = shim)
            val wal = DefaultWriteAheadLogFactory().openOrCreate("ns-trunc", config, shim)
            val ts = KdbTimestamp.now()
            wal.append(WalRecord(0, ts, WalRecordKind.PutBlob, byteArrayOf(1)))
            wal.append(WalRecord(0, ts, WalRecordKind.PutBlob, byteArrayOf(2)))
            wal.append(WalRecord(0, ts, WalRecordKind.PutBlob, byteArrayOf(3)))
            assertEquals(3L, wal.lastSequence)

            wal.truncate(2)

            val replayedSequences = mutableListOf<Long>()
            val summary = wal.recover { replayedSequences += it.sequence }
            assertEquals(listOf(3L), replayedSequences, "truncate(2) must drop sequences <= 2 and keep the rest")
            assertEquals(1, summary.recordsReplayed)
        }

    private fun WalPutBlob.encode(): ByteArray = contentHash.bytes + bytes
}

/**
 * Regression tests for WalCodec's skipCorrupt handling (1-K3): on a bad magic value, skipCorrupt
 * used to `break` out of the whole decode loop, discarding every record after the first corrupt
 * byte even if the rest of the segment was intact. It must instead resync by scanning forward for
 * the next plausible frame start. Also covers the recovery summary's corruption count, which used
 * to be hardcoded to 0 regardless of how many records were actually skipped.
 */
class WalCodecCorruptionTest {
    @Test
    fun timestampRoundTripsThroughEncodeDecode() {
        val ts = KdbTimestamp.fromEpochMicros(555_555L)
        val record = WalRecord(7, ts, WalRecordKind.PutBlob, byteArrayOf(9, 9, 9))
        val decoded = WalCodec.decodeRecords(WalCodec.encodeRecord(record), "ns", "seg", skipCorrupt = false)
        assertEquals(1, decoded.records.size)
        assertEquals(ts, decoded.records[0].timestamp)
        assertEquals(0L, decoded.skippedCorrupt)
    }

    @Test
    fun badMagicWithoutSkipCorruptThrows() {
        val ts = KdbTimestamp.now()
        val bytes = WalCodec.encodeRecord(WalRecord(1, ts, WalRecordKind.PutBlob, byteArrayOf(1)))
        bytes[0] = (bytes[0].toInt() xor 0xFF).toByte()
        assertTrue(
            runCatching { WalCodec.decodeRecords(bytes, "ns", "seg", skipCorrupt = false) }.isFailure,
            "a corrupted magic must throw when skipCorrupt=false",
        )
    }

    @Test
    fun skipCorruptResyncsPastBadMagicInsteadOfDroppingTheRestOfTheSegment() {
        val ts = KdbTimestamp.fromEpochMicros(1_000_000L)
        val first = WalCodec.encodeRecord(WalRecord(1, ts, WalRecordKind.PutBlob, byteArrayOf(1)))
        val second = WalCodec.encodeRecord(WalRecord(2, ts, WalRecordKind.PutBlob, byteArrayOf(2)))
        first[0] = (first[0].toInt() xor 0xFF).toByte() // corrupt only the first record's magic

        val decoded = WalCodec.decodeRecords(first + second, "ns", "seg", skipCorrupt = true)

        assertEquals(listOf(2L), decoded.records.map { it.sequence }, "the second, intact record must still be recovered")
        assertEquals(1L, decoded.skippedCorrupt)
    }

    @Test
    fun recoverReportsCorruptionCountInsteadOfHardcodedZero() =
        runTest {
            val shim = InMemoryPlatformIoShim()
            val walId = KdbUuid.random()
            val segmentName = SegmentNameBuilder.wal("ns-corrupt", walId.toString())
            val ts = KdbTimestamp.fromEpochMicros(42L)
            val good1 = WalCodec.encodeRecord(WalRecord(1, ts, WalRecordKind.PutBlob, byteArrayOf(1)))
            val bad = WalCodec.encodeRecord(WalRecord(2, ts, WalRecordKind.PutBlob, byteArrayOf(2)))
            bad[0] = (bad[0].toInt() xor 0xFF).toByte()
            val good2 = WalCodec.encodeRecord(WalRecord(3, ts, WalRecordKind.PutBlob, byteArrayOf(3)))
            shim.appendToSegment(segmentName, good1 + bad + good2)

            val config = StorageEngineConfig(globalMemoryBudgetBytes = 1_000_000, ioShim = shim)
            val wal = DefaultWriteAheadLogFactory(skipCorrupt = true).openOrCreate("ns-corrupt", config, shim)
            val replayedSequences = mutableListOf<Long>()
            val summary = wal.recover { replayedSequences += it.sequence }

            assertEquals(listOf(1L, 3L), replayedSequences)
            assertEquals(1L, summary.recordsSkippedCorrupt, "the one corrupt record must be counted, not silently reported as zero")
        }
}
