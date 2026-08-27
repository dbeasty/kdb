package dev.kdb.storage.wal

import dev.kdb.codec.KdbTimestamp
import dev.kdb.compression.Crc32

/** Result of [WalCodec.decodeRecords]: the successfully decoded records plus how many were skipped as corrupt. */
internal data class WalDecodeResult(val records: List<WalRecord>, val skippedCorrupt: Long)

internal object WalCodec {
    // Bumped from 0x4B444257 when the record format grew an 8-byte timestamp field (previously
    // unwritten - replay fabricated KdbTimestamp.now() for every record). A stream encoded under
    // the old format now fails the magic check on its first record instead of being silently
    // misparsed with the wrong field layout.
    const val MAGIC: Int = 0x4B444358
    const val BATCH_MAGIC: Int = 0x4B444242

    fun kindOrdinal(kind: WalRecordKind): Int =
        when (kind) {
            WalRecordKind.PutBlob -> 0
            WalRecordKind.DeleteBlob -> 1
            WalRecordKind.FlushCheckpoint -> 2
            WalRecordKind.Marker -> 3
        }

    fun kindFromOrdinal(o: Int): WalRecordKind =
        when (o) {
            0 -> WalRecordKind.PutBlob
            1 -> WalRecordKind.DeleteBlob
            2 -> WalRecordKind.FlushCheckpoint
            else -> WalRecordKind.Marker
        }

    // header = sequence(8) + epochMicros(8) + kind(1) + payloadCrc(4)
    private const val HEADER_LEN = 21

    fun encodeRecord(record: WalRecord): ByteArray {
        val payload = record.payload
        val bodyLen = HEADER_LEN + payload.size
        val total = 4 + 4 + bodyLen + 4
        val arr = ByteArray(total)
        writeInt(arr, 0, MAGIC)
        writeInt(arr, 4, bodyLen)
        writeLong(arr, 8, record.sequence)
        writeLong(arr, 16, record.timestamp.toEpochMicros())
        arr[24] = kindOrdinal(record.kind).toByte()
        writeInt(arr, 25, Crc32.of(payload))
        payload.copyInto(arr, destinationOffset = 29)
        writeInt(arr, total - 4, Crc32.of(arr, 0, total - 4))
        return arr
    }

    fun decodeRecords(
        bytes: ByteArray,
        partitionKey: String,
        segmentName: String,
        skipCorrupt: Boolean,
    ): WalDecodeResult {
        val out = mutableListOf<WalRecord>()
        var skipped = 0L
        var offset = 0
        while (offset + 12 <= bytes.size) {
            val magic = readInt(bytes, offset)
            if (magic == BATCH_MAGIC) break
            if (magic != MAGIC) {
                if (skipCorrupt) {
                    // Resync: this record's length is unknown (its header is exactly what's
                    // corrupt), so scan forward byte-by-byte for the next plausible frame start
                    // instead of abandoning the rest of the segment.
                    var scan = offset + 1
                    while (scan + 4 <= bytes.size) {
                        val candidate = readInt(bytes, scan)
                        if (candidate == MAGIC || candidate == BATCH_MAGIC) break
                        scan++
                    }
                    skipped++
                    offset = scan
                    continue
                }
                throw WalCorruptionException("bad magic", partitionKey, segmentName, offset.toLong())
            }
            val recordLen = readInt(bytes, offset + 4)
            val recordEnd = offset + 12 + recordLen
            if (recordEnd > bytes.size || recordLen < HEADER_LEN) break
            val headerCrc = readInt(bytes, recordEnd - 4)
            if (headerCrc != Crc32.of(bytes, offset, recordEnd - offset - 4)) {
                if (skipCorrupt) {
                    skipped++
                    offset = recordEnd
                    continue
                }
                throw WalCorruptionException("header crc", partitionKey, segmentName, offset.toLong())
            }
            val seq = readLong(bytes, offset + 8)
            val epochMicros = readLong(bytes, offset + 16)
            val kind = kindFromOrdinal(bytes[offset + 24].toInt())
            val payloadCrc = readInt(bytes, offset + 25)
            val payloadOff = offset + 29
            val payloadLen = recordLen - HEADER_LEN
            val payload = bytes.copyOfRange(payloadOff, payloadOff + payloadLen)
            if (Crc32.of(payload) != payloadCrc) {
                if (skipCorrupt) {
                    skipped++
                    offset = recordEnd
                    continue
                }
                throw WalCorruptionException("payload crc", partitionKey, segmentName, offset.toLong())
            }
            out.add(WalRecord(seq, KdbTimestamp.fromEpochMicros(epochMicros), kind, payload))
            offset = recordEnd
        }
        return WalDecodeResult(out, skipped)
    }

    private fun writeInt(arr: ByteArray, off: Int, v: Int) {
        arr[off] = (v ushr 24).toByte()
        arr[off + 1] = (v ushr 16).toByte()
        arr[off + 2] = (v ushr 8).toByte()
        arr[off + 3] = v.toByte()
    }

    private fun writeLong(arr: ByteArray, off: Int, v: Long) {
        for (i in 0 until 8) arr[off + i] = (v ushr (56 - i * 8)).toByte()
    }

    private fun readInt(b: ByteArray, off: Int): Int =
        ((b[off].toInt() and 0xFF) shl 24) or
            ((b[off + 1].toInt() and 0xFF) shl 16) or
            ((b[off + 2].toInt() and 0xFF) shl 8) or
            (b[off + 3].toInt() and 0xFF)

    private fun readLong(b: ByteArray, off: Int): Long {
        var v = 0L
        for (i in 0 until 8) v = (v shl 8) or (b[off + i].toLong() and 0xFF)
        return v
    }
}
