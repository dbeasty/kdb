package dev.kdb.storage.wal

import dev.kdb.codec.KdbTimestamp
import dev.kdb.compression.Crc32

internal object WalCodec {
    const val MAGIC: Int = 0x4B444257
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

    fun encodeRecord(record: WalRecord): ByteArray {
        val payload = record.payload
        val bodyLen = 8 + 1 + 4 + payload.size
        val total = 4 + 4 + bodyLen + 4
        val arr = ByteArray(total)
        writeInt(arr, 0, MAGIC)
        writeInt(arr, 4, bodyLen)
        writeLong(arr, 8, record.sequence)
        arr[16] = kindOrdinal(record.kind).toByte()
        writeInt(arr, 17, Crc32.of(payload))
        payload.copyInto(arr, destinationOffset = 21)
        writeInt(arr, total - 4, Crc32.of(arr, 0, total - 4))
        return arr
    }

    fun decodeRecords(
        bytes: ByteArray,
        partitionKey: String,
        segmentName: String,
        skipCorrupt: Boolean,
    ): List<WalRecord> {
        val out = mutableListOf<WalRecord>()
        var offset = 0
        while (offset + 12 <= bytes.size) {
            val magic = readInt(bytes, offset)
            if (magic == BATCH_MAGIC) break
            if (magic != MAGIC) {
                if (skipCorrupt) break
                throw WalCorruptionException("bad magic", partitionKey, segmentName, offset.toLong())
            }
            val recordLen = readInt(bytes, offset + 4)
            val recordEnd = offset + 12 + recordLen
            if (recordEnd > bytes.size) break
            val headerCrc = readInt(bytes, recordEnd - 4)
            if (headerCrc != Crc32.of(bytes, offset, recordEnd - offset - 4)) {
                if (skipCorrupt) {
                    offset = recordEnd
                    continue
                }
                throw WalCorruptionException("header crc", partitionKey, segmentName, offset.toLong())
            }
            val seq = readLong(bytes, offset + 8)
            val kind = kindFromOrdinal(bytes[offset + 16].toInt())
            val payloadCrc = readInt(bytes, offset + 17)
            val payloadOff = offset + 21
            val payloadLen = recordLen - 13
            val payload = bytes.copyOfRange(payloadOff, payloadOff + payloadLen)
            if (Crc32.of(payload) != payloadCrc) {
                if (skipCorrupt) {
                    offset = recordEnd
                    continue
                }
                throw WalCorruptionException("payload crc", partitionKey, segmentName, offset.toLong())
            }
            out.add(WalRecord(seq, KdbTimestamp.now(), kind, payload))
            offset = recordEnd
        }
        return out
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
