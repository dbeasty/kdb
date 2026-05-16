package dev.kdb.storage.delta

import dev.kdb.codec.KdbHash
import dev.kdb.document.KdbCommit
import dev.kdb.storage.CompressionCodec

/**
 * Scans v1 delta segment bytes: sequential KDBP-framed commit payloads.
 * Full 10d page layout may extend this scanner later.
 */
public object DeltaSegmentScanner {
    private const val FRAME_HEADER_SIZE: Int = 16
    private const val MAGIC_K: Int = 0x4B
    private const val MAGIC_D: Int = 0x44
    private const val MAGIC_B: Int = 0x42
    private const val MAGIC_P: Int = 0x50

    public data class ScannedCommit(
        val commitHash: KdbHash,
        val commit: KdbCommit,
        val frameOffset: Int,
    )

    public fun scanSegmentBytes(
        bytes: ByteArray,
        compression: CompressionCodec,
    ): List<ScannedCommit> {
        val out = mutableListOf<ScannedCommit>()
        var offset = 0
        while (offset + FRAME_HEADER_SIZE <= bytes.size) {
            if (!isKdbpFrame(bytes, offset)) break
            val compressedSize = readIntBe(bytes, offset + 4)
            val frameEnd = offset + FRAME_HEADER_SIZE + compressedSize
            if (frameEnd > bytes.size) break
            val frame = bytes.copyOfRange(offset, frameEnd)
            val payload = DeltaPageCodec.parse(frame, compression)
            val commit = KdbCommit.fromPayloadBytes(payload)
            out.add(ScannedCommit(commit.hash, commit, offset))
            offset = frameEnd
        }
        return out
    }

    private fun isKdbpFrame(bytes: ByteArray, offset: Int): Boolean =
        bytes[offset].toInt() and 0xFF == MAGIC_K &&
            bytes[offset + 1].toInt() and 0xFF == MAGIC_D &&
            bytes[offset + 2].toInt() and 0xFF == MAGIC_B &&
            bytes[offset + 3].toInt() and 0xFF == MAGIC_P

    private fun readIntBe(bytes: ByteArray, offset: Int): Int =
        ((bytes[offset].toInt() and 0xFF) shl 24) or
            ((bytes[offset + 1].toInt() and 0xFF) shl 16) or
            ((bytes[offset + 2].toInt() and 0xFF) shl 8) or
            (bytes[offset + 3].toInt() and 0xFF)
}
