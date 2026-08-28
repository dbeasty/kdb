package dev.kdb.storage.delta

import dev.kdb.codec.KdbHash
import dev.kdb.compression.Crc32
import dev.kdb.document.KdbCommit

/**
 * Scans delta segment bytes: sequential KDBP-framed commit payloads. Each
 * frame records its own codec (see DeltaPageCodec), so no codec argument is
 * needed or accepted - a segment may even mix codecs.
 * Full 10d page layout may extend this scanner later.
 */
public object DeltaSegmentScanner {
    private const val FRAME_HEADER_SIZE: Int = DeltaPageCodec.FRAME_HEADER_SIZE
    private const val MAGIC_K: Int = 0x4B
    private const val MAGIC_D: Int = 0x44
    private const val MAGIC_B: Int = 0x42
    private const val MAGIC_P: Int = 0x50

    public data class ScannedCommit(
        val commitHash: KdbHash,
        val commit: KdbCommit,
        val frameOffset: Int,
    )

    /**
     * A frame whose stored CRC32 (written by [DeltaPageCodec.frame]) does
     * not match its actual body bytes, or whose body fails to parse
     * despite fitting entirely within the scanned range - i.e. not simply
     * truncated (a short/missing tail, the expected shape of an unclean
     * shutdown, is handled silently by [scanSegmentBytes] itself and
     * never reaches here). [partialCommits] holds every commit scanned
     * successfully before this one - callers that want torn-tail-tolerant
     * behavior (kdb-spec-layer13 Component 47 §4.3) use that instead of
     * discarding it.
     */
    public class CorruptFrameException(
        public val offset: Int,
        public val reason: String,
        public val partialCommits: List<ScannedCommit>,
    ) : Exception("delta segment: corrupt frame at offset $offset: $reason")

    /**
     * @throws CorruptFrameException if a frame's stored CRC doesn't match
     *   its body, or its body fails to parse - see that exception's doc
     *   comment for why this is not simply treated as a truncated tail.
     */
    public fun scanSegmentBytes(bytes: ByteArray): List<ScannedCommit> {
        val out = mutableListOf<ScannedCommit>()
        var offset = 0
        while (offset + FRAME_HEADER_SIZE <= bytes.size) {
            if (!isKdbpFrame(bytes, offset)) break
            val compressedSize = readIntBe(bytes, offset + 8)
            if (compressedSize < 0) break
            val frameEnd = offset + FRAME_HEADER_SIZE + compressedSize
            if (frameEnd > bytes.size) break
            val frame = bytes.copyOfRange(offset, frameEnd)
            val storedCrc = readIntBe(frame, 16)
            val actualCrc = Crc32.of(frame, FRAME_HEADER_SIZE, frame.size - FRAME_HEADER_SIZE)
            if (actualCrc != storedCrc) {
                throw CorruptFrameException(
                    offset,
                    "crc mismatch: stored=${storedCrc.toUInt().toString(16)} actual=${actualCrc.toUInt().toString(16)}",
                    out,
                )
            }
            val commit =
                try {
                    val payload = DeltaPageCodec.parse(frame)
                    KdbCommit.fromPayloadBytes(payload)
                } catch (e: Exception) {
                    throw CorruptFrameException(offset, e.message ?: e.toString(), out)
                }
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
