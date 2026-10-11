package dev.kdb.storage.delta

import dev.kdb.codec.KdbHash
import dev.kdb.compression.Crc32
import dev.kdb.document.KdbCommit

/**
 * Scans delta segment bytes: sequential KDBP-framed commit payloads. Each
 * frame records its own codec (see DeltaPageCodec), so no codec argument is
 * needed or accepted - a segment may even mix codecs.
 * Full 10d page layout may extend this scanner later.
 *
 * How a segment's frames end follows one rule, the same as Go's (go/kdb/storage/delta/tail.go,
 * storage.TailKind): a torn tail - what an append that never completed leaves - can only be bytes
 * at the very end of a segment with nothing intact after them. Damage that an intact frame
 * follows is never a torn tail, and zero bytes after the last frame (a preallocated segment's
 * unused space) are simply the end.
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
     * despite fitting entirely within the scanned range, or a garbled frame
     * header that intact frames follow. A short/missing tail with nothing
     * intact after it - the expected shape of an unclean shutdown - and zero
     * padding are handled silently by [scanSegmentBytes] itself and never
     * reach here. [partialCommits] holds every commit scanned successfully
     * before this one - callers that want torn-tail-tolerant behavior
     * (kdb-spec-layer13 Component 47 §4.3) use that instead of discarding it,
     * but only when [torn] says the fault has a torn tail's shape.
     */
    public class CorruptFrameException(
        public val offset: Int,
        public val reason: String,
        public val partialCommits: List<ScannedCommit>,
        /**
         * How many complete frames with a valid CRC follow the damaged one. Zero is the shape of
         * a torn tail - the last write never completed. Anything more means intact data follows:
         * the frame was damaged in place (bit rot, a bad sector), and treating it as a torn tail
         * would silently drop every commit after it.
         */
        public val intactFramesAfter: Int = 0,
        /** Where the first of [intactFramesAfter] starts, or -1. */
        public val nextIntactFrame: Int = -1,
        /**
         * The frame passed its CRC and still would not decode. A torn write cannot produce that -
         * a frame's CRC only matches once its whole body has been written - so it is damage.
         */
        public val undecodable: Boolean = false,
    ) : Exception(
            "delta segment: corrupt frame at offset $offset: $reason" +
                if (intactFramesAfter > 0) " ($intactFramesAfter intact frame(s) follow it, the first at offset $nextIntactFrame)" else "",
        ) {
        /** Whether this fault has a torn tail's shape: nothing intact after it, and not merely undecodable. */
        public val torn: Boolean get() = !undecodable && intactFramesAfter == 0
    }

    /** How a segment's intact frames end - see [scanSegment]. */
    public enum class TailKind {
        /** The bytes end at the last intact frame, or only zero padding follows it. */
        CLEAN,

        /** Bytes that are not zero follow the last intact frame, and no intact frame follows them. */
        TORN,

        /** An intact frame follows the stop, or the frame there passes its CRC and will not decode. */
        DAMAGED,
    }

    public data class SegmentTail(
        val kind: TailKind,
        /** Where the intact frames stop. */
        val offset: Int,
        val reason: String,
        /** The first intact frame after [offset], or -1. */
        val nextIntactFrame: Int,
    )

    public data class SegmentScan(
        val commits: List<ScannedCommit>,
        val tail: SegmentTail,
    )

    /**
     * @throws CorruptFrameException if a frame's stored CRC doesn't match
     *   its body, or its body fails to parse, or a garbled header is
     *   followed by intact frames - see that exception's doc comment.
     */
    public fun scanSegmentBytes(bytes: ByteArray): List<ScannedCommit> {
        val out = mutableListOf<ScannedCommit>()
        var offset = 0
        while (offset + FRAME_HEADER_SIZE <= bytes.size) {
            if (!isKdbpFrame(bytes, offset)) break
            val compressedSize = readIntBe(bytes, offset + 8)
            // The writer never frames an empty payload, and a header torn just after its magic
            // (the rest still zero, as in a preallocated segment) declares exactly this - with a
            // CRC of zero that an empty body matches.
            if (compressedSize <= 0) break
            val frameEnd = offset + FRAME_HEADER_SIZE + compressedSize
            if (frameEnd > bytes.size || frameEnd < 0) break
            val frame = bytes.copyOfRange(offset, frameEnd)
            val storedCrc = readIntBe(frame, 16)
            val actualCrc = Crc32.of(frame, FRAME_HEADER_SIZE, frame.size - FRAME_HEADER_SIZE)
            if (actualCrc != storedCrc) {
                throw corruptAt(
                    bytes,
                    offset,
                    "crc mismatch: stored=${storedCrc.toUInt().toString(16)} actual=${actualCrc.toUInt().toString(16)}",
                    out,
                    undecodable = false,
                )
            }
            val commit =
                try {
                    val payload = DeltaPageCodec.parse(frame)
                    KdbCommit.fromPayloadBytes(payload)
                } catch (e: Exception) {
                    throw corruptAt(bytes, offset, e.message ?: e.toString(), out, undecodable = true)
                }
            out.add(ScannedCommit(commit.hash, commit, offset))
            offset = frameEnd
        }
        if (offset < bytes.size) {
            // A torn tail, zero padding or a garbled header: only what follows tells them apart.
            // This used to end the scan as quietly as a torn tail even with intact frames after
            // it, so a flipped byte in a frame header silently truncated the segment (N5).
            val next = nextIntactFrame(bytes, offset)
            if (next >= 0) {
                throw CorruptFrameException(offset, describeStop(bytes, offset), out, intactFramesFrom(bytes, next), next)
            }
        }
        return out
    }

    /**
     * Scans a whole segment and classifies how its intact frames end, never throwing on damage:
     * the tail reports it, and commits holds every commit before the stop.
     */
    public fun scanSegment(bytes: ByteArray): SegmentScan =
        try {
            val commits = scanSegmentBytes(bytes)
            val end = if (commits.isEmpty()) 0 else frameEndAt(bytes, commits.last().frameOffset)
            val kind = if (end < bytes.size && !allZero(bytes, end)) TailKind.TORN else TailKind.CLEAN
            val reason = if (kind == TailKind.TORN) describeStop(bytes, end) else ""
            SegmentScan(commits, SegmentTail(kind, end, reason, -1))
        } catch (e: CorruptFrameException) {
            SegmentScan(
                e.partialCommits,
                SegmentTail(if (e.torn) TailKind.TORN else TailKind.DAMAGED, e.offset, e.reason, e.nextIntactFrame),
            )
        }

    /**
     * The commits in the intact frames that start after [from], reading past damage - for
     * assessment and repair only (replay never skips a frame). Mirrors Go's
     * delta.ScanSkippingCorrupt.
     */
    public fun intactCommitsAfter(
        bytes: ByteArray,
        from: Int,
    ): List<ScannedCommit> {
        val out = mutableListOf<ScannedCommit>()
        var offset = nextIntactFrame(bytes, from)
        while (offset >= 0) {
            val end = intactFrameEnd(bytes, offset)
            if (end < 0) {
                offset = nextIntactFrame(bytes, offset)
                continue
            }
            runCatching { KdbCommit.fromPayloadBytes(DeltaPageCodec.parse(bytes.copyOfRange(offset, end))) }
                .onSuccess { out += ScannedCommit(it.hash, it, offset) }
            offset = if (intactFrameEnd(bytes, end) >= 0) end else nextIntactFrame(bytes, end - 1)
        }
        return out
    }

    private fun corruptAt(
        bytes: ByteArray,
        offset: Int,
        reason: String,
        partial: List<ScannedCommit>,
        undecodable: Boolean,
    ): CorruptFrameException {
        val next = nextIntactFrame(bytes, offset)
        return CorruptFrameException(offset, reason, partial, intactFramesFrom(bytes, next), next, undecodable)
    }

    private fun frameEndAt(
        bytes: ByteArray,
        offset: Int,
    ): Int = offset + FRAME_HEADER_SIZE + readIntBe(bytes, offset + 8)

    private fun describeStop(
        bytes: ByteArray,
        offset: Int,
    ): String {
        val rest = bytes.size - offset
        return when {
            rest < FRAME_HEADER_SIZE -> "$rest byte(s) after the last frame, too few for a frame header"
            !isKdbpFrame(bytes, offset) -> "no frame header here (magic missing)"
            readIntBe(bytes, offset + 8) <= 0 -> "frame header declares an empty body"
            else -> "frame declares ${readIntBe(bytes, offset + 8)} body byte(s) but only ${rest - FRAME_HEADER_SIZE} follow its header"
        }
    }

    /**
     * Whether a complete frame with a well-formed v2 header and a body matching its CRC starts at
     * [offset]; returns where it ends, or -1. Stricter than the scan's own framing, because it is
     * asked of bytes that may be anything: a stray "KDBP" passes only if a valid header and a
     * matching CRC follow it.
     */
    private fun intactFrameEnd(
        bytes: ByteArray,
        offset: Int,
    ): Int {
        if (offset < 0 || offset + FRAME_HEADER_SIZE > bytes.size || !isKdbpFrame(bytes, offset)) return -1
        if (bytes[offset + 4] != DeltaPageCodec.PAGE_FORMAT_VERSION) return -1
        val codec = bytes[offset + 5]
        if (codec != DeltaPageCodec.CODEC_NONE && codec != DeltaPageCodec.CODEC_ZSTD) return -1
        if (bytes[offset + 6].toInt() != 0 || bytes[offset + 7].toInt() != 0) return -1
        val n = readIntBe(bytes, offset + 8)
        if (n <= 0 || n > bytes.size - offset - FRAME_HEADER_SIZE) return -1
        val storedCrc = readIntBe(bytes, offset + 16)
        if (Crc32.of(bytes, offset + FRAME_HEADER_SIZE, n) != storedCrc) return -1
        return offset + FRAME_HEADER_SIZE + n
    }

    /**
     * The offset of the first intact frame starting strictly after [from], or -1. Searches for the
     * frame magic byte by byte, so it finds frames past a garbled header as well as past a damaged
     * body. Mirrors Go's delta.NextIntactFrame.
     */
    public fun nextIntactFrame(
        bytes: ByteArray,
        from: Int,
    ): Int {
        var i = maxOf(from + 1, 0)
        while (i + FRAME_HEADER_SIZE <= bytes.size) {
            if ((bytes[i].toInt() and 0xFF) == MAGIC_K && isKdbpFrame(bytes, i) && intactFrameEnd(bytes, i) >= 0) return i
            i++
        }
        return -1
    }

    /** Counts the intact frames from [start] on (start included; -1 means none), stepping past damage between them. */
    internal fun intactFramesFrom(
        bytes: ByteArray,
        start: Int,
    ): Int {
        var count = 0
        var offset = start
        while (offset >= 0 && offset < bytes.size) {
            val end = intactFrameEnd(bytes, offset)
            if (end >= 0) {
                count++
                offset = end
                continue
            }
            offset = nextIntactFrame(bytes, offset)
        }
        return count
    }

    private fun allZero(
        bytes: ByteArray,
        from: Int,
    ): Boolean {
        for (i in from until bytes.size) if (bytes[i].toInt() != 0) return false
        return true
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

/**
 * A delta segment whose intact frames stop at damage that is not a torn tail: intact frames follow
 * it, the frame there passes its CRC and will not decode, or ([builtOn]) commits in later segments
 * descend from what its unreadable tail held. Replaying past it would silently drop committed
 * history, so replay refuses. Mirrors Go's delta.DamagedSegmentError.
 */
public class DamagedSegmentException(
    public val namespaceId: String,
    public val sequenceNumber: Long,
    public val offset: Int,
    public val reason: String,
    public val nextIntactFrame: Int,
    public val builtOn: Boolean = false,
    cause: Throwable? = null,
) : IllegalStateException(
        "kdb: namespace '$namespaceId': delta segment $sequenceNumber is damaged at byte $offset ($reason); " +
            when {
                builtOn -> "its last frame(s) cannot be read, and commits in later segments build on them, so this is not a torn tail"
                nextIntactFrame >= 0 -> "intact frames follow the damage (the next at byte $nextIntactFrame), so this is not a torn tail"
                else -> "the frame there is complete and passes its checksum but will not decode, so this is not a torn tail"
            } +
            ", and opening past it would silently drop committed history. Run 'kdb-inspect verify' to see the damage, " +
            "then restore the namespace from a backup or a peer, or 'kdb-inspect repair-segments', which repairs only " +
            "what it can prove loses nothing" + (cause?.message?.let { ": $it" } ?: ""),
        cause,
    )
