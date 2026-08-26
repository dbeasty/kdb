/**
 * Implements kdb-spec-layer15 Component 58 (integrity verification) and
 * Component 59 (repair, quarantine): read-only detection of delta-log
 * corruption, and the repair actions that are provably safe to take on
 * what verification finds. Mirrors go/kdb/integrity exactly.
 *
 * Verification is deliberately independent of [DefaultDeltaSegmentReader]:
 * that reader's listSegments/readAll are replay-oriented and silently
 * discard a [DeltaSegmentScanner.CorruptFrameException] on any segment
 * that isn't the caller's concern at replay time (see
 * DefaultDeltaSegmentReader.scanSegmentRef, which catches the exception
 * and just uses its partialCommits, for every segment - not just the
 * last one). A verification tool exists specifically to not silently
 * discard that information, so it scans raw segment bytes itself.
 */
package dev.kdb.integrity

import dev.kdb.document.KdbCommit
import dev.kdb.storage.CompressionCodec
import dev.kdb.storage.PlatformIoShim
import dev.kdb.storage.delta.DeltaSegmentScanner
import dev.kdb.storage.delta.LegacySegmentFormatException
import dev.kdb.storage.io.SegmentNameBuilder

/** The kind of problem a [Finding] reports - see kdb-spec-layer15 §4.2. */
public enum class Classification {
    /**
     * A corrupt or short final frame in the highest-sequence segment - by
     * kdb-spec-layer13 §4.3's rule, the expected shape of an unclean
     * shutdown (a commit that was never acknowledged), not real
     * corruption.
     */
    TORN_TAIL,

    /** A corrupt or short frame anywhere other than the tail of the last segment - real corruption, never silently tolerated. */
    MID_LOG_CORRUPTION,

    /** A commit whose parent hash is not present anywhere in the namespace's scanned log. */
    MISSING_PARENT,

    /** A gap in segment sequence numbers. */
    SEQUENCE_GAP,
}

/**
 * A verification depth - see kdb-spec-layer15 §4.1. L3 (semantic:
 * document tree / blob re-materialization) is specified but not
 * implemented by this module yet.
 */
public enum class Level {
    L1, // physical: frame CRC / framing
    L2, // logical: commit hash + parent closure
}

/** One verification result. offset/commitHash are empty/zero when not applicable to the finding's classification. */
public data class Finding(
    val namespaceId: String,
    val level: Level,
    val segment: Long,
    val offset: Int = 0,
    val classification: Classification,
    val detail: String,
    val commitHash: String = "",
)

/** One segment's scan, independent of whether it produced any findings. */
public data class SegmentSummary(
    val sequence: Long,
    val sizeBytes: Long,
    val frameCount: Int,
)

/** The output of [verify] - the sole input contract [repair] acts on. */
public data class Report(
    val namespaceId: String,
    val segments: List<SegmentSummary>,
    val findings: List<Finding>,
) {
    /** Whether verification found nothing wrong. */
    public val isClean: Boolean get() = findings.isEmpty()
}

/**
 * Configures a verification run. compression must name the codec the
 * namespace was written with - it is never recorded in a frame, so a
 * mismatch here cannot be detected from the bytes alone and [verify]
 * will not guess.
 */
public data class Options(
    val level: Level,
    val compression: CompressionCodec,
)

internal const val FRAME_HEADER_SIZE = 16

/**
 * One segment's independently-scanned bytes and result.
 *
 * consumedBytes is how much of raw the scan actually accounted for -
 * either up to the corrupt frame's offset, or (on a clean scan) up to the
 * end of the last frame. Anything past it and before raw.size is either
 * the expected shape of a torn tail (a short/missing tail that
 * DeltaSegmentScanner.scanSegmentBytes stops at silently) or, if this
 * isn't the last segment, evidence of truncation that the scanner's
 * silent short-tail tolerance was never meant to hide.
 */
internal class ScannedSegment(
    val sequence: Long,
    val raw: ByteArray,
    val commits: List<DeltaSegmentScanner.ScannedCommit>,
    val corrupt: DeltaSegmentScanner.CorruptFrameException?,
    val consumedBytes: Int,
)

/**
 * Returns namespaceId's delta segment sequence numbers in ascending
 * (commit) order, refusing to guess at order for any pre-Layer-13 legacy
 * (random-UUID) segment name - the same refusal
 * DefaultDeltaSegmentReader.listSegments and DeltaSegmentFactory.openWriter
 * apply.
 */
internal suspend fun listSequencedSegments(shim: PlatformIoShim, namespaceId: String): List<Long> {
    val prefix = SegmentNameBuilder.namespacePrefix(namespaceId) + "delta/"
    val names = shim.listSegments(namespaceId).filter { it.startsWith(prefix) }
    val legacy = mutableListOf<String>()
    val seqs = mutableListOf<Long>()
    for (name in names) {
        val fileName = name.removePrefix(prefix)
        val seq = SegmentNameBuilder.parseDeltaSequencedFileName(fileName)
        if (seq == null) {
            legacy += name
            continue
        }
        seqs += seq
    }
    if (legacy.isNotEmpty()) throw LegacySegmentFormatException(namespaceId, legacy)
    return seqs.sorted()
}

internal suspend fun readAndScanSegment(
    shim: PlatformIoShim,
    namespaceId: String,
    seq: Long,
    compression: CompressionCodec,
): ScannedSegment {
    val name = SegmentNameBuilder.deltaSequenced(namespaceId, seq)
    val raw = shim.readFromSegment(name, 0, Int.MAX_VALUE / 4)
    return try {
        val commits = DeltaSegmentScanner.scanSegmentBytes(raw, compression)
        val consumed =
            if (commits.isEmpty()) {
                0
            } else {
                val last = commits.last()
                last.frameOffset + frameLen(raw, last.frameOffset)
            }
        ScannedSegment(seq, raw, commits, null, consumed)
    } catch (e: DeltaSegmentScanner.CorruptFrameException) {
        ScannedSegment(seq, raw, e.partialCommits, e, e.offset)
    }
}

/**
 * Reads the 16-byte frame header's compressed-body-length field (the
 * same layout DeltaSegmentScanner parses) to compute the full on-disk
 * size of the frame starting at offset, so verify can tell whether a
 * clean scan actually consumed every byte of the segment.
 */
internal fun frameLen(raw: ByteArray, offset: Int): Int {
    val b0 = raw[offset + 4].toInt() and 0xFF
    val b1 = raw[offset + 5].toInt() and 0xFF
    val b2 = raw[offset + 6].toInt() and 0xFF
    val b3 = raw[offset + 7].toInt() and 0xFF
    return FRAME_HEADER_SIZE + ((b0 shl 24) or (b1 shl 16) or (b2 shl 8) or b3)
}

/** Walks namespaceId's delta log at the requested level and reports exactly what it finds - see kdb-spec-layer15 Component 58. */
public suspend fun verify(shim: PlatformIoShim, namespaceId: String, opts: Options): Report {
    val seqs = listSequencedSegments(shim, namespaceId)
    val findings = mutableListOf<Finding>()
    val segments = mutableListOf<SegmentSummary>()
    val allCommits = mutableMapOf<String, KdbCommit>()
    val commitSegment = mutableMapOf<String, Long>()

    seqs.forEachIndexed { i, seq ->
        if (i > 0 && seqs[i - 1] != seq - 1) {
            findings +=
                Finding(
                    namespaceId, Level.L1, seq, classification = Classification.SEQUENCE_GAP,
                    detail = "sequence jumps from ${seqs[i - 1]} to $seq - a segment is missing",
                )
        }
        val ss = readAndScanSegment(shim, namespaceId, seq, opts.compression)
        segments += SegmentSummary(seq, ss.raw.size.toLong(), ss.commits.size)
        val isLastSegment = i == seqs.lastIndex

        if (ss.corrupt != null) {
            val cls = if (isLastSegment) Classification.TORN_TAIL else Classification.MID_LOG_CORRUPTION
            findings += Finding(namespaceId, Level.L1, seq, ss.corrupt.offset, cls, ss.corrupt.reason)
        } else if (ss.consumedBytes < ss.raw.size) {
            val trailing = ss.raw.size - ss.consumedBytes
            val cls: Classification
            val detail: String
            if (isLastSegment) {
                cls = Classification.TORN_TAIL
                detail = "$trailing trailing byte(s) after the last valid frame - an incomplete write never fsynced before shutdown"
            } else {
                cls = Classification.MID_LOG_CORRUPTION
                detail = "$trailing trailing byte(s) after the last valid frame were never consumed by a frame - truncated data in a sealed segment"
            }
            findings += Finding(namespaceId, Level.L1, seq, ss.consumedBytes, cls, detail)
        }

        for (sc in ss.commits) {
            allCommits[sc.commitHash.toHex()] = sc.commit
            commitSegment[sc.commitHash.toHex()] = seq
        }
    }

    if (opts.level == Level.L2) {
        val genesis = genesisCommitHash(namespaceId)
        for ((hex, c) in allCommits) {
            for (parent in c.parentHashes) {
                // The genesis commit is a fixed, deterministically reconstructed root (see
                // genesisCommitHash) - it is never written to the delta log, by design, so its
                // absence here is not evidence of anything missing.
                if (parent == genesis) continue
                if (!allCommits.containsKey(parent.toHex())) {
                    findings +=
                        Finding(
                            namespaceId, Level.L2, commitSegment[hex] ?: -1L,
                            classification = Classification.MISSING_PARENT,
                            detail = "commit $hex references parent ${parent.toHex()}, which is not present anywhere in the scanned log",
                            commitHash = parent.toHex(),
                        )
                }
            }
        }
    }

    val sorted = findings.sortedWith(compareBy({ it.segment }, { it.offset }))
    return Report(namespaceId, segments, sorted)
}
