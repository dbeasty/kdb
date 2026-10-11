/**
 * Implements kdb-spec-layer15 Component 58 (integrity verification) and
 * Component 59 (repair, quarantine): read-only detection of delta-log
 * corruption, and the repair actions that are provably safe to take on
 * what verification finds. Mirrors go/kdb/integrity exactly.
 *
 * Verification reads raw segment bytes itself rather than going through
 * [DefaultDeltaSegmentReader], whose listing keeps only what a segment's
 * intact frames say. It classifies what follows them with the rule replay
 * applies ([DeltaSegmentScanner.scanSegment]), so verify and open cannot
 * disagree about whether a segment lost data.
 */
package dev.kdb.integrity

import dev.kdb.document.KdbCommit
import dev.kdb.storage.PlatformIoShim
import dev.kdb.storage.delta.DeltaSegmentScanner
import dev.kdb.storage.delta.LegacySegmentFormatException
import dev.kdb.storage.io.SegmentNameBuilder

/** The kind of problem a [Finding] reports - see kdb-spec-layer15 §4.2. */
public enum class Classification {
    /**
     * A corrupt or short final frame in the highest-sequence segment, with
     * nothing intact after it - by kdb-spec-layer13 §4.3's rule, the
     * expected shape of an unclean shutdown (a commit that was never
     * acknowledged), not real corruption.
     */
    TORN_TAIL,

    /**
     * A corrupt or short frame anywhere else: any fault that intact frames follow, in any segment
     * (N5), and a faulty last frame in a segment that is no longer the newest - real corruption,
     * never silently tolerated.
     */
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
 * Configures a verification run.
 *
 * There is deliberately no compression setting: since the v2 KDBP frame
 * (DeltaPageCodec) records its codec per frame, verification reads whatever a
 * segment was actually written with. Previously the caller had to supply it,
 * and a wrong answer was indistinguishable from real corruption.
 */
public data class Options(
    val level: Level,
)

internal const val FRAME_HEADER_SIZE = 20

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
    /** How the intact frames end - the rule replay applies (DeltaSegmentScanner.TailKind). */
    val tail: DeltaSegmentScanner.SegmentTail,
) {
    /** Where the intact frames end. */
    val consumedBytes: Int get() = tail.offset
}

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
): ScannedSegment {
    val name = SegmentNameBuilder.deltaSequenced(namespaceId, seq)
    val raw = shim.readFromSegment(name, 0, Int.MAX_VALUE / 4)
    val scan = DeltaSegmentScanner.scanSegment(raw)
    return ScannedSegment(seq, raw, scan.commits, scan.tail)
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
        val ss = readAndScanSegment(shim, namespaceId, seq)
        segments += SegmentSummary(seq, ss.raw.size.toLong(), ss.commits.size)
        val isLastSegment = i == seqs.lastIndex

        // The rule replay applies (DeltaSegmentScanner.TailKind; Go's storage.TailKind). Zero
        // padding after the last frame is no finding. Damage that intact frames follow is
        // mid-log corruption wherever it is, the newest segment included: calling it a torn
        // tail there let repair truncate away the intact commits after it (N5).
        val tail = ss.tail
        when (tail.kind) {
            DeltaSegmentScanner.TailKind.DAMAGED -> {
                val detail =
                    if (tail.nextIntactFrame >= 0) {
                        "${tail.reason} - intact frames follow it (the next at offset ${tail.nextIntactFrame}), so this is damage in place, not a torn tail"
                    } else {
                        "${tail.reason} - damaged in place, not a torn tail"
                    }
                findings += Finding(namespaceId, Level.L1, seq, tail.offset, Classification.MID_LOG_CORRUPTION, detail)
            }
            DeltaSegmentScanner.TailKind.TORN -> {
                // Nothing intact follows. In an older segment that is a torn tail from an earlier
                // unclean shutdown or a damaged last frame - indistinguishable here - so it stays
                // mid-log corruption, which repair removes only if no later commit builds on it.
                val cls = if (isLastSegment) Classification.TORN_TAIL else Classification.MID_LOG_CORRUPTION
                val detail =
                    if (isLastSegment) {
                        "${tail.reason}, with no intact frame after it - an incomplete write never fsynced before shutdown"
                    } else {
                        "${tail.reason}, ${ss.raw.size - tail.offset} byte(s) before the end of the segment, with no intact frame after it - in a segment that is no longer the newest, so either a torn tail from an earlier unclean shutdown or a damaged last frame"
                    }
                findings += Finding(namespaceId, Level.L1, seq, tail.offset, cls, detail)
            }
            DeltaSegmentScanner.TailKind.CLEAN -> Unit
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
