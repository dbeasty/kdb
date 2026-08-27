package dev.kdb.integrity

import dev.kdb.codec.KdbUuid
import dev.kdb.storage.PlatformIoShim
import dev.kdb.storage.io.SegmentNameBuilder

/** What [repair] did (or refused to do) for one [Finding]. */
public enum class Action {
    TRUNCATED_TORN_TAIL,
    REWROTE_SEGMENT_PREFIX,
    REFUSED,
}

/** What [repair] did for one finding, and why. */
public data class RepairStep(
    val finding: Finding,
    val action: Action,
    /** Set when bytes were preserved before mutating anything. */
    val quarantineName: String = "",
    /** Set only when action == REFUSED. */
    val missingHashes: List<String> = emptyList(),
    val detail: String,
)

/** The outcome of one [repair] run. */
public data class RepairResult(
    val namespaceId: String,
    val steps: List<RepairStep>,
) {
    /** Whether any step could not be safely repaired. */
    public val anyRefused: Boolean get() = steps.any { it.action == Action.REFUSED }
}

/**
 * Acts on a verification [Report] - see kdb-spec-layer15 Component 59. It
 * never invents its own opinion of what is wrong: every step corresponds
 * to exactly one L1 finding already produced by [verify]. Legacy
 * (pre-Layer-13 random-UUID) segment migration is not yet implemented by
 * this module - see kdb-spec-layer15 §10 and the execution plan's Phase 7
 * follow-up.
 *
 * Repair is idempotent (kdb-spec-layer15 P3): re-running it against a
 * report from an already-repaired directory finds nothing to do, since a
 * re-[verify] of that directory would no longer surface the fixed
 * findings.
 */
public suspend fun repair(shim: PlatformIoShim, report: Report, opts: Options): RepairResult {
    val steps = mutableListOf<RepairStep>()
    for (f in report.findings) {
        // L2 findings (missing_parent, sequence_gap) name a gap repair cannot fabricate data to
        // fill; see Component 61 (restore, in the kdb-recovery module).
        if (f.level != Level.L1) continue
        when (f.classification) {
            Classification.TORN_TAIL -> steps += repairTornTail(shim, report.namespaceId, f)
            Classification.MID_LOG_CORRUPTION -> steps += repairMidLogCorruption(shim, report.namespaceId, f, opts)
            else -> Unit
        }
    }
    return RepairResult(report.namespaceId, steps)
}

private suspend fun repairTornTail(shim: PlatformIoShim, namespaceId: String, f: Finding): RepairStep {
    val name = SegmentNameBuilder.deltaSequenced(namespaceId, f.segment)
    val raw = shim.readFromSegment(name, 0, Int.MAX_VALUE / 4)
    require(f.offset in 0..raw.size) { "finding offset ${f.offset} out of range for segment ${f.segment} (${raw.size} bytes)" }
    val good = raw.copyOfRange(0, f.offset)
    val torn = raw.copyOfRange(f.offset, raw.size)
    val quarantineName = quarantineSegmentName(namespaceId, f.segment)
    shim.appendToSegment(quarantineName, torn)
    shim.deleteSegment(name)
    if (good.isNotEmpty()) shim.appendToSegment(name, good)
    return RepairStep(
        finding = f,
        action = Action.TRUNCATED_TORN_TAIL,
        quarantineName = quarantineName,
        detail = "truncated segment ${f.segment} at byte ${f.offset}; ${torn.size} torn byte(s) preserved in $quarantineName",
    )
}

/**
 * Quarantines the full original segment and, only if the namespace's
 * parent closure holds using just that segment's good prefix (the frames
 * before the corrupt one - the scanner cannot resync past a corrupt frame
 * to recover anything after it), rewrites the segment to contain that
 * prefix alone. If closure would break, it touches nothing and reports
 * exactly which commit hashes would go missing (kdb-spec-layer15 §5.2,
 * P3: never guess, never destroy evidence unless the repair it would
 * enable is actually safe).
 */
private suspend fun repairMidLogCorruption(shim: PlatformIoShim, namespaceId: String, f: Finding, opts: Options): RepairStep {
    val seqs = listSequencedSegments(shim, namespaceId)
    var goodPrefixLen = -1
    val allCommits = mutableSetOf<String>()
    var prefixCount = 0
    for (seq in seqs) {
        val ss = readAndScanSegment(shim, namespaceId, seq, opts.compression)
        if (seq == f.segment) {
            goodPrefixLen = f.offset
            for (c in ss.commits) {
                if (c.frameOffset < f.offset) {
                    allCommits += c.commitHash.toHex()
                    prefixCount++
                }
            }
            continue
        }
        for (c in ss.commits) allCommits += c.commitHash.toHex()
    }
    check(goodPrefixLen >= 0) { "segment ${f.segment} named by finding not found among namespace $namespaceId's segments" }

    val genesis = genesisCommitHash(namespaceId)
    val missing = mutableListOf<String>()
    for (seq in seqs) {
        if (seq <= f.segment) continue
        val ss = readAndScanSegment(shim, namespaceId, seq, opts.compression)
        for (c in ss.commits) {
            for (p in c.commit.parentHashes) {
                if (p == genesis) continue // never persisted, by design - see genesisCommitHash
                if (!allCommits.contains(p.toHex())) missing += p.toHex()
            }
        }
    }
    if (missing.isNotEmpty()) {
        return RepairStep(
            finding = f,
            action = Action.REFUSED,
            missingHashes = missing,
            detail = "removing the corrupt frame in segment ${f.segment} would drop ${missing.size} commit(s) still referenced as parents by later segments - run kdb restore instead",
        )
    }

    val name = SegmentNameBuilder.deltaSequenced(namespaceId, f.segment)
    val raw = shim.readFromSegment(name, 0, Int.MAX_VALUE / 4)
    val quarantineName = quarantineSegmentName(namespaceId, f.segment)
    shim.appendToSegment(quarantineName, raw)
    shim.deleteSegment(name)
    val good = raw.copyOfRange(0, goodPrefixLen)
    if (good.isNotEmpty()) shim.appendToSegment(name, good)
    return RepairStep(
        finding = f,
        action = Action.REWROTE_SEGMENT_PREFIX,
        quarantineName = quarantineName,
        detail = "kept $prefixCount of ${prefixCount + 1} commit(s) from segment ${f.segment} (frames before the corrupt one); full original preserved in $quarantineName",
    )
}

/**
 * Builds a path for preserved bytes deliberately outside
 * ns/&lt;namespaceId&gt;/delta/ - alongside the live .seg files, a
 * quarantine file's name would not parse as a sequenced segment and
 * DefaultDeltaSegmentReader.listSegments / DeltaSegmentFactory.openWriter
 * would treat it exactly like a pre-Layer-13 legacy segment (refusing to
 * open the namespace at all). A dedicated quarantine/ subdirectory keeps
 * evidence preserved (kdb-spec-layer15 P3) without the production delta
 * module ever needing to know this convention exists.
 */
private fun quarantineSegmentName(namespaceId: String, seq: Long): String {
    val padded = seq.toString().padStart(20, '0')
    return "ns/$namespaceId/quarantine/$padded.quarantine-${KdbUuid.random()}"
}
