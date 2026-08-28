package dev.kdb.integrity

import dev.kdb.document.KdbCommit
import dev.kdb.storage.PlatformIoShim

/**
 * Returns every commit whose frame passed L1 CRC verification, across
 * every segment of namespaceId, keyed by hex hash. Commits at or after
 * the first corrupt or short frame in any given segment are excluded from
 * that segment's contribution - restore (see the kdb-recovery module)
 * uses this so a source with any unrepaired corruption still contributes
 * everything it safely can, and nothing it can't.
 */
public suspend fun scanVerifiedCommits(shim: PlatformIoShim, namespaceId: String): Map<String, KdbCommit> {
    val seqs = listSequencedSegments(shim, namespaceId)
    val out = mutableMapOf<String, KdbCommit>()
    for (seq in seqs) {
        val ss = readAndScanSegment(shim, namespaceId, seq)
        for (c in ss.commits) out[c.commitHash.toHex()] = c.commit
    }
    return out
}
