/**
 * Implements kdb-spec-layer15 Component 61: restore and hybrid restore.
 * There is exactly one algorithm - a verified union of whatever sources
 * are available, applied topologically by commit hash (see
 * [hybridRestore]'s doc comment and kdb-spec-layer15 P6). A "plain"
 * restore from a single backup and a "hybrid" restore that also salvages
 * a damaged local log are the same call with a different sources list.
 * Mirrors go/kdb/recovery exactly.
 *
 * This first implementation supports directory-backed sources only (a
 * damaged local data directory, or another directory holding a backup
 * copy of segments). Peer and S3-backed sources are kdb-spec-layer15
 * Components 60 and 62 and are follow-up work - see that spec's §10 and
 * execution plan.
 */
package dev.kdb.recovery

import dev.kdb.codec.KdbHash
import dev.kdb.document.KdbCommit
import dev.kdb.integrity.genesisCommitHash
import dev.kdb.integrity.scanVerifiedCommits
import dev.kdb.storage.CompressionCodec
import dev.kdb.storage.DeltaAuthorshipEnvelope
import dev.kdb.storage.DeltaRecord
import dev.kdb.storage.PlatformIoShim
import dev.kdb.storage.StorageEngineConfig
import dev.kdb.storage.delta.DeltaSegmentFactory

/**
 * One input to a restore - a namespace-scoped, directory-backed shim
 * that may hold some of the commits a restore needs. label is diagnostic
 * only (surfaced in [Result.sourcesUsed]).
 */
public data class Source(val label: String, val shim: PlatformIoShim)

/** What a restore produced. */
public data class Result(
    val namespaceId: String,
    /** Labels of sources that contributed at least one commit. */
    val sourcesUsed: List<String>,
    val appliedCount: Int,
    /** Non-empty only if the union could not resolve every parent. */
    val missingHashes: List<String>,
)

/**
 * Unions every source's CRC-verified commits by hash (see
 * [scanVerifiedCommits] - only frames that pass L1 verification ever
 * contribute, per kdb-spec-layer15 P4/P5: an unverified byte is never
 * trusted just because it happens to be the only copy available), orders
 * the union topologically, and writes the result as a fresh sequenced
 * delta log to out.
 *
 * A commit whose parent hash is present in no source at all can never be
 * safely applied - it and everything depending on it are reported in
 * [Result.missingHashes] instead of being applied out of dependency
 * order, which is the same "erroring only if a genuine parent is missing"
 * rule kdb-spec-layer13 Component 47 uses for ordinary replay
 * ([dev.kdb.jdbc.file.DeltaNamespaceReplayer]).
 */
public suspend fun hybridRestore(
    sources: List<Source>,
    namespaceId: String,
    compression: CompressionCodec,
    out: PlatformIoShim,
): Result {
    val union = mutableMapOf<String, KdbCommit>()
    val used = mutableListOf<String>()
    for (src in sources) {
        val commits = scanVerifiedCommits(src.shim, namespaceId)
        if (commits.isNotEmpty()) used += src.label
        for ((hex, c) in commits) {
            if (!union.containsKey(hex)) union[hex] = c
        }
    }

    val genesis = genesisCommitHash(namespaceId)
    val (ordered, missing) = topologicalOrder(union, genesis)

    val factory = DeltaSegmentFactory(StorageEngineConfig(compressionCodec = compression, ioShim = out))
    val writer = factory.openWriter(namespaceId)
    for (c in ordered) {
        writer.append(
            DeltaRecord(
                commitHash = c.hash,
                namespaceId = namespaceId,
                authorship = DeltaAuthorshipEnvelope(principal = "unknown", timestamp = c.timestamp, rightsToken = "", clientContext = ""),
                commitPayload = c.toPayloadBytes(),
                documentPatches = emptyList(),
            ),
        )
    }
    writer.flush()
    writer.seal()

    return Result(namespaceId, used.sorted(), ordered.size, missing.sorted())
}

/**
 * Mirrors DeltaNamespaceReplayer's round-based algorithm exactly
 * (kdb-spec-layer13 Component 47): a commit is ordered only once every
 * parent it references has already been ordered. A parent absent from
 * the union entirely never becomes "applied", so anything depending on
 * it - directly or transitively - never progresses and lands in missing
 * once a round makes no further progress, rather than being guessed at.
 * genesis is exempt from that rule: it is never persisted to any log by
 * design (see genesisCommitHash), so a commit whose sole parent is
 * genesis is ready immediately.
 */
private fun topologicalOrder(union: Map<String, KdbCommit>, genesis: KdbHash): Pair<List<KdbCommit>, List<String>> {
    val applied = mutableSetOf<String>()
    var pending = union.values.sortedBy { it.hash.toHex() }
    val ordered = mutableListOf<KdbCommit>()
    val missing = mutableListOf<String>()

    while (pending.isNotEmpty()) {
        val next = mutableListOf<KdbCommit>()
        var progressed = false
        for (c in pending) {
            val ready = c.parentHashes.all { it == genesis || applied.contains(it.toHex()) }
            if (!ready) {
                next += c
                continue
            }
            ordered += c
            applied += c.hash.toHex()
            progressed = true
        }
        if (!progressed) {
            missing += next.map { it.hash.toHex() }
            break
        }
        pending = next
    }
    return Pair(ordered, missing)
}
