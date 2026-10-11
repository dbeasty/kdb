package dev.kdb.jdbc.file

import dev.kdb.codec.KdbHash
import dev.kdb.dag.CommitDag
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.storage.DeltaAuthorshipEnvelope
import dev.kdb.storage.DeltaRecord
import dev.kdb.storage.DeltaSegmentReader
import dev.kdb.storage.StorageAdapter
import dev.kdb.storage.delta.DamagedSegmentException
import dev.kdb.storage.delta.DeltaSegmentScanner

/** Replay's "the log is missing data": commits whose parents no segment holds. */
internal class MissingParentsException(
    message: String,
) : IllegalStateException(message)

/**
 * Rebuilds dag and storage from every commit durably logged for this
 * namespace. Two properties make this safe to run on every single start,
 * including after an unclean shutdown or a hard kill - see
 * kdb-spec-layer13 Component 47:
 *
 *  1. Segments are read in sequence (commit) order (DeltaSegmentReader's
 *     own listSegments contract), but commits are *applied* in
 *     dependency order, not file order (see applyCommitsTopologically
 *     below). File order is a fast path, not a correctness requirement.
 *  2. A torn tail - bytes at the very end of a segment with nothing intact
 *     after them - is tolerated as the expected shape of an unclean
 *     shutdown, in the newest segment or, after a restart opened a newer
 *     one, in an older one. Damage that an intact frame follows is never a
 *     torn tail, wherever it is, and is refused (DamagedSegmentException;
 *     the rule Go's replay applies, N5).
 */
public object DeltaNamespaceReplayer {
    public suspend fun replay(
        dag: CommitDag,
        storage: StorageAdapter,
        deltaReader: DeltaSegmentReader,
        decisions: CrossNamespaceDecisions? = null,
    ): KdbHash {
        val segments = deltaReader.listSegments()
        val allCommits = mutableListOf<KdbCommit>()
        // Segments other than the newest whose tails were torn - see the missing-parents case below.
        val torn = mutableListOf<Pair<Long, DeltaSegmentScanner.CorruptFrameException>>()
        segments.forEachIndexed { i, segment ->
            val isMostRecent = i == segments.lastIndex
            val records =
                try {
                    deltaReader.readAll(segment)
                } catch (e: DeltaSegmentScanner.CorruptFrameException) {
                    if (!e.torn) {
                        // Damage in place, not a torn tail: intact commits follow the damaged
                        // frame (or it passes its CRC and will not decode). Truncating here would
                        // silently drop them - and everything descending from them - so refuse,
                        // whichever segment it is in. A garbled frame header used to end the scan
                        // as quietly as a torn tail, in any segment (N5).
                        throw DamagedSegmentException(
                            dag.namespaceId, segment.sequenceNumber, e.offset, e.reason, e.nextIntactFrame, cause = e,
                        )
                    }
                    // A torn tail: nothing intact follows it, so it is the last append before an
                    // unclean shutdown, never acknowledged. In the newest segment, or - once a
                    // restart has opened a newer one - in an older segment: the writer always
                    // starts a fresh segment, so that one is never appended to again and this
                    // decision is stable across future restarts too. Keep every commit scanned
                    // before it and continue.
                    if (!isMostRecent) torn += segment.sequenceNumber to e
                    e.partialCommits.map { scanned ->
                        DeltaRecord(
                            commitHash = scanned.commitHash,
                            namespaceId = segment.namespaceId,
                            authorship =
                                DeltaAuthorshipEnvelope(
                                    principal = "unknown",
                                    timestamp = scanned.commit.timestamp,
                                    rightsToken = "",
                                    clientContext = "",
                                ),
                            commitPayload = scanned.commit.toPayloadBytes(),
                            documentPatches = emptyList(),
                        )
                    }
                }
            for (record in records) {
                allCommits += KdbCommit.fromPayloadBytes(record.commitPayload)
            }
        }
        try {
            applyCommitsTopologically(dag, storage, withoutUncommittedGroups(dag, allCommits, decisions))
        } catch (e: MissingParentsException) {
            // A torn tail is never built on: what never finished writing was never acknowledged.
            // Later commits naming parents the log no longer holds therefore mean an older
            // segment's last frame(s) were damaged in place after being read and built on - the
            // one damage a tail alone cannot show. Point at that segment.
            val (seq, tornAt) = torn.firstOrNull() ?: throw e
            throw DamagedSegmentException(dag.namespaceId, seq, tornAt.offset, tornAt.reason, -1, builtOn = true, cause = e)
        }
        return dag.head()
    }

    /**
     * Drops the parts of cross-namespace groups that never committed, and every commit descending
     * from one - the rule Go's replay applies (go/kdb/embed/delta_replay.go, groupGate). Nothing
     * descending from an undecided part was ever acknowledged, and dropping by ancestry rather
     * than log position keeps the answer the same on every later open.
     */
    internal fun withoutUncommittedGroups(
        dag: CommitDag,
        commits: List<KdbCommit>,
        decisions: CrossNamespaceDecisions?,
    ): List<KdbCommit> {
        if (decisions == null) return commits
        val dropped = HashSet<KdbHash>()
        for (c in commits) {
            if (decisions.resolve(c) == CrossNamespaceDecisions.Decision.ABORTED) dropped += c.hash
        }
        if (dropped.isEmpty()) return commits
        // Descendants, to a fixpoint: a commit can appear before its parent in the log.
        var grew = true
        while (grew) {
            grew = false
            for (c in commits) {
                if (c.hash !in dropped && c.parentHashes.any { it in dropped }) {
                    dropped += c.hash
                    grew = true
                }
            }
        }
        System.err.println(
            "kdb: namespace ${dag.namespaceId}: rolled back ${dropped.size} commit(s) belonging to or built on " +
                "cross-namespace transactions that never committed",
        )
        return commits.filter { it.hash !in dropped }
    }

    /**
     * Applies commits in dependency order: a commit is applied only once
     * every one of its parents has already been applied (or was already
     * present in dag, e.g. from a prior replay). This is what makes
     * replay correct independent of the order commits are handed to it
     * in - see [replay]'s doc comment point 1.
     */
    private suspend fun applyCommitsTopologically(
        dag: CommitDag,
        storage: StorageAdapter,
        commits: List<KdbCommit>,
    ) {
        var pending = commits.filterNot { dag.hasCommit(it.hash) }
        while (pending.isNotEmpty()) {
            val next = mutableListOf<KdbCommit>()
            var progressed = false
            for (commit in pending) {
                val ready = commit.parentHashes.all { dag.hasCommit(it) }
                if (!ready) {
                    next += commit
                    continue
                }
                applyOne(dag, storage, commit)
                progressed = true
            }
            if (!progressed) {
                throw MissingParentsException(
                    "kdb: namespace ${dag.namespaceId}: delta replay: ${next.size} commit(s) " +
                        "reference parent commits never found in the log - the log is missing data " +
                        "(first unresolved: ${next.first().hash})",
                )
            }
            pending = next
        }
    }

    private suspend fun applyOne(
        dag: CommitDag,
        storage: StorageAdapter,
        commit: KdbCommit,
    ) {
        applyOps(storage, dag.namespaceId, commit)
        val parentTreeHash =
            commit.parentHashes.firstOrNull()
                ?: DocumentTree.EMPTY.treeHash
        val tree = storage.commitTree(dag.namespaceId, parentTreeHash)
        dag.putDocumentTree(tree)
        dag.putCommit(commit, requireParents = true)
        dag.setHead("main", commit.hash)
    }

    private suspend fun applyOps(
        storage: StorageAdapter,
        namespaceId: String,
        commit: KdbCommit,
    ) {
        for (op in commit.operations) {
            when (op) {
                is KdbOp.Write -> {
                    val doc =
                        runCatching {
                            KdbDocument.fromJson(op.docId, op.patch)
                        }.getOrElse {
                            KdbDocument(op.docId, op.patch)
                        }
                    storage.putDocument(namespaceId, doc)
                }
                is KdbOp.Delete -> storage.deleteDocument(namespaceId, op.docId)
                else -> Unit
            }
        }
    }
}
