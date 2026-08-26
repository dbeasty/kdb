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
import dev.kdb.storage.delta.DeltaSegmentScanner

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
 *  2. Corruption in the most recently written segment is tolerated as an
 *     expected torn tail; corruption anywhere else is not (§4.3).
 */
public object DeltaNamespaceReplayer {
    public suspend fun replay(
        dag: CommitDag,
        storage: StorageAdapter,
        deltaReader: DeltaSegmentReader,
    ): KdbHash {
        val segments = deltaReader.listSegments()
        val allCommits = mutableListOf<KdbCommit>()
        segments.forEachIndexed { i, segment ->
            val isMostRecent = i == segments.lastIndex
            val records =
                try {
                    deltaReader.readAll(segment)
                } catch (e: DeltaSegmentScanner.CorruptFrameException) {
                    if (!isMostRecent) {
                        throw IllegalStateException(
                            "kdb: namespace ${dag.namespaceId}: delta segment (sequence " +
                                "${segment.sequenceNumber}) is corrupt and is not the most recently " +
                                "written segment, so this is not an expected torn tail - data may be " +
                                "unrecoverable: ${e.message}",
                            e,
                        )
                    }
                    // Torn tail on the most recently written segment: the expected shape of an
                    // unclean shutdown. Keep every commit scanned before it and continue - this
                    // segment is never appended to again, so this decision is stable across
                    // future restarts too.
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
        applyCommitsTopologically(dag, storage, allCommits)
        return dag.head()
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
                throw IllegalStateException(
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
