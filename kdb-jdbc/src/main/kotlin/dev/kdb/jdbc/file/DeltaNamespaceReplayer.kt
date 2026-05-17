package dev.kdb.jdbc.file

import dev.kdb.codec.KdbHash
import dev.kdb.dag.CommitDag
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.storage.DeltaSegmentReader
import dev.kdb.storage.StorageAdapter
public object DeltaNamespaceReplayer {
    public suspend fun replay(
        dag: CommitDag,
        storage: StorageAdapter,
        deltaReader: DeltaSegmentReader,
    ): KdbHash {
        val segments = deltaReader.listSegments().sortedBy { it.segmentId.toString() }
        val commits = mutableListOf<KdbCommit>()
        for (segment in segments) {
            val records = deltaReader.readAll(segment)
            for (record in records) {
                commits += KdbCommit.fromPayloadBytes(record.commitPayload)
            }
        }
        for (commit in commits) {
            if (dag.hasCommit(commit.hash)) continue
            applyOps(storage, dag.namespaceId, commit)
            val parentTreeHash =
                commit.parentHashes.firstOrNull()
                    ?: DocumentTree.EMPTY.treeHash
            val tree = storage.commitTree(dag.namespaceId, parentTreeHash)
            dag.putDocumentTree(tree)
            dag.putCommit(commit, requireParents = true)
            dag.setHead("main", commit.hash)
        }
        return dag.head()
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
