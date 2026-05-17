package dev.kdb.file

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.storage.StorageAdapter

public suspend fun commitAttachmentTransaction(
    namespaceId: String,
    dag: CommitDag,
    storage: StorageAdapter,
    documents: List<KdbDocument>,
    fileWriteOps: List<KdbOp.FileWrite> = emptyList(),
    message: String = "",
): KdbCommit {
    require(documents.isNotEmpty()) { "commit requires at least one document" }
    for (doc in documents) {
        storage.putDocument(namespaceId, doc)
    }
    val parent = dag.head()
    val parentTree = dag.getCommitOrThrow(parent).documentTreeHash
    val tree = storage.commitTree(namespaceId, parentTree)
    val writeOps = documents.map { KdbOp.Write(it.id, it.json) }
    val tx =
        KdbTransaction(
            id = KdbUuid.random(),
            baseVersion = parent,
            operations = writeOps + fileWriteOps,
            timestamp = KdbTimestamp.now(),
            authorNodeId = KdbUuid.random(),
        )
    val commit = dag.appendCommit(tx, parent, tree, null, message)
    storage.flush(namespaceId)
    return commit
}
