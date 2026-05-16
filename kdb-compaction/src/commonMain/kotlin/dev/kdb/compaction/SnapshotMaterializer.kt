package dev.kdb.compaction

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.document.DocumentTree
import dev.kdb.storage.StorageAdapter

public interface SnapshotMaterializer {
    public suspend fun materializeAt(commit: KdbHash): DocumentTree
}

public class DefaultSnapshotMaterializer(
    private val storage: StorageAdapter,
    private val dag: CommitDag,
    private val namespaceId: String,
) : SnapshotMaterializer {
    override suspend fun materializeAt(commit: KdbHash): DocumentTree {
        try {
            val c = dag.getCommitOrThrow(commit)
            val tree = dag.getDocumentTreeOrThrow(c.documentTreeHash)
            val entries = linkedMapOf<KdbUuid, KdbHash>()
            for ((docId, contentHash) in tree.entries) {
                val doc =
                    storage.getDocument(namespaceId, docId, c.documentTreeHash)
                        ?: continue
                entries[docId] = doc.contentHash
            }
            return DocumentTree.build(entries)
        } catch (e: Exception) {
            throw SnapshotMaterializationException(commit, e)
        }
    }
}
