package dev.kdb.storage

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbDocument

/**
 * Core storage interface for document + tree reads/writes ([Component 9] §C).
 * Physical engines implement this in Layer 4a/b.
 */
public interface StorageAdapter {

    public val capabilities: StorageCapabilitySet

    public suspend fun getDocument(
        namespaceId: String,
        docId: KdbUuid,
        atCommit: KdbHash,
    ): KdbDocument?

    public suspend fun getDocumentOrThrow(
        namespaceId: String,
        docId: KdbUuid,
        atCommit: KdbHash,
    ): KdbDocument

    public suspend fun getDocuments(
        namespaceId: String,
        docIds: List<KdbUuid>,
        atCommit: KdbHash,
    ): List<KdbDocument?>

    public suspend fun scanDocuments(
        namespaceId: String,
        atCommit: KdbHash,
        batchSize: Int = 256,
        onBatch: suspend (List<KdbDocument>) -> Unit,
    )

    public suspend fun putDocument(
        namespaceId: String,
        document: KdbDocument,
    )

    public suspend fun deleteDocument(
        namespaceId: String,
        docId: KdbUuid,
    )

    public suspend fun commitTree(
        namespaceId: String,
        parentTreeHash: KdbHash,
    ): DocumentTree

    public suspend fun flush(namespaceId: String)

    public suspend fun readBlob(contentHash: KdbHash): ByteArray?

    public suspend fun writeBlob(bytes: ByteArray): KdbHash

    public suspend fun ingestDeltaSegment(segment: DeltaSegmentRef)
}
