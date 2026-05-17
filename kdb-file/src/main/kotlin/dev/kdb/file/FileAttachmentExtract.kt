package dev.kdb.file

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.document.KdbDocument
import dev.kdb.storage.StorageAdapter

public object FileAttachmentExtract {
    public suspend fun metadataAtHead(
        namespaceId: String,
        dag: CommitDag,
        storage: StorageAdapter,
        fileId: KdbUuid,
    ): KdbDocument {
        val treeHash = dag.getCommitOrThrow(dag.head()).documentTreeHash
        return storage.getDocument(namespaceId, fileId, treeHash)
            ?: throw IllegalArgumentException("file metadata not found: $fileId")
    }

    public suspend fun readFileBytes(
        namespaceId: String,
        dag: CommitDag,
        storage: StorageAdapter,
        fileId: KdbUuid,
    ): ByteArray {
        val doc = metadataAtHead(namespaceId, dag, storage, fileId)
        val meta = FileMetadata.fromDocument(doc)
        val blob =
            storage.readBlob(meta.blobHashValue())
                ?: throw IllegalArgumentException("blob missing for file ${meta.fileId}: ${meta.blobHash}")
        return decodePayload(blob, FileEncoding.fromWire(meta.encoding), meta.name)
    }

    public suspend fun readBundleArchiveBytes(
        namespaceId: String,
        dag: CommitDag,
        storage: StorageAdapter,
        bundleId: KdbUuid,
    ): ByteArray {
        val doc = metadataAtHead(namespaceId, dag, storage, bundleId)
        val bundle = BundleMetadata.fromDocument(doc)
        val blob =
            storage.readBlob(bundle.blobHashValue())
                ?: throw IllegalArgumentException("blob missing for bundle ${bundle.bundleId}")
        return blob
    }

    public suspend fun readBundleMemberBytes(
        namespaceId: String,
        dag: CommitDag,
        storage: StorageAdapter,
        bundleId: KdbUuid,
        memberFileId: KdbUuid,
    ): ByteArray {
        val bundleDoc = metadataAtHead(namespaceId, dag, storage, bundleId)
        val bundle = BundleMetadata.fromDocument(bundleDoc)
        val member =
            bundle.members.find { it.fileId == memberFileId.toString() }
                ?: throw IllegalArgumentException("bundle ${bundle.bundleId} has no member $memberFileId")
        val archive =
            storage.readBlob(bundle.blobHashValue())
                ?: throw IllegalArgumentException("bundle blob missing")
        return when (FileEncoding.fromWire(bundle.encoding)) {
            FileEncoding.ZIP -> ZipArchive.extractEntry(archive, member.pathInBundle)
            FileEncoding.RAW -> archive
        }
    }

    private fun decodePayload(
        blob: ByteArray,
        encoding: FileEncoding,
        entryName: String,
    ): ByteArray =
        when (encoding) {
            FileEncoding.RAW -> blob
            FileEncoding.ZIP -> ZipArchive.soleEntryBytes(blob)
        }
}
