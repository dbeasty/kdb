package dev.kdb.file

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.storage.StorageAdapter
import java.nio.file.Path
import kotlin.io.path.readBytes

public data class FileIngestInput(
    val bytes: ByteArray,
    val name: String,
    val pathInBundle: String = name,
)

public data class FileIngestResult(
    val fileId: KdbUuid,
    val blobHash: KdbHash,
    val commit: KdbCommit,
)

public data class BundleIngestResult(
    val bundleId: KdbUuid,
    val blobHash: KdbHash,
    val memberFileIds: List<KdbUuid>,
    val commit: KdbCommit,
)

public object FileAttachmentIngest {
    public suspend fun ingestSingle(
        namespaceId: String,
        dag: CommitDag,
        storage: StorageAdapter,
        bytes: ByteArray,
        name: String,
        fileId: KdbUuid = KdbUuid.random(),
        namespacePath: String? = null,
        zip: Boolean = false,
        mimeType: String? = guessMimeType(name),
        message: String = "",
    ): FileIngestResult {
        val encoding = if (zip) FileEncoding.ZIP else FileEncoding.RAW
        val payload =
            when (encoding) {
                FileEncoding.RAW -> bytes
                FileEncoding.ZIP -> ZipArchive.zipSingle(name, bytes)
            }
        val blobHash = storage.writeBlob(payload)
        val meta =
            FileMetadata(
                fileId = fileId.toString(),
                name = name,
                path = namespacePath,
                mimeType = mimeType,
                encoding = encoding.wireName(),
                blobHash = blobHash.toHex(),
                sizeBytes = bytes.size.toLong(),
                compressedSizeBytes = if (encoding == FileEncoding.ZIP) payload.size.toLong() else null,
                createdAt = isoTimestampNow(),
            )
        val fileWrites =
            if (namespacePath != null) {
                listOf(KdbOp.FileWrite(namespacePath, blobHash))
            } else {
                emptyList()
            }
        val commit =
            commitAttachmentTransaction(
                namespaceId,
                dag,
                storage,
                listOf(meta.toDocument()),
                fileWrites,
                message,
            )
        return FileIngestResult(fileId, blobHash, commit)
    }

    public suspend fun ingestSingleFromPath(
        namespaceId: String,
        dag: CommitDag,
        storage: StorageAdapter,
        path: Path,
        fileId: KdbUuid = KdbUuid.random(),
        namespacePath: String? = null,
        zip: Boolean = false,
        message: String = "",
    ): FileIngestResult {
        val name = path.fileName?.toString() ?: path.toString()
        return ingestSingle(
            namespaceId,
            dag,
            storage,
            path.readBytes(),
            name,
            fileId,
            namespacePath ?: name,
            zip,
            guessMimeType(name),
            message,
        )
    }

    /** Mode A: one ZIP blob containing all members; per-member [kdb.file] metadata docs. */
    public suspend fun ingestBundleZip(
        namespaceId: String,
        dag: CommitDag,
        storage: StorageAdapter,
        inputs: List<FileIngestInput>,
        bundleId: KdbUuid = KdbUuid.random(),
        bundleName: String = "bundle-${bundleId.toString().take(8)}.zip",
        message: String = "",
    ): BundleIngestResult {
        require(inputs.isNotEmpty()) { "bundle requires at least one file" }
        val zipEntries =
            inputs.map { ZipEntryPayload(it.pathInBundle, it.bytes) }
        val zipBytes = ZipArchive.zip(zipEntries)
        val blobHash = storage.writeBlob(zipBytes)
        val totalSize = inputs.sumOf { it.bytes.size.toLong() }
        val memberRefs = mutableListOf<FileMemberRef>()
        val memberDocs = mutableListOf<KdbDocument>()
        val memberIds = mutableListOf<KdbUuid>()
        for (input in inputs) {
            val memberId = KdbUuid.random()
            memberIds += memberId
            memberRefs +=
                FileMemberRef(
                    fileId = memberId.toString(),
                    name = input.name,
                    pathInBundle = input.pathInBundle,
                    sizeBytes = input.bytes.size.toLong(),
                )
            memberDocs +=
                FileMetadata(
                    fileId = memberId.toString(),
                    name = input.name,
                    encoding = FileEncoding.ZIP.wireName(),
                    blobHash = blobHash.toHex(),
                    sizeBytes = input.bytes.size.toLong(),
                    compressedSizeBytes = null,
                    bundleId = bundleId.toString(),
                    createdAt = isoTimestampNow(),
                ).toDocument()
        }
        val bundleMeta =
            BundleMetadata(
                bundleId = bundleId.toString(),
                name = bundleName,
                encoding = FileEncoding.ZIP.wireName(),
                blobHash = blobHash.toHex(),
                sizeBytes = totalSize,
                compressedSizeBytes = zipBytes.size.toLong(),
                memberCount = inputs.size,
                members = memberRefs,
                createdAt = isoTimestampNow(),
            ).toDocument()
        val commit =
            commitAttachmentTransaction(
                namespaceId,
                dag,
                storage,
                listOf(bundleMeta) + memberDocs,
                fileWriteOps = emptyList(),
                message = message,
            )
        return BundleIngestResult(bundleId, blobHash, memberIds, commit)
    }

    public suspend fun ingestBundleFromPaths(
        namespaceId: String,
        dag: CommitDag,
        storage: StorageAdapter,
        paths: List<Path>,
        bundleId: KdbUuid = KdbUuid.random(),
        zip: Boolean = true,
        message: String = "",
    ): BundleIngestResult {
        val inputs =
            paths.map { p ->
                val name = p.fileName?.toString() ?: p.toString()
                FileIngestInput(p.readBytes(), name, name)
            }
        return if (zip && inputs.size > 1) {
            ingestBundleZip(namespaceId, dag, storage, inputs, bundleId, message = message)
        } else if (inputs.size == 1) {
            val single =
                ingestSingle(
                    namespaceId,
                    dag,
                    storage,
                    inputs.single().bytes,
                    inputs.single().name,
                    bundleId,
                    zip = zip,
                    message = message,
                )
            BundleIngestResult(
                bundleId = single.fileId,
                blobHash = single.blobHash,
                memberFileIds = listOf(single.fileId),
                commit = single.commit,
            )
        } else {
            ingestBundleZip(namespaceId, dag, storage, inputs, bundleId, message = message)
        }
    }
}
