package dev.kdb.file

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbDocument
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.time.Instant
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

public const val KDB_KIND_FILE: String = "kdb.file"
public const val KDB_KIND_BUNDLE: String = "kdb.file.bundle"

@Serializable
public data class FileMemberRef(
    val fileId: String,
    val name: String,
    val pathInBundle: String,
    val sizeBytes: Long,
    val blobHash: String? = null,
)

@Serializable
public data class FileMetadata(
    @SerialName("kdbKind") val kdbKind: String = KDB_KIND_FILE,
    val fileId: String,
    val name: String,
    val path: String? = null,
    val mimeType: String? = null,
    val encoding: String,
    val blobHash: String,
    val sizeBytes: Long,
    val compressedSizeBytes: Long? = null,
    val bundleId: String? = null,
    val createdAt: String? = null,
) {
    public fun toDocument(): KdbDocument =
        KdbDocument(KdbUuid.fromString(fileId), FileMetadataJson.encoder.encodeToString(FileMetadata.serializer(), this))

    public fun blobHashValue(): KdbHash = KdbHash.fromHex(blobHash)

    public companion object {
        public fun fromDocument(doc: KdbDocument): FileMetadata {
            val el = FileMetadataJson.encoder.parseToJsonElement(doc.json).jsonObject
            val kind = el["kdbKind"]?.jsonPrimitive?.content
            require(kind == KDB_KIND_FILE) { "not a kdb.file document: $kind" }
            return FileMetadataJson.encoder.decodeFromString(FileMetadata.serializer(), doc.json)
        }

        public fun tryFromDocument(doc: KdbDocument): FileMetadata? =
            runCatching { fromDocument(doc) }.getOrNull()
    }
}

@Serializable
public data class BundleMetadata(
    @SerialName("kdbKind") val kdbKind: String = KDB_KIND_BUNDLE,
    val bundleId: String,
    val name: String,
    val encoding: String,
    val blobHash: String,
    val sizeBytes: Long,
    val compressedSizeBytes: Long? = null,
    val memberCount: Int,
    val members: List<FileMemberRef>,
    val createdAt: String? = null,
) {
    public fun toDocument(): KdbDocument =
        KdbDocument(KdbUuid.fromString(bundleId), FileMetadataJson.encoder.encodeToString(BundleMetadata.serializer(), this))

    public fun blobHashValue(): KdbHash = KdbHash.fromHex(blobHash)

    public companion object {
        public fun fromDocument(doc: KdbDocument): BundleMetadata {
            val el = FileMetadataJson.encoder.parseToJsonElement(doc.json).jsonObject
            val kind = el["kdbKind"]?.jsonPrimitive?.content
            require(kind == KDB_KIND_BUNDLE) { "not a kdb.file.bundle document: $kind" }
            return FileMetadataJson.encoder.decodeFromString(BundleMetadata.serializer(), doc.json)
        }

        public fun tryFromDocument(doc: KdbDocument): BundleMetadata? =
            runCatching { fromDocument(doc) }.getOrNull()
    }
}

internal object FileMetadataJson {
    val encoder: Json = Json { ignoreUnknownKeys = true; encodeDefaults = true; explicitNulls = false }
}

internal fun guessMimeType(fileName: String): String? {
    val ext = fileName.substringAfterLast('.', "").lowercase()
    return when (ext) {
        "pdf" -> "application/pdf"
        "json" -> "application/json"
        "png" -> "image/png"
        "jpg", "jpeg" -> "image/jpeg"
        "gif" -> "image/gif"
        "txt" -> "text/plain"
        "csv" -> "text/csv"
        "zip" -> "application/zip"
        "html", "htm" -> "text/html"
        else -> null
    }
}

internal fun isoTimestampNow(): String = Instant.now().toString()
