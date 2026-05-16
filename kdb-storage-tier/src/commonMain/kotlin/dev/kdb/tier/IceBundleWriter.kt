package dev.kdb.tier

import dev.kdb.codec.KdbHash
import dev.kdb.codec.encodeToBytes
import dev.kdb.compression.ZstdCompression
import dev.kdb.dag.CommitDag
import dev.kdb.document.DocumentTreeWireType
import dev.kdb.document.toKdbValue
import dev.kdb.document.KdbDocumentWireRegistry
import dev.kdb.document.kdbSha256
import dev.kdb.error.ArchiveRestoreException
import dev.kdb.error.VersionNotFoundException
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.toBytes
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

public interface IceBundleWriter {
    public suspend fun writeBundle(
        dag: CommitDag,
        commit: KdbHash,
        namespaceId: String,
        schema: KdbSchema?,
        backend: TierBackend,
    ): IceBundleArtifact
}

public class DefaultIceBundleWriter : IceBundleWriter {
    private val json = Json { ignoreUnknownKeys = true }

    override suspend fun writeBundle(
        dag: CommitDag,
        commit: KdbHash,
        namespaceId: String,
        schema: KdbSchema?,
        backend: TierBackend,
    ): IceBundleArtifact {
        val kcommit =
            dag.getCommit(commit)
                ?: throw VersionNotFoundException("commit not found", namespaceId, commit.toHex())
        val tree =
            dag.getDocumentTreeOrThrow(kcommit.documentTreeHash)
        val reg = KdbDocumentWireRegistry()
        val treeBytes = tree.toKdbValue().encodeToBytes(DocumentTreeWireType, reg)
        val schemaBytes = schema?.toBytes()
        val manifest =
            IceBundleManifest(
                formatVersion = 1,
                namespaceId = namespaceId,
                commitHashHex = commit.toHex(),
                commitPayloadHex = kcommit.toPayloadBytes().toHexString(),
                documentTreeBytesHex = treeBytes.toHexString(),
                schemaBytesHex = schemaBytes?.toHexString(),
                indexSnapshotsHex = null,
            )
        val body = json.encodeToString(manifest).encodeToByteArray()
        val compressed = ZstdCompression.compress(body)
        val contentHash = KdbHash.fromBytes(kdbSha256(compressed))
        val key = "$namespaceId/${commit.toHex().take(16)}.kdbice"
        val location = backend.put(key, compressed)
        return IceBundleArtifact(location, contentHash, compressed.size.toLong())
    }

    public suspend fun readBundle(
        location: String,
        backend: TierBackend,
        verifyBundle: Boolean,
    ): IceBundleManifest {
        val bytes = backend.get(location)
        val decompressed =
            try {
                ZstdCompression.decompress(bytes)
            } catch (_: Throwable) {
                bytes
            }
        val manifest = json.decodeFromString<IceBundleManifest>(decompressed.decodeToString())
        if (verifyBundle) {
            if (manifest.formatVersion != 1) {
                throw ArchiveRestoreException("unsupported bundle version", location)
            }
        }
        return manifest
    }
}

@Serializable
public data class IceBundleManifest(
    val formatVersion: Int,
    val namespaceId: String,
    val commitHashHex: String,
    val commitPayloadHex: String,
    val documentTreeBytesHex: String,
    val schemaBytesHex: String? = null,
    val indexSnapshotsHex: String? = null,
)

private fun ByteArray.toHexString(): String = joinToString("") { b -> "%02x".format(b.toInt() and 0xFF) }

private fun String.decodeHex(): ByteArray {
    require(length % 2 == 0)
    return ByteArray(length / 2) { i ->
        substring(i * 2, i * 2 + 2).toInt(16).toByte()
    }
}

internal fun IceBundleManifest.commitPayloadBytes(): ByteArray = commitPayloadHex.decodeHex()

internal fun IceBundleManifest.documentTreeBytes(): ByteArray = documentTreeBytesHex.decodeHex()
