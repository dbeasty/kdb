package dev.kdb.inspect

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.index.IndexHint
import dev.kdb.index.IndexHintAction
import dev.kdb.index.IndexKey
import dev.kdb.index.IndexType
import dev.kdb.storage.DeltaAuthorshipEnvelope
import dev.kdb.storage.DeltaRecord
import dev.kdb.storage.DocumentPatch
import dev.kdb.wire.DeltaCommitPayload
import dev.kdb.wire.WireMessage
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

private val json =
    Json {
        ignoreUnknownKeys = true
        encodeDefaults = true
    }

public object InspectJson {
    public fun commitToJsonLine(commit: KdbCommit): String =
        json.encodeToString(commitDto(commit))

    public fun deltaRecordToJsonLine(
        record: DeltaRecord,
        segmentId: KdbUuid,
        offset: Long,
    ): String =
        json.encodeToString(
            DeltaJsonLine(
                type = "delta",
                timestamp = KdbTimestamp.now().toEpochMicros(),
                namespaceId = record.namespaceId,
                segmentId = segmentId.toString(),
                offset = offset,
                commitHash = record.commitHash.toHex(),
                commit = commitDto(KdbCommit.fromPayloadBytes(record.commitPayload)),
                patches = record.documentPatches.map { patchDto(it) },
            ),
        )

    public fun wireMessageToJsonLine(
        message: WireMessage,
        direction: String,
    ): String =
        json.encodeToString(
            WireJsonLine(
                type = "wire",
                timestamp = KdbTimestamp.now().toEpochMicros(),
                direction = direction,
                messageType = message.header.messageType.name,
                correlationId = message.header.correlationId,
                summary = wireSummary(message),
            ),
        )

    internal fun commitDto(commit: KdbCommit): CommitJsonDto =
        CommitJsonDto(
            hash = commit.hash.toHex(),
            namespaceId = commit.namespaceId,
            transactionId = commit.transactionId.toString(),
            timestampMicros = commit.timestamp.toEpochMicros(),
            authorNodeId = commit.authorNodeId.toString(),
            parentHashes = commit.parentHashes.map { it.toHex() },
            documentTreeHash = commit.documentTreeHash.toHex(),
            schemaHash = commit.schemaHash?.toHex(),
            message = commit.message,
            operations = commit.operations.map { opDto(it) },
        )

    internal fun patchDto(patch: DocumentPatch): PatchJsonDto =
        PatchJsonDto(
            docId = patch.docId.toString(),
            before = patch.before?.json,
            after = patch.after?.json,
            contentHashAfter = patch.contentHashAfter?.toHex(),
        )

    internal fun opDto(op: KdbOp): OpJsonDto =
        when (op) {
            is KdbOp.Write ->
                OpJsonDto(
                    kind = "write",
                    docId = op.docId.toString(),
                    patch = op.patch,
                )
            is KdbOp.Delete ->
                OpJsonDto(
                    kind = "delete",
                    docId = op.docId.toString(),
                )
            is KdbOp.FileWrite ->
                OpJsonDto(
                    kind = "fileWrite",
                    path = op.path,
                    blobHash = op.blobHash.toHex(),
                )
            is KdbOp.SchemaMigration ->
                OpJsonDto(
                    kind = "schemaMigration",
                    migrationId = op.migrationId.toString(),
                    migrationPayload = op.migrationPayload,
                )
        }

    internal fun hintDto(hint: IndexHint): IndexHintJsonDto =
        IndexHintJsonDto(
            indexId = hint.indexId.toString(),
            fieldName = hint.fieldName,
            indexType = hint.type.name,
            action = hint.action.name,
            docId = hint.docId.toString(),
            key = hint.key?.let { indexKeyLabel(it) },
            commitHash = hint.commitHash.toHex(),
        )

    private fun indexKeyLabel(key: IndexKey): String =
        when (key) {
            IndexKey.NullKey -> "null"
            is IndexKey.BoolKey -> key.value.toString()
            is IndexKey.Int32Key -> key.value.toString()
            is IndexKey.Int64Key -> key.value.toString()
            is IndexKey.Float64Key -> key.value.toString()
            is IndexKey.TimestampKey -> key.epochMillis.toString()
            is IndexKey.StringKey -> key.value
            is IndexKey.UuidKey -> key.id.toString()
            is IndexKey.VectorKey -> "vector(${key.asFloatArray().size})"
            is IndexKey.CompositeKey -> "composite(${key.parts.size})"
        }

    private fun wireSummary(message: WireMessage): String =
        when (message) {
            is WireMessage.Handshake -> "handshake node=${message.request.nodeId}"
            is WireMessage.HandshakeAck -> "handshakeAck accepted=${message.response.accepted}"
            is WireMessage.DeltaCommit ->
                "deltaCommit hash=${message.payload.commitHash.toHex()} ops=${message.payload.operations.size}"
            is WireMessage.PositionAck -> "positionAck ${message.commitHash.toHex()}"
            is WireMessage.CompactionNotice -> "compaction ${message.intent.boundary.toHex()}"
            is WireMessage.IceArchiveNotice -> "ice ${message.originalHash.toHex()}"
            else -> message.header.messageType.name
        }
}

@Serializable
internal data class DeltaJsonLine(
    val type: String,
    val timestamp: Long,
    val namespaceId: String,
    val segmentId: String,
    val offset: Long,
    val commitHash: String,
    val commit: CommitJsonDto,
    val patches: List<PatchJsonDto>,
)

@Serializable
internal data class WireJsonLine(
    val type: String,
    val timestamp: Long,
    val direction: String,
    val messageType: String,
    val correlationId: Int,
    val summary: String,
)

@Serializable
internal data class CommitJsonDto(
    val hash: String,
    val namespaceId: String,
    val transactionId: String,
    val timestampMicros: Long,
    val authorNodeId: String,
    val parentHashes: List<String>,
    val documentTreeHash: String,
    val schemaHash: String?,
    val message: String,
    val operations: List<OpJsonDto>,
)

@Serializable
internal data class PatchJsonDto(
    val docId: String,
    val before: String?,
    val after: String?,
    val contentHashAfter: String?,
)

@Serializable
internal data class OpJsonDto(
    val kind: String,
    val docId: String? = null,
    val patch: String? = null,
    val path: String? = null,
    val blobHash: String? = null,
    val migrationId: String? = null,
    val migrationPayload: String? = null,
)

@Serializable
internal data class IndexHintJsonDto(
    val indexId: String,
    val fieldName: String,
    val indexType: String,
    val action: String,
    val docId: String,
    val key: String?,
    val commitHash: String,
)

public fun DeltaCommitPayload.toInspectJson(): String =
    json.encodeToString(
        mapOf(
            "namespace" to namespace,
            "commitHash" to commitHash.toHex(),
            "parentHash" to parentHash.toHex(),
            "timestampMicros" to timestampMicros,
            "operations" to operations.map { InspectJson.opDto(it) },
            "indexHints" to indexHints.map { InspectJson.hintDto(it) },
        ),
    )
