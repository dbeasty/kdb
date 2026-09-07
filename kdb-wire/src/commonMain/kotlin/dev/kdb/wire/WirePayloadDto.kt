package dev.kdb.wire

import kotlinx.serialization.Serializable

@Serializable
internal data class WirePayloadEnvelope(
    val kind: String,
    val handshake: HandshakeDto? = null,
    val handshakeAck: HandshakeAckDto? = null,
    val deltaCommit: DeltaCommitDto? = null,
    val commitFetch: CommitFetchDto? = null,
    val commitPush: CommitPushDto? = null,
    val dagDiff: DagDiffDto? = null,
    val transactionReplay: TransactionReplayDto? = null,
    val conflictReport: ConflictReportDto? = null,
    val compactionNotice: CompactionNoticeDto? = null,
    val iceArchiveNotice: IceArchiveNoticeDto? = null,
    val snapshotRequest: SnapshotRequestDto? = null,
    val snapshotResponse: SnapshotResponseDto? = null,
    val positionAck: PositionAckDto? = null,
    val schemaPush: SchemaPushDto? = null,
    val sessionBegin: SessionBeginDto? = null,
    val sessionBeginAck: SessionBeginAckDto? = null,
    val sqlExec: SqlExecDto? = null,
    val sqlResult: SqlResultDto? = null,
    val txCommit: TxCommitDto? = null,
    val txRollback: TxRollbackDto? = null,
    val documentGet: DocumentGetDto? = null,
    val documentGetResult: DocumentGetResultDto? = null,
    val upsert: UpsertDto? = null,
    val upsertResult: UpsertResultDto? = null,
    val search: SearchDto? = null,
    val searchResult: SearchResultDto? = null,
)

// Layer 16 Component 69 - shapes follow the §11 body table (camelCase, optional fields omitted).
@Serializable
internal data class SearchTextArmDto(
    val index: String,
    val query: String,
    val depth: Int? = null,
    val minScore: Float? = null,
    val weight: Double? = null,
)

@Serializable
internal data class SearchVectorArmDto(
    val index: String,
    val vector: List<Double>,
    val depth: Int? = null,
    val minScore: Float? = null,
    val weight: Double? = null,
)

@Serializable
internal data class SearchDto(
    val namespace: String,
    val sessionId: String? = null,
    val text: SearchTextArmDto? = null,
    val vector: SearchVectorArmDto? = null,
    val fusion: String? = null,
    val limit: Int,
    val includeJson: Boolean = false,
    val atCommitHex: String? = null,
)

@Serializable
internal data class SearchHitDto(
    val docId: String,
    val score: Float,
    val json: String? = null,
)

@Serializable
internal data class SearchResultDto(
    val namespace: String,
    val hits: List<SearchHitDto> = emptyList(),
    val resolvedCommitHex: String,
    val error: String? = null,
    val errorCode: String? = null,
    val retryAfterMs: Int? = null,
)

// Component 40 direct-document ops - shapes mirror go/kdb/wire/document_ops.go's DTOs exactly.
@Serializable
internal data class DocumentGetDto(
    val namespace: String,
    val docId: String,
)

@Serializable
internal data class DocumentGetResultDto(
    val namespace: String,
    val docId: String,
    val json: String? = null,
    val commitHex: String,
    val error: String? = null,
    val errorCode: String? = null,
    val retryAfterMs: Int? = null,
)

@Serializable
internal data class UpsertDto(
    val namespace: String,
    val docId: String,
    val json: String,
)

@Serializable
internal data class UpsertResultDto(
    val namespace: String,
    val commitHex: String,
    val error: String? = null,
    val errorCode: String? = null,
    val retryAfterMs: Int? = null,
)

@Serializable
internal data class HandshakeDto(
    val nodeId: String,
    val namespaces: List<String>,
    val localHeads: Map<String, String>,
    val supportsZstd: Boolean,
    val supportsIndexHints: Boolean,
    val supportsDirectDeltaIngest: Boolean,
    val maxFrameBytes: Int,
    val preferredEncodings: List<String>,
    val clientMode: String,
    val protocolVersion: Int,
)

@Serializable
internal data class HandshakeAckDto(
    val accepted: Boolean,
    val negotiatedEncoding: String,
    val protocolVersion: Int,
    val remoteHeads: Map<String, String>,
    val rejectionReason: String? = null,
)

@Serializable
internal data class DeltaCommitDto(
    val namespace: String,
    val commitHashHex: String,
    val parentHashHex: String,
    val timestampMicros: Long,
    val operations: List<OpDto> = emptyList(),
    val indexHints: List<IndexHintDto> = emptyList(),
    val schemaDeltaBytes: ByteArray? = null,
)

@Serializable
internal data class CommitFetchDto(
    val namespace: String,
    val sinceHashHex: String?,
    val maxCommits: Int,
)

@Serializable
internal data class CommitPushDto(
    val namespace: String,
    val commitsPayload: ByteArray,
)

@Serializable
internal data class DagDiffDto(
    val namespace: String,
    val localHeadHex: String,
    val remoteHeadHex: String,
)

@Serializable
internal data class TransactionReplayDto(
    val namespace: String,
    val baseVersionHex: String,
    val transactionBytes: ByteArray,
)

@Serializable
internal data class ConflictReportDto(
    val namespace: String,
    val reportBytes: ByteArray,
    val errorCode: String? = null,
    val retryAfterMs: Int? = null,
)

@Serializable
internal data class CompactionNoticeDto(
    val namespaceId: String,
    val boundaryHex: String,
    val issuedAtMillis: Long,
)

@Serializable
internal data class IceArchiveNoticeDto(
    val namespace: String,
    val originalHashHex: String,
    val archiveLocation: String,
    val bundleHashHex: String,
)

@Serializable
internal data class SnapshotRequestDto(
    val namespace: String,
    val anchorHashHex: String?,
)

@Serializable
internal data class SnapshotResponseDto(
    val namespace: String,
    val anchorHashHex: String,
    val snapshotBytes: ByteArray,
    val compressed: Boolean,
)

@Serializable
internal data class PositionAckDto(
    val namespace: String,
    val commitHashHex: String,
)

@Serializable
internal data class SchemaPushDto(
    val namespace: String,
    val schemaBytes: ByteArray,
    val revision: Long,
)

@Serializable
internal data class SessionBeginDto(
    val namespace: String,
    val sessionId: String? = null,
    val readConsistency: String,
    val baseVersionHex: String? = null,
)

@Serializable
internal data class SessionBeginAckDto(
    val namespace: String,
    val sessionId: String,
    val headHex: String,
    val readConsistency: String,
    // Explains a rejected session begin (empty sessionId). Nullable/defaulted so frames from
    // peers that predate the field still decode; Go's sessionBeginAckDto carries the same
    // optional field (coordinated change - kdb-finish-up-plan Phase 2.7).
    val error: String? = null,
)

@Serializable
internal data class SqlExecDto(
    val namespace: String,
    val sessionId: String,
    val sql: String,
    val parametersJson: String? = null,
)

@Serializable
internal data class SqlResultDto(
    val namespace: String,
    val sessionId: String,
    val columns: List<String> = emptyList(),
    val rows: List<List<String>> = emptyList(),
    val rowsAffected: Int = 0,
    val resolvedCommitHex: String = "",
    val readOnly: Boolean = true,
    val error: String? = null,
    val generatedIds: List<String> = emptyList(),
    val errorCode: String? = null,
    val retryAfterMs: Int? = null,
)

@Serializable
internal data class TxCommitDto(
    val namespace: String,
    val sessionId: String,
    val transactionBytes: ByteArray,
)

@Serializable
internal data class TxRollbackDto(
    val namespace: String,
    val sessionId: String,
)
