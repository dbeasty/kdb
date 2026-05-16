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
    val operationsPayload: ByteArray,
    val indexHintsPayload: ByteArray,
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
