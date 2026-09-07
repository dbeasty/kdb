package dev.kdb.wire

import dev.kdb.codec.KdbHash
import dev.kdb.compaction.CompactionIntent
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbOp
import dev.kdb.index.IndexHint

public const val KDB_WIRE_PROTOCOL_VERSION: Int = 1

public const val MIN_SUPPORTED_WIRE_PROTOCOL_VERSION: Int = 1

public const val DEFAULT_MAX_FRAME_BYTES: Int = 16 * 1024 * 1024

public data class WireHeader(
    val messageType: WireMessageType,
    val protocolVersion: Int = KDB_WIRE_PROTOCOL_VERSION,
    val correlationId: Int,
    val payloadLength: Int,
)

public enum class WireMessageType(public val code: Short) {
    HANDSHAKE(0x01),
    DELTA_COMMIT(0x02),
    COMMIT_FETCH(0x03),
    COMMIT_PUSH(0x04),
    DAG_DIFF(0x05),
    TRANSACTION_REPLAY(0x06),
    CONFLICT_REPORT(0x07),
    COMPACTION_NOTICE(0x08),
    ICE_ARCHIVE_NOTICE(0x09),
    SNAPSHOT_REQUEST(0x0A),
    SNAPSHOT_RESPONSE(0x0B),
    POSITION_ACK(0x0C),
    SCHEMA_PUSH(0x0D),
    SESSION_BEGIN(0x0E),
    SQL_EXEC(0x0F),
    SQL_RESULT(0x10),
    TX_COMMIT(0x11),
    TX_ROLLBACK(0x12),
    SESSION_BEGIN_ACK(0x13),

    // Component 40 direct-document ops (codes reserved by the Go implementation - see
    // go/kdb/wire/types.go MsgDocumentGet..MsgUpsertResult; they must match exactly).
    DOCUMENT_GET(0x14),
    DOCUMENT_GET_RESULT(0x15),
    UPSERT(0x16),
    UPSERT_RESULT(0x17),

    // Layer 16 Component 69 hybrid search (0x18..0x1C are Go's CommitPushAck and lock ops).
    SEARCH(0x1D),
    SEARCH_RESULT(0x1E),
    ;

    public companion object {
        public fun fromCode(code: Short): WireMessageType? = entries.find { it.code == code }
    }
}

public enum class PayloadEncoding {
    KDB_BINARY,
    JSON,
}

public enum class WireClientMode {
    STREAM_READ_ONLY,
    STREAM_WRITE_BACK,
    FULL_PEER,
    SQL_CLIENT,
}

public data class WireCapabilitySet(
    val supportsZstd: Boolean = true,
    val supportsIndexHints: Boolean = true,
    val supportsDirectDeltaIngest: Boolean = false,
    val maxFrameBytes: Int = DEFAULT_MAX_FRAME_BYTES,
)

public data class HandshakePayload(
    val nodeId: String,
    val namespaces: List<String>,
    val localHeads: Map<String, String>,
    val capabilities: WireCapabilitySet = WireCapabilitySet(),
    val preferredEncodings: List<PayloadEncoding> = listOf(PayloadEncoding.KDB_BINARY, PayloadEncoding.JSON),
    val clientMode: WireClientMode,
    val protocolVersion: Int = KDB_WIRE_PROTOCOL_VERSION,
)

public data class HandshakeAckPayload(
    val accepted: Boolean,
    val negotiatedEncoding: PayloadEncoding,
    val protocolVersion: Int,
    val remoteHeads: Map<String, String> = emptyMap(),
    val rejectionReason: String? = null,
)

public data class DeltaCommitPayload(
    val namespace: String,
    val commitHash: KdbHash,
    val parentHash: KdbHash,
    val timestampMicros: Long,
    val operations: List<KdbOp>,
    val indexHints: List<IndexHint> = emptyList(),
    val schemaDeltaBytes: ByteArray? = null,
) {
    override fun equals(other: Any?): Boolean {
        if (other !is DeltaCommitPayload) return false
        return namespace == other.namespace &&
            commitHash == other.commitHash &&
            parentHash == other.parentHash &&
            timestampMicros == other.timestampMicros &&
            operations == other.operations &&
            indexHints == other.indexHints &&
            (schemaDeltaBytes?.contentEquals(other.schemaDeltaBytes)
                ?: other.schemaDeltaBytes == null)
    }

    override fun hashCode(): Int {
        var result = namespace.hashCode()
        result = 31 * result + commitHash.hashCode()
        result = 31 * result + parentHash.hashCode()
        result = 31 * result + timestampMicros.hashCode()
        result = 31 * result + operations.hashCode()
        result = 31 * result + indexHints.hashCode()
        result = 31 * result + (schemaDeltaBytes?.contentHashCode() ?: 0)
        return result
    }
}

public sealed class WireMessage {
    public abstract val header: WireHeader

    public data class Handshake(
        override val header: WireHeader,
        val request: HandshakePayload,
    ) : WireMessage()

    public data class HandshakeAck(
        override val header: WireHeader,
        val response: HandshakeAckPayload,
    ) : WireMessage()

    public data class DeltaCommit(
        override val header: WireHeader,
        val payload: DeltaCommitPayload,
    ) : WireMessage()

    public data class CommitFetch(
        override val header: WireHeader,
        val namespace: String,
        val sinceHash: KdbHash?,
        val maxCommits: Int = 100,
    ) : WireMessage()

    public data class CommitPush(
        override val header: WireHeader,
        val namespace: String,
        val commits: List<KdbCommit>,
    ) : WireMessage()

    public data class DagDiff(
        override val header: WireHeader,
        val namespace: String,
        val localHead: KdbHash,
        val remoteHead: KdbHash,
    ) : WireMessage()

    public data class TransactionReplay(
        override val header: WireHeader,
        val namespace: String,
        val baseVersion: KdbHash,
        val transactionBytes: ByteArray,
    ) : WireMessage() {
        override fun equals(other: Any?): Boolean =
            other is TransactionReplay &&
                header == other.header &&
                namespace == other.namespace &&
                baseVersion == other.baseVersion &&
                transactionBytes.contentEquals(other.transactionBytes)

        override fun hashCode(): Int =
            header.hashCode() xor namespace.hashCode() xor baseVersion.hashCode() xor transactionBytes.contentHashCode()
    }

    public data class ConflictReport(
        override val header: WireHeader,
        val namespace: String,
        val reportBytes: ByteArray,
        // Additive to reportBytes, mirroring UpsertResult's errorCode/retryAfterMs below -
        // kdb-spec-layer13 Component 51 §8.1. A lost optimistic-concurrency race is the refusal
        // a contended workload produces the most of, and it used to be the one the server could
        // not pace: there was nowhere on this message to say when to retry, so a client's only
        // move was to retry instantly and re-collide. See KdbServerRuntime.conflictRetryAfterMs.
        val errorCode: String? = null,
        val retryAfterMs: Int? = null,
    ) : WireMessage() {
        override fun equals(other: Any?): Boolean =
            other is ConflictReport &&
                header == other.header &&
                namespace == other.namespace &&
                reportBytes.contentEquals(other.reportBytes) &&
                errorCode == other.errorCode &&
                retryAfterMs == other.retryAfterMs

        override fun hashCode(): Int =
            header.hashCode() xor namespace.hashCode() xor reportBytes.contentHashCode() xor
                errorCode.hashCode() xor retryAfterMs.hashCode()
    }

    public data class CompactionNotice(
        override val header: WireHeader,
        val intent: CompactionIntent,
    ) : WireMessage()

    public data class IceArchiveNotice(
        override val header: WireHeader,
        val namespace: String,
        val originalHash: KdbHash,
        val archiveLocation: String,
        val bundleHash: KdbHash,
    ) : WireMessage()

    public data class SnapshotRequest(
        override val header: WireHeader,
        val namespace: String,
        val anchorHash: KdbHash?,
    ) : WireMessage()

    public data class SnapshotResponse(
        override val header: WireHeader,
        val namespace: String,
        val anchorHash: KdbHash,
        val snapshotBytes: ByteArray,
        val compressed: Boolean,
    ) : WireMessage() {
        override fun equals(other: Any?): Boolean =
            other is SnapshotResponse &&
                header == other.header &&
                namespace == other.namespace &&
                anchorHash == other.anchorHash &&
                compressed == other.compressed &&
                snapshotBytes.contentEquals(other.snapshotBytes)

        override fun hashCode(): Int =
            header.hashCode() xor namespace.hashCode() xor anchorHash.hashCode() xor compressed.hashCode() xor
                snapshotBytes.contentHashCode()
    }

    public data class PositionAck(
        override val header: WireHeader,
        val namespace: String,
        val commitHash: KdbHash,
    ) : WireMessage()

    public data class SchemaPush(
        override val header: WireHeader,
        val namespace: String,
        val schemaBytes: ByteArray,
        val revision: Long,
    ) : WireMessage() {
        override fun equals(other: Any?): Boolean =
            other is SchemaPush &&
                header == other.header &&
                namespace == other.namespace &&
                revision == other.revision &&
                schemaBytes.contentEquals(other.schemaBytes)

        override fun hashCode(): Int =
            header.hashCode() xor namespace.hashCode() xor revision.hashCode() xor schemaBytes.contentHashCode()
    }

    public data class SessionBegin(
        override val header: WireHeader,
        val namespace: String,
        val sessionId: String?,
        val readConsistency: String,
        val baseVersionHex: String?,
    ) : WireMessage()

    public data class SessionBeginAck(
        override val header: WireHeader,
        val namespace: String,
        val sessionId: String,
        val headHex: String,
        val readConsistency: String,
        // Set (with sessionId empty) when the session begin was rejected - the authentication
        // or authorization failure a client should surface. Null on success and on frames from
        // peers that predate the field.
        val error: String? = null,
    ) : WireMessage()

    // Component 40 direct-document ops, ported from the Go implementation (see
    // go/kdb/wire/document_ops.go - field shapes must match its DTOs exactly).
    public data class DocumentGet(
        override val header: WireHeader,
        val namespace: String,
        val docId: String,
    ) : WireMessage()

    public data class DocumentGetResult(
        override val header: WireHeader,
        val namespace: String,
        val docId: String,
        // Null if the document doesn't exist at commitHex (or on error).
        val json: String?,
        val commitHex: String,
        val error: String? = null,
        // Additive to error, exactly like UpsertResult's fields above - a point read can be
        // refused for reasons purely about server load, and until these existed that arrived as
        // bare prose. Writes have carried this since Component 51; reads were the gap. No
        // Kotlin path populates these yet (the server has no admission/cost-model layer to
        // classify against, mirroring UpsertResult's own errorCode - present on the wire for a
        // Go peer, unpopulated here); decodable now so interop is ready when one is added.
        val errorCode: String? = null,
        val retryAfterMs: Int? = null,
    ) : WireMessage()

    // Upsert writes json at docId unconditionally - create if absent, replace if present, no
    // BaseVersion, no conflict possible (component 40 spec §5).
    public data class Upsert(
        override val header: WireHeader,
        val namespace: String,
        val docId: String,
        val json: String,
    ) : WireMessage()

    public data class UpsertResult(
        override val header: WireHeader,
        val namespace: String,
        val commitHex: String,
        val error: String? = null,
        // Additive to error - kdb-spec-layer13 Component 51 §8.1's typed backpressure codes.
        val errorCode: String? = null,
        val retryAfterMs: Int? = null,
    ) : WireMessage()

    public data class SqlExec(
        override val header: WireHeader,
        val namespace: String,
        val sessionId: String,
        val sql: String,
        val parametersJson: String?,
    ) : WireMessage()

    public data class SqlResult(
        override val header: WireHeader,
        val namespace: String,
        val sessionId: String,
        val columns: List<String>,
        val rows: List<List<String>>,
        val rowsAffected: Int,
        val resolvedCommitHex: String,
        val readOnly: Boolean,
        val error: String? = null,
        val generatedIds: List<String> = emptyList(),
        // Additive to error, like UpsertResult/DocumentGetResult - Go's SqlResultMessage has carried
        // these since Component 51. Layer 16 §9.6 populates UNIQUE_VIOLATION / SCHEMA_VIOLATION here.
        val errorCode: String? = null,
        val retryAfterMs: Int? = null,
    ) : WireMessage()

    /** One lexical arm of a SEARCH (Layer 16 §11): [index] is a FULLTEXT index name or the first field
     * of one. [depth] 0/null = every hit; [minScore]/[weight] feed rank fusion (§8). */
    public data class SearchTextArm(
        val index: String,
        val query: String,
        val depth: Int? = null,
        val minScore: Float? = null,
        val weight: Double? = null,
    )

    /** One vector arm of a SEARCH: [index] is a VECTOR index name or its field. */
    public data class SearchVectorArm(
        val index: String,
        val vector: List<Double>,
        val depth: Int? = null,
        val minScore: Float? = null,
        val weight: Double? = null,
    )

    // Layer 16 Component 69 (§11): sessionless like DocumentGet, authorized as DocumentRead on the
    // namespace. With one arm the result is that arm's ranking; with both, the fused ranking.
    public data class Search(
        override val header: WireHeader,
        val namespace: String,
        val sessionId: String? = null,
        val text: SearchTextArm? = null,
        val vector: SearchVectorArm? = null,
        /** "rrf" (default) or "weighted". */
        val fusion: String? = null,
        val limit: Int,
        val includeJson: Boolean = false,
        val atCommitHex: String? = null,
    ) : WireMessage()

    public data class SearchHit(
        val docId: String,
        val score: Float,
        val json: String? = null,
    )

    public data class SearchResult(
        override val header: WireHeader,
        val namespace: String,
        val hits: List<SearchHit>,
        val resolvedCommitHex: String,
        val error: String? = null,
        val errorCode: String? = null,
        val retryAfterMs: Int? = null,
    ) : WireMessage()

    public data class TxCommit(
        override val header: WireHeader,
        val namespace: String,
        val sessionId: String,
        val transactionBytes: ByteArray,
    ) : WireMessage() {
        override fun equals(other: Any?): Boolean =
            other is TxCommit &&
                header == other.header &&
                namespace == other.namespace &&
                sessionId == other.sessionId &&
                transactionBytes.contentEquals(other.transactionBytes)

        override fun hashCode(): Int =
            header.hashCode() xor namespace.hashCode() xor sessionId.hashCode() xor transactionBytes.contentHashCode()
    }

    public data class TxRollback(
        override val header: WireHeader,
        val namespace: String,
        val sessionId: String,
    ) : WireMessage()
}

public interface WireCodec {
    public val encoding: PayloadEncoding
    public fun encode(message: WireMessage): ByteArray
    public fun decode(frame: ByteArray): WireMessage
    public fun encodeFrameOnly(header: WireHeader, payload: ByteArray): ByteArray
    public fun decodeHeader(frame: ByteArray): WireHeader
}

public fun defaultWireCodec(encoding: PayloadEncoding = PayloadEncoding.KDB_BINARY): WireCodec =
    DefaultWireCodec(encoding)

public interface HandshakeNegotiator {
    public fun negotiate(local: HandshakePayload, remote: HandshakePayload): HandshakeAckPayload
}

public fun defaultHandshakeNegotiator(): HandshakeNegotiator = DefaultHandshakeNegotiator()

public fun validateFrameLength(length: Int, maxFrameBytes: Int = DEFAULT_MAX_FRAME_BYTES) {
    // The floor is the frame header's own size: a frame cannot declare fewer bytes than the
    // header it must begin with. This read 8 while FRAME_HEADER_SIZE has been 12, so Kotlin
    // accepted three declared lengths (8, 9, 10, 11) that Go's ValidateFrameLength rejects -
    // a divergence in what the two implementations consider a valid frame at all.
    if (length < FRAME_HEADER_SIZE || length > maxFrameBytes) {
        throw FrameTooLargeException(length, maxFrameBytes)
    }
}
