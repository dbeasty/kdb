package dev.kdb.wire

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
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

    // Attachments (go/kdb/wire/attachment_ops.go). Always binary bodies (tag 2); the client ones are
    // sent only once WIRE_FEATURE_ATTACHMENTS is agreed, BLOB_FETCH only to a peer with that
    // capability.
    BLOB_FETCH(0x42),
    BLOB_FETCH_RESULT(0x43),
    ATTACHMENT_UPLOAD(0x44),
    ATTACHMENT_UPLOAD_RESULT(0x45),
    ATTACHMENT_GET(0x46),
    ATTACHMENT_DATA(0x47),
    ATTACHMENT_LIST(0x48),
    ATTACHMENT_LIST_RESULT(0x49),
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
    /** Optional frame encodings offered (WIRE_FEATURE_BINARY); see negotiateWireFeatures. */
    val wireFeatures: List<String> = emptyList(),
)

public data class HandshakeAckPayload(
    val accepted: Boolean,
    val negotiatedEncoding: PayloadEncoding,
    val protocolVersion: Int,
    val remoteHeads: Map<String, String> = emptyMap(),
    val rejectionReason: String? = null,
    /** The offered features the server accepted. Neither side sends a frame in one until it is here. */
    val wireFeatures: List<String> = emptyList(),
    /** The release version of the server that sent the ack (Go's ServerVersion: "0.7.1", "0.0.0-dev"
     * from source). The Go server always sends it; the Kotlin server leaves it empty. */
    val serverVersion: String = "",
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
        /** Every cell's kind, one decimal digit each, row by row: the SQL type each string in
         * [rows] is the text of (SqlCellKind). Null from a server that predates it. */
        val cellKinds: String? = null,
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

    // Attachments (go/kdb/wire/attachment_ops.go; field numbers in AttachmentWireBody.kt). Each is
    // always a binary body. An absent UUID or hash is null (Go's zero value); an absent error null.

    /** One chunk of a blob upload. The first has no [uploadId]; the result names the upload, and
     * every later chunk carries it with the [offset] it starts at (the bytes received so far, so an
     * interrupted upload resumes from the result's received). [final] finishes the upload. */
    public class AttachmentUpload(
        override val header: WireHeader,
        public val namespace: String,
        public val sessionId: String = "",
        public val uploadId: String = "",
        public val offset: Long = 0,
        public val data: ByteArray = ByteArray(0),
        public val final: Boolean = false,
        /** On a first chunk, the blob about to be sent: a server already holding it answers with
         * its hash and size at once (and holds it for the commit), and the client skips the bytes. */
        public val expect: KdbHash? = null,
    ) : WireMessage() {
        public fun copy(header: WireHeader): AttachmentUpload =
            AttachmentUpload(header, namespace, sessionId, uploadId, offset, data, final, expect)

        override fun equals(other: Any?): Boolean =
            other is AttachmentUpload && header == other.header && namespace == other.namespace &&
                sessionId == other.sessionId && uploadId == other.uploadId && offset == other.offset &&
                data.contentEquals(other.data) && final == other.final && expect == other.expect

        override fun hashCode(): Int =
            listOf(header, namespace, sessionId, uploadId, offset, final, expect).hashCode() * 31 + data.contentHashCode()

        override fun toString(): String =
            "AttachmentUpload(header=$header, namespace=$namespace, sessionId=$sessionId, uploadId=$uploadId, " +
                "offset=$offset, data=${data.size} bytes, final=$final, expect=$expect)"
    }

    /** Answers an upload chunk: the bytes received so far and, after the final chunk, the blob's
     * content hash and size. */
    public data class AttachmentUploadResult(
        override val header: WireHeader,
        val namespace: String,
        val uploadId: String = "",
        val received: Long = 0,
        val hash: KdbHash? = null,
        val size: Long = 0,
        val error: String? = null,
        val errorCode: String? = null,
    ) : WireMessage()

    /** Reads one attachment of a document - by [attachmentId], or by [name] - as of [asOfHex] (empty:
     * the session's read head), from [offset], at most [maxBytes] (0: a server default). The first
     * page's result carries a readId that pins the version; later pages send it back. [close] (with
     * a readId) ends a read early; its result's superseded says whether the attachment has changed
     * since the read began.
     *
     * [proofs] (behind [WIRE_FEATURE_ATTACHMENT_PROOFS]) asks for verifiable pages: the server rounds
     * [offset] down and the page's end up to 1 MiB chunk boundaries (the end at most to the
     * attachment's), and the result carries the blob's tree and an audit path per chunk, as
     * BLOB_FETCH does. */
    public data class AttachmentGet(
        override val header: WireHeader,
        val namespace: String,
        val sessionId: String = "",
        val docId: KdbUuid? = null,
        val attachmentId: KdbUuid? = null,
        val name: String = "",
        val asOfHex: String = "",
        val offset: Long = 0,
        val maxBytes: Long = 0,
        val readId: String = "",
        val close: Boolean = false,
        val proofs: Boolean = false,
    ) : WireMessage()

    /** One page of an attachment. [eof] marks the last; [superseded], on the last page, says the
     * document's attachment changed since the read began.
     *
     * Answering a get with proofs, [data] covers whole chunks from a chunk-aligned [offset] (the last
     * may be short), [chunks] proves each (bottom-up audit paths to [tree]) and [scheme] is the
     * content hash's scheme (1); info.hash is then attachmentIdentity(tree, info.size). A zero-byte
     * attachment's page proves its one empty chunk. */
    public class AttachmentData(
        override val header: WireHeader,
        public val namespace: String,
        public val readId: String = "",
        public val docId: KdbUuid? = null,
        public val info: WireAttachmentInfo = WireAttachmentInfo(),
        public val commitHex: String = "",
        public val offset: Long = 0,
        public val data: ByteArray = ByteArray(0),
        public val eof: Boolean = false,
        public val superseded: Boolean = false,
        public val error: String? = null,
        public val errorCode: String? = null,
        public val tree: KdbHash? = null,
        public val chunks: List<WireBlobChunkProof> = emptyList(),
        public val scheme: Int = 0,
    ) : WireMessage() {
        override fun equals(other: Any?): Boolean =
            other is AttachmentData && header == other.header && namespace == other.namespace &&
                readId == other.readId && docId == other.docId && info == other.info &&
                commitHex == other.commitHex && offset == other.offset && data.contentEquals(other.data) &&
                eof == other.eof && superseded == other.superseded && error == other.error &&
                errorCode == other.errorCode && tree == other.tree && chunks == other.chunks && scheme == other.scheme

        override fun hashCode(): Int =
            listOf(header, namespace, readId, docId, info, commitHex, offset, eof, superseded, error, errorCode, tree, chunks, scheme)
                .hashCode() * 31 + data.contentHashCode()

        override fun toString(): String =
            "AttachmentData(header=$header, namespace=$namespace, readId=$readId, docId=$docId, info=$info, " +
                "commitHex=$commitHex, offset=$offset, data=${data.size} bytes, eof=$eof, superseded=$superseded, " +
                "error=$error, errorCode=$errorCode, tree=$tree, chunks=$chunks, scheme=$scheme)"
    }

    /** Lists a document's attachments as of [asOfHex] (empty: the read head). */
    public data class AttachmentList(
        override val header: WireHeader,
        val namespace: String,
        val sessionId: String = "",
        val docId: KdbUuid? = null,
        val asOfHex: String = "",
    ) : WireMessage()

    /** A document's attachments, sorted by name, as of [commitHex]. */
    public data class AttachmentListResult(
        override val header: WireHeader,
        val namespace: String,
        val commitHex: String = "",
        val items: List<WireAttachmentInfo> = emptyList(),
        val error: String? = null,
        val errorCode: String? = null,
    ) : WireMessage()

    /** A batch of blob reads (peer sync, BLOB_FETCH 0x42). [maxBytes] bounds the result page. */
    public data class BlobFetch(
        override val header: WireHeader,
        val namespace: String,
        val entries: List<WireBlobFetchEntry> = emptyList(),
        val maxBytes: Long = 0,
    ) : WireMessage()

    /** Answers BLOB_FETCH, parts in request order; [more] when some entry had no room. */
    public data class BlobFetchResult(
        override val header: WireHeader,
        val namespace: String,
        val parts: List<WireBlobFetchPart> = emptyList(),
        val more: Boolean = false,
        val error: String? = null,
        val errorCode: String? = null,
    ) : WireMessage()
}

/** One attachment of a document (Go's wire.AttachmentInfo). */
public data class WireAttachmentInfo(
    val id: KdbUuid? = null,
    val name: String = "",
    val hash: KdbHash? = null,
    val size: Long = 0,
    val mime: String = "",
)

/** Part of one attachment blob asked for by BLOB_FETCH (Go's wire.BlobFetchEntry). */
public data class WireBlobFetchEntry(
    val docId: KdbUuid? = null,
    val attachmentId: KdbUuid? = null,
    val commitHex: String = "",
    val hash: KdbHash? = null,
    val offset: Long = 0,
    val maxBytes: Long = 0,
)

/** One chunk's RFC 6962 audit path: [leaf] is its index. */
public data class WireBlobChunkProof(
    val leaf: Long = 0,
    val path: List<KdbHash> = emptyList(),
)

/** The answer to one BLOB_FETCH entry, by its [index] in the request (Go's wire.BlobFetchPart). */
public class WireBlobFetchPart(
    public val index: Int = 0,
    public val hash: KdbHash? = null,
    public val size: Long = 0,
    public val tree: KdbHash? = null,
    public val offset: Long = 0,
    public val data: ByteArray = ByteArray(0),
    public val chunks: List<WireBlobChunkProof> = emptyList(),
    public val eof: Boolean = false,
    public val superseded: Boolean = false,
    public val error: String? = null,
    public val errorCode: String? = null,
) {
    override fun equals(other: Any?): Boolean =
        other is WireBlobFetchPart && index == other.index && hash == other.hash && size == other.size &&
            tree == other.tree && offset == other.offset && data.contentEquals(other.data) &&
            chunks == other.chunks && eof == other.eof && superseded == other.superseded &&
            error == other.error && errorCode == other.errorCode

    override fun hashCode(): Int =
        listOf(index, hash, size, tree, offset, chunks, eof, superseded, error, errorCode).hashCode() * 31 +
            data.contentHashCode()

    override fun toString(): String =
        "WireBlobFetchPart(index=$index, hash=$hash, size=$size, tree=$tree, offset=$offset, " +
            "data=${data.size} bytes, chunks=$chunks, eof=$eof, superseded=$superseded, error=$error, errorCode=$errorCode)"
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

/**
 * A codec like this one that also encodes in the negotiated [features]' formats - the list a
 * handshake ack carries. Decoding is unaffected: every codec decodes every encoding tag.
 */
public fun WireCodec.withWireFeatures(features: List<String>): WireCodec {
    val binary = WIRE_FEATURE_BINARY in features
    return DefaultWireCodec(
        encoding,
        binaryFrames = binary,
        binaryFrames2 = binary && WIRE_FEATURE_BINARY_2 in features,
        zstdFrames = binary && WIRE_FEATURE_ZSTD in features,
    )
}

/** The SQL type of one result cell, as [WireMessage.SqlResult.cellKinds] digits name it. */
public object SqlCellKind {
    public const val STRING: Int = 0
    public const val NULL: Int = 1
    public const val LONG: Int = 2
    public const val DOUBLE: Int = 3
    public const val BOOL: Int = 4
    public const val JSON: Int = 5

    /** Binary (an attachment's bytes, an X'..' literal): the row string is standard base64. */
    public const val BYTES: Int = 6
}

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
