package dev.kdb.document

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.codec.KdbValue
import dev.kdb.codec.encodeToBytes
import dev.kdb.codec.toKdbTimestamp
import dev.kdb.codec.toKdbUuid
import dev.kdb.codec.toTimestampVal
import dev.kdb.codec.toUuidVal
import dev.kdb.document.internal.sha256Digest

internal fun commitPayloadRecord(
    parentHashes: List<KdbHash>,
    namespaceId: String,
    transactionId: KdbUuid,
    timestamp: KdbTimestamp,
    authorNodeId: KdbUuid,
    operations: List<KdbOp>,
    documentTreeHash: KdbHash,
    schemaHash: KdbHash?,
    message: String,
): KdbValue.RecordVal =
    KdbValue.RecordVal(
        buildMap {
            put(1, KdbValue.ArrayVal(parentHashes.map { KdbValue.FixedVal(it.bytes.copyOf()) }))
            put(2, KdbValue.StringVal(namespaceId))
            put(3, transactionId.toUuidVal())
            put(4, timestamp.toTimestampVal())
            put(5, authorNodeId.toUuidVal())
            put(6, KdbValue.ArrayVal(operations.map { it.toKdbValue() }))
            put(7, KdbValue.FixedVal(documentTreeHash.bytes.copyOf()))
            put(
                8,
                if (schemaHash == null) {
                    KdbValue.Null
                } else {
                    KdbValue.FixedVal(schemaHash.bytes.copyOf())
                },
            )
            put(9, KdbValue.StringVal(message))
        },
    )

/**
 * Immutable content-addressed commit DAG node.
 */
public data class KdbCommit(
    val hash: KdbHash,
    val parentHashes: List<KdbHash>,
    val namespaceId: String,
    val transactionId: KdbUuid,
    val timestamp: KdbTimestamp,
    val authorNodeId: KdbUuid,
    val operations: List<KdbOp>,
    val documentTreeHash: KdbHash,
    val schemaHash: KdbHash?,
    val message: String = "",
) {
    public fun toCommitPayloadValue(): KdbValue =
        commitPayloadRecord(
            parentHashes,
            namespaceId,
            transactionId,
            timestamp,
            authorNodeId,
            operations,
            documentTreeHash,
            schemaHash,
            message,
        )

    public fun toPayloadBytes(): ByteArray {
        val reg = KdbDocumentWireRegistry()
        return toCommitPayloadValue().encodeToBytes(CommitPayloadType, reg)
    }

    public fun toBytes(): ByteArray = toPayloadBytes()

    public companion object {
        /**
         * Build a commit whose [hash] is SHA-256 of the canonical Layer 0 commit payload bytes.
         */
        public fun build(
            parentHashes: List<KdbHash>,
            namespaceId: String,
            transactionId: KdbUuid,
            timestamp: KdbTimestamp,
            authorNodeId: KdbUuid,
            operations: List<KdbOp>,
            documentTreeHash: KdbHash,
            schemaHash: KdbHash?,
            message: String = "",
        ): KdbCommit {
            val reg = KdbDocumentWireRegistry()
            val bytes =
                commitPayloadRecord(
                    parentHashes,
                    namespaceId,
                    transactionId,
                    timestamp,
                    authorNodeId,
                    operations,
                    documentTreeHash,
                    schemaHash,
                    message,
                ).encodeToBytes(CommitPayloadType, reg)
            val hash = KdbHash.fromBytes(sha256Digest(bytes))
            return KdbCommit(
                hash,
                parentHashes,
                namespaceId,
                transactionId,
                timestamp,
                authorNodeId,
                operations,
                documentTreeHash,
                schemaHash,
                message,
            )
        }

        public fun fromPayloadBytes(bytes: ByteArray): KdbCommit {
            val reg = KdbDocumentWireRegistry()
            val v =
                try {
                    KdbValue.decodeFromBytes(bytes, CommitPayloadType, reg)
                } catch (e: Throwable) {
                    throw CommitDecodeException("commit payload decode failed", cause = e)
                }
            val rec = v as? KdbValue.RecordVal ?: throw CommitDecodeException("commit payload: expected record")
            val hash = KdbHash.fromBytes(sha256Digest(bytes))
            return parseCommitFromPayloadRecord(rec, hash)
        }

        public fun fromBytes(bytes: ByteArray): KdbCommit = fromPayloadBytes(bytes)

        internal fun parseCommitFromPayloadRecord(
            rec: KdbValue.RecordVal,
            hash: KdbHash,
        ): KdbCommit {
            val parentsArr = rec.fields[1] as? KdbValue.ArrayVal ?: throw CommitDecodeException("parentHashes")
            val parents =
                parentsArr.elements.map { el ->
                    val fv = el as? KdbValue.FixedVal ?: throw CommitDecodeException("parent hash")
                    KdbHash.fromBytes(fv.v.copyOf())
                }
            val ns =
                (rec.fields[2] as? KdbValue.StringVal)?.v
                    ?: throw CommitDecodeException("namespaceId")
            val txId =
                (rec.fields[3] as? KdbValue.UuidVal)?.toKdbUuid()
                    ?: throw CommitDecodeException("transactionId")
            val ts =
                (rec.fields[4] as? KdbValue.TimestampVal)?.toKdbTimestamp()
                    ?: throw CommitDecodeException("timestamp")
            val author =
                (rec.fields[5] as? KdbValue.UuidVal)?.toKdbUuid()
                    ?: throw CommitDecodeException("authorNodeId")
            val opsArr = rec.fields[6] as? KdbValue.ArrayVal ?: throw CommitDecodeException("operations")
            val ops = opsArr.elements.map { KdbOp.fromKdbValue(it) }
            val docTreeH =
                (rec.fields[7] as? KdbValue.FixedVal)?.v?.copyOf()
                    ?: throw CommitDecodeException("documentTreeHash")
            val schemaField = rec.fields[8]
            val schemaH =
                when (schemaField) {
                    null, KdbValue.Null -> null
                    is KdbValue.FixedVal -> KdbHash.fromBytes(schemaField.v.copyOf())
                    else -> throw CommitDecodeException("schemaHash")
                }
            val msg =
                (rec.fields[9] as? KdbValue.StringVal)?.v
                    ?: throw CommitDecodeException("message")
            return KdbCommit(
                hash = hash,
                parentHashes = parents,
                namespaceId = ns,
                transactionId = txId,
                timestamp = ts,
                authorNodeId = author,
                operations = ops,
                documentTreeHash = KdbHash.fromBytes(docTreeH),
                schemaHash = schemaH,
                message = msg,
            )
        }
    }
}

/** Canonical commit hash: SHA-256 of `CommitPayload` Layer 0 bytes. */
public fun computeCommitHash(commit: KdbCommit): KdbHash =
    KdbHash.fromBytes(sha256Digest(commit.toPayloadBytes()))
