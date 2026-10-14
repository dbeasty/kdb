package dev.kdb.document

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.codec.KdbValue
import dev.kdb.codec.encodeToBytes
import dev.kdb.codec.toUuidVal
import dev.kdb.codec.toKdbUuid
import dev.kdb.document.internal.sha256Digest

/**
 * Atomic unit of change within a transaction.
 */
public sealed class KdbOp {
    public data class Write(
        val docId: KdbUuid,
        val patch: String,
        /**
         * [patch] is the whole document rather than a merge patch, so keys it omits are removed.
         * A property of the request only: it is not part of the encoded op (see the Go engine's
         * operationsAsStored for why), and a replayed write is always read as a whole document.
         */
        val replace: Boolean = false,
    ) : KdbOp()

    public data class Delete(
        val docId: KdbUuid,
    ) : KdbOp()

    public data class FileWrite(
        val path: String,
        val blobHash: KdbHash,
    ) : KdbOp()

    public data class SchemaMigration(
        val migrationId: KdbUuid,
        val migrationPayload: String,
    ) : KdbOp()

    /** Creates or replaces one of the document's attachments - [attachmentId], under [name] -
     * (go/kdb/document AttachmentPutOp; branch 4). [scheme] says how [contentHash] was computed. */
    public data class AttachmentPut(
        val docId: KdbUuid,
        val attachmentId: KdbUuid,
        val name: String,
        val scheme: Int,
        val contentHash: KdbHash,
        val size: Long,
        val mime: String,
    ) : KdbOp()

    /** Removes one of the document's attachments, leaving the document and its others
     * (AttachmentDeleteOp; branch 5). */
    public data class AttachmentDelete(
        val docId: KdbUuid,
        val attachmentId: KdbUuid,
    ) : KdbOp()

    public fun toKdbValue(): KdbValue =
        when (this) {
            is Write ->
                KdbValue.UnionVal(
                    0,
                    KdbValue.RecordVal(
                        mapOf(
                            1 to docId.toUuidVal(),
                            2 to KdbValue.StringVal(patch),
                        ),
                    ),
                )
            is Delete ->
                KdbValue.UnionVal(
                    1,
                    KdbValue.RecordVal(
                        mapOf(1 to docId.toUuidVal()),
                    ),
                )
            is FileWrite ->
                KdbValue.UnionVal(
                    2,
                    KdbValue.RecordVal(
                        mapOf(
                            1 to KdbValue.StringVal(path),
                            2 to KdbValue.FixedVal(blobHash.bytes.copyOf()),
                        ),
                    ),
                )
            is SchemaMigration ->
                KdbValue.UnionVal(
                    3,
                    KdbValue.RecordVal(
                        mapOf(
                            1 to migrationId.toUuidVal(),
                            2 to KdbValue.StringVal(migrationPayload),
                        ),
                    ),
                )
            is AttachmentPut ->
                KdbValue.UnionVal(
                    4,
                    KdbValue.RecordVal(
                        mapOf(
                            1 to docId.toUuidVal(),
                            2 to attachmentId.toUuidVal(),
                            3 to KdbValue.StringVal(name),
                            4 to KdbValue.Int32Val(scheme),
                            5 to KdbValue.FixedVal(contentHash.bytes.copyOf()),
                            6 to KdbValue.Int64Val(size),
                            7 to KdbValue.StringVal(mime),
                        ),
                    ),
                )
            is AttachmentDelete ->
                KdbValue.UnionVal(
                    5,
                    KdbValue.RecordVal(mapOf(1 to docId.toUuidVal(), 2 to attachmentId.toUuidVal())),
                )
        }

    public companion object {
        public fun fromKdbValue(value: KdbValue): KdbOp {
            val uv = value as? KdbValue.UnionVal ?: throw CommitDecodeException("KdbOp: expected union")
            return when (uv.branch) {
                0 -> {
                    val r = uv.value as? KdbValue.RecordVal ?: throw CommitDecodeException("OpWrite record")
                    val id =
                        (r.fields[1] as? KdbValue.UuidVal)?.toKdbUuid()
                            ?: throw CommitDecodeException("OpWrite docId")
                    val patch =
                        (r.fields[2] as? KdbValue.StringVal)?.v
                            ?: throw CommitDecodeException("OpWrite patch")
                    Write(id, patch)
                }
                1 -> {
                    val r = uv.value as? KdbValue.RecordVal ?: throw CommitDecodeException("OpDelete record")
                    val id =
                        (r.fields[1] as? KdbValue.UuidVal)?.toKdbUuid()
                            ?: throw CommitDecodeException("OpDelete docId")
                    Delete(id)
                }
                2 -> {
                    val r = uv.value as? KdbValue.RecordVal ?: throw CommitDecodeException("OpFileWrite record")
                    val path =
                        (r.fields[1] as? KdbValue.StringVal)?.v
                            ?: throw CommitDecodeException("OpFileWrite path")
                    val fh =
                        (r.fields[2] as? KdbValue.FixedVal)?.v
                            ?: throw CommitDecodeException("OpFileWrite blobHash")
                    FileWrite(path, KdbHash.fromBytes(fh.copyOf()))
                }
                3 -> {
                    val r = uv.value as? KdbValue.RecordVal ?: throw CommitDecodeException("OpSchemaMigration record")
                    val mid =
                        (r.fields[1] as? KdbValue.UuidVal)?.toKdbUuid()
                            ?: throw CommitDecodeException("OpSchemaMigration migrationId")
                    val payload =
                        (r.fields[2] as? KdbValue.StringVal)?.v
                            ?: throw CommitDecodeException("OpSchemaMigration payload")
                    SchemaMigration(mid, payload)
                }
                4 -> {
                    val r = uv.value as? KdbValue.RecordVal ?: throw CommitDecodeException("OpAttachmentPut record")
                    val id =
                        (r.fields[1] as? KdbValue.UuidVal)?.toKdbUuid()
                            ?: throw CommitDecodeException("OpAttachmentPut docId")
                    val attId =
                        (r.fields[2] as? KdbValue.UuidVal)?.toKdbUuid()
                            ?: throw CommitDecodeException("OpAttachmentPut attachmentId")
                    val name =
                        (r.fields[3] as? KdbValue.StringVal)?.v
                            ?: throw CommitDecodeException("OpAttachmentPut name")
                    val scheme =
                        (r.fields[4] as? KdbValue.Int32Val)?.v?.takeIf { it in 0..255 }
                            ?: throw CommitDecodeException("OpAttachmentPut scheme")
                    val h =
                        (r.fields[5] as? KdbValue.FixedVal)?.v
                            ?: throw CommitDecodeException("OpAttachmentPut contentHash")
                    val size =
                        (r.fields[6] as? KdbValue.Int64Val)?.v?.takeIf { it >= 0 }
                            ?: throw CommitDecodeException("OpAttachmentPut size")
                    val mime =
                        (r.fields[7] as? KdbValue.StringVal)?.v
                            ?: throw CommitDecodeException("OpAttachmentPut mime")
                    AttachmentPut(id, attId, name, scheme, KdbHash.fromBytes(h.copyOf()), size, mime)
                }
                5 -> {
                    val r = uv.value as? KdbValue.RecordVal ?: throw CommitDecodeException("OpAttachmentDelete record")
                    val id =
                        (r.fields[1] as? KdbValue.UuidVal)?.toKdbUuid()
                            ?: throw CommitDecodeException("OpAttachmentDelete docId")
                    val attId =
                        (r.fields[2] as? KdbValue.UuidVal)?.toKdbUuid()
                            ?: throw CommitDecodeException("OpAttachmentDelete attachmentId")
                    AttachmentDelete(id, attId)
                }
                else -> throw CommitDecodeException("unknown KdbOp branch ${uv.branch}")
            }
        }
    }
}
