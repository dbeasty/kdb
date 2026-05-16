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
                else -> throw CommitDecodeException("unknown KdbOp branch ${uv.branch}")
            }
        }
    }
}
