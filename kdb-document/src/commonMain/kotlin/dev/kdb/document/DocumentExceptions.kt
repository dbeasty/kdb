package dev.kdb.document

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.error.KdbErrorCode
import dev.kdb.error.KdbException

public class DocumentDecodeException(
    message: String,
    public val docId: KdbUuid? = null,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.KDB_DECODE_ERROR
}

public class CommitDecodeException(
    message: String,
    public val hash: KdbHash? = null,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.KDB_DECODE_ERROR
}
