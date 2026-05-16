package dev.kdb.query.hybrid

import dev.kdb.codec.KdbHash
import dev.kdb.error.KdbErrorCode
import dev.kdb.error.KdbException

public class ReadOnlyCheckoutException(
    public val namespaceId: String,
    public val atCommit: KdbHash,
) : KdbException("namespace $namespaceId is read-only at ${atCommit.toHex()}") {
    override val code: KdbErrorCode get() = KdbErrorCode.VERSION_NOT_FOUND
}

public class HistoryDisabledException(
    public val namespaceId: String,
) : KdbException("versioned queries are disabled for namespace $namespaceId") {
    override val code: KdbErrorCode get() = KdbErrorCode.VERSION_NOT_FOUND
}

public class HybridDmlNotSupportedException(
    message: String,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.SCHEMA_VIOLATION
}
