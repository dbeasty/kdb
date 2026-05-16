package dev.kdb.storage.io

import dev.kdb.error.KdbErrorCode
import dev.kdb.error.KdbException

public class PlatformIoException(
    message: String,
    public val segmentName: String? = null,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.STORAGE_TIER_ERROR
}

internal fun validateSegmentName(segmentName: String) {
    require(segmentName.isNotEmpty()) { "segment name must not be empty" }
    require(!segmentName.contains("..")) { "segment name must not contain '..'" }
    require(segmentName.startsWith("ns/")) { "segment name must start with ns/" }
}
