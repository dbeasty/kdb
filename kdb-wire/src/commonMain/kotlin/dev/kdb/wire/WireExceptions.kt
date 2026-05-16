package dev.kdb.wire

import dev.kdb.error.KdbErrorCode
import dev.kdb.error.KdbException

public class WireDecodeException(message: String, cause: Throwable? = null) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.KDB_DECODE_ERROR
}

public class FrameTooLargeException(
    public val length: Int,
    public val max: Int,
) : KdbException("frame length $length exceeds max $max") {
    override val code: KdbErrorCode get() = KdbErrorCode.KDB_DECODE_ERROR
}

public class InvalidCorrelationException(public val correlationId: Int) :
    KdbException("unknown correlation $correlationId") {
    override val code: KdbErrorCode get() = KdbErrorCode.KDB_DECODE_ERROR
}
