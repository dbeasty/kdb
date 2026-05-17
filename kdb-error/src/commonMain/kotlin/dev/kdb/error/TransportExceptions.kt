package dev.kdb.error

public open class TransportException(
    message: String,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.TRANSPORT_ERROR
}

public class ConnectionClosedException(
    message: String = "connection closed",
    cause: Throwable? = null,
) : TransportException(message, cause)

public class TransportTimeoutException(
    public val timeoutMs: Long,
) : TransportException("read timeout after ${timeoutMs}ms")

public class ComputeUnavailableException(message: String) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.COMPUTE_UNAVAILABLE
}

public class ComputeDispatchException(message: String, cause: Throwable? = null) :
    KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.COMPUTE_DISPATCH_ERROR
}
