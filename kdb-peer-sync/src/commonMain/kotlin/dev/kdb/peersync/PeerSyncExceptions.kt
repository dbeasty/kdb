package dev.kdb.peersync

import dev.kdb.error.KdbErrorCode
import dev.kdb.error.KdbException

public class PeerSyncException(
    message: String,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.UNSUPPORTED_PROTOCOL_VERSION
}
