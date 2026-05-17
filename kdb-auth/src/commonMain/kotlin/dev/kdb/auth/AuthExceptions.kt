package dev.kdb.auth

import dev.kdb.error.KdbErrorCode
import dev.kdb.error.KdbException

public class KdbAuthenticationException(
    message: String,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.AUTHENTICATION_FAILED
}

public class KdbAuthorizationException(
    message: String,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.AUTHORIZATION_FAILED
}
