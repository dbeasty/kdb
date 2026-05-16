package dev.kdb.policy

import dev.kdb.error.KdbErrorCode
import dev.kdb.error.KdbException

public class PolicyParseException(
    message: String,
    public val offset: Int = -1,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.SCHEMA_VIOLATION
}

public class PolicyValidationException(
    public val errors: List<PolicyValidationError>,
) : KdbException(errors.joinToString { it.toString() }) {
    override val code: KdbErrorCode get() = KdbErrorCode.SCHEMA_VIOLATION
}

public class PolicyConflictException(
    message: String,
    public val expectedRevision: Long,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.CONFLICT
}
