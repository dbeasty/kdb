package dev.kdb.script

import dev.kdb.error.KdbErrorCode
import dev.kdb.error.KdbException
import kotlinx.serialization.Serializable

/**
 * A stored, versioned procedure definition (Component 32, master spec Layer 11).
 * Source is restricted JS; see `kdb-spec-layer11-component32-stored-procedures.md` §5.4/§5.5.
 */
@Serializable
public data class ProcedureDefinition(
    val namespaceId: String,
    val name: String,
    val source: String,
    val requiredPermission: String? = null,
    val revision: Long = 1L,
    val createdBy: String = "",
    val createdAt: Long = 0L,
)

public data class ProcResult(
    val value: String,
    val logs: List<String>,
)

public data class ProcLimits(
    val wallClockMillis: Long = 5_000,
    val maxHostCalls: Int = 1_000,
    val maxLogBytes: Int = 64 * 1024,
    val maxStatements: Long = 1_000_000,
) {
    public companion object {
        public val DEFAULT: ProcLimits = ProcLimits()
    }
}

public sealed class ProcException(
    message: String,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    public class NotFound(namespace: String, name: String) :
        ProcException("no such procedure: $namespace/$name") {
        override val code: KdbErrorCode get() = KdbErrorCode.SCRIPT_NOT_FOUND
    }

    public class CompileError(detail: String, cause: Throwable? = null) :
        ProcException(detail, cause) {
        override val code: KdbErrorCode get() = KdbErrorCode.SCRIPT_COMPILE_ERROR
    }

    public class Timeout(millis: Long) :
        ProcException("procedure exceeded ${millis}ms") {
        override val code: KdbErrorCode get() = KdbErrorCode.SCRIPT_TIMEOUT
    }

    public class ResourceLimitExceeded(detail: String) :
        ProcException(detail) {
        override val code: KdbErrorCode get() = KdbErrorCode.SCRIPT_RESOURCE_LIMIT
    }

    public class ScriptRuntimeError(detail: String, cause: Throwable? = null) :
        ProcException(detail, cause) {
        override val code: KdbErrorCode get() = KdbErrorCode.SCRIPT_RUNTIME_ERROR
    }

    /** Authorization failure for a specific `kdb.*` call made *inside* a running script. */
    public class Denied(detail: String) :
        ProcException(detail) {
        override val code: KdbErrorCode get() = KdbErrorCode.AUTHORIZATION_FAILED
    }
}
