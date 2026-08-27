package dev.kdb.auth.token

import dev.kdb.auth.AuthCredentials
import dev.kdb.auth.Authenticator
import dev.kdb.auth.KdbAuthenticationException
import dev.kdb.auth.Principal
import dev.kdb.codec.KdbTimestamp
import dev.kdb.document.KdbDocument
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull

/**
 * A document-backed session lookup, kept deliberately generic (not "Zolik's Session shape"
 * specifically) so any application can adopt this by pointing it at its own sessions namespace
 * and field names. [expiresAtField] is stored as epoch microseconds (a JSON number), matching
 * [KdbTimestamp]'s own convention used throughout this codebase, rather than an ISO-8601 string.
 */
public data class TokenAuthConfig(
    val sessionsNamespace: String,
    val expiresAtField: String = "expiresAt",
    val principalIdField: String = "userId",
    val rolesField: String = "roles",
)

/** Narrow, read-only capability [TokenAuthEngine] needs - not a full EmbedRuntime/StorageAdapter,
 * so this can be unit-tested with a trivial in-memory fake (component 41 spec §9). Looks up a
 * session document by its id directly (see [sessionDocId]) rather than scanning for one whose
 * token field matches - an O(1) lookup instead of an O(n) collection scan, and it means the
 * session document itself never needs to hold the bearer token in the clear (see
 * [SessionIssuer.issue]'s doc comment). */
public fun interface DocumentReader {
    public suspend fun getById(
        namespace: String,
        docId: String,
    ): KdbDocument?
}

public enum class RejectReason { TOKEN_NOT_FOUND, TOKEN_EXPIRED, MALFORMED_CREDENTIALS }

/**
 * Thrown by [TokenAuthEngine.authenticate] on rejection - a [KdbAuthenticationException]
 * subtype, so this still satisfies the real [Authenticator] contract (which returns [Principal]
 * and throws on failure, not a sealed result type), while carrying [reason] for callers/tests
 * that want to distinguish why without parsing the message.
 */
public class TokenAuthRejectedException(
    public val reason: RejectReason,
    message: String,
) : KdbAuthenticationException(message)

/**
 * Authenticates a wire connection against an application-issued session token stored as an
 * ordinary KDB document (component 41 spec §1) - "authenticate using a token I created
 * programmatically," not "re-send username and password on every connection"
 * ([dev.kdb.auth.store.DynamicAuthEngine]'s job, unchanged and untouched by this class) and not
 * a static config file ([dev.kdb.auth.static.StaticAuthEngine]'s job, also untouched).
 *
 * Never issues, hashes, or checks passwords - see [SessionIssuer] for the issuance half, which
 * runs after a `DynamicAuthEngine.authenticate` call already succeeded.
 */
public class TokenAuthEngine(
    private val config: TokenAuthConfig,
    private val documentReader: DocumentReader,
) : Authenticator {
    override suspend fun authenticate(credentials: AuthCredentials): Principal {
        val token =
            credentials.token
                ?: throw TokenAuthRejectedException(RejectReason.MALFORMED_CREDENTIALS, "missing bearer token")
        val doc =
            documentReader.getById(config.sessionsNamespace, sessionDocId(token))
                ?: throw TokenAuthRejectedException(RejectReason.TOKEN_NOT_FOUND, "no session for token")
        return principalFromSessionDocument(doc)
    }

    private fun principalFromSessionDocument(doc: KdbDocument): Principal {
        val json = Json.parseToJsonElement(doc.json).jsonObject
        // Malformed/missing expiresAt is treated as invalid, not "never expires" (spec §7 test
        // 10) - closest fit among the three RejectReason values is TOKEN_EXPIRED: a document
        // was found (not TOKEN_NOT_FOUND), the *credentials* aren't malformed (not
        // MALFORMED_CREDENTIALS), but validity can't be confirmed, so it's treated the same as
        // an expired one rather than granting access.
        val expiresAtMicros =
            (json[config.expiresAtField] as? JsonPrimitive)?.longOrNull
                ?: throw TokenAuthRejectedException(RejectReason.TOKEN_EXPIRED, "missing or malformed ${config.expiresAtField}")
        // Boundary is exclusive of validity: a token is good strictly before expiresAt, not
        // through it (spec §7 test 4) - "expires at T" means invalid at-or-after T.
        if (KdbTimestamp.now().toEpochMicros() >= expiresAtMicros) {
            throw TokenAuthRejectedException(RejectReason.TOKEN_EXPIRED, "token expired")
        }
        val principalId =
            (json[config.principalIdField] as? JsonPrimitive)?.jsonPrimitive?.content?.takeIf { it.isNotEmpty() }
        // Roles the principal held at issue() time (see SessionIssuer.issue), not looked up
        // fresh here - previously omitted entirely, so every token-authenticated Principal had
        // an empty role set and any RBAC authorizer denied it everything regardless of what the
        // original login actually granted.
        val roles =
            (json[config.rolesField] as? JsonArray)
                ?.mapNotNull { (it as? JsonPrimitive)?.content }
                ?.toSet()
                ?: emptySet()
        // Guest session (no/blank principalIdField, spec §7 test 5): identify by the session
        // document's own id rather than leaving Principal.id empty.
        return Principal(id = principalId ?: doc.id.toString(), roles = roles)
    }
}

/**
 * Lets a deployment accept static config-file admin/service accounts, username/password RBAC
 * logins, and application-issued session tokens on the same server, trying each in turn - all
 * coexist, none shadows the others (component 41 spec §3). Returns the first successful
 * [Principal]; if every engine rejects, rethrows the *last* engine's exception (an arbitrary but
 * deterministic tie-break, per spec §5).
 */
public class CompositeAuthEngine(
    private val engines: List<Authenticator>,
) : Authenticator {
    init {
        require(engines.isNotEmpty()) { "CompositeAuthEngine needs at least one engine" }
    }

    override suspend fun authenticate(credentials: AuthCredentials): Principal {
        var lastFailure: Throwable? = null
        for (engine in engines) {
            try {
                return engine.authenticate(credentials)
            } catch (e: KdbAuthenticationException) {
                lastFailure = e
            }
        }
        throw lastFailure!!
    }
}
