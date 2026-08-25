package dev.kdb.auth.token

import dev.kdb.auth.Principal
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.kdbSha256
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlin.time.Duration

/** Result of [SessionIssuer.issue]. */
public data class SessionToken(
    val token: String,
    val expiresAt: KdbTimestamp,
)

/** Narrow write capability [SessionIssuer] needs - symmetric to [DocumentReader], so this can be
 * unit-tested with a trivial in-memory fake. Writes are unconditional (Upsert-shaped, per
 * component 41 spec §5 and the gap analysis's finding that session writes aren't CAS) - no
 * BaseVersion. */
public interface DocumentWriter {
    public suspend fun upsert(
        namespace: String,
        docId: String,
        json: String,
    )

    public suspend fun delete(
        namespace: String,
        docId: String,
    )
}

/**
 * Mints and stores a session document after a successful login - the seam between
 * [dev.kdb.auth.store.DynamicAuthEngine] (proves who you are with a password) and
 * [TokenAuthEngine] (validates a token against what this class wrote). Not new engine machinery:
 * a thin helper over an ordinary unconditional document write.
 *
 * [revoke] derives the same document id [issue] used from the token value alone (see
 * [sessionDocId]), so it needs no [DocumentReader] dependency to look the session up first.
 */
public class SessionIssuer(
    private val config: TokenAuthConfig,
    private val documentWriter: DocumentWriter,
) {
    public suspend fun issue(
        principal: Principal,
        ttl: Duration,
    ): SessionToken {
        val token = KdbUuid.random().toString()
        val expiresAt = KdbTimestamp.fromEpochMicros(KdbTimestamp.now().toEpochMicros() + ttl.inWholeMicroseconds)
        val json =
            buildJsonObject {
                put(config.tokenField, JsonPrimitive(token))
                put(config.expiresAtField, JsonPrimitive(expiresAt.toEpochMicros()))
                if (principal.id.isNotEmpty()) {
                    put(config.principalIdField, JsonPrimitive(principal.id))
                }
            }
        documentWriter.upsert(config.sessionsNamespace, sessionDocId(token), json.toString())
        return SessionToken(token, expiresAt)
    }

    /** Deletes the session document for [token] - a subsequent [TokenAuthEngine.authenticate]
     * call with this token returns [RejectReason.TOKEN_NOT_FOUND], not a stale success. */
    public suspend fun revoke(token: String) {
        documentWriter.delete(config.sessionsNamespace, sessionDocId(token))
    }

    /** Deterministic doc id from the token value alone, matching
     * [dev.kdb.auth.store.RegistryAuthStore]'s own deterministicId convention - lets [revoke]
     * compute the same id [issue] used without a lookup. */
    private fun sessionDocId(token: String): String {
        val digest = kdbSha256(token.encodeToByteArray())
        return KdbUuid.fromBytes(digest.copyOfRange(0, 16)).toString()
    }
}
