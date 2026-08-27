package dev.kdb.auth.token

import dev.kdb.auth.AuthCredentials
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbDocument
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.hours

/** Trivial in-memory fake per component 41 spec §9: TokenAuthEngine is tested with no
 * password/RBAC machinery involved at all. */
class FakeDocumentStore : DocumentReader, DocumentWriter {
    private val docs = mutableMapOf<String, MutableMap<String, KdbDocument>>()

    override suspend fun getById(
        namespace: String,
        docId: String,
    ): KdbDocument? = docs[namespace]?.get(docId)

    override suspend fun upsert(
        namespace: String,
        docId: String,
        json: String,
    ) {
        docs.getOrPut(namespace) { mutableMapOf() }[docId] = KdbDocument(KdbUuid.fromString(docId), json)
    }

    override suspend fun delete(
        namespace: String,
        docId: String,
    ) {
        docs[namespace]?.remove(docId)
    }

    /** Writes a session document at the same derived id [TokenAuthEngine.authenticate] will look
     * it up by (see [sessionDocId]) - matching what [SessionIssuer.issue] does in production,
     * now that lookup is id-based rather than a scan for a matching token field. */
    fun putSession(
        namespace: String,
        token: String,
        json: String,
    ) {
        val docId = sessionDocId(token)
        docs.getOrPut(namespace) { mutableMapOf() }[docId] = KdbDocument(KdbUuid.fromString(docId), json)
    }
}

class TokenAuthEngineTest {
    private val ns = "zolik/sessions"
    private val config = TokenAuthConfig(sessionsNamespace = ns)

    private fun sessionJson(
        expiresAtMicros: Long?,
        userId: String? = "user-1",
        roles: List<String>? = null,
    ): String =
        buildJsonObject {
            if (expiresAtMicros != null) put("expiresAt", JsonPrimitive(expiresAtMicros))
            if (userId != null) put("userId", JsonPrimitive(userId))
            if (roles != null) put("roles", buildJsonArray { roles.forEach { add(JsonPrimitive(it)) } })
        }.toString()

    private fun futureMicros(deltaMicros: Long = 3_600_000_000L): Long = KdbTimestamp.now().toEpochMicros() + deltaMicros

    private fun pastMicros(deltaMicros: Long = 3_600_000_000L): Long = KdbTimestamp.now().toEpochMicros() - deltaMicros

    // test 1
    @Test
    fun validUnexpiredTokenAuthenticates() =
        runTest {
            val store = FakeDocumentStore()
            store.putSession(ns, "tok-1", sessionJson(futureMicros(), "user-42"))
            val engine = TokenAuthEngine(config, store)
            val principal = engine.authenticate(AuthCredentials(token = "tok-1"))
            assertEquals("user-42", principal.id)
        }

    // test 2
    @Test
    fun unknownTokenRejectedWithTokenNotFound() =
        runTest {
            val engine = TokenAuthEngine(config, FakeDocumentStore())
            val e = assertFailsWith<TokenAuthRejectedException> { engine.authenticate(AuthCredentials(token = "no-such-token")) }
            assertEquals(RejectReason.TOKEN_NOT_FOUND, e.reason)
        }

    // test 3
    @Test
    fun expiredTokenRejectedWithTokenExpired() =
        runTest {
            val store = FakeDocumentStore()
            store.putSession(ns, "tok-expired", sessionJson(pastMicros()))
            val engine = TokenAuthEngine(config, store)
            val e = assertFailsWith<TokenAuthRejectedException> { engine.authenticate(AuthCredentials(token = "tok-expired")) }
            assertEquals(RejectReason.TOKEN_EXPIRED, e.reason)
        }

    // test 4: boundary is exclusive - a token expiring exactly now is invalid, not valid.
    @Test
    fun tokenExactlyAtExpiryBoundaryIsRejected() =
        runTest {
            val store = FakeDocumentStore()
            val now = KdbTimestamp.now().toEpochMicros()
            store.putSession(ns, "tok-boundary", sessionJson(now))
            val engine = TokenAuthEngine(config, store)
            val e = assertFailsWith<TokenAuthRejectedException> { engine.authenticate(AuthCredentials(token = "tok-boundary")) }
            assertEquals(RejectReason.TOKEN_EXPIRED, e.reason)
        }

    // test 5
    @Test
    fun guestSessionWithoutPrincipalIdStillAuthenticates() =
        runTest {
            val store = FakeDocumentStore()
            store.putSession(ns, "tok-guest", sessionJson(futureMicros(), userId = null))
            val engine = TokenAuthEngine(config, store)
            val principal = engine.authenticate(AuthCredentials(token = "tok-guest"))
            assertTrue(principal.id.isNotEmpty(), "guest principal must still have some id")
        }

    // test 6
    @Test
    fun missingBearerTokenRejectedAsMalformed() =
        runTest {
            val engine = TokenAuthEngine(config, FakeDocumentStore())
            val e = assertFailsWith<TokenAuthRejectedException> { engine.authenticate(AuthCredentials()) }
            assertEquals(RejectReason.MALFORMED_CREDENTIALS, e.reason)
        }

    // test 9: a genuine storage failure propagates, not mapped to Rejected.
    @Test
    fun storageFailurePropagatesAsException() =
        runTest {
            val failing = DocumentReader { _, _ -> throw IllegalStateException("storage outage") }
            val engine = TokenAuthEngine(config, failing)
            assertFailsWith<IllegalStateException> { engine.authenticate(AuthCredentials(token = "whatever")) }
        }

    // test 10
    @Test
    fun sessionDocumentWithMissingExpiresAtTreatedAsInvalid() =
        runTest {
            val store = FakeDocumentStore()
            store.putSession(ns, "tok-no-expiry", sessionJson(expiresAtMicros = null))
            val engine = TokenAuthEngine(config, store)
            val e = assertFailsWith<TokenAuthRejectedException> { engine.authenticate(AuthCredentials(token = "tok-no-expiry")) }
            assertEquals(RejectReason.TOKEN_EXPIRED, e.reason)
        }

    /**
     * Regression test for docs/kdb-finish-up-plan.md's 1-K9: the token-authenticated Principal
     * used to always get an empty role set (SessionIssuer.issue never persisted the roles it was
     * given, and TokenAuthEngine never read any back), so any RBAC authorizer denied a
     * token-authenticated principal everything regardless of what its original login actually
     * granted.
     */
    @Test
    fun rolesFromTheSessionDocumentSurviveOntoTheAuthenticatedPrincipal() =
        runTest {
            val store = FakeDocumentStore()
            store.putSession(ns, "tok-roles", sessionJson(futureMicros(), "user-7", roles = listOf("reader", "writer")))
            val engine = TokenAuthEngine(config, store)
            val principal = engine.authenticate(AuthCredentials(token = "tok-roles"))
            assertEquals(setOf("reader", "writer"), principal.roles)
        }

    @Test
    fun sessionWithNoRolesFieldAuthenticatesWithEmptyRoles() =
        runTest {
            val store = FakeDocumentStore()
            store.putSession(ns, "tok-noroles", sessionJson(futureMicros(), "user-7"))
            val engine = TokenAuthEngine(config, store)
            val principal = engine.authenticate(AuthCredentials(token = "tok-noroles"))
            assertEquals(emptySet(), principal.roles)
        }

    /**
     * Regression test for the other half of 1-K9: SessionIssuer.issue used to write the raw
     * bearer token into the session document's own body (needed for the old O(n) field-scan
     * lookup) - a cleartext copy of the credential sitting in whatever storage backs the sessions
     * namespace. Lookup is id-derived now (see sessionDocId), so the token must not appear in the
     * document at all.
     */
    @Test
    fun issuedSessionDocumentDoesNotContainTheRawToken() =
        runTest {
            val writes = mutableMapOf<String, String>()
            val writer =
                object : DocumentWriter {
                    override suspend fun upsert(namespace: String, docId: String, json: String) {
                        writes[docId] = json
                    }

                    override suspend fun delete(namespace: String, docId: String) = Unit
                }
            val issuer = SessionIssuer(config, writer)
            val token = issuer.issue(dev.kdb.auth.Principal(id = "user-1"), 1.hours).token

            val storedJson = writes.getValue(sessionDocId(token))
            val fields = Json.parseToJsonElement(storedJson).jsonObject.keys
            assertFalse(fields.any { it.contains("token", ignoreCase = true) }, "session document must not carry a token-shaped field: $fields")
            assertFalse(storedJson.contains(token), "session document body must not contain the raw token value at all")
        }
}
