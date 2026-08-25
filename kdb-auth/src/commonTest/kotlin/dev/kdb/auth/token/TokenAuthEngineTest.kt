package dev.kdb.auth.token

import dev.kdb.auth.AuthCredentials
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbDocument
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** Trivial in-memory fake per component 41 spec §9: TokenAuthEngine is tested with no
 * password/RBAC machinery involved at all. */
class FakeDocumentStore : DocumentReader, DocumentWriter {
    private val docs = mutableMapOf<String, MutableMap<String, KdbDocument>>()

    override suspend fun findByField(
        namespace: String,
        field: String,
        value: String,
    ): KdbDocument? {
        val ns = docs[namespace] ?: return null
        return ns.values.firstOrNull { doc ->
            val json = Json.parseToJsonElement(doc.json).jsonObject
            (json[field] as? JsonPrimitive)?.content == value
        }
    }

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

    fun putRaw(
        namespace: String,
        docId: String,
        json: String,
    ) {
        docs.getOrPut(namespace) { mutableMapOf() }[docId] = KdbDocument(KdbUuid.fromString(docId), json)
    }
}

class TokenAuthEngineTest {
    private val ns = "zolik/sessions"
    private val config = TokenAuthConfig(sessionsNamespace = ns)

    private fun sessionJson(
        token: String,
        expiresAtMicros: Long?,
        userId: String? = "user-1",
    ): String =
        buildJsonObject {
            put("token", JsonPrimitive(token))
            if (expiresAtMicros != null) put("expiresAt", JsonPrimitive(expiresAtMicros))
            if (userId != null) put("userId", JsonPrimitive(userId))
        }.toString()

    private fun futureMicros(deltaMicros: Long = 3_600_000_000L): Long = KdbTimestamp.now().toEpochMicros() + deltaMicros

    private fun pastMicros(deltaMicros: Long = 3_600_000_000L): Long = KdbTimestamp.now().toEpochMicros() - deltaMicros

    // test 1
    @Test
    fun validUnexpiredTokenAuthenticates() =
        runTest {
            val store = FakeDocumentStore()
            store.putRaw(ns, KdbUuid.random().toString(), sessionJson("tok-1", futureMicros(), "user-42"))
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
            store.putRaw(ns, KdbUuid.random().toString(), sessionJson("tok-expired", pastMicros()))
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
            store.putRaw(ns, KdbUuid.random().toString(), sessionJson("tok-boundary", now))
            val engine = TokenAuthEngine(config, store)
            val e = assertFailsWith<TokenAuthRejectedException> { engine.authenticate(AuthCredentials(token = "tok-boundary")) }
            assertEquals(RejectReason.TOKEN_EXPIRED, e.reason)
        }

    // test 5
    @Test
    fun guestSessionWithoutPrincipalIdStillAuthenticates() =
        runTest {
            val store = FakeDocumentStore()
            val docId = KdbUuid.random().toString()
            store.putRaw(ns, docId, sessionJson("tok-guest", futureMicros(), userId = null))
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
            val failing =
                DocumentReader { _, _, _ -> throw IllegalStateException("storage outage") }
            val engine = TokenAuthEngine(config, failing)
            assertFailsWith<IllegalStateException> { engine.authenticate(AuthCredentials(token = "whatever")) }
        }

    // test 10
    @Test
    fun sessionDocumentWithMissingExpiresAtTreatedAsInvalid() =
        runTest {
            val store = FakeDocumentStore()
            store.putRaw(ns, KdbUuid.random().toString(), sessionJson("tok-no-expiry", expiresAtMicros = null))
            val engine = TokenAuthEngine(config, store)
            val e = assertFailsWith<TokenAuthRejectedException> { engine.authenticate(AuthCredentials(token = "tok-no-expiry")) }
            assertEquals(RejectReason.TOKEN_EXPIRED, e.reason)
        }
}
