package dev.kdb.auth.store

import dev.kdb.auth.AuthCredentials
import dev.kdb.auth.KdbAuthenticationException
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.document.KdbDocument
import dev.kdb.schema.KdbSchema
import dev.kdb.storage.mem.InMemoryStorageAdapter
import dev.kdb.transaction.ConflictPolicy
import dev.kdb.transaction.TransactionBuilder
import dev.kdb.transaction.TransactionResult
import dev.kdb.transaction.transactionEngine
import kotlinx.coroutines.test.runTest
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.hours

/**
 * Access tokens (qtask production plan U12), in parity with Go's `go/kdb/auth/tokens.go`. The
 * fixed vectors are pinned on the Go side too (`TestAccessTokenFixedVector`), so a registry
 * written by either language authenticates the other's tokens.
 */
class RegistryAuthStoreTokenTest {
    private companion object {
        const val TOKEN = "kdbt_0123456789abcdef_c2VjcmV0LXNlY3JldC1zZWNyZXQtc2VjcmV0LXNlY3I"
        const val DOC_ID = "7f6f4dd1-927f-28f6-317c-fc029e481bb9"
        const val GO_RECORD =
            """{"kind":"token","tokenId":"0123456789abcdef","user":"svc","label":"qtask-api",""" +
                """"secretSha256":"2c4580586d9cf21dc4222d31dee8d845a7ed9bd9bf8a0937391bc96cb1a2c25a",""" +
                """"createdAt":"2026-10-15T10:00:00Z","expiresAt":"2099-01-01T00:00:00Z"}"""
    }

    @Test
    fun tokensAuthenticateAndRevoke() =
        runTest {
            val store = RegistryAuthStore.inMemory()
            store.createUser("svc", "pw", roles = setOf("reader"))
            val engine = dynamicAuthEngine(store)
            val (tok, rec) = store.createToken("svc", "qtask-api")
            val (other, _) = store.createToken("svc", "worker")
            assertTrue(tok.startsWith("kdbt_${rec.tokenId}_"))
            assertFalse(rec.secretSha256.contains(tok.substringAfterLast('_')), "the record holds the secret")

            val p = engine.authenticator.authenticate(AuthCredentials(token = tok))
            assertEquals("svc", p.id)
            assertEquals(setOf("reader"), p.roles)
            assertEquals("svc", engine.authenticator.authenticate(AuthCredentials(user = "svc", password = tok)).id)

            val tampered = tok.dropLast(2) + "xx"
            assertFailsWith<KdbAuthenticationException> { engine.authenticator.authenticate(AuthCredentials(token = tampered)) }
            assertFailsWith<KdbAuthenticationException> { engine.authenticator.authenticate(AuthCredentials(token = "kdbt_nope")) }

            store.revokeToken(rec.tokenId)
            val revoked = assertFailsWith<KdbAuthenticationException> { engine.authenticator.authenticate(AuthCredentials(token = tok)) }
            assertTrue(revoked.message!!.contains("revoked"))
            assertEquals("svc", engine.authenticator.authenticate(AuthCredentials(token = other)).id)
            assertEquals("svc", engine.authenticator.authenticate(AuthCredentials(user = "svc", password = "pw")).id)

            val (short, shortRec) = store.createToken("svc", "short", 1.hours)
            store.verifyToken(short)
            val expired = assertFailsWith<KdbAuthenticationException> { store.verifyToken(short, Instant.now().plusSeconds(7200)) }
            assertTrue(expired.message!!.contains("expired"))
            assertTrue(shortRec.expiresAt!!.endsWith("Z"))

            assertEquals(3, store.listTokens().size)
            assertTrue(store.listTokens().single { it.tokenId == rec.tokenId }.revoked)
            // Token records share the users namespace; listing users must not trip over them.
            assertEquals(listOf("svc"), store.listUsers().map { it.id })
            assertFailsWith<UserNotFoundException> { store.createToken("nobody") }
            assertFailsWith<TokenNotFoundException> { store.revokeToken("ffffffffffffffff") }
        }

    @Test
    fun goWrittenRecordAuthenticates() =
        runTest {
            val userDag = inMemoryCommitDag(RegistryAuthStore.USERS_NAMESPACE)
            val storage = InMemoryStorageAdapter()
            val store = RegistryAuthStore(userDag, inMemoryCommitDag(RegistryAuthStore.ROLES_NAMESPACE), storage)
            store.createUser("svc", "pw", roles = setOf("reader"))
            // Write the record exactly as Go stores it, under the id Go derives.
            val tx =
                TransactionBuilder(RegistryAuthStore.USERS_NAMESPACE, userDag.head(), KdbUuid.random(), KdbSchema.NONE)
                    .writeDocument(KdbDocument.fromJson(KdbUuid.fromString(DOC_ID), GO_RECORD))
                    .build()
            assertIs<TransactionResult.Success>(transactionEngine(ConflictPolicy.LAST_WRITE).commit(tx, userDag, storage, KdbSchema.NONE))

            val p = dynamicAuthEngine(store).authenticator.authenticate(AuthCredentials(token = TOKEN))
            assertEquals("svc", p.id)
            assertEquals(setOf("reader"), p.roles)
            assertFailsWith<KdbAuthenticationException> {
                dynamicAuthEngine(store).authenticator.authenticate(AuthCredentials(token = TOKEN.dropLast(1) + "A"))
            }
            val listed = store.listTokens().single()
            assertEquals("qtask-api", listed.label)
            assertEquals("2099-01-01T00:00:00Z", listed.expiresAt)
        }

    @Test
    fun kotlinWritesGoBytesUnderGoId() {
        assertEquals(DOC_ID, RegistryAuthStore.tokenDocId("0123456789abcdef").toString())
        val (id, secret) = AccessTokens.parse(TOKEN)!!
        val record =
            TokenRecord(
                tokenId = id,
                user = "svc",
                label = "qtask-api",
                secretSha256 = AccessTokens.secretDigest(secret),
                createdAt = "2026-10-15T10:00:00Z",
                expiresAt = "2099-01-01T00:00:00Z",
            )
        assertEquals(GO_RECORD, AccessTokens.encode(record))
        assertEquals(record, AccessTokens.decode(GO_RECORD))
        assertEquals(
            """{"kind":"token","tokenId":"a","user":"u","secretSha256":"d","createdAt":"c","revoked":true}""",
            AccessTokens.encode(TokenRecord("a", "u", secretSha256 = "d", createdAt = "c", revoked = true)),
        )
    }
}
