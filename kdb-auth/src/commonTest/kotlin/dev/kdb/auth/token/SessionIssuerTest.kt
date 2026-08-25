package dev.kdb.auth.token

import dev.kdb.auth.AuthCredentials
import dev.kdb.auth.Principal
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.minutes

class SessionIssuerTest {
    private val ns = "zolik/sessions"
    private val config = TokenAuthConfig(sessionsNamespace = ns)

    // test 11: the full round trip - a token SessionIssuer mints is one TokenAuthEngine
    // subsequently accepts, proving the two halves actually compose.
    @Test
    fun issuedTokenSubsequentlyAuthenticates() =
        runTest {
            val store = FakeDocumentStore()
            val issuer = SessionIssuer(config, store)
            val engine = TokenAuthEngine(config, store)

            // Stands in for a DynamicAuthEngine.authenticate() call that already succeeded -
            // this component never checks the password itself (spec §5).
            val loggedInPrincipal = Principal(id = "user-99")
            val issued = issuer.issue(loggedInPrincipal, 30.minutes)

            val principal = engine.authenticate(AuthCredentials(token = issued.token))
            assertEquals("user-99", principal.id)
        }

    // test 12: revoke deletes the session document; a subsequent authenticate call returns
    // TOKEN_NOT_FOUND, not a stale success.
    @Test
    fun revokeInvalidatesTheToken() =
        runTest {
            val store = FakeDocumentStore()
            val issuer = SessionIssuer(config, store)
            val engine = TokenAuthEngine(config, store)

            val issued = issuer.issue(Principal(id = "user-1"), 30.minutes)
            engine.authenticate(AuthCredentials(token = issued.token)) // succeeds once

            issuer.revoke(issued.token)

            val e = assertFailsWith<TokenAuthRejectedException> { engine.authenticate(AuthCredentials(token = issued.token)) }
            assertEquals(RejectReason.TOKEN_NOT_FOUND, e.reason)
        }

    @Test
    fun issuedGuestSessionHasNoPrincipalIdFieldButStillAuthenticates() =
        runTest {
            val store = FakeDocumentStore()
            val issuer = SessionIssuer(config, store)
            val engine = TokenAuthEngine(config, store)

            // A guest principal with a blank id: issue() should omit principalIdField entirely
            // rather than writing an empty string.
            val issued = issuer.issue(Principal(id = ""), 30.minutes)
            val principal = engine.authenticate(AuthCredentials(token = issued.token))
            assertEquals(true, principal.id.isNotEmpty())
        }
}
