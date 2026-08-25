package dev.kdb.auth.token

import dev.kdb.auth.AuthCredentials
import dev.kdb.auth.Authenticator
import dev.kdb.auth.KdbAuthenticationException
import dev.kdb.auth.Principal
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

private class RejectingAuthenticator(
    private val message: String,
) : Authenticator {
    override suspend fun authenticate(credentials: AuthCredentials): Principal =
        throw KdbAuthenticationException(message)
}

private class AcceptingAuthenticator(
    private val principal: Principal,
) : Authenticator {
    override suspend fun authenticate(credentials: AuthCredentials): Principal = principal
}

class CompositeAuthEngineTest {
    // test 7: static reject -> dynamic reject -> token success, proving all three coexist.
    @Test
    fun fallsThroughToFirstSuccessfulEngine() =
        runTest {
            val staticEngine = RejectingAuthenticator("static: no such account")
            val dynamicEngine = RejectingAuthenticator("dynamic: bad password")
            val tokenEngine = AcceptingAuthenticator(Principal(id = "user-from-token"))
            val composite = CompositeAuthEngine(listOf(staticEngine, dynamicEngine, tokenEngine))

            val principal = composite.authenticate(AuthCredentials(token = "tok"))
            assertEquals("user-from-token", principal.id)
        }

    // test 8: deterministic tie-break - the *last* engine's rejection is what's rethrown.
    @Test
    fun rejectsWithLastEnginesReasonWhenAllReject() =
        runTest {
            val first = RejectingAuthenticator("first: rejected")
            val second = RejectingAuthenticator("second: rejected")
            val third = RejectingAuthenticator("third: rejected")
            val composite = CompositeAuthEngine(listOf(first, second, third))

            val e = assertFailsWith<KdbAuthenticationException> { composite.authenticate(AuthCredentials()) }
            assertEquals("third: rejected", e.message)
        }

    @Test
    fun firstEngineWinsWhenItAccepts() =
        runTest {
            val first = AcceptingAuthenticator(Principal(id = "first-wins"))
            val second = RejectingAuthenticator("never reached")
            val composite = CompositeAuthEngine(listOf(first, second))

            val principal = composite.authenticate(AuthCredentials())
            assertEquals("first-wins", principal.id)
        }
}
