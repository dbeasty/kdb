package dev.kdb.auth.static

import dev.kdb.auth.AuthAction
import dev.kdb.auth.AuthCredentials
import dev.kdb.auth.KdbAuthenticationException
import dev.kdb.auth.KdbAuthorizationException
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class StaticAuthEngineTest {
    private val engine =
        staticAuthEngine(
            StaticAuthConfig(
                users =
                    mapOf(
                        "reader" to StaticUserConfig(secret = "r-secret", roles = listOf("reader")),
                        "writer" to StaticUserConfig(secret = "w-secret", roles = listOf("writer")),
                    ),
                roles =
                    mapOf(
                        "reader" to listOf("read:demo/*"),
                        "writer" to listOf("read:demo/*", "write:demo/*"),
                    ),
            ),
        )

    @Test
    fun authenticate_validUser() =
        runTest {
            val p = engine.authenticator.authenticate(AuthCredentials(user = "reader", password = "r-secret"))
            assertEquals("reader", p.id)
        }

    @Test
    fun authenticate_rejectsBadSecret() =
        runTest {
            assertFailsWith<KdbAuthenticationException> {
                engine.authenticator.authenticate(AuthCredentials(user = "reader", password = "wrong"))
            }
        }

    @Test
    fun authorize_readerCanSelect() =
        runTest {
            val p = engine.authenticator.authenticate(AuthCredentials(user = "reader", password = "r-secret"))
            engine.authorizer.authorize(p, AuthAction.SqlExec("demo/users", readOnly = true))
        }

    @Test
    fun authorize_readerCannotWrite() =
        runTest {
            val p = engine.authenticator.authenticate(AuthCredentials(user = "reader", password = "r-secret"))
            assertFailsWith<KdbAuthorizationException> {
                engine.authorizer.authorize(p, AuthAction.TxCommit("demo/users"))
            }
        }
}
