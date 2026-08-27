package dev.kdb.auth.static

import dev.kdb.auth.AuthAction
import dev.kdb.auth.AuthCredentials
import dev.kdb.auth.KdbAuthenticationException
import dev.kdb.auth.KdbAuthorizationException
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

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

    @Test
    fun authorize_syncRoleForPeerSync() =
        runTest {
            val syncEngine =
                staticAuthEngine(
                    StaticAuthConfig(
                        users = mapOf("syncer" to StaticUserConfig("s-secret", listOf("syncer"))),
                        roles = mapOf("syncer" to listOf("sync:demo/*")),
                    ),
                )
            val p = syncEngine.authenticator.authenticate(AuthCredentials(user = "syncer", password = "s-secret"))
            syncEngine.authorizer.authorize(p, AuthAction.PeerSync("demo/users"))
        }

    /**
     * Regression tests for docs/kdb-finish-up-plan.md's 1-K8: authenticate() used to compare
     * secrets with plain String `!=`, which short-circuits at the first differing character - a
     * measurable timing side channel an attacker could use to brute-force a static-config secret
     * character-by-character. [constantTimeEquals] must be functionally equivalent to `==` for
     * every case `!=` already handled correctly (equal strings, unequal strings, different
     * lengths, empty strings) - a true timing-channel difference isn't practically provable in a
     * unit test, so this verifies the replacement introduces no functional regression.
     */
    @Test
    fun constantTimeEquals_matchesRegularEqualityForEqualStrings() {
        assertTrue(constantTimeEquals("same-secret", "same-secret"))
        assertTrue(constantTimeEquals("", ""))
    }

    @Test
    fun constantTimeEquals_matchesRegularEqualityForUnequalStrings() {
        assertFalse(constantTimeEquals("secret", "secreu")) // differs in the last character
        assertFalse(constantTimeEquals("secret", "aecret")) // differs in the first character
        assertFalse(constantTimeEquals("secret", "SECRET"))
    }

    @Test
    fun constantTimeEquals_handlesDifferentLengthsWithoutShortCircuiting() {
        assertFalse(constantTimeEquals("short", "much-longer-secret"))
        assertFalse(constantTimeEquals("much-longer-secret", "short"))
        assertFalse(constantTimeEquals("", "nonempty"))
        assertFalse(constantTimeEquals("nonempty", ""))
        // A common length-mismatch pitfall: the shorter string is a strict prefix of the longer
        // one, so a naive char-by-char loop bounded by the shorter length would see no
        // differences at all.
        assertFalse(constantTimeEquals("secret", "secretplus"))
    }

    @Test
    fun authenticate_rejectsSecretsOfDifferentLength() =
        runTest {
            assertFailsWith<KdbAuthenticationException> {
                engine.authenticator.authenticate(AuthCredentials(user = "reader", password = "r-secret-but-longer"))
            }
        }
}
