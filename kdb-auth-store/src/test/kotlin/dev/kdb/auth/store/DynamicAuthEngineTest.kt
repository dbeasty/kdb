package dev.kdb.auth.store

import dev.kdb.auth.AuthAction
import dev.kdb.auth.AuthCredentials
import dev.kdb.auth.KdbAuthenticationException
import dev.kdb.auth.KdbAuthorizationException
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class DynamicAuthEngineTest {
    @Test
    fun authenticatesAndAuthorizesFromLiveStore() =
        runTest {
            val store = RegistryAuthStore.inMemory()
            store.createRole("analyst", setOf("read:orders/*"))
            store.createUser("alice", "hunter2", roles = setOf("analyst"))
            val engine = dynamicAuthEngine(store)

            val principal = engine.authenticator.authenticate(AuthCredentials(user = "alice", password = "hunter2"))
            assertEquals("alice", principal.id)

            engine.authorizer.authorize(principal, AuthAction.SqlExec("orders/invoices", readOnly = true))

            assertFailsWith<KdbAuthorizationException> {
                engine.authorizer.authorize(principal, AuthAction.SqlExec("orders/invoices", readOnly = false))
            }
        }

    @Test
    fun rejectsUnknownUserAndBadPassword() =
        runTest {
            val store = RegistryAuthStore.inMemory()
            store.createUser("alice", "hunter2")
            val engine = dynamicAuthEngine(store)

            assertFailsWith<KdbAuthenticationException> {
                engine.authenticator.authenticate(AuthCredentials(user = "ghost", password = "x"))
            }
            assertFailsWith<KdbAuthenticationException> {
                engine.authenticator.authenticate(AuthCredentials(user = "alice", password = "wrong"))
            }
        }

    @Test
    fun roleChangesTakeEffectImmediately() =
        runTest {
            val store = RegistryAuthStore.inMemory()
            store.createRole("analyst", setOf("read:orders/*"))
            store.createUser("alice", "hunter2", roles = setOf("analyst"))
            val engine = dynamicAuthEngine(store)
            val principal = engine.authenticator.authenticate(AuthCredentials(user = "alice", password = "hunter2"))

            assertFailsWith<KdbAuthorizationException> {
                engine.authorizer.authorize(principal, AuthAction.SqlExec("orders/invoices", readOnly = false))
            }

            store.updateGrants("analyst", setOf("read:orders/*", "write:orders/*"))
            engine.authorizer.authorize(principal, AuthAction.SqlExec("orders/invoices", readOnly = false))
        }
}
