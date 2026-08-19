package dev.kdb.auth.store

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class RegistryAuthStoreTest {
    @Test
    fun createsAndReadsUser() =
        runTest {
            val store = RegistryAuthStore.inMemory()
            store.createUser("alice", "hunter2", roles = setOf("analyst"))

            val record = store.getUser("alice")
            assertEquals("alice", record?.id)
            assertEquals(setOf("analyst"), record?.roles)
            assertTrue(store.verifyPassword("alice", "hunter2"))
            assertFalse(store.verifyPassword("alice", "wrong"))
        }

    @Test
    fun rejectsDuplicateUser() =
        runTest {
            val store = RegistryAuthStore.inMemory()
            store.createUser("alice", "hunter2")
            assertFailsWith<UserAlreadyExistsException> {
                store.createUser("alice", "other")
            }
        }

    @Test
    fun updatesCredentialsAndRoles() =
        runTest {
            val store = RegistryAuthStore.inMemory()
            store.createUser("bob", "pw1")

            store.updateCredentials("bob", "pw2")
            assertFalse(store.verifyPassword("bob", "pw1"))
            assertTrue(store.verifyPassword("bob", "pw2"))

            store.assignRole("bob", "analyst")
            store.assignRole("bob", "auditor")
            assertEquals(setOf("analyst", "auditor"), store.getUser("bob")?.roles)

            store.revokeRole("bob", "analyst")
            assertEquals(setOf("auditor"), store.getUser("bob")?.roles)
        }

    @Test
    fun deletesUser() =
        runTest {
            val store = RegistryAuthStore.inMemory()
            store.createUser("carol", "pw")
            store.deleteUser("carol")
            assertNull(store.getUser("carol"))
            assertFailsWith<UserNotFoundException> { store.deleteUser("carol") }
        }

    @Test
    fun listsUsers() =
        runTest {
            val store = RegistryAuthStore.inMemory()
            store.createUser("a", "pw")
            store.createUser("b", "pw")
            val ids = store.listUsers().map { it.id }.toSet()
            assertEquals(setOf("a", "b"), ids)
        }

    @Test
    fun roleCrud() =
        runTest {
            val store = RegistryAuthStore.inMemory()
            store.createRole("analyst", setOf("read:orders/*"))
            assertEquals(setOf("read:orders/*"), store.getRole("analyst")?.grants)

            assertFailsWith<RoleAlreadyExistsException> { store.createRole("analyst") }

            store.updateGrants("analyst", setOf("read:orders/*", "write:orders/invoices"))
            assertEquals(setOf("read:orders/*", "write:orders/invoices"), store.getRole("analyst")?.grants)

            assertEquals(mapOf("analyst" to setOf("read:orders/*", "write:orders/invoices")), store.grantsByRole())

            store.deleteRole("analyst")
            assertNull(store.getRole("analyst"))
            assertFailsWith<RoleNotFoundException> { store.deleteRole("analyst") }
        }
}
