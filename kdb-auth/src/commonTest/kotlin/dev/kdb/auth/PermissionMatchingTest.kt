package dev.kdb.auth

import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class PermissionMatchingTest {
    @Test
    fun wildcardMatchesChildNamespace() {
        assertTrue(permissionMatchesPath("read:demo/*", "demo/users"))
        assertTrue(permissionMatchesPath("read:demo/*", "demo"))
        assertFalse(permissionMatchesPath("read:demo/*", "other/users"))
    }

    private val roles =
        mapOf(
            "db-writer" to setOf("write:orders"),
            "wildcard-writer" to setOf("write:orders/*"),
            "collection-reader" to setOf("read:orders/invoices"),
            "doc-writer" to setOf("write:orders/invoices/doc-1"),
        )

    @Test
    fun databaseGrantCoversEveryCollectionAndDocumentBeneathIt() {
        val principal = Principal(id = "u1", roles = setOf("db-writer"))
        assertTrue(principalHasPermission(principal, roles, "write", ResourcePath("orders")))
        assertTrue(principalHasPermission(principal, roles, "write", ResourcePath("orders", "invoices")))
        assertTrue(
            principalHasPermission(principal, roles, "write", ResourcePath("orders", "invoices", "doc-1")),
        )
        assertFalse(principalHasPermission(principal, roles, "write", ResourcePath("shipping")))
    }

    @Test
    fun wildcardGrantBehavesLikeDatabaseGrantForBackwardCompatibility() {
        val principal = Principal(id = "u2", roles = setOf("wildcard-writer"))
        assertTrue(
            principalHasPermission(principal, roles, "write", ResourcePath("orders", "invoices", "doc-1")),
        )
    }

    @Test
    fun collectionGrantCoversDocumentsInItButNotSiblingCollections() {
        val principal = Principal(id = "u3", roles = setOf("collection-reader"))
        assertTrue(
            principalHasPermission(principal, roles, "read", ResourcePath("orders", "invoices", "doc-1")),
        )
        assertFalse(principalHasPermission(principal, roles, "read", ResourcePath("orders", "shipments")))
    }

    @Test
    fun documentGrantDoesNotLeakToSiblingDocuments() {
        val principal = Principal(id = "u4", roles = setOf("doc-writer"))
        assertTrue(
            principalHasPermission(principal, roles, "write", ResourcePath("orders", "invoices", "doc-1")),
        )
        assertFalse(
            principalHasPermission(principal, roles, "write", ResourcePath("orders", "invoices", "doc-2")),
        )
    }

    @Test
    fun namespaceOnlyOverloadStaysCompatible() {
        val principal = Principal(id = "u5", roles = setOf("wildcard-writer"))
        assertTrue(principalHasPermission(principal, roles, "write", "orders/invoices"))
    }

    /**
     * "resolve" is the Go server's permission to settle replication conflicts a resolver authority
     * owns. Kotlin enforces no action with it, but grants are shared strings: a role holding
     * "resolve:app/data" must match exactly the resources the Go matcher gives it
     * (go/kdb/auth TestResolveGrantMatchesLikeKotlin), and grant no other kind.
     */
    @Test
    fun resolveGrantIsAnOrdinaryKindScopedLikeAnyOther() {
        val grants = mapOf("resolver" to setOf("resolve:app/data"), "writer" to setOf("write:app/data"))
        val resolver = Principal(id = "r", roles = setOf("resolver"))
        val writer = Principal(id = "w", roles = setOf("writer"))
        assertTrue(permissionKind("resolve:app/data") == "resolve")
        assertTrue(principalHasPermission(resolver, grants, "resolve", ResourcePath.of("app/data")))
        assertFalse(principalHasPermission(resolver, grants, "resolve", ResourcePath.of("app/other")))
        assertFalse(principalHasPermission(resolver, grants, "write", ResourcePath.of("app/data")))
        assertFalse(principalHasPermission(writer, grants, "resolve", ResourcePath.of("app/data")))
    }
}
