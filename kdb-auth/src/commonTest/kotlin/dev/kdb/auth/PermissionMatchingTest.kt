package dev.kdb.auth

import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class PermissionMatchingTest {
    @Test
    fun wildcardMatchesChildNamespace() {
        assertTrue(permissionMatchesNamespace("read:demo/*", "demo/users"))
        assertTrue(permissionMatchesNamespace("read:demo/*", "demo"))
        assertFalse(permissionMatchesNamespace("read:demo/*", "other/users"))
    }
}
