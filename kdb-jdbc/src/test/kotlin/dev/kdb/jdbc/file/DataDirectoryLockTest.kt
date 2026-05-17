package dev.kdb.jdbc.file

import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import org.junit.After
import org.junit.Before

class DataDirectoryLockTest {
    @Before
    fun setUp() {
        DataDirectoryLockRegistry.clearAllForTests()
    }

    @After
    fun tearDown() {
        DataDirectoryLockRegistry.clearAllForTests()
    }

    @Test
    fun acquire_createsLockFile() {
        val dir = createTempDirectory("kdb-lock").toString()
        DataDirectoryLockRegistry.acquire(dir, "test").use {
            assertTrue(NamespacePathsLock.lockPath(dir).toFile().exists())
            val info = readDataDirectoryLockInfo(dir)
            assertEquals(ProcessHandle.current().pid(), info?.pid)
            assertEquals("test", info?.holder)
        }
    }

    @Test
    fun refCount_allowsNestedAcquireInSameJvm() {
        val dir = createTempDirectory("kdb-lock-ref").toString()
        DataDirectoryLockRegistry.acquire(dir, "a").use {
            DataDirectoryLockRegistry.acquire(dir, "b").use { }
        }
        openFileRuntime(dir, "app", "app/t", acquireDirectoryLock = true)
        DataDirectoryLockRegistry.releaseBlocking(dir)
    }

    @Test
    fun secondAcquireInNewRegistryEntryAfterRelease() {
        val dir = createTempDirectory("kdb-lock-re").toString()
        DataDirectoryLockRegistry.acquire(dir, "a").use { }
        DataDirectoryLockRegistry.acquire(dir, "b").use { }
    }

    @Test
    fun releaseStale_removesLockWhenPidGone() {
        val dir = createTempDirectory("kdb-lock-stale").toString()
        val path = NamespacePathsLock.lockPath(dir)
        path.parent.toFile().mkdirs()
        path.toFile().writeText(
            """
            pid=999999999
            holder=crashed-app
            host=test
            acquiredAt=2020-01-01T00:00:00Z
            """.trimIndent(),
        )
        val result = releaseStaleDataDirectoryLock(dir)
        assertTrue(result is StaleLockReleaseResult.Removed)
        assertTrue(!path.toFile().exists())
    }

    @Test
    fun releaseStale_reportsStillHeldWhenLockActive() {
        val dir = createTempDirectory("kdb-lock-live").toString()
        DataDirectoryLockRegistry.acquire(dir, "live").use {
            val result = releaseStaleDataDirectoryLock(dir)
            assertTrue(result is StaleLockReleaseResult.StillHeld)
        }
    }

}
