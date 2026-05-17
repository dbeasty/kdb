package dev.kdb.cli

import dev.kdb.jdbc.file.NamespacePathsLock
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class UnlockCliTest {
    @Test
    fun unlock_removesStaleLockFile() {
        val dir = createTempDirectory("kdb-unlock-cli").toString()
        val path = NamespacePathsLock.lockPath(dir)
        path.parent.toFile().mkdirs()
        path.toFile().writeText(
            """
            pid=999999999
            holder=crashed
            host=test
            acquiredAt=2020-01-01T00:00:00Z
            """.trimIndent(),
        )
        assertEquals(0, KdbCli.run(arrayOf("--data-dir", dir, "unlock")))
        assertTrue(!path.toFile().exists())
    }

    @Test
    fun unlock_noLockFile_ok() {
        val dir = createTempDirectory("kdb-unlock-empty").toString()
        assertEquals(0, KdbCli.run(arrayOf("--data-dir", dir, "unlock")))
    }
}
