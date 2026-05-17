package dev.kdb.cli

import dev.kdb.codec.KdbUuid
import dev.kdb.jdbc.file.openFileRuntime
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking

class KdbCliTest {
    @Test
    fun init_createsMeta() {
        val dir = createTempDirectory("kdb-cli").toString()
        val code = KdbCli.run(arrayOf("--data-dir", dir, "init", "app/demo"))
        assertEquals(0, code)
        assertTrue(java.nio.file.Files.exists(java.nio.file.Path.of(dir, "ns", "app", "demo", "meta.json")))
    }

    @Test
    fun putAndGet_survivesReopen() {
        val dir = createTempDirectory("kdb-cli").toString()
        val docId = "00000000-0000-0000-0000-000000000001"
        assertEquals(0, KdbCli.run(arrayOf("--data-dir", dir, "init", "app/t")))
        assertEquals(
            0,
            KdbCli.run(
                arrayOf("--data-dir", dir, "put", "app/t", """{"id":"$docId","v":1}"""),
            ),
        )
        runBlocking {
            val rt = openFileRuntime(dir, "app", "app/t")
            val head = rt.dag.head()
            val doc = rt.storage.getDocument("app/t", KdbUuid.fromString(docId), head)
            assertNotNull(doc)
            assertTrue(doc!!.json.contains("\"v\":1"))
        }
    }

    @Test
    fun usage_unknownCommand() {
        val code = KdbCli.run(arrayOf("unknown-cmd"))
        assertEquals(2, code)
    }

    @Test
    fun help_noArgs() {
        assertEquals(2, KdbCli.run(emptyArray()))
    }
}
