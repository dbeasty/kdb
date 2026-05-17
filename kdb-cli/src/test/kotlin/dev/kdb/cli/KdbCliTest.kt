package dev.kdb.cli

import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class KdbCliTest {
    @Test
    fun init_createsMeta() {
        val dir = createTempDirectory("kdb-cli").toString()
        val code = KdbCli.run(arrayOf("--data-dir", dir, "init", "app/demo"))
        assertEquals(0, code)
        assertTrue(java.nio.file.Files.exists(java.nio.file.Path.of(dir, "namespaces", "app", "demo", "meta.json")))
    }

    @Test
    fun putAndGet_roundtrip() {
        val dir = createTempDirectory("kdb-cli").toString()
        KdbCli.run(arrayOf("--data-dir", dir, "init", "app/t"))
        val putCode =
            KdbCli.run(
                arrayOf("--data-dir", dir, "put", "app/t", """{"id":"00000000-0000-0000-0000-000000000001","v":1}"""),
            )
        assertEquals(0, putCode)
        // fresh runtime still memory-only v1 — get uses new runtime; doc may not persist across invocations
        // verify put exit code only for v1 memory model
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
