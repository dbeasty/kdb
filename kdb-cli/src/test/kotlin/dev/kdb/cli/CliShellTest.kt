package dev.kdb.cli

import dev.kdb.codec.KdbUuid
import dev.kdb.jdbc.file.openFileRuntime
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking

class CliShellTest {
    @Test
    fun shell_putAndQuery() {
        val dir = createTempDirectory("kdb-cli-shell").toString()
        val docId = "00000000-0000-0000-0000-000000000002"
        val lines =
            listOf(
                """put {"id":"$docId","userId":"u1"}""",
                "query SELECT _doc FROM users",
                "exit",
            )
        val code =
            runShell(
                CliConfig(dataDir = dir, quiet = true),
                "app/users",
                ListLineReader(lines),
            )
        assertEquals(0, code)
        runBlocking {
            val rt = openFileRuntime(dir, "app", "app/users")
            val head = rt.dag.head()
            val doc = rt.storage.getDocument("app/users", KdbUuid.fromString(docId), head)
            assertNotNull(doc)
            assertTrue(doc!!.json.contains("\"userId\":\"u1\""))
        }
    }

    @Test
    fun shell_use_switchesNamespace() {
        val dir = createTempDirectory("kdb-cli-shell").toString()
        assertEquals(0, KdbCli.run(arrayOf("--data-dir", dir, "init", "app/a")))
        assertEquals(0, KdbCli.run(arrayOf("--data-dir", dir, "init", "app/b")))
        val out =
            captureStdout {
                val code =
                    runShell(
                        CliConfig(dataDir = dir, quiet = true),
                        "app/a",
                        ListLineReader(listOf("use app/b", "status", "exit")),
                    )
                assertEquals(0, code)
            }
        assertTrue(out.contains("namespace app/b"))
        assertTrue(out.contains("HEAD "))
    }

    @Test
    fun shell_unknownCommand() {
        val dir = createTempDirectory("kdb-cli-shell").toString()
        val err =
            captureStderr {
                val code =
                    runShell(
                        CliConfig(dataDir = dir, quiet = true),
                        "app/t",
                        ListLineReader(listOf("nope", "exit")),
                    )
                assertEquals(0, code)
            }
        assertTrue(err.contains("unknown command: nope"))
    }

    @Test
    fun shell_help() {
        val dir = createTempDirectory("kdb-cli-shell").toString()
        val out =
            captureStdout {
                runBlocking {
                    val rt = openCliRuntime(CliConfig(dataDir = dir, quiet = true), "app/t")
                    val session = CliSession(CliConfig(dataDir = dir, quiet = true), "app/t", rt)
                    executeShellLine(session, "help")
                    executeShellLine(session, "exit")
                }
            }
        assertTrue(out.contains("put <file|json>"))
        assertTrue(out.contains("use <namespace>"))
    }

    @Test
    fun parseArgs_shell() {
        val (_, cmd) = parseArgs(arrayOf("--data-dir", "/tmp/kdb", "shell", "app/ns"))
        assertEquals("app/ns", (cmd as CliCommand.Shell).namespace)
    }

    private fun captureStdout(block: () -> Unit): String {
        val old = System.out
        val buf = ByteArrayOutputStream()
        System.setOut(PrintStream(buf))
        try {
            block()
        } finally {
            System.setOut(old)
        }
        return buf.toString(Charsets.UTF_8)
    }

    private fun captureStderr(block: () -> Unit): String {
        val old = System.err
        val buf = ByteArrayOutputStream()
        System.setErr(PrintStream(buf))
        try {
            block()
        } finally {
            System.setErr(old)
        }
        return buf.toString(Charsets.UTF_8)
    }
}
