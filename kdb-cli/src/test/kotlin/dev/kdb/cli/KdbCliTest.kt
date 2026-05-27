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
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

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
    fun put_withoutId_printsDocIdAndInjectsId() {
        val dir = createTempDirectory("kdb-cli").toString()
        assertEquals(0, KdbCli.run(arrayOf("--data-dir", dir, "init", "app/t")))
        val out =
            captureStdout {
                assertEquals(
                    0,
                    KdbCli.run(
                        arrayOf("--data-dir", dir, "put", "app/t", """{"name":"Ada"}"""),
                    ),
                )
            }
        val parsed = Json.parseToJsonElement(out.trim()).jsonObject
        val docId = parsed["docId"]!!.jsonPrimitive.content
        assertTrue(parsed["commit"]!!.jsonPrimitive.content.length == 64)
        runBlocking {
            val rt = openFileRuntime(dir, "app", "app/t")
            val head = rt.dag.head()
            val doc = rt.storage.getDocument("app/t", KdbUuid.fromString(docId), head)
            assertNotNull(doc)
            assertTrue(doc!!.json.contains("\"id\":"))
            assertTrue(doc.json.contains("\"name\":\"Ada\""))
        }
    }

    @Test
    fun put_withExplicitId_stdoutDocIdMatches() {
        val dir = createTempDirectory("kdb-cli").toString()
        val docId = "00000000-0000-0000-0000-000000000002"
        assertEquals(0, KdbCli.run(arrayOf("--data-dir", dir, "init", "app/t")))
        val out =
            captureStdout {
                assertEquals(
                    0,
                    KdbCli.run(
                        arrayOf(
                            "--data-dir",
                            dir,
                            "put",
                            "app/t",
                            """{"id":"$docId","v":1}""",
                        ),
                    ),
                )
            }
        val parsed = Json.parseToJsonElement(out.trim()).jsonObject
        assertEquals(docId, parsed["docId"]!!.jsonPrimitive.content)
    }

    @Test
    fun get_acceptsDocIdPrefix() {
        val dir = createTempDirectory("kdb-cli").toString()
        val docId = "00000000-0000-0000-0000-00000000000a"
        assertEquals(0, KdbCli.run(arrayOf("--data-dir", dir, "init", "app/t")))
        assertEquals(
            0,
            KdbCli.run(
                arrayOf("--data-dir", dir, "put", "app/t", """{"id":"$docId","v":1}"""),
            ),
        )
        val out =
            captureStdout {
                assertEquals(
                    0,
                    KdbCli.run(arrayOf("--data-dir", dir, "get", "app/t", "00000000")),
                )
            }
        assertTrue(out.contains("\"v\":1"))
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
}
