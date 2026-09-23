package dev.kdb.script

import dev.kdb.json.JsonValue
import dev.kdb.json.canonicalText
import java.io.File
import java.nio.file.Paths
import kotlin.io.path.isDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * Runs the shared conflict corpus, the same files Go's `kdb/script` runs. It is what turns "the
 * same procedure gives the same answer on either runtime" from an assumption into an assertion:
 * a conflict resolved by a procedure on a Go node and the same conflict examined on a Kotlin one
 * must not disagree.
 *
 * The Go tree authors the files; see the corpus directory's README for their shape.
 */
class ConflictCorpusTest {
    private val corpus: File? by lazy {
        generateSequence(Paths.get(System.getProperty("user.dir"))) { it.parent }
            .firstOrNull { it.resolve("go/testdata/golden/conflict_corpus").isDirectory() }
            ?.resolve("go/testdata/golden/conflict_corpus")
            ?.toFile()
    }

    private fun JsonValue.field(name: String): JsonValue? = (this as JsonValue.JObject).fields[name]

    private fun JsonValue.text(name: String): String? = (field(name) as? JsonValue.JString)?.value

    /** A document body, or null where the case says the side is absent. */
    private fun JsonValue.body(name: String): String? = field(name)?.takeIf { it != JsonValue.JNull }?.toJsonString()

    private fun origin(v: JsonValue?): ProcOrigin =
        ProcOrigin(
            nodeId = v?.text("nodeId") ?: "",
            commit = v?.text("commit") ?: "",
            timestampMicros = ((v?.field("timestampMicros") as? JsonValue.JInt)?.value) ?: 0,
        )

    @Test
    fun everyCaseAgreesWithGo() {
        val dir = corpus ?: fail("corpus not found under go/testdata/golden/conflict_corpus")
        val files = dir.listFiles { f: File -> f.name.endsWith(".json") }?.sortedBy { it.name } ?: emptyList()
        assertTrue(files.isNotEmpty(), "no corpus files in ${dir.path}")
        val runtime = PureProcedureRuntime()
        for (file in files) {
            val case = JsonValue.fromJsonString(file.readText())
            val name = case.text("name") ?: file.name
            val input = case.field("input") ?: fail("$name: no input")
            val origins = input.field("origins")
            val args =
                conflictArgs(
                    ConflictInput(
                        docId = input.text("docId") ?: "",
                        base = input.body("base"),
                        local = input.body("local"),
                        remote = input.body("remote"),
                        localOrigin = origin(origins?.field("local")),
                        remoteOrigin = origin(origins?.field("remote")),
                    ),
                )
            val source = case.text("source") ?: fail("$name: no source")
            val wantError = case.text("errorContains")
            if (wantError != null) {
                val thrown =
                    runCatching { parseDecision(runtime.call(source, args)) }.exceptionOrNull()
                        ?: fail("$name: expected a failure containing \"$wantError\"")
                assertTrue(
                    thrown.message?.contains(wantError) == true,
                    "$name: error \"${thrown.message}\" does not mention \"$wantError\"",
                )
                continue
            }
            val expected = case.field("expected") ?: fail("$name: no expectation")
            val got = parseDecision(runtime.call(source, args))
            val side = ((expected.field("side") as? JsonValue.JInt)?.value ?: 0L).toInt()
            when (expected.text("kind")) {
                "take" -> assertEquals(ProcDecision.Take(side), got, name)
                "fork" -> assertEquals(ProcDecision.Fork(side), got, name)
                "delete" -> assertEquals(ProcDecision.Delete, got, name)
                "defer" -> assertEquals(ProcDecision.Defer, got, name)
                "doc" -> {
                    val doc = got as? ProcDecision.Doc ?: fail("$name: expected a document, got $got")
                    assertEquals(
                        canonicalText(expected.field("doc")!!),
                        canonicalText(JsonValue.fromJsonString(doc.json)),
                        name,
                    )
                }
                else -> fail("$name: unknown expected kind")
            }
        }
    }
}
