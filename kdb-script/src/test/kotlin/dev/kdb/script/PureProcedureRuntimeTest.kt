package dev.kdb.script

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * The Kotlin half of Go's `kdb/script/runtime_test.go`. The corpus proves the two runtimes agree
 * on what a procedure decides; these prove they agree on what a procedure may not do.
 */
class PureProcedureRuntimeTest {
    private val runtime = PureProcedureRuntime()

    @Test
    fun returnsJson() {
        assertEquals("""{"sum":5}""", runtime.call("function main(a) { return { sum: a.x + a.y }; }", """{"x":2,"y":3}"""))
    }

    @Test
    fun missingMainIsReported() {
        val e = assertFailsWith<ProcException> { runtime.call("var notMain = 1;", "{}") }
        assertTrue(e.message!!.contains("defines no main"), e.message!!)
    }

    @Test
    fun returningUndefinedIsReported() {
        val e = assertFailsWith<ProcException> { runtime.call("function main() {}", "{}") }
        assertTrue(e.message!!.contains("returned undefined"), e.message!!)
    }

    @Test
    fun compileRefusesBrokenSource() {
        assertFailsWith<ProcException.CompileError> { runtime.compile("function main( {") }
    }

    @Test
    fun deniesEverySourceOfNonDeterminism() {
        val denied =
            listOf(
                "new Date()",
                "Date.now()",
                "Math.random()",
                "Math.pow(2, 3)",
                "Math.sin(1)",
                "\"a\".localeCompare(\"b\")",
                "(1234.5).toLocaleString()",
            )
        for (expr in denied) {
            val e = assertFailsWith<ProcException>("$expr was allowed") { runtime.call("function main() { return { v: $expr }; }", "{}") }
            assertTrue(e.message!!.contains("not available in a deterministic procedure"), "$expr: ${e.message}")
        }
    }

    @Test
    fun exactArithmeticStaysAvailable() {
        // Only the implementation-dependent Math functions go; a procedure that rounds or clamps
        // a number is ordinary and deterministic.
        assertEquals(
            """{"v":3}""",
            runtime.call("function main() { return { v: Math.max(Math.floor(2.7), Math.abs(-1), Math.sqrt(9)) }; }", "{}"),
        )
    }

    @Test
    fun hasNoDatabaseAtAll() {
        for (expr in listOf("typeof kdb", "typeof require", "typeof process", "typeof setTimeout")) {
            assertEquals("""{"t":"undefined"}""", runtime.call("function main() { return { t: $expr }; }", "{}"), expr)
        }
    }

    @Test
    fun redefiningJsonDoesNotChangeTheBridge() {
        val source = """JSON.stringify = function () { return "\"hijacked\""; }; function main(a) { return { x: a.x }; }"""
        assertEquals("""{"x":1}""", runtime.call(source, """{"x":1}"""))
    }

    @Test
    fun redefiningTheBridgeDoesNothing() {
        val source = """globalThis.__kdb_call = function () { return "\"hijacked\""; }; function main() { return { ok: true }; }"""
        assertEquals("""{"ok":true}""", runtime.call(source, "{}"))
    }

    @Test
    fun theFunctionConstructorCannotEscape() {
        val e =
            assertFailsWith<ProcException> {
                runtime.call("""function main() { return (function(){}).constructor("return Date.now()")(); }""", "{}")
            }
        assertTrue(e.message!!.contains("Date.now is not available"), e.message!!)
    }

    @Test
    fun anInfiniteLoopIsStopped() {
        val short = PureProcedureRuntime(ProcLimits(wallClockMillis = 300, maxStatements = 50_000))
        val started = System.currentTimeMillis()
        assertFailsWith<ProcException> { short.call("function main() { while (true) {} }", "{}") }
        assertTrue(System.currentTimeMillis() - started < 20_000, "the interrupt should not take that long")
    }

    @Test
    fun callsCannotShareState() {
        // A fresh context per call, so a procedure that counts its invocations always sees 1 -
        // otherwise a document's resolution would depend on how many conflicts preceded it.
        val source =
            "if (typeof globalThis.n !== \"number\") { globalThis.n = 0; } function main() { globalThis.n++; return { n: globalThis.n }; }"
        repeat(3) { assertEquals("""{"n":1}""", runtime.call(source, "{}")) }
    }
}
