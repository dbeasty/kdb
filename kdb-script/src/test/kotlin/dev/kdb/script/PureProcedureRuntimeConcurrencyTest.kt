package dev.kdb.script

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * The Kotlin half of Go's `kdb/script/runtime_concurrency_test.go`. [PureProcedureRuntime.call]
 * builds a fresh GraalVM `Context` per call specifically so a procedure cannot see another call's
 * leftovers (see the class doc comment); `PureProcedureRuntimeTest.callsShareNoState` proves that
 * sequentially, three calls in a row. These drive it from real, concurrently running threads -
 * the only way a per-call `Context` that was accidentally reused, or a watchdog thread from one
 * call that reached into another's, would actually show up.
 */
class PureProcedureRuntimeConcurrencyTest {
    @Test
    fun concurrentCallsShareNoState() =
        runBlocking(Dispatchers.Default) {
            val runtime = PureProcedureRuntime()
            val src =
                """
                var seen = (typeof seen === "undefined") ? 0 : seen;
                seen++;
                function main() { return { seen: seen }; }
                """.trimIndent()
            // Compile once so every coroutine races the same source through the runtime at once.
            runtime.compile(src)

            val results =
                (1..200)
                    .map { async { runtime.call(src, "{}") } }
                    .awaitAll()
            for ((i, json) in results.withIndex()) {
                assertEquals("""{"seen":1}""", json, "call $i saw $json - state leaked across concurrent calls")
            }
        }

    @Test
    fun concurrentCallsAreIndependentlyCorrect() =
        runBlocking(Dispatchers.Default) {
            val runtime = PureProcedureRuntime()
            val src = """function main(c) { return { take: c.local.qty >= c.remote.qty ? "local" : "remote" }; }"""
            runtime.compile(src)

            val n = 300
            val outcomes =
                (0 until n)
                    .map { i ->
                        async {
                            val localQty = i % 7
                            val remoteQty = (i * 3) % 11
                            val args = """{"local":{"qty":$localQty},"remote":{"qty":$remoteQty}}"""
                            Triple(i, runtime.call(src, args), if (localQty >= remoteQty) "local" else "remote")
                        }
                    }.awaitAll()
            for ((i, got, want) in outcomes) {
                assertEquals("""{"take":"$want"}""", got, "goroutine-equivalent $i")
            }
        }
}
