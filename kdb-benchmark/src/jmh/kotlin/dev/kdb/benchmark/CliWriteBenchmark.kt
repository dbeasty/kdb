package dev.kdb.benchmark

import dev.kdb.cli.CliSession
import dev.kdb.cli.KdbCli
import dev.kdb.cli.cliExecutePut
import dev.kdb.cli.openCliRuntime
import kotlinx.coroutines.runBlocking
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Level
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown
import java.io.ByteArrayOutputStream
import java.io.PrintStream

/**
 * [cliPut_batch] reuses one [CliSession] (shell-like).
 * [cliPut_oneShot] calls [KdbCli.run] per document (current one-shot `put` command).
 */
@State(Scope.Benchmark)
open class CliWriteBenchmark {
    @Param("100", "1000", "10000")
    var docCount: Int = 100

    private lateinit var seeded: BenchmarkFixture.SeededFile

    @Setup(Level.Trial)
    fun trialSetup() {
        seeded = BenchmarkFixture.seedFileDataRoot(docCount = 0)
    }

    @TearDown(Level.Trial)
    fun trialTearDown() {
        BenchmarkFixture.removeDataRoot(seeded.dataRoot)
    }

    @Benchmark
    fun cliPut_batch() {
        runBlocking {
            val rt = openCliRuntime(seeded.config, BenchmarkFixture.NAMESPACE_ID)
            val session = CliSession(seeded.config, BenchmarkFixture.NAMESPACE_ID, rt)
            for (i in 0 until docCount) {
                cliExecutePut(session, """{"userId":"batch-$i"}""")
            }
        }
    }

    /**
     * Models one-shot `kdb put` (runtime reopen per document). Uses smaller [oneShotCount]
     * than [docCount] so the suite finishes in reasonable time; scale cost via [cliPut_batch].
     */
    @Param("10", "100")
    var oneShotCount: Int = 10

    @Benchmark
    fun cliPut_oneShot() {
        val stdout = ByteArrayOutputStream()
        val prior = System.out
        System.setOut(PrintStream(stdout))
        try {
            for (i in 0 until oneShotCount) {
                KdbCli.run(
                    arrayOf(
                        "--quiet",
                        "--data-dir",
                        seeded.dataRoot,
                        "put",
                        BenchmarkFixture.NAMESPACE_ID,
                        """{"userId":"oneshot-$i"}""",
                    ),
                )
            }
        } finally {
            System.setOut(prior)
        }
    }
}
