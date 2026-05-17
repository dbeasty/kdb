package dev.kdb.benchmark

import dev.kdb.cli.CliSession
import dev.kdb.cli.cliExecuteGet
import dev.kdb.cli.cliExecuteQuery
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

@State(Scope.Benchmark)
open class CliQueryBenchmark {
    @Param("100", "1000", "10000")
    var docCount: Int = 1000

    private lateinit var seeded: BenchmarkFixture.SeededFile
    private lateinit var session: CliSession
    private val stdoutCapture = ByteArrayOutputStream()

    @Setup(Level.Trial)
    fun trialSetup() {
        seeded = BenchmarkFixture.seedFileDataRoot(docCount)
        val rt = openCliRuntime(seeded.config, BenchmarkFixture.NAMESPACE_ID)
        session = CliSession(seeded.config, BenchmarkFixture.NAMESPACE_ID, rt)
    }

    @TearDown(Level.Trial)
    fun trialTearDown() {
        BenchmarkFixture.removeDataRoot(seeded.dataRoot)
    }

    @Benchmark
    fun cliQuery_pointSelect() {
        runBlocking {
            val prior = System.out
            System.setOut(PrintStream(stdoutCapture))
            try {
                cliExecuteQuery(session, "SELECT _doc FROM users WHERE userId = 'u1'")
            } finally {
                System.setOut(prior)
            }
        }
    }

    @Benchmark
    fun cliQuery_fullScan() {
        runBlocking {
            val prior = System.out
            System.setOut(PrintStream(stdoutCapture))
            try {
                cliExecuteQuery(session, "SELECT _doc FROM users")
            } finally {
                System.setOut(prior)
            }
        }
    }

    @Benchmark
    fun cliGet() {
        runBlocking {
            val prior = System.out
            System.setOut(PrintStream(stdoutCapture))
            try {
                cliExecuteGet(session, seeded.firstDocId)
            } finally {
                System.setOut(prior)
            }
        }
    }
}
