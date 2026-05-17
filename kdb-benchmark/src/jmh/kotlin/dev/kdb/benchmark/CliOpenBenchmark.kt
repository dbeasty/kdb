package dev.kdb.benchmark

import dev.kdb.cli.openCliRuntime
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Level
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown
import kotlin.io.path.createTempDirectory

/**
 * Measures [openCliRuntime] on file-backed namespaces.
 * One-shot CLI commands pay this cost on every invocation unless using the shell.
 */
@State(Scope.Benchmark)
open class CliOpenBenchmark {
    private lateinit var dataRoot: String

    @Setup(Level.Trial)
    fun trialSetup() {
        val seeded = BenchmarkFixture.seedFileDataRoot(docCount = 100)
        dataRoot = seeded.dataRoot
    }

    @TearDown(Level.Trial)
    fun trialTearDown() {
        BenchmarkFixture.removeDataRoot(dataRoot)
    }

    @Benchmark
    fun openCliRuntime_warm() {
        val config = dev.kdb.cli.CliConfig(dataDir = dataRoot, quiet = true)
        openCliRuntime(config, BenchmarkFixture.NAMESPACE_ID)
    }
}

@State(Scope.Benchmark)
open class CliOpenColdBenchmark {
    private lateinit var dataRoot: String

    @Setup(Level.Invocation)
    fun invocationSetup() {
        dataRoot = createTempDirectory("kdb-bench-cold-").toString()
    }

    @TearDown(Level.Invocation)
    fun invocationTearDown() {
        BenchmarkFixture.removeDataRoot(dataRoot)
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    fun openCliRuntime_cold() {
        val config = dev.kdb.cli.CliConfig(dataDir = dataRoot, quiet = true)
        openCliRuntime(config, BenchmarkFixture.NAMESPACE_ID)
    }
}
