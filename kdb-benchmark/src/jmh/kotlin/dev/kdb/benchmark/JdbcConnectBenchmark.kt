package dev.kdb.benchmark

import dev.kdb.jdbc.KdbConnection
import dev.kdb.jdbc.KdbDriver
import java.sql.DriverManager
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Level
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown

@State(Scope.Benchmark)
open class JdbcConnectBenchmark {
    init {
        KdbDriver
    }

    @Benchmark
    fun jdbcConnect_memory() {
        val conn = DriverManager.getConnection(BenchmarkFixture.memoryJdbcUrl())
        conn.close()
    }
}

@State(Scope.Benchmark)
open class JdbcConnectFileWarmBenchmark {
    init {
        KdbDriver
    }

    private lateinit var seeded: BenchmarkFixture.SeededFile

    @Setup(Level.Trial)
    fun trialSetup() {
        seeded = BenchmarkFixture.seedFileDataRoot(docCount = 100)
        DriverManager.getConnection(BenchmarkFixture.fileJdbcUrl(seeded.dataRoot)).close()
    }

    @TearDown(Level.Trial)
    fun trialTearDown() {
        BenchmarkFixture.removeDataRoot(seeded.dataRoot)
    }

    @Benchmark
    fun jdbcConnect_file_warm() {
        val conn = DriverManager.getConnection(BenchmarkFixture.fileJdbcUrl(seeded.dataRoot))
        conn.close()
    }
}

@State(Scope.Benchmark)
open class JdbcConnectFileColdBenchmark {
    init {
        KdbDriver
    }

    private lateinit var seeded: BenchmarkFixture.SeededFile

    @Setup(Level.Trial)
    fun trialSetup() {
        seeded = BenchmarkFixture.seedFileDataRoot(docCount = 100)
    }

    @TearDown(Level.Trial)
    fun trialTearDown() {
        BenchmarkFixture.removeDataRoot(seeded.dataRoot)
    }

    /** First JDBC connection to an on-disk dataset (includes open + delta replay). */
    @Benchmark
    fun jdbcConnect_file_cold() {
        val conn = DriverManager.getConnection(BenchmarkFixture.fileJdbcUrl(seeded.dataRoot))
        conn.close()
    }
}
