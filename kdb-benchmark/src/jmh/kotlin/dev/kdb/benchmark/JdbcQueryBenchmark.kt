package dev.kdb.benchmark

import dev.kdb.jdbc.KdbConnection
import dev.kdb.jdbc.KdbDriver
import dev.kdb.jdbc.EmbeddedKdbRuntime
import dev.kdb.jdbc.file.openFileRuntime
import dev.kdb.query.hybrid.HybridQueryRequest
import dev.kdb.schema.KdbSchema
import java.sql.DriverManager
import kotlinx.coroutines.runBlocking
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Level
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown

@State(Scope.Benchmark)
open class JdbcQueryBenchmark {
    init {
        KdbDriver
    }

    private lateinit var seeded: BenchmarkFixture.SeededFile
    private lateinit var fileRuntime: EmbeddedKdbRuntime
    private lateinit var jdbcUrl: String

    @Setup(Level.Trial)
    fun trialSetup() {
        seeded = BenchmarkFixture.seedFileDataRoot(docCount = 1000)
        jdbcUrl = BenchmarkFixture.fileJdbcUrl(seeded.dataRoot)
        fileRuntime =
            runBlocking {
                openFileRuntime(
                    seeded.dataRoot,
                    BenchmarkFixture.CATALOG,
                    BenchmarkFixture.NAMESPACE_ID,
                    BenchmarkFixture.usersSchema,
                )
            }
    }

    @TearDown(Level.Trial)
    fun trialTearDown() {
        BenchmarkFixture.removeDataRoot(seeded.dataRoot)
    }

    @Benchmark
    fun jdbcSelect_loop() {
        val conn = DriverManager.getConnection(jdbcUrl) as KdbConnection
        conn.use {
            val st = it.createStatement()
            repeat(50) {
                val rs = st.executeQuery("SELECT _doc FROM users WHERE userId = 'u1'")
                while (rs.next()) {
                    rs.getString(1)
                }
                rs.close()
            }
        }
    }

    @Benchmark
    fun jdbcPreparedSelect_loop() {
        val conn = DriverManager.getConnection(jdbcUrl) as KdbConnection
        conn.use {
            val ps = it.prepareStatement("SELECT _doc FROM users WHERE userId = 'u1'")
            repeat(50) {
                val rs = ps.executeQuery()
                while (rs.next()) {
                    rs.getString(1)
                }
                rs.close()
            }
        }
    }

    @Benchmark
    fun hybridExecute_direct() {
        runBlocking {
            repeat(50) {
                fileRuntime.hybrid.execute(
                    "SELECT _doc FROM users WHERE userId = 'u1'",
                    HybridQueryRequest(BenchmarkFixture.NAMESPACE_ID, KdbSchema.NONE),
                )
            }
        }
    }

    @Benchmark
    fun jdbcStatement_executeQuery() {
        val conn = DriverManager.getConnection(jdbcUrl) as KdbConnection
        conn.use {
            val st = it.createStatement()
            repeat(50) {
                val rs = st.executeQuery("SELECT _doc FROM users WHERE userId = 'u1'")
                while (rs.next()) {
                    rs.getString(1)
                }
                rs.close()
            }
        }
    }
}

@State(Scope.Benchmark)
open class JdbcQueryFileBenchmark {
    init {
        KdbDriver
    }

    private lateinit var seeded: BenchmarkFixture.SeededFile
    private lateinit var jdbcUrl: String

    @Setup(Level.Trial)
    fun trialSetup() {
        seeded = BenchmarkFixture.seedFileDataRoot(docCount = 1000)
        jdbcUrl = BenchmarkFixture.fileJdbcUrl(seeded.dataRoot)
    }

    @TearDown(Level.Trial)
    fun trialTearDown() {
        BenchmarkFixture.removeDataRoot(seeded.dataRoot)
    }

    @Benchmark
    fun jdbcSelect_file() {
        val conn = DriverManager.getConnection(jdbcUrl) as KdbConnection
        conn.use {
            val st = it.createStatement()
            val rs = st.executeQuery("SELECT _doc FROM users WHERE userId = 'u1'")
            while (rs.next()) {
                rs.getString(1)
            }
            rs.close()
        }
    }
}
