package dev.kdb.storage.engine

import dev.kdb.storage.StorageEngineConfig
import dev.kdb.storage.io.FileBackedPlatformIoShimFactory
import dev.kdb.storage.io.PlatformIoConfig
import dev.kdb.storage.wal.DefaultWriteAheadLogFactory
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.time.measureTime

/**
 * Direct engine-level write throughput measurement, mirroring
 * go/kdb/storage/engine/write_throughput_bench_test.go. Not run as part
 * of the default test task's assertions (it prints results and asserts
 * nothing) - a lightweight, bounded substitute for a full JMH benchmark
 * so real hardware concurrency (Dispatchers.Default, real OS threads) is
 * exercised without JMH's fork/warmup overhead. See
 * docs/benchmarks/phase0-baseline.md for the Go-side numbers this is
 * meant to be compared against.
 */
class WriteThroughputBenchTest {
    @Test
    fun writeBlobThroughput_diskWal() =
        runBlocking {
            for (parallelism in listOf(1, 8, 64, 256)) {
                val root = createTempDirectory("kdb-bench-wal-kt").toString()
                val shim = FileBackedPlatformIoShimFactory.open(PlatformIoConfig(rootDirectory = root))
                val config = StorageEngineConfig(globalMemoryBudgetBytes = 64L shl 20, ioShim = shim)
                val wal = DefaultWriteAheadLogFactory().openOrCreate("bench-ns", config, shim)
                val engine = ServerStorageEngine("bench-ns", config, wal)

                val opsPerWorker = 2000 / parallelism.coerceAtMost(2000)
                val totalOps = opsPerWorker * parallelism

                val elapsed =
                    measureTime {
                        coroutineScope {
                            (0 until parallelism).map { worker ->
                                async(Dispatchers.Default) {
                                    val payload = ByteArray(128)
                                    for (i in 0 until opsPerWorker) {
                                        payload[0] = (worker + i).toByte()
                                        engine.writeBlob(payload)
                                    }
                                }
                            }.forEach { it.await() }
                        }
                    }

                val nsPerOp = elapsed.inWholeNanoseconds / totalOps.coerceAtLeast(1)
                val opsPerSec = if (nsPerOp == 0L) 0 else 1_000_000_000L / nsPerOp
                println(
                    "parallel=$parallelism totalOps=$totalOps elapsed=$elapsed " +
                        "nsPerOp=$nsPerOp opsPerSec=$opsPerSec",
                )
                for (s in StageRecorder.Default.snapshot()) {
                    println(
                        "  stage=${s.stage} count=${s.count} mean=${s.mean} p50=${s.p50} p99=${s.p99} max=${s.max}",
                    )
                }
                StageRecorder.Default.reset()
            }
        }
}
