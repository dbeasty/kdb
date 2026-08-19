package dev.kdb.storage.engine

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbDocument
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
 * Read-path counterpart to WriteThroughputBenchTest, requested alongside
 * the write numbers already in docs/benchmarks/phases-1-6-summary.md.
 * readBlob is a memTable lookup (no disk I/O once written); getDocument
 * is a ShardedDocStore lookup (Phase 2).
 */
class ReadThroughputBenchTest {
    @Test
    fun readBlobThroughput() =
        runBlocking {
            for (parallelism in listOf(1, 8, 64, 256)) {
                val root = createTempDirectory("kdb-bench-read-blob-kt").toString()
                val shim = FileBackedPlatformIoShimFactory.open(PlatformIoConfig(rootDirectory = root))
                val config = StorageEngineConfig(globalMemoryBudgetBytes = 64L shl 20, ioShim = shim)
                val wal = DefaultWriteAheadLogFactory().openOrCreate("bench-ns", config, shim)
                val engine = ServerStorageEngine("bench-ns", config, wal)

                val n = 5000
                val hashes = (0 until n).map { engine.writeBlob("payload-$it".encodeToByteArray()) }

                val opsPerWorker = 20000 / parallelism.coerceAtMost(20000)
                val totalOps = opsPerWorker * parallelism
                val elapsed =
                    measureTime {
                        coroutineScope {
                            (0 until parallelism).map {
                                async(Dispatchers.Default) {
                                    for (i in 0 until opsPerWorker) {
                                        engine.readBlob(hashes[i % n])
                                    }
                                }
                            }.forEach { it.await() }
                        }
                    }
                report("readBlob", parallelism, totalOps, elapsed)
            }
        }

    @Test
    fun getDocumentThroughput() =
        runBlocking {
            for (parallelism in listOf(1, 8, 64, 256)) {
                val root = createTempDirectory("kdb-bench-read-doc-kt").toString()
                val shim = FileBackedPlatformIoShimFactory.open(PlatformIoConfig(rootDirectory = root))
                val config = StorageEngineConfig(globalMemoryBudgetBytes = 64L shl 20, ioShim = shim)
                val wal = DefaultWriteAheadLogFactory().openOrCreate("bench-ns", config, shim)
                val engine = ServerStorageEngine("bench-ns", config, wal)

                val n = 5000
                val ids = (0 until n).map { KdbUuid.random() }
                ids.forEachIndexed { i, id -> engine.putDocument("bench-ns", KdbDocument(id, """{"v":$i}""")) }
                // getDocument ignores atCommit (see commitTree's doc comment - this
                // engine reflects live state, not per-branch history); pass any hash.
                val dummyCommit = KdbHash(ByteArray(32))

                val opsPerWorker = 20000 / parallelism.coerceAtMost(20000)
                val totalOps = opsPerWorker * parallelism
                val elapsed =
                    measureTime {
                        coroutineScope {
                            (0 until parallelism).map {
                                async(Dispatchers.Default) {
                                    for (i in 0 until opsPerWorker) {
                                        engine.getDocument("bench-ns", ids[i % n], dummyCommit)
                                    }
                                }
                            }.forEach { it.await() }
                        }
                    }
                report("getDocument", parallelism, totalOps, elapsed)
            }
        }

    private fun report(name: String, parallelism: Int, totalOps: Int, elapsed: kotlin.time.Duration) {
        val nsPerOp = elapsed.inWholeNanoseconds / totalOps.coerceAtLeast(1)
        val opsPerSec = if (nsPerOp == 0L) 0 else 1_000_000_000L / nsPerOp
        println("$name parallel=$parallelism totalOps=$totalOps elapsed=$elapsed nsPerOp=$nsPerOp opsPerSec=$opsPerSec")
    }
}
