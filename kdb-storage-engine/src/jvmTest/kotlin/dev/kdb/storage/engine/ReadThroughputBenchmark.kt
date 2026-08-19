package dev.kdb.storage.engine

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbDocument
import dev.kdb.storage.StorageEngineConfig
import dev.kdb.storage.io.FileBackedPlatformIoShimFactory
import dev.kdb.storage.io.PlatformIoConfig
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import kotlin.test.Test

/**
 * Measures actual read throughput and document-commit throughput on this machine, real disk --
 * companion to WriteThroughputBenchmark's blob-write numbers, so "reads and writes" both have a
 * real measured answer rather than an assumption.
 */
class ReadThroughputBenchmark {

    private val concurrency = 64
    private val opsPerWorker = 100
    private val payloadBytes = 256
    private val documentCount = 500

    @Test
    fun measureRealThroughput() =
        runBlocking(Dispatchers.IO) {
            val blobRead = runBlobReadScenario()
            val (docWrite, docRead) = runDocumentScenario()

            val report =
                buildString {
                    appendLine("=== Read / document throughput benchmark (this machine, real disk) ===")
                    appendLine("blob read:  concurrency=$concurrency opsPerWorker=$opsPerWorker payloadBytes=$payloadBytes")
                    appendLine("  ServerStorageEngine.readBlob (shipped): %.1f ops/sec".format(blobRead))
                    appendLine("document commit (sequential, one write+commitTree per doc, $documentCount docs):")
                    appendLine("  putDocument + commitTree: %.1f ops/sec".format(docWrite))
                    appendLine("  getDocument:              %.1f ops/sec".format(docRead))
                }
            println(report)
            java.io.File("/tmp/kdb-read-throughput-benchmark.txt").writeText(report)
        }

    /** Pre-populate blobs via the real engine, then measure concurrent readBlob() throughput. */
    private suspend fun runBlobReadScenario(): Double {
        val root = Files.createTempDirectory("kdb-read-bench").toFile()
        try {
            val ioShim = FileBackedPlatformIoShimFactory.open(PlatformIoConfig(rootDirectory = root.absolutePath, fsyncOnFlush = true))
            val config = StorageEngineConfig(globalMemoryBudgetBytes = 64_000_000, ioShim = ioShim)
            val handle = DefaultStorageEngineFactory(StorageEngineTarget.SERVER).open("bench", config)

            val hashes =
                (0 until concurrency * opsPerWorker).map { i ->
                    val bytes = ByteArray(payloadBytes) { (it + i).toByte() }
                    handle.adapter.writeBlob(bytes)
                }

            val start = System.nanoTime()
            coroutineScope {
                (0 until concurrency).map { worker ->
                    async {
                        repeat(opsPerWorker) { i ->
                            val hash = hashes[worker * opsPerWorker + i]
                            handle.adapter.readBlob(hash)
                        }
                    }
                }.awaitAll()
            }
            val elapsedSeconds = (System.nanoTime() - start) / 1_000_000_000.0
            handle.close()
            return (concurrency * opsPerWorker) / elapsedSeconds
        } finally {
            root.deleteRecursively()
        }
    }

    /**
     * Document commits go through ServerStorageEngine's own mutex (unaffected by the WAL
     * group-commit work -- that's blob-store only) and commitTree rebuilds the whole tree from
     * the live doc map each call, so this is measured sequentially, one doc at a time, matching
     * what a stream of single-row INSERTs actually costs today.
     */
    private suspend fun runDocumentScenario(): Pair<Double, Double> {
        val root = Files.createTempDirectory("kdb-doc-bench").toFile()
        try {
            val ioShim = FileBackedPlatformIoShimFactory.open(PlatformIoConfig(rootDirectory = root.absolutePath, fsyncOnFlush = true))
            val config = StorageEngineConfig(globalMemoryBudgetBytes = 64_000_000, ioShim = ioShim)
            val handle = DefaultStorageEngineFactory(StorageEngineTarget.SERVER).open("bench", config)
            val ns = "bench"

            val ids = (0 until documentCount).map { KdbUuid.random() }
            var treeHash = DocumentTree.EMPTY.treeHash

            val writeStart = System.nanoTime()
            for (id in ids) {
                handle.adapter.putDocument(ns, KdbDocument(id, """{"v":1}"""))
                treeHash = handle.adapter.commitTree(ns, treeHash).treeHash
            }
            val writeElapsed = (System.nanoTime() - writeStart) / 1_000_000_000.0
            val writeOpsPerSec = documentCount / writeElapsed

            val finalTreeHash: KdbHash = treeHash
            val readStart = System.nanoTime()
            for (id in ids) {
                handle.adapter.getDocument(ns, id, finalTreeHash)
            }
            val readElapsed = (System.nanoTime() - readStart) / 1_000_000_000.0
            val readOpsPerSec = documentCount / readElapsed

            handle.close()
            return writeOpsPerSec to readOpsPerSec
        } finally {
            root.deleteRecursively()
        }
    }
}
