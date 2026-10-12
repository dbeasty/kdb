package dev.kdb.index.vector

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.index.IndexDescriptor
import dev.kdb.index.IndexType
import dev.kdb.index.InMemoryIndexBlobStore
import dev.kdb.storage.mem.InMemoryStorageAdapter
import kotlinx.coroutines.test.runTest
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * A vector query with no LIMIT asks the index for every document (k up to Int.MAX_VALUE). k must
 * never size an allocation or let the beam grow past the graph. The Go twin is
 * `go/kdb/index/vector/huge_k_test.go`; both build 20k vectors of 32 dimensions.
 */
class HugeKTest {
    @Test
    fun anUnboundedKIsAnswerFromTheGraphWithoutBlowingUp() =
        runTest {
            val n = 20_000
            val dims = 32
            val dag = inMemoryCommitDag("ns/vec-huge-k")
            val descriptor =
                IndexDescriptor(
                    indexId = KdbUuid.fromString("33333333-3333-4333-8333-333333333333"),
                    namespaceId = "ns",
                    fieldName = "embedding",
                    fields = listOf("embedding"),
                    type = IndexType.VECTOR,
                    unique = false,
                    schemaVersion = 1,
                    createdAtHash = KdbHash.fromBytes(ByteArray(32)),
                    options = emptyMap(),
                )
            val s = DefaultVectorIndexStore(descriptor, dag, InMemoryStorageAdapter(), dims, blobs = InMemoryIndexBlobStore())
            val parent = dag.head()
            val tx =
                KdbTransaction(
                    id = KdbUuid.random(),
                    baseVersion = parent,
                    operations = listOf<KdbOp>(),
                    timestamp = KdbTimestamp.now(),
                    authorNodeId = KdbUuid.random(),
                )
            dag.putDocumentTree(DocumentTree.EMPTY)
            val c = dag.appendCommit(tx, parent, DocumentTree.EMPTY, null).hash

            val random = Random(7)
            val first = FloatArray(dims) { (random.nextDouble() * 2.0 - 1.0).toFloat() }
            for (i in 0 until n) {
                val v = if (i == 0) first else FloatArray(dims) { (random.nextDouble() * 2.0 - 1.0).toFloat() }
                s.putDocument(KdbUuid(0L, i + 1L), c, """{"embedding":[${v.joinToString(",")}]}""")
            }

            val base = s.nearestNeighbours(first, 10).map { it.docId }
            val rt = Runtime.getRuntime()
            for (k in listOf(n, 1_000_000_000, Int.MAX_VALUE)) {
                System.gc()
                val before = rt.totalMemory() - rt.freeMemory()
                val start = System.nanoTime()
                val got = s.nearestNeighbours(first, k)
                val ms = (System.nanoTime() - start) / 1_000_000
                val after = rt.totalMemory() - rt.freeMemory()
                println("HugeK k=$k: ${got.size} hits in $ms ms, heap delta ${(after - before) shr 20} MiB")
                assertTrue(got.size in 1..n, "k=$k returned ${got.size} hits from $n vectors")
                assertEquals(base, got.take(10).map { it.docId }, "k=$k must rank the same top results as k=10")
                assertTrue(ms < 10_000, "k=$k took $ms ms")
                assertTrue(after - before < 256L shl 20, "k=$k retained ${(after - before) shr 20} MiB")
            }
        }
}
