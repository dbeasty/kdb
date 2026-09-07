package dev.kdb.index.vector

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.index.IndexBlobStore
import dev.kdb.index.IndexDescriptor
import dev.kdb.index.IndexType
import dev.kdb.index.InMemoryIndexBlobStore
import dev.kdb.index.SnapshotRestoreStatus
import dev.kdb.storage.mem.InMemoryStorageAdapter
import kotlinx.coroutines.test.runTest
import kotlin.math.abs
import kotlin.math.sqrt
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class VectorStoreTest {

    // ------------------------------------------------------------------ fixtures

    private fun descriptor(
        field: String = "embedding",
        options: Map<String, String> = emptyMap(),
    ) = IndexDescriptor(
        indexId = KdbUuid.fromString("22222222-2222-4222-8222-222222222222"),
        namespaceId = "ns",
        fieldName = field,
        fields = listOf(field),
        type = IndexType.VECTOR,
        unique = false,
        schemaVersion = 1,
        createdAtHash = KdbHash.fromBytes(ByteArray(32)),
        options = options,
    )

    private fun store(
        dag: CommitDag,
        dimensions: Int = 3,
        options: Map<String, String> = emptyMap(),
        blobs: IndexBlobStore = InMemoryIndexBlobStore(),
        exactThreshold: Int = DEFAULT_EXACT_THRESHOLD,
        flushEvery: Int = 0,
    ) = DefaultVectorIndexStore(
        descriptor(options = options),
        dag,
        InMemoryStorageAdapter(),
        dimensions,
        exactThreshold = exactThreshold,
        blobs = blobs,
        flushEvery = flushEvery,
    )

    private suspend fun commit(dag: CommitDag): KdbHash {
        val parent = dag.head()
        val tx =
            KdbTransaction(
                id = KdbUuid.random(),
                baseVersion = parent,
                operations = listOf<KdbOp>(),
                timestamp = KdbTimestamp.now(),
                authorNodeId = KdbUuid.random(),
            )
        val tree = DocumentTree.EMPTY
        dag.putDocumentTree(tree)
        return dag.appendCommit(tx, parent, tree, null).hash
    }

    private fun doc(vararg values: Double) = """{"embedding":[${values.joinToString(",")}]}"""

    private val docA = KdbUuid.fromString("aaaaaaaa-0000-4000-8000-000000000001")
    private val docB = KdbUuid.fromString("bbbbbbbb-0000-4000-8000-000000000002")
    private val docC = KdbUuid.fromString("cccccccc-0000-4000-8000-000000000003")

    // ------------------------------------------------------------------ metrics

    /** Guards §7 cosine, including the 0 result when either vector has zero norm. */
    @Test
    fun cosineMatchesTheSpecFormula() {
        val a = floatArrayOf(1f, 2f, 3f)
        val b = floatArrayOf(4f, 5f, 6f)
        val expected = (32.0 / (sqrt(14.0) * sqrt(77.0))).toFloat()
        assertTrue(abs(VectorMetrics.score(VectorMetric.COSINE, a, b) - expected) < 1e-6f)
        assertEquals(0f, VectorMetrics.score(VectorMetric.COSINE, a, floatArrayOf(0f, 0f, 0f)))
    }

    /** Guards §7 l2: the score is 1 / (1 + distance), so nearer is always higher. */
    @Test
    fun l2IsInverseDistance() {
        val a = floatArrayOf(0f, 0f, 0f)
        val b = floatArrayOf(3f, 4f, 0f)
        // ‖a − b‖ = 5, so the score is 1 / (1 + 5).
        assertEquals(1f / 6f, VectorMetrics.score(VectorMetric.L2, a, b))
        assertEquals(1f, VectorMetrics.score(VectorMetric.L2, a, a))
    }

    /** Guards §7 inner product: the raw dot product, negatives included. */
    @Test
    fun innerProductIsTheDotProduct() {
        assertEquals(
            -6f,
            VectorMetrics.score(VectorMetric.INNER_PRODUCT, floatArrayOf(1f, 2f), floatArrayOf(2f, -4f)),
        )
    }

    /** Guards the option spellings a `CREATE INDEX ... WITH (metric = …)` may use. */
    @Test
    fun parsesMetricOptionSpellings() {
        assertEquals(VectorMetric.COSINE, VectorMetric.fromOption("cosine"))
        assertEquals(VectorMetric.L2, VectorMetric.fromOption(" L2 "))
        assertEquals(VectorMetric.INNER_PRODUCT, VectorMetric.fromOption("inner_product"))
        assertFailsWith<IllegalArgumentException> { VectorMetric.fromOption("manhattan") }
    }

    // ------------------------------------------------------------------ store

    /** Guards exact search ordering: score descending, ties by ascending document id. */
    @Test
    fun exactSearchOrdersByScoreThenDocumentId() =
        runTest {
            val dag = inMemoryCommitDag("ns/vec-order")
            val s = store(dag, options = mapOf("metric" to "l2"))
            val c = commit(dag)
            s.putDocument(docA, c, doc(1.0, 0.0, 0.0))
            s.putDocument(docB, c, doc(1.0, 0.0, 0.0))
            s.putDocument(docC, c, doc(0.0, 5.0, 0.0))

            val hits = s.nearestNeighbours(floatArrayOf(1f, 0f, 0f), 3)
            assertEquals(listOf(docA, docB, docC), hits.map { it.docId })
            assertEquals(hits[0].score, hits[1].score)
        }

    /** Guards `k` truncating after ranking. */
    @Test
    fun kTruncatesAfterRanking() =
        runTest {
            val dag = inMemoryCommitDag("ns/vec-k")
            val s = store(dag)
            val c = commit(dag)
            s.putDocument(docA, c, doc(1.0, 0.0, 0.0))
            s.putDocument(docB, c, doc(0.0, 1.0, 0.0))

            assertEquals(1, s.nearestNeighbours(floatArrayOf(1f, 0f, 0f), 1).size)
            assertTrue(s.nearestNeighbours(floatArrayOf(1f, 0f, 0f), 0).isEmpty())
        }

    /** Guards §7: a wrong-length vector on write is a dimension mismatch, not a silent skip. */
    @Test
    fun putRejectsAWrongLengthVector() =
        runTest {
            val dag = inMemoryCommitDag("ns/vec-dims-put")
            val s = store(dag)
            val c = commit(dag)
            assertFailsWith<VectorDimensionMismatchException> {
                s.putDocument(docA, c, doc(1.0, 2.0))
            }
        }

    /** Guards §7: a wrong-length query vector is rejected too. */
    @Test
    fun queryRejectsAWrongLengthVector() =
        runTest {
            val dag = inMemoryCommitDag("ns/vec-dims-query")
            val s = store(dag)
            assertFailsWith<VectorDimensionMismatchException> {
                s.nearestNeighbours(floatArrayOf(1f, 0f), 5)
            }
        }

    /** Guards §10: validateDocument surfaces the mismatch before any store is mutated. */
    @Test
    fun validateDocumentRejectsBeforeMutating() =
        runTest {
            val dag = inMemoryCommitDag("ns/vec-validate")
            val s = store(dag)
            assertFailsWith<VectorDimensionMismatchException> { s.validateDocument(docA, doc(1.0)) }
            assertEquals(0, s.liveVectorCount(), "nothing may have been indexed")
        }

    /** Guards a document without the indexed path indexing as "no vector" rather than failing. */
    @Test
    fun documentWithoutTheFieldIndexesNothing() =
        runTest {
            val dag = inMemoryCommitDag("ns/vec-absent")
            val s = store(dag)
            val c = commit(dag)
            s.putDocument(docA, c, """{"other":1}""")
            assertEquals(0, s.liveVectorCount())
            assertTrue(s.nearestNeighbours(floatArrayOf(1f, 0f, 0f), 5).isEmpty())
        }

    /** Guards §7 tombstones: head hides the delete, an earlier atCommit read still sees it. */
    @Test
    fun deleteIsATombstoneHonouringAtCommit() =
        runTest {
            val dag = inMemoryCommitDag("ns/vec-tombstone")
            val s = store(dag)
            val c1 = commit(dag)
            s.putDocument(docA, c1, doc(1.0, 0.0, 0.0))
            val c2 = commit(dag)
            s.delete(docA, c2)

            assertTrue(s.nearestNeighbours(floatArrayOf(1f, 0f, 0f), 5).isEmpty())
            assertEquals(
                listOf(docA),
                s.nearestNeighbours(floatArrayOf(1f, 0f, 0f), 5, atCommit = c1).map { it.docId },
            )
        }

    /** Guards §9.2: descriptor options override the constructor's dimensions and metric. */
    @Test
    fun descriptorOptionsOverrideConstructorDefaults() =
        runTest {
            val dag = inMemoryCommitDag("ns/vec-options")
            val s =
                store(
                    dag,
                    dimensions = 128,
                    options =
                        mapOf(
                            "dimensions" to "3",
                            "metric" to "inner_product",
                            "m" to "8",
                            "ef_construction" to "64",
                            "ef_search" to "32",
                        ),
                )
            assertEquals(3, s.dimensions)
            assertEquals(VectorMetric.INNER_PRODUCT, s.metric)
            assertEquals(8, s.config.m)
            assertEquals(64, s.config.efConstruction)
            assertEquals(32, s.config.efSearch)

            val c = commit(dag)
            s.putDocument(docA, c, doc(1.0, 1.0, 1.0))
            assertEquals(6f, s.nearestNeighbours(floatArrayOf(1f, 2f, 3f), 1).single().score)
        }

    /** Guards §7 determinism: a node's level comes from its id, never from insertion order. */
    @Test
    fun hnswLevelIsDerivedFromTheDocumentId() {
        val level = hnswLevelFor(docA, 16)
        assertEquals(level, hnswLevelFor(docA, 16), "the level must be a pure function of the id")
        assertTrue(level >= 0)
    }

    /** Guards §6.5/§7: a snapshot round-trips and the graph is rebuilt from the stored vectors. */
    @Test
    fun snapshotRoundTripsThroughStorage() =
        runTest {
            val dag = inMemoryCommitDag("ns/vec-snapshot")
            val blobs = InMemoryIndexBlobStore()
            val s = store(dag, blobs = blobs)
            val c1 = commit(dag)
            s.putDocument(docA, c1, doc(1.0, 0.0, 0.0))
            s.putDocument(docB, c1, doc(0.0, 1.0, 0.0))
            val c2 = commit(dag)
            s.delete(docB, c2)
            s.flush()

            val restored = store(dag, blobs = blobs)
            assertEquals(SnapshotRestoreStatus.RESTORED, restored.restoreFromStorage().status)
            assertEquals(
                s.nearestNeighbours(floatArrayOf(1f, 0f, 0f), 5),
                restored.nearestNeighbours(floatArrayOf(1f, 0f, 0f), 5),
            )
            assertEquals(2, restored.liveVectorCount(atCommit = c1), "history must survive the round trip")
        }

    /** Guards §6.5: a snapshot from an older head is reported STALE instead of being loaded. */
    @Test
    fun staleSnapshotIsReported() =
        runTest {
            val dag = inMemoryCommitDag("ns/vec-stale")
            val blobs = InMemoryIndexBlobStore()
            val s = store(dag, blobs = blobs)
            val c1 = commit(dag)
            s.putDocument(docA, c1, doc(1.0, 0.0, 0.0))
            s.flush()
            commit(dag)

            val restored = store(dag, blobs = blobs)
            assertEquals(SnapshotRestoreStatus.STALE, restored.restoreFromStorage().status)
            assertEquals(0, restored.liveVectorCount())
        }

    /**
     * Guards snapshot replay order: puts and deletes must replay in their original sequence
     * order, or a document deleted and then re-indexed loses its vector on restore.
     */
    @Test
    fun snapshotReplaysEventsInSequenceOrder() =
        runTest {
            val dag = inMemoryCommitDag("ns/vec-snapshot-order")
            val blobs = InMemoryIndexBlobStore()
            val s = store(dag, blobs = blobs)
            val c1 = commit(dag)
            s.putDocument(docA, c1, doc(1.0, 0.0, 0.0))
            val c2 = commit(dag)
            s.delete(docA, c2)
            val c3 = commit(dag)
            s.putDocument(docA, c3, doc(0.0, 1.0, 0.0)) // re-indexed after the delete
            s.flush()

            val restored = store(dag, blobs = blobs)
            assertEquals(SnapshotRestoreStatus.RESTORED, restored.restoreFromStorage().status)
            assertEquals(
                listOf(docA),
                restored.nearestNeighbours(floatArrayOf(0f, 1f, 0f), 5).map { it.docId },
                "the re-index must survive the round trip",
            )
            assertTrue(
                restored.nearestNeighbours(floatArrayOf(1f, 0f, 0f), 5, atCommit = c2).isEmpty(),
                "and the delete must still be visible at its own commit",
            )
        }

    /**
     * Guards §7's headline quality bar: HNSW recall ≥ 0.95 at k = 10 against the exact oracle,
     * over 2 000 random 32-d vectors. A negative exactThreshold forces the graph path at every
     * size so the comparison is graph-vs-oracle rather than oracle-vs-oracle.
     */
    @Test
    fun hnswRecallAtTenIsAtLeastNinetyFivePercent() =
        runTest {
            val dag = inMemoryCommitDag("ns/vec-recall")
            val random = Random(20260905)
            val dims = 32
            val s = store(dag, dimensions = dims, exactThreshold = -1)
            val c = commit(dag)
            repeat(2000) {
                val v = DoubleArray(dims) { random.nextDouble() * 2.0 - 1.0 }
                s.putDocument(KdbUuid.random(), c, """{"embedding":[${v.joinToString(",")}]}""")
            }

            var hits = 0
            val queries = 20
            repeat(queries) {
                val q = FloatArray(dims) { (random.nextDouble() * 2.0 - 1.0).toFloat() }
                val truth = s.exactNearestNeighbours(q, 10).map { it.docId }.toSet()
                val approx = s.nearestNeighbours(q, 10).map { it.docId }
                hits += approx.count { it in truth }
            }
            val recall = hits.toDouble() / (queries * 10).toDouble()
            assertTrue(recall >= 0.95, "HNSW recall@10 was $recall, expected >= 0.95")
        }

    /** Guards the threshold switch: below it, approximate search is exactly the oracle. */
    @Test
    fun belowTheThresholdSearchIsExact() =
        runTest {
            val dag = inMemoryCommitDag("ns/vec-threshold")
            val random = Random(7)
            val s = store(dag, dimensions = 8, exactThreshold = 1000)
            val c = commit(dag)
            repeat(50) {
                val v = DoubleArray(8) { random.nextDouble() }
                s.putDocument(KdbUuid.random(), c, """{"embedding":[${v.joinToString(",")}]}""")
            }
            val q = FloatArray(8) { random.nextDouble().toFloat() }
            assertEquals(s.exactNearestNeighbours(q, 10), s.nearestNeighbours(q, 10))
        }
}
