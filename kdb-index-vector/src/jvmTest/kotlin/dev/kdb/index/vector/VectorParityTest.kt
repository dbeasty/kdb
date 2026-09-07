package dev.kdb.index.vector

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.index.IndexDescriptor
import dev.kdb.index.IndexType
import dev.kdb.index.InMemoryIndexBlobStore
import dev.kdb.storage.mem.InMemoryStorageAdapter
import kotlinx.coroutines.test.runTest
import kotlin.math.abs
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Parity gate for Component 64: over the shared corpus, Kotlin's exact (brute-force) search must
 * return the same documents and scores as Go, within 1e-5, for every metric.
 */
class VectorParityTest {

    private companion object {
        const val FIXTURE = "vector_corpus.json"
        const val TOLERANCE = 1e-5
    }

    @Test
    fun exactResultsMatchTheGoldenCorpus() =
        runTest {
            val fixture = GoldenFixtures.json(FIXTURE)
            if (fixture == null) {
                println(GoldenFixtures.missing(FIXTURE))
                return@runTest
            }

            val dimensions = fixture.field("dimensions")!!.int()
            val documents = fixture.field("documents")!!.arr()
            val queries = fixture.field("queries")!!.arr()
            assertTrue(documents.isNotEmpty(), "$FIXTURE holds no documents")
            assertTrue(queries.isNotEmpty(), "$FIXTURE holds no queries")

            // One store per metric: the metric is a property of the index, not of the query.
            val metrics = queries.map { it.field("metric")!!.str() }.toSet()
            val stores =
                metrics.associateWith { metric ->
                    val dag = inMemoryCommitDag("ns/vec-parity-$metric")
                    val store =
                        DefaultVectorIndexStore(
                            IndexDescriptor(
                                indexId = KdbUuid.fromString("44444444-4444-4444-8444-444444444444"),
                                namespaceId = "ns",
                                fieldName = "embedding",
                                fields = listOf("embedding"),
                                type = IndexType.VECTOR,
                                unique = false,
                                schemaVersion = 1,
                                createdAtHash = KdbHash.fromBytes(ByteArray(32)),
                                options = mapOf("dimensions" to "$dimensions", "metric" to metric),
                            ),
                            dag,
                            InMemoryStorageAdapter(),
                            dimensions,
                            blobs = InMemoryIndexBlobStore(),
                            flushEvery = 0,
                        )
                    val head = dag.head()
                    for (document in documents) {
                        val values = document.field("vector")!!.arr().joinToString(",") { it.num().toString() }
                        store.putDocument(
                            KdbUuid.fromString(document.field("id")!!.str()),
                            head,
                            """{"embedding":[$values]}""",
                        )
                    }
                    store
                }

            for (query in queries) {
                val metric = query.field("metric")!!.str()
                val k = query.field("k")!!.int()
                val vector = query.field("vector")!!.arr().map { it.num().toFloat() }.toFloatArray()
                val expected =
                    query.field("expected")!!.arr().map { pair ->
                        val cells = pair.arr()
                        cells[0].str() to cells[1].num()
                    }
                val label = "$metric k=$k"

                val actual = stores.getValue(metric).exactNearestNeighbours(vector, k)

                assertEquals(
                    expected.map { it.first },
                    actual.map { it.docId.toString() },
                    "$label: result set or order differs",
                )
                for (i in expected.indices) {
                    assertTrue(
                        abs(actual[i].score.toDouble() - expected[i].second) <= TOLERANCE,
                        "$label: score at rank $i was ${actual[i].score}, expected ${expected[i].second}",
                    )
                }
            }
        }

    /** Guards the default search path agreeing with the oracle on a corpus below the threshold. */
    @Test
    fun defaultSearchMatchesTheOracleOnTheGoldenCorpus() =
        runTest {
            val fixture = GoldenFixtures.json(FIXTURE)
            if (fixture == null) {
                println(GoldenFixtures.missing(FIXTURE))
                return@runTest
            }
            val dimensions = fixture.field("dimensions")!!.int()
            val documents = fixture.field("documents")!!.arr()
            val dag = inMemoryCommitDag("ns/vec-parity-default")
            val store =
                DefaultVectorIndexStore(
                    IndexDescriptor(
                        indexId = KdbUuid.fromString("55555555-5555-4555-8555-555555555555"),
                        namespaceId = "ns",
                        fieldName = "embedding",
                        fields = listOf("embedding"),
                        type = IndexType.VECTOR,
                        unique = false,
                        schemaVersion = 1,
                        createdAtHash = KdbHash.fromBytes(ByteArray(32)),
                        options = mapOf("dimensions" to "$dimensions"),
                    ),
                    dag,
                    InMemoryStorageAdapter(),
                    dimensions,
                    blobs = InMemoryIndexBlobStore(),
                    flushEvery = 0,
                )
            val head = dag.head()
            for (document in documents) {
                val values = document.field("vector")!!.arr().joinToString(",") { it.num().toString() }
                store.putDocument(
                    KdbUuid.fromString(document.field("id")!!.str()),
                    head,
                    """{"embedding":[$values]}""",
                )
            }
            val q = FloatArray(dimensions) { if (it == 0) 1f else 0f }
            assertEquals(store.exactNearestNeighbours(q, 5), store.nearestNeighbours(q, 5))
        }
}
