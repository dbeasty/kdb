package dev.kdb.index.fulltext

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
 * Parity gate for Component 64: over the shared corpus, Kotlin must return the same BM25F-lite
 * ranking as Go, with scores within a relative tolerance of 1e-4.
 */
class Bm25ParityTest {

    private companion object {
        const val FIXTURE = "bm25_corpus.json"
        const val RELATIVE_TOLERANCE = 1e-4
    }

    @Test
    fun rankingsMatchTheGoldenCorpus() =
        runTest {
            val fixture = GoldenFixtures.json(FIXTURE)
            if (fixture == null) {
                println(GoldenFixtures.missing(FIXTURE))
                return@runTest
            }

            val indexDef = fixture.field("index")!!
            val fieldDefs = indexDef.field("fields")!!.arr()
            val paths = fieldDefs.map { it.field("path")!!.str() }
            val weights = fieldDefs.joinToString(",") { "${it.field("path")!!.str()}=${it.field("weight")?.num() ?: 1.0}" }

            val dag = inMemoryCommitDag("ns/bm25-parity")
            val store =
                DefaultFullTextIndexStore(
                    IndexDescriptor(
                        indexId = KdbUuid.fromString("33333333-3333-4333-8333-333333333333"),
                        namespaceId = "ns",
                        fieldName = paths.first(),
                        fields = paths,
                        type = IndexType.FULLTEXT,
                        unique = false,
                        schemaVersion = 1,
                        createdAtHash = KdbHash.fromBytes(ByteArray(32)),
                        options = mapOf("weights" to weights),
                    ),
                    dag,
                    InMemoryStorageAdapter(),
                    blobs = InMemoryIndexBlobStore(),
                    flushEvery = 0,
                )

            val head = dag.head()
            val documents = fixture.field("documents")!!.arr()
            assertTrue(documents.isNotEmpty(), "$FIXTURE holds no documents")
            for (document in documents) {
                store.putDocument(
                    KdbUuid.fromString(document.field("id")!!.str()),
                    head,
                    document.field("json")!!.str(),
                )
            }

            val queries = fixture.field("queries")!!.arr()
            assertTrue(queries.isNotEmpty(), "$FIXTURE holds no queries")
            for (query in queries) {
                val text = query.field("query")!!.str()
                val expected =
                    query.field("expected")!!.arr().map { pair ->
                        val cells = pair.arr()
                        cells[0].str() to cells[1].num()
                    }

                val actual = store.search(text)

                assertEquals(
                    expected.map { it.first },
                    actual.map { it.docId.toString() },
                    "query ${quote(text)}: ranking differs",
                )
                for (i in expected.indices) {
                    val want = expected[i].second
                    val got = actual[i].score.toDouble()
                    val tolerance = RELATIVE_TOLERANCE * maxOf(abs(want), 1e-9)
                    assertTrue(
                        abs(got - want) <= tolerance,
                        "query ${quote(text)}: score at rank $i was $got, expected $want",
                    )
                }
            }
        }

    private fun quote(s: String) = "\"$s\""
}
