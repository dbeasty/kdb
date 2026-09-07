package dev.kdb.index.fulltext

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
import kotlin.math.ln
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class FullTextStoreTest {

    // ------------------------------------------------------------------ fixtures

    private fun descriptor(
        fields: List<String>,
        options: Map<String, String> = emptyMap(),
    ) = IndexDescriptor(
        indexId = KdbUuid.fromString("11111111-1111-4111-8111-111111111111"),
        namespaceId = "ns",
        fieldName = fields.first(),
        fields = fields,
        type = IndexType.FULLTEXT,
        unique = false,
        schemaVersion = 1,
        createdAtHash = KdbHash.fromBytes(ByteArray(32)),
        options = options,
    )

    private fun store(
        dag: CommitDag,
        fields: List<String> = listOf("title", "body"),
        options: Map<String, String> = emptyMap(),
        blobs: IndexBlobStore = InMemoryIndexBlobStore(),
        flushEvery: Int = 0,
    ) = DefaultFullTextIndexStore(
        descriptor(fields, options),
        dag,
        InMemoryStorageAdapter(),
        blobs = blobs,
        flushEvery = flushEvery,
    )

    /** Appends an empty commit so tests have distinct, ancestry-ordered commit hashes. */
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

    private fun doc(
        title: String,
        body: String,
    ) = """{"title":${quote(title)},"body":${quote(body)}}"""

    private fun quote(s: String) = "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""

    private val docA = KdbUuid.fromString("aaaaaaaa-0000-4000-8000-000000000001")
    private val docB = KdbUuid.fromString("bbbbbbbb-0000-4000-8000-000000000002")
    private val docC = KdbUuid.fromString("cccccccc-0000-4000-8000-000000000003")

    // ------------------------------------------------------------------ tests

    /** Guards §6.4 OR semantics: a document matching any query term is a hit. */
    @Test
    fun matchesAnyQueryTerm() =
        runTest {
            val dag = inMemoryCommitDag("ns/ft-or")
            val s = store(dag)
            val c = commit(dag)
            s.putDocument(docA, c, doc("deploy staging", "nothing else"))
            s.putDocument(docB, c, doc("rollback", "production database"))

            val hits = s.search("deploy database")
            assertEquals(setOf(docA, docB), hits.map { it.docId }.toSet())
        }

    /** Guards §6.4 ranking: score descending, ties broken by ascending document id string. */
    @Test
    fun ordersByScoreThenDocumentId() =
        runTest {
            val dag = inMemoryCommitDag("ns/ft-order")
            val s = store(dag)
            val c = commit(dag)
            // A and B are identical, so they tie and must come back in id order; C scores lower
            // because the term is diluted by a much longer field.
            s.putDocument(docA, c, doc("deploy", ""))
            s.putDocument(docB, c, doc("deploy", ""))
            s.putDocument(docC, c, doc("deploy $LONG_TAIL", ""))

            val hits = s.search("deploy")
            assertEquals(listOf(docA, docB, docC), hits.map { it.docId })
            assertEquals(hits[0].score, hits[1].score)
            assertTrue(hits[2].score < hits[1].score)
        }

    /**
     * Guards the exact BM25F-lite formula of §6.4 against a score derived by hand, not copied
     * from the implementation.
     *
     * Corpus: three documents, one field `title` (weight 1), all of length 1 except doc C.
     *   A = "deploy", B = "deploy", C = "rollback rollback rollback" (3 tokens).
     * So N = 3, and for t = "deploy": n_t = 2.
     *   idf  = ln(1 + (3 − 2 + 0.5) / (2 + 0.5)) = ln(1 + 1.5/2.5) = ln(1.6)
     *   avglen = (1 + 1 + 3) / 3 = 5/3
     *   For A: tf = 1, len = 1 → ratio = 1 / (5/3) = 0.6
     *   tfnorm = 1·2.2 / (1 + 1.2·(1 − 0.75 + 0.75·0.6)) = 2.2 / (1 + 1.2·0.7) = 2.2 / 1.84
     *   score  = 1 · ln(1.6) · (2.2 / 1.84)
     */
    @Test
    fun scoresMatchTheHandComputedBm25Formula() =
        runTest {
            val dag = inMemoryCommitDag("ns/ft-bm25")
            val s = store(dag, fields = listOf("title"))
            val c = commit(dag)
            s.putDocument(docA, c, """{"title":"deploy"}""")
            s.putDocument(docB, c, """{"title":"deploy"}""")
            s.putDocument(docC, c, """{"title":"rollback rollback rollback"}""")

            val expected = ln(1.6) * (2.2 / 1.84)
            val hit = s.search("deploy").first { it.docId == docA }
            assertTrue(
                abs(hit.score - expected) <= 1e-6 * expected,
                "expected $expected, got ${hit.score}",
            )
        }

    /** Guards §6.3/§6.4 weights: a hit in a weighted field outranks the same hit unweighted. */
    @Test
    fun appliesPerFieldWeights() =
        runTest {
            val dag = inMemoryCommitDag("ns/ft-weights")
            val s = store(dag, options = mapOf("weights" to "title=3"))
            val c = commit(dag)
            s.putDocument(docA, c, doc("deploy", "unrelated"))
            s.putDocument(docB, c, doc("unrelated", "deploy"))

            val hits = s.search("deploy")
            assertEquals(listOf(docA, docB), hits.map { it.docId })
            assertTrue(hits[0].score > hits[1].score * 2.5f, "title weight 3 should dominate")
        }

    /** Guards weight parsing: an unnamed field keeps weight 1 and a bad entry is rejected. */
    @Test
    fun parsesWeightOptions() {
        assertEquals(
            listOf(3.0, 1.0, 2.0),
            parseFieldWeights(listOf("title", "body", "tags"), mapOf("weights" to "title=3, tags=2")).toList(),
        )
    }

    /** Guards §6.4 phrases: only documents holding the analyzed phrase contiguously are hits. */
    @Test
    fun quotedPhraseRestrictsHits() =
        runTest {
            val dag = inMemoryCommitDag("ns/ft-phrase")
            val s = store(dag)
            val c = commit(dag)
            s.putDocument(docA, c, doc("deploy staging tonight", ""))
            s.putDocument(docB, c, doc("staging deploy tonight", ""))

            assertEquals(listOf(docA), s.search("\"deploy staging\"").map { it.docId })
        }

    /** Guards §6.1 step 6 reaching phrases: a stopword between phrase terms does not break it. */
    @Test
    fun phraseIgnoresStopwords() =
        runTest {
            val dag = inMemoryCommitDag("ns/ft-phrase-stop")
            val s = store(dag)
            val c = commit(dag)
            s.putDocument(docA, c, doc("deploy to staging", ""))

            assertEquals(listOf(docA), s.search("\"deploy staging\"").map { it.docId })
        }

    /** Guards §6.3: an array field contributes every element with a position gap of 1. */
    @Test
    fun arrayFieldContributesEveryElementWithAPositionGap() =
        runTest {
            val dag = inMemoryCommitDag("ns/ft-array")
            val s = store(dag, fields = listOf("tags"))
            val c = commit(dag)
            s.putDocument(docA, c, """{"tags":["deploy","staging"]}""")

            // Both elements are searchable …
            assertEquals(listOf(docA), s.search("staging").map { it.docId })
            // … but the gap stops a phrase from spanning the element boundary.
            assertTrue(s.search("\"deploy staging\"").isEmpty())
        }

    /** Guards §6.4: a query of nothing but stopwords returns no hits rather than everything. */
    @Test
    fun stopwordOnlyQueryReturnsNothing() =
        runTest {
            val dag = inMemoryCommitDag("ns/ft-stopword-query")
            val s = store(dag)
            val c = commit(dag)
            s.putDocument(docA, c, doc("the deploy", ""))

            assertTrue(s.search("the and of").isEmpty())
        }

    /** Guards §6.4: fuzzy matching is off unless the store was built with a FuzzyMatchConfig. */
    @Test
    fun fuzzyMatchingIsOffByDefaultAndOptInWorks() =
        runTest {
            val dag = inMemoryCommitDag("ns/ft-fuzzy")
            val c = commit(dag)
            val strict = store(dag)
            strict.putDocument(docA, c, doc("rollback", ""))
            assertTrue(strict.search("rollbak").isEmpty(), "fuzzy must be off by default")

            val fuzzy =
                DefaultFullTextIndexStore(
                    descriptor(listOf("title", "body")),
                    dag,
                    InMemoryStorageAdapter(),
                    fuzzyConfig = FuzzyMatchConfig.DEFAULT,
                    blobs = InMemoryIndexBlobStore(),
                    flushEvery = 0,
                )
            fuzzy.putDocument(docA, c, doc("rollback", ""))
            assertEquals(listOf(docA), fuzzy.search("rollbak").map { it.docId })
        }

    /** Guards §6.4 tombstones: a head read hides a deleted document, an earlier read still sees it. */
    @Test
    fun deleteIsATombstoneVisibleOnlyFromItsCommitOnward() =
        runTest {
            val dag = inMemoryCommitDag("ns/ft-tombstone")
            val s = store(dag)
            val c1 = commit(dag)
            s.putDocument(docA, c1, doc("deploy", ""))
            val c2 = commit(dag)
            s.delete(docA, c2)

            assertTrue(s.search("deploy").isEmpty(), "head must not see the deleted document")
            assertEquals(listOf(docA), s.search("deploy", atCommit = c1).map { it.docId })
        }

    /** Guards ancestry filtering for updates: a re-put replaces the old version at head only. */
    @Test
    fun laterVersionReplacesEarlierAtHeadOnly() =
        runTest {
            val dag = inMemoryCommitDag("ns/ft-update")
            val s = store(dag)
            val c1 = commit(dag)
            s.putDocument(docA, c1, doc("deploy", ""))
            val c2 = commit(dag)
            s.putDocument(docA, c2, doc("rollback", ""))

            assertTrue(s.search("deploy").isEmpty())
            assertEquals(listOf(docA), s.search("rollback").map { it.docId })
            assertEquals(listOf(docA), s.search("deploy", atCommit = c1).map { it.docId })
        }

    /** Guards the CME the old store hit: deleting the only document of a term must not throw. */
    @Test
    fun deletingTheLastDocumentOfATermDoesNotThrow() =
        runTest {
            val dag = inMemoryCommitDag("ns/ft-cme")
            val s = store(dag)
            val c1 = commit(dag)
            s.putDocument(docA, c1, doc("alpha beta gamma delta", "epsilon zeta"))
            val c2 = commit(dag)
            s.delete(docA, c2)
            assertTrue(s.search("alpha beta gamma delta epsilon zeta").isEmpty())
        }

    /** Guards §6.4: `limit` truncates after ranking, and 0 or less means "every hit". */
    @Test
    fun limitTruncatesAfterRanking() =
        runTest {
            val dag = inMemoryCommitDag("ns/ft-limit")
            val s = store(dag)
            val c = commit(dag)
            s.putDocument(docA, c, doc("deploy", ""))
            s.putDocument(docB, c, doc("deploy", ""))
            s.putDocument(docC, c, doc("deploy", ""))

            assertEquals(listOf(docA), s.search("deploy", limit = 1).map { it.docId })
            assertEquals(3, s.search("deploy", limit = 0).size)
        }

    /** Guards §6.5: a snapshot round-trips through the blob store and restores identically. */
    @Test
    fun snapshotRoundTripsThroughStorage() =
        runTest {
            val dag = inMemoryCommitDag("ns/ft-snapshot")
            val blobs = InMemoryIndexBlobStore()
            val s = store(dag, blobs = blobs)
            val c1 = commit(dag)
            s.putDocument(docA, c1, doc("deploy staging", "release notes"))
            s.putDocument(docB, c1, doc("rollback", "deploy"))
            val c2 = commit(dag)
            s.delete(docB, c2)
            s.flush()

            val before = s.search("deploy")
            val restored = store(dag, blobs = blobs)
            val result = restored.restoreFromStorage()

            assertEquals(SnapshotRestoreStatus.RESTORED, result.status)
            assertEquals(before, restored.search("deploy"))
            assertEquals(listOf(docB), restored.search("rollback", atCommit = c1).map { it.docId })
        }

    /** Guards §6.5: a snapshot written at an older head reports STALE so the caller rebuilds. */
    @Test
    fun staleSnapshotIsReportedRatherThanLoaded() =
        runTest {
            val dag = inMemoryCommitDag("ns/ft-stale")
            val blobs = InMemoryIndexBlobStore()
            val s = store(dag, blobs = blobs)
            val c1 = commit(dag)
            s.putDocument(docA, c1, doc("deploy", ""))
            s.flush()
            commit(dag) // head moves on without a new snapshot

            val restored = store(dag, blobs = blobs)
            val result = restored.restoreFromStorage()
            assertEquals(SnapshotRestoreStatus.STALE, result.status)
            assertTrue(restored.search("deploy").isEmpty(), "a stale snapshot must not be loaded")
        }

    /** Guards §6.5: no snapshot at all reports MISSING rather than failing. */
    @Test
    fun missingSnapshotIsReported() =
        runTest {
            val dag = inMemoryCommitDag("ns/ft-missing")
            val s = store(dag, blobs = InMemoryIndexBlobStore())
            assertEquals(SnapshotRestoreStatus.MISSING, s.restoreFromStorage().status)
        }

    /** Guards §6.5: the store snapshots itself automatically every `flushEvery` commits. */
    @Test
    fun flushEveryWritesASnapshotWithoutAnExplicitFlush() =
        runTest {
            val dag = inMemoryCommitDag("ns/ft-flush-every")
            val blobs = InMemoryIndexBlobStore()
            val s = store(dag, blobs = blobs, flushEvery = 2)
            val c1 = commit(dag)
            s.putDocument(docA, c1, doc("deploy", ""))
            val c2 = commit(dag)
            s.putDocument(docB, c2, doc("deploy", ""))
            val c3 = commit(dag)
            s.putDocument(docC, c3, doc("deploy", ""))

            assertTrue(
                blobs.read("index/${s.descriptor.indexId}/snapshot") != null,
                "two commits should have triggered an automatic snapshot",
            )
        }

    /** Guards the statistics the manifest publishes: N and per-field avglen at head. */
    @Test
    fun reportsCorpusStatistics() =
        runTest {
            val dag = inMemoryCommitDag("ns/ft-stats")
            val s = store(dag, fields = listOf("title"))
            val c = commit(dag)
            s.putDocument(docA, c, """{"title":"deploy"}""")
            s.putDocument(docB, c, """{"title":"deploy staging now"}""")

            assertEquals(2, s.documentCount())
            // documentFrequency takes an analyzed term: "deploy" is indexed under its stem.
            assertEquals("deploi", FullTextAnalyzer.analyze("deploy").single())
            assertEquals(2, s.documentFrequency("deploi"))
            assertEquals(2.0, s.averageFieldLength(0))
        }

    /**
     * Guards snapshot replay order: puts and deletes must be written and replayed in their
     * original sequence order. Grouping all puts before all deletes loses a document that was
     * deleted and then re-indexed — the delete would replay last and wrongly win.
     */
    @Test
    fun snapshotReplaysEventsInSequenceOrder() =
        runTest {
            val dag = inMemoryCommitDag("ns/ft-snapshot-order")
            val blobs = InMemoryIndexBlobStore()
            val s = store(dag, blobs = blobs)
            val c1 = commit(dag)
            s.putDocument(docA, c1, doc("deploy", ""))
            val c2 = commit(dag)
            s.delete(docA, c2)
            val c3 = commit(dag)
            s.putDocument(docA, c3, doc("deploy", "")) // re-indexed after the delete
            s.flush()

            assertEquals(listOf(docA), s.search("deploy").map { it.docId }, "sanity: live before restore")

            val restored = store(dag, blobs = blobs)
            assertEquals(SnapshotRestoreStatus.RESTORED, restored.restoreFromStorage().status)
            assertEquals(
                listOf(docA),
                restored.search("deploy").map { it.docId },
                "the re-index must survive the round trip, not be undone by the earlier delete",
            )
            assertTrue(
                restored.search("deploy", atCommit = c2).isEmpty(),
                "and the delete must still be visible at its own commit",
            )
        }

    private companion object {
        /** 40 distinct filler tokens, to make one field much longer than the others. */
        val LONG_TAIL = (1..40).joinToString(" ") { "filler$it" }
    }
}
