package dev.kdb.server

import dev.kdb.auth.ConnectionContext
import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.embed.openMemoryRuntime
import dev.kdb.index.IndexDescriptor
import dev.kdb.index.IndexEntry
import dev.kdb.index.IndexKey
import dev.kdb.index.IndexStore
import dev.kdb.index.IndexStoreFactory
import dev.kdb.index.IndexType
import dev.kdb.index.RankedResult
import dev.kdb.policy.DocumentExpiryPolicy
import dev.kdb.schema.KdbSchema
import dev.kdb.stream.WireConnection
import dev.kdb.wire.KDB_WIRE_PROTOCOL_VERSION
import dev.kdb.wire.WireHeader
import dev.kdb.wire.WireMessage
import dev.kdb.wire.WireMessageType
import dev.kdb.wire.defaultWireCodec
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Layer 16 Component 69 (§11): the SEARCH/SEARCH_RESULT pair against the real wire host.
 *
 * The index stores are fakes returning canned [RankedResult]s: the real FULLTEXT/VECTOR stores are
 * a separate component, and what is under test here is the host's index resolution, arm handling,
 * fusion, limit, includeJson and error shaping - not anyone's ranking maths.
 */
class SearchHostTest {
    private val wire = defaultWireCodec()
    private val ns = "demo/search"

    // Fixed ids so expected orderings are readable.
    private val d1 = KdbUuid.fromString("00000000-0000-4000-8000-000000000001")
    private val d2 = KdbUuid.fromString("00000000-0000-4000-8000-000000000002")
    private val d3 = KdbUuid.fromString("00000000-0000-4000-8000-000000000003")

    /** Guards: a single text arm returns that arm's ranking, capped by `limit`, with scores intact. */
    @Test
    fun singleTextArmReturnsThatArmsRanking() =
        runTest {
            val h = harness(text = listOf(RankedResult(d1, 2.5f), RankedResult(d2, 1.5f), RankedResult(d3, 0.5f)))
            val result = h.search(WireMessage.SearchTextArm("tasks_text", "deploy"), limit = 2, corr = 1)
            assertNull(result.error)
            assertEquals(listOf(d1.toString(), d2.toString()), result.hits.map { it.docId })
            assertEquals(2.5f, result.hits[0].score)
            assertEquals(h.server.runtime.dag.head().toHex(), result.resolvedCommitHex)
            assertTrue(result.hits.all { it.json == null }, "includeJson=false must not carry bodies")
        }

    /** Guards: a single vector arm resolves the VECTOR index by its field name and returns its ranking. */
    @Test
    fun singleVectorArmResolvesTheIndexByField() =
        runTest {
            val h = harness(vector = listOf(RankedResult(d3, 0.9f), RankedResult(d1, 0.4f)))
            val result =
                h.search(vector = WireMessage.SearchVectorArm("embedding", listOf(0.1, 0.2)), limit = 10, corr = 1)
            assertNull(result.error)
            assertEquals(listOf(d3.toString(), d1.toString()), result.hits.map { it.docId })
        }

    /** Guards: with both arms present the result is the fused ranking (§8 RRF), not either arm's. */
    @Test
    fun bothArmsAreFused() =
        runTest {
            // Text ranks d1 > d2; vector ranks d2 > d1. RRF gives d2 the better combined rank sum
            // (1+2 vs 2+1 is a tie, so d1 wins on the docId tiebreak) - what matters is that both
            // arms contribute and d3, present in neither top slot, still appears.
            val h =
                harness(
                    text = listOf(RankedResult(d1, 3f), RankedResult(d2, 2f)),
                    vector = listOf(RankedResult(d2, 0.9f), RankedResult(d3, 0.8f)),
                )
            val result =
                h.search(
                    text = WireMessage.SearchTextArm("tasks_text", "deploy"),
                    vector = WireMessage.SearchVectorArm("embedding", listOf(0.1, 0.2)),
                    limit = 10,
                    corr = 1,
                )
            assertNull(result.error)
            assertEquals(setOf(d1.toString(), d2.toString(), d3.toString()), result.hits.map { it.docId }.toSet())
            // d2 is in both arms, so it must outrank d3, which is only in one.
            val ids = result.hits.map { it.docId }
            assertTrue(ids.indexOf(d2.toString()) < ids.indexOf(d3.toString()))
            // Fused scores are RRF sums, not raw arm scores.
            assertTrue(result.hits.all { it.score < 1f })
        }

    /** Guards: includeJson fetches the bodies at the resolved commit, byte-exact. */
    @Test
    fun includeJsonCarriesTheStoredBodies() =
        runTest {
            val h = harness(text = listOf(RankedResult(d1, 1f)))
            h.server.upsert(ns, d1, """{"title":"alpha","n":1}""")
            val result = h.search(WireMessage.SearchTextArm("tasks_text", "alpha"), limit = 5, includeJson = true, corr = 1)
            assertEquals("""{"title":"alpha","n":1}""", result.hits.single().json)
        }

    /** Guards: expired documents (§9.5) are skipped at head. */
    @Test
    fun expiredDocumentsAreSkippedAtHead() =
        runTest {
            val now = 1_700_000_000_000L
            val h =
                harness(
                    text = listOf(RankedResult(d1, 2f), RankedResult(d2, 1f)),
                    expiry = DocumentExpiryPolicy("expiresAt"),
                    clock = { now },
                )
            h.server.upsert(ns, d1, """{"expiresAt":${now - 1}}""")
            h.server.upsert(ns, d2, """{"expiresAt":${now + 60_000}}""")
            val result = h.search(WireMessage.SearchTextArm("tasks_text", "x"), limit = 10, corr = 1)
            assertEquals(listOf(d2.toString()), result.hits.map { it.docId })
        }

    /** Guards: an index that is not configured is an error naming the missing index, not an empty
     * result and not a dropped connection. */
    @Test
    fun aMissingIndexIsReportedAsAnError() =
        runTest {
            val h = harness(text = listOf(RankedResult(d1, 1f)))
            val noFullText = h.search(WireMessage.SearchTextArm("nope", "deploy"), limit = 5, corr = 1)
            assertEquals("no FULLTEXT index for nope", noFullText.error)
            assertEquals("SCHEMA_VIOLATION", noFullText.errorCode)
            assertTrue(noFullText.hits.isEmpty())

            val noVector = h.search(vector = WireMessage.SearchVectorArm("nope", listOf(0.1)), limit = 5, corr = 2)
            assertEquals("no VECTOR index for nope", noVector.error)

            // And the connection survives both.
            val ok = h.search(WireMessage.SearchTextArm("tasks_text", "deploy"), limit = 5, corr = 3)
            assertNull(ok.error)
        }

    /** Guards: a request with no arm at all, or a nonsense fusion mode, is refused with a message. */
    @Test
    fun malformedRequestsAreRefused() =
        runTest {
            val h = harness(text = listOf(RankedResult(d1, 1f)))
            val noArms = h.search(limit = 5, corr = 1)
            assertNotNull(noArms.error)
            val badFusion =
                h.search(
                    text = WireMessage.SearchTextArm("tasks_text", "x"),
                    vector = WireMessage.SearchVectorArm("embedding", listOf(0.1)),
                    fusion = "magic",
                    limit = 5,
                    corr = 2,
                )
            assertEquals("unknown fusion mode: magic", badFusion.error)
        }

    // ---- harness ---------------------------------------------------------------------------

    private suspend fun kotlinx.coroutines.test.TestScope.harness(
        text: List<RankedResult>? = null,
        vector: List<RankedResult>? = null,
        expiry: DocumentExpiryPolicy? = null,
        clock: () -> Long = { System.currentTimeMillis() },
    ): Harness {
        val runtime = openMemoryRuntime("demo", ns, KdbSchema.NONE)
        if (expiry != null) {
            val current = runtime.policyRegistry.get(ns)
            runtime.policyRegistry.put(current.copy(namespaceId = ns, documentExpiry = expiry))
        }
        val registry = runtime.indexManager.registryFor(ns)
        if (text != null) {
            registry.registerSqlIndex(
                descriptor(IndexType.FULLTEXT, "title", "tasks_text"),
                FakeStoreFactory(text, emptyList()),
                runtime.dag,
                runtime.storage,
                KdbSchema.NONE,
                "tasks_text",
                rebuild = false,
            )
        }
        if (vector != null) {
            registry.registerSqlIndex(
                descriptor(IndexType.VECTOR, "embedding", "tasks_vec"),
                FakeStoreFactory(emptyList(), vector),
                runtime.dag,
                runtime.storage,
                KdbSchema.NONE,
                "tasks_vec",
                rebuild = false,
            )
        }
        val server = KdbServerRuntime(runtime, nowMillis = clock)
        val host = sqlWireHostFactory(wire, server, ns)(ConnectionContext.EMPTY)
        val conn = FakeWireConnection()
        // backgroundScope, not the test scope: the connection read loop runs until the connection
        // closes, and runTest fails a test that leaves a child of its own scope running.
        backgroundScope.launch { pipelinedPerConnection(conn, host) }
        return Harness(server, conn)
    }

    private inner class Harness(val server: KdbServerRuntime, val conn: FakeWireConnection) {
        suspend fun search(
            text: WireMessage.SearchTextArm? = null,
            vector: WireMessage.SearchVectorArm? = null,
            fusion: String? = null,
            limit: Int,
            includeJson: Boolean = false,
            corr: Int,
        ): WireMessage.SearchResult {
            val frame =
                wire.encode(
                    WireMessage.Search(
                        WireHeader(WireMessageType.SEARCH, KDB_WIRE_PROTOCOL_VERSION, corr, 0),
                        namespace = ns,
                        text = text,
                        vector = vector,
                        fusion = fusion,
                        limit = limit,
                        includeJson = includeJson,
                    ),
                )
            return wire.decode(conn.roundTrip(frame)) as WireMessage.SearchResult
        }
    }

    private fun descriptor(
        type: IndexType,
        field: String,
        name: String,
    ): IndexDescriptor =
        IndexDescriptor(
            indexId = KdbUuid.random(),
            namespaceId = ns,
            fieldName = field,
            fields = listOf(field),
            type = type,
            unique = false,
            schemaVersion = 1,
            createdAtHash = KdbHash(ByteArray(32)),
            options = mapOf("index_name" to name),
        )

    private class FakeStoreFactory(
        private val text: List<RankedResult>,
        private val vector: List<RankedResult>,
    ) : IndexStoreFactory {
        override fun create(descriptor: IndexDescriptor): IndexStore = FakeIndexStore(descriptor, text, vector)
    }

    /** Canned-results store: the real FULLTEXT/VECTOR stores are a different component's business,
     * and MemoryIndexStore throws on search/nearestNeighbours. */
    private class FakeIndexStore(
        override val descriptor: IndexDescriptor,
        private val text: List<RankedResult>,
        private val vector: List<RankedResult>,
    ) : IndexStore {
        override suspend fun put(entry: IndexEntry) {}

        override suspend fun delete(docId: KdbUuid, atCommit: KdbHash) {}

        override suspend fun bulkLoad(entries: List<IndexEntry>) {}

        override suspend fun lookup(key: IndexKey, atCommit: KdbHash?): List<KdbUuid> = emptyList()

        override suspend fun range(
            from: IndexKey?,
            to: IndexKey?,
            atCommit: KdbHash?,
            limit: Int,
            ascending: Boolean,
        ): List<KdbUuid> = emptyList()

        override suspend fun search(query: String, atCommit: KdbHash?, limit: Int): List<RankedResult> =
            if (text.size > limit) text.take(limit) else text

        override suspend fun nearestNeighbours(queryVector: FloatArray, k: Int, atCommit: KdbHash?): List<RankedResult> =
            if (vector.size > k) vector.take(k) else vector

        override suspend fun rebuild(entries: List<IndexEntry>) {}

        override suspend fun clear() {}

        override suspend fun isValid(atCommit: KdbHash): Boolean = true

        override suspend fun snapshot(): ByteArray = ByteArray(0)

        override suspend fun restoreSnapshot(data: ByteArray) {}
    }

    /** See SqlWireDisconnectCleanupTest's identically-named class for the rationale. */
    private class FakeWireConnection : WireConnection {
        private val inbound = Channel<ByteArray>(Channel.UNLIMITED)
        private val outbound = Channel<ByteArray>(Channel.UNLIMITED)

        suspend fun roundTrip(frame: ByteArray): ByteArray {
            inbound.send(frame)
            return outbound.receive()
        }

        override suspend fun send(frame: ByteArray) {
            outbound.send(frame)
        }

        override fun incoming(): Flow<ByteArray> = inbound.receiveAsFlow()

        override suspend fun close() {
            inbound.close()
        }
    }
}
