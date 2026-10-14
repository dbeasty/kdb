package dev.kdb.replica

import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.AfterClass
import org.junit.Assume.assumeTrue
import org.junit.BeforeClass
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Live interop for filtered namespaces (plan §4.2, §7): projections of a Go node's namespace
 * through a filter and the token's read rights, with write-back and conflicts - the Kotlin twin of
 * packages/kdb-replica/test/filtered.test.ts. Skipped when there is no Go toolchain.
 */
class LiveFilteredTest {
    companion object {
        private var host: GoHost? = null
        private const val EU = "region = 'EU'"

        @BeforeClass
        @JvmStatic
        fun startHost() {
            if (goAvailable()) host = GoHost.start()
        }

        @AfterClass
        @JvmStatic
        fun stopHost() {
            host?.stop()
        }
    }

    private val opened = ArrayList<Replica>()

    @AfterTest
    fun closeReplicas() =
        runBlocking {
            for (r in opened) r.close()
        }

    private fun host(): GoHost {
        assumeTrue("no Go toolchain", host != null)
        return host!!
    }

    private suspend fun filtered(
        ns: String,
        filter: String = EU,
        writeBack: Boolean = true,
        token: String = "rw-token",
        store: ReplicaStore = MemoryReplicaStore(),
        pageBytes: Int = DEFAULT_PAGE_BYTES,
    ): Replica {
        val r =
            Replica.open(
                ReplicaOptions(
                    store = store,
                    url = host().url,
                    namespaces = listOf(NamespaceSpec.Filtered(ns, filter, writeBack)),
                    token = token,
                    autoSync = false,
                    pageBytes = pageBytes,
                ),
            )
        opened += r
        return r
    }

    private fun onlyEU(docs: Map<String, String>, unreadable: Set<String> = emptySet()): Map<String, String> =
        docs.filter { (id, j) ->
            wireJson.parseToJsonElement(j).obj["region"]?.jsonPrimitive?.content == "EU" && id !in unreadable
        }

    @Test
    fun holdsExactlyTheMatchingReadableDocumentsAndFollowsThemOut() =
        runBlocking {
            val h = host()
            val ns = "shop/orders"
            val ids = List(12) { randomUuid() }
            for ((i, id) in ids.withIndex()) h.put(ns, id, """{"region":"${if (i % 3 == 0) "US" else "EU"}","i":$i}""")
            val secret = ids[1]
            h.cmd("denyRead", "ns" to ns, "id" to secret)
            val r = filtered(ns, pageBytes = 64) // a page per document or so
            r.syncNow()
            assertEquals(onlyEU(h.docs(ns), setOf(secret)), docsOf(r, ns))
            assertNull(r.get(ns, secret), "a document the token may not read never arrives")

            // Out of the filter, deleted at the source, and losing read access: each leaves the replica.
            h.put(ns, ids[2], """{"region":"US","moved":true}""")
            h.cmd(
                "commit",
                "ns" to ns,
                "ops" to JsonArray(listOf(buildJsonObject { put("id", JsonPrimitive(ids[4])); put("delete", JsonPrimitive(true)) })),
            )
            h.cmd("denyRead", "ns" to ns, "id" to ids[5])
            h.put(ns, ids[5], """{"region":"EU","changed":true}""")
            val id = randomUuid()
            h.put(ns, id, """{"region":"EU","fresh":true}""")
            r.syncNow()
            assertFalse(r.lastProjectionResults.single().reset, "a catch-up is a delta, not a reset")
            assertEquals(onlyEU(h.docs(ns), setOf(secret, ids[5])), docsOf(r, ns))
            assertNull(r.get(ns, ids[2]))
            assertNull(r.get(ns, ids[4]))
            assertNull(r.get(ns, ids[5]))
            assertEquals("""{"region":"EU","fresh":true}""", r.get(ns, id))
        }

    @Test
    fun anInterruptedTransferResumesWhereItStopped() =
        runBlocking {
            val h = host()
            val ns = "shop/resume"
            for (i in 0 until 30) h.put(ns, randomUuid(), """{"region":"EU","i":$i,"pad":"${"z".repeat(50)}"}""")
            val store = MemoryReplicaStore()
            // Once armed, the third apply fails: two pages of the transfer land, then the disk fills.
            var applies = -1
            val failing =
                object : ReplicaStore by store {
                    override suspend fun apply(batch: Batch) {
                        if (applies >= 0 && ++applies == 3) throw IllegalStateException("disk full")
                        store.apply(batch)
                    }
                }
            val r2 = filtered(ns, store = failing, pageBytes = 200)
            applies = 0
            val e = assertFailsWith<ReplicaSyncException> { r2.syncNow() }
            assertTrue("disk full" in e.message.orEmpty(), e.message)
            val midway = r2.projection(ns).transfer
            assertNotNull(midway)
            assertTrue(midway.after.isNotEmpty(), "the transfer's cursor was kept")
            r2.syncNow()
            assertNull(r2.projection(ns).transfer)
            assertEquals(onlyEU(h.docs(ns)), docsOf(r2, ns))
        }

    @Test
    fun offlineWritesQueueAndApplyAtTheSourceOnReconnect() =
        runBlocking {
            val h = host()
            val ns = "shop/writes"
            val existing = randomUuid()
            h.put(ns, existing, """{"region":"EU","qty":1}""")
            val r = filtered(ns)
            r.syncNow()
            val created = randomUuid()
            r.put(ns, existing, """{"region":"EU","qty":2}""")
            r.put(ns, created, """{"region":"EU","qty":9}""")
            r.put(ns, existing, """{"region":"EU","qty":3}""") // a second write on top of the first
            assertEquals(3, r.status().namespaces.getValue(ns).pending)
            r.syncNow()
            assertEquals(3, r.lastProjectionResults.single().applied)
            assertEquals(0, r.status().namespaces.getValue(ns).pending)
            val hostDocs = h.docs(ns)
            assertEquals("""{"region":"EU","qty":3}""", hostDocs[existing])
            assertEquals("""{"region":"EU","qty":9}""", hostDocs[created])
            r.syncNow()
            assertEquals(onlyEU(hostDocs), docsOf(r, ns))
        }

    @Test
    fun aWriteTheSourceChangedMeanwhileIsAConflictAndTheSourceStateComesBack() =
        runBlocking {
            val h = host()
            val ns = "shop/conflict"
            val id = randomUuid()
            h.put(ns, id, """{"region":"EU","owner":"nobody"}""")
            val r = filtered(ns)
            r.syncNow()
            val events = ArrayList<ConflictEvent>()
            r.onConflict { events += it }
            r.put(ns, id, """{"region":"EU","owner":"replica"}""")
            h.put(ns, id, """{"region":"EU","owner":"host"}""")
            r.syncNow()
            assertEquals("""{"region":"EU","owner":"host"}""", r.get(ns, id))
            assertEquals(1, events.size)
            assertEquals("conflict", events[0].outcome)
            assertEquals("""{"region":"EU","owner":"replica"}""", events[0].report?.conflicts?.firstOrNull()?.localDoc)
            assertEquals("""{"region":"EU","owner":"host"}""", events[0].report?.conflicts?.firstOrNull()?.incomingDoc)
            assertEquals("""{"region":"EU","owner":"host"}""", h.docs(ns)[id])
        }

    @Test
    fun aSingleHomeNamespaceDefersWriteBackUntilTheHomeIsReachable() =
        runBlocking {
            val h = host()
            val ns = "shop/homed"
            h.put(ns, randomUuid(), """{"region":"EU"}""")
            val r = filtered(ns)
            r.syncNow()
            h.cmd("setHome", "ns" to ns)
            val id = randomUuid()
            r.put(ns, id, """{"region":"EU","queued":true}""")
            val e = assertFailsWith<ReplicaSyncException> { r.syncNow() }
            assertTrue(Regex("deferred.*NOT_HOME").containsMatchIn(e.message.orEmpty()), e.message)
            assertEquals(1, r.status().namespaces.getValue(ns).pending, "the write stays queued")
            assertEquals("""{"region":"EU","queued":true}""", r.get(ns, id), "and visible locally")
            h.cmd("clearHome", "ns" to ns)
            r.syncNow()
            assertEquals(0, r.status().namespaces.getValue(ns).pending)
            assertEquals("""{"region":"EU","queued":true}""", h.docs(ns)[id])
        }

    @Test
    fun aFullReplicaOfASingleHomeNamespaceIsToldToHoldItFiltered() =
        runBlocking {
            val h = host()
            val ns = "shop/homed-full"
            h.put(ns, randomUuid(), """{"region":"EU"}""")
            val r =
                Replica.open(ReplicaOptions(MemoryReplicaStore(), h.url, listOf(NamespaceSpec.Full(ns)), token = "rw-token", autoSync = false))
                    .also { opened += it }
            r.syncNow()
            h.cmd("setHome", "ns" to ns)
            try {
                r.put(ns, randomUuid(), """{"region":"EU"}""")
                val e = assertFailsWith<ReplicaSyncException> { r.syncNow() }
                assertTrue(Regex("single-home.*FILTERED").containsMatchIn(e.message.orEmpty()), e.message)
            } finally {
                h.cmd("clearHome", "ns" to ns)
            }
        }

    @Test
    fun aReadOnlyFilteredNamespaceRefusesWritesAndARefusedWriteBackIsUndone() =
        runBlocking {
            val h = host()
            val ns = "shop/ro"
            val id = randomUuid()
            h.put(ns, id, """{"region":"EU","v":1}""")
            val ro = filtered(ns, writeBack = false)
            ro.syncNow()
            assertFailsWith<ReadOnlyProjectionException> { ro.put(ns, id, """{"region":"EU","v":2}""") }

            val wb = filtered(ns, token = "ro-token")
            wb.syncNow()
            val events = ArrayList<ConflictEvent>()
            wb.onConflict { events += it }
            wb.put(ns, id, """{"region":"EU","v":3}""")
            wb.syncNow()
            assertEquals("refused", events.firstOrNull()?.outcome)
            assertEquals("""{"region":"EU","v":1}""", wb.get(ns, id), "the source's state is back")
            assertEquals(0, wb.status().namespaces.getValue(ns).pending)
        }
}
