package dev.kdb.replica

import kotlinx.coroutines.runBlocking
import org.junit.AfterClass
import org.junit.Assume.assumeTrue
import org.junit.BeforeClass
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * Live interop: a Kotlin replica syncing with a real Go node (go/cmd/kdb-replicahost) over
 * WebSocket, with the same peer sync v2 messages Go nodes use - the Kotlin twin of
 * packages/kdb-replica/test/sync.test.ts. Skipped when there is no Go toolchain.
 */
class LiveSyncTest {
    companion object {
        private var host: GoHost? = null

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

    private suspend fun replica(
        ns: String,
        store: ReplicaStore = MemoryReplicaStore(),
        configure: (ReplicaOptions) -> ReplicaOptions = { it },
    ): Replica {
        val r =
            Replica.open(
                configure(
                    ReplicaOptions(
                        store = store,
                        url = host().url,
                        namespaces = listOf(NamespaceSpec.Full(ns)),
                        token = "rw-token",
                        autoSync = false,
                    ),
                ),
            )
        opened += r
        return r
    }

    @Test
    fun anEmptyReplicaBootstrapsFromASnapshotThenReplaysNewCommits() =
        runBlocking {
            val h = host()
            val ns = "app/u/boot"
            for (i in 0 until 25) h.put(ns, randomUuid(), """{"i":$i,"pad":"${"x".repeat(i * 10)}"}""")
            val r = replica(ns) { it.copy(pageBytes = 512) } // several snapshot pages
            val first = r.syncNow().single()
            assertEquals("snapshot", first.route)
            assertEquals(h.head(ns).first, r.namespace(ns).head)
            assertEquals(h.docs(ns), docsOf(r, ns))

            val id = randomUuid()
            h.put(ns, id, """{"late":true}""")
            val second = r.syncNow().single()
            assertEquals("replay", second.route)
            assertEquals(listOf(id), second.changed)
            assertEquals("""{"late":true}""", r.get(ns, id))
            assertEquals(h.head(ns).first, r.namespace(ns).head)
        }

    @Test
    fun aLongOfflineReplicaCatchesUpBySquash() =
        runBlocking {
            val h = host()
            val ns = "app/u/squash"
            h.put(ns, randomUuid(), """{"start":1}""")
            val r = replica(ns)
            r.syncNow()
            h.cmd("seed", "ns" to ns, "commits" to 1500, "docs" to 10) // a rewrite-heavy backlog
            val res = r.syncNow().single()
            assertEquals("squash", res.route)
            assertEquals(10, res.changed.size)
            val (head, tree) = h.head(ns)
            assertEquals(head, r.namespace(ns).head)
            assertEquals(tree, r.namespace(ns).rootHash())
            assertEquals(h.docs(ns), docsOf(r, ns))
            // After a squash the replica is an ordinary peer again: the next commit replays.
            h.put(ns, randomUuid(), """{"after":"squash"}""")
            assertEquals("replay", r.syncNow().single().route)
            assertEquals(h.docs(ns), docsOf(r, ns))
        }

    @Test
    fun catchUpReplayReplaysTheSameKindOfBacklog() =
        runBlocking {
            val h = host()
            val ns = "app/u/replay"
            h.put(ns, randomUuid(), """{"start":1}""")
            val r = replica(ns) { it.copy(catchUp = CatchUp.REPLAY, pageBytes = 4096) }
            r.syncNow()
            h.cmd("seed", "ns" to ns, "commits" to 300, "docs" to 7)
            assertEquals("replay", r.syncNow().single().route)
            assertEquals(h.head(ns).first, r.namespace(ns).head)
            assertEquals(h.docs(ns), docsOf(r, ns))
        }

    @Test
    fun localWritesPushAsRealCommitsWithTheHostsHashes() =
        runBlocking {
            val h = host()
            val ns = "app/u/push"
            h.put(ns, randomUuid(), """{"seed":1}""")
            val r = replica(ns)
            r.syncNow()
            val id = randomUuid()
            val local = r.put(ns, id, """{"from":"replica"}""")
            val res = r.syncNow().single()
            assertEquals("fast-forwarded", res.pushOutcome)
            assertEquals(local, h.head(ns).first, "the host's head is the replica's commit, byte for byte")
            assertEquals("""{"from":"replica"}""", h.docs(ns)[id])
            assertEquals(0, r.status().namespaces.getValue(ns).pending)
        }

    @Test
    fun concurrentWritesTheHostMergesAndTheReplicaFastForwardsToTheMerge() =
        runBlocking {
            val h = host()
            val ns = "app/u/merge"
            h.put(ns, randomUuid(), """{"seed":1}""")
            val r = replica(ns)
            r.syncNow()
            val mine = randomUuid()
            val theirs = randomUuid()
            r.put(ns, mine, """{"side":"replica"}""")
            h.put(ns, theirs, """{"side":"host"}""")
            val res = r.syncNow().single()
            assertEquals("merged", res.pushOutcome)
            assertEquals(h.head(ns).first, r.namespace(ns).head)
            val docs = docsOf(r, ns)
            assertEquals("""{"side":"replica"}""", docs[mine])
            assertEquals("""{"side":"host"}""", docs[theirs])
            assertEquals(h.docs(ns), docs)
        }

    @Test
    fun twoReplicasConvergeThroughTheHost() =
        runBlocking {
            val ns = "app/u/two"
            val a = replica(ns)
            val b = replica(ns)
            a.put(ns, randomUuid(), """{"by":"a"}""")
            b.put(ns, randomUuid(), """{"by":"b"}""")
            a.syncNow()
            b.syncNow()
            a.syncNow()
            assertEquals(a.namespace(ns).head, b.namespace(ns).head)
            assertEquals(docsOf(a, ns), docsOf(b, ns))
            assertEquals(2, docsOf(a, ns).size)
        }

    @Test
    fun aReplicaReopenedFromItsStoreKeepsItsStateAndSyncsOn() =
        runBlocking {
            val h = host()
            val ns = "app/u/reopen"
            h.put(ns, randomUuid(), """{"v":1}""")
            val store = MemoryReplicaStore()
            val r1 = replica(ns, store)
            r1.syncNow()
            val offline = randomUuid()
            r1.put(ns, offline, """{"offline":true}""")
            // The store outlives the replica: close only the engine, keep the memory.
            opened.remove(r1)
            r1.close()
            val r2 = replica(ns, store)
            assertEquals("""{"offline":true}""", r2.get(ns, offline))
            assertEquals(1, r2.status().namespaces.getValue(ns).pending)
            r2.syncNow()
            assertEquals("""{"offline":true}""", h.docs(ns)[offline])
        }

    @Test
    fun aBadTokenIsRefusedAndAReadOnlyTokenPullsButCannotPush() =
        runBlocking {
            val h = host()
            val ns = "app/u/auth"
            h.put(ns, randomUuid(), """{"v":1}""")
            val bad = replica(ns) { it.copy(token = "nope") }
            val e = assertFailsWith<Exception> { bad.syncNow() }
            assertTrue(Regex("unauthenticated|refused", RegexOption.IGNORE_CASE).containsMatchIn(e.message.orEmpty()), e.message)
            assertTrue(Regex("unauthenticated|refused", RegexOption.IGNORE_CASE).containsMatchIn(bad.status().lastError.orEmpty()))

            val ro = replica(ns) { it.copy(token = null, tokenProvider = { "ro-token" }) }
            ro.syncNow()
            assertEquals(h.docs(ns), docsOf(ro, ns))
            ro.put(ns, randomUuid(), """{"denied":true}""")
            val e2 = assertFailsWith<ReplicaSyncException> { ro.syncNow() }
            assertTrue("read-only" in e2.message.orEmpty(), e2.message)
        }

    @Test
    fun liveAHostWriteReachesTheReplicaByRefsWatchWithNoPoll() =
        runBlocking {
            val h = host()
            val ns = "app/u/live"
            h.put(ns, randomUuid(), """{"v":1}""")
            val r = replica(ns) { it.copy(autoSync = true, pollMs = 3_600_000) }
            until("the first sync") { r.namespace(ns).head == h.head(ns).first }
            until("the watch to be set up") { r.status().connected }
            val id = randomUuid()
            h.put(ns, id, """{"pushed":"live"}""")
            until("the host's write to arrive") { r.get(ns, id) != null }
            assertEquals(true, r.status().connected, "the connection stays open while live")
            // And a local write goes up without a poll either.
            val mine = randomUuid()
            r.put(ns, mine, """{"from":"replica"}""")
            until("the replica's write to reach the host") { h.docs(ns)[mine] != null }
        }

    @Test
    fun aNamespaceLargerThanTheCacheSyncsWithinTheCachesBudget() =
        runBlocking {
            val h = host()
            val ns = "app/u/big"
            val body = """{"pad":"${"y".repeat(300)}"}"""
            for (i in 0 until 40) {
                val ops =
                    kotlinx.serialization.json.JsonArray(
                        (0 until 50).map {
                            kotlinx.serialization.json.buildJsonObject {
                                put("id", kotlinx.serialization.json.JsonPrimitive(randomUuid()))
                                put("json", kotlinx.serialization.json.JsonPrimitive(body))
                            }
                        },
                    )
                h.cmd("commit", "ns" to ns, "ops" to ops)
            }
            val budget = 64L * 1024 // the namespace is ~700 KB of bodies alone
            val r = replica(ns) { it.copy(cacheBytes = budget) }
            r.syncNow()
            assertEquals(2000, docsOf(r, ns).size)
            assertTrue(r.status().residentBytes <= budget, "resident ${r.status().residentBytes} > $budget")
            assertEquals(h.head(ns).second, r.namespace(ns).rootHash())
        }

    @Test
    fun aConflictListenerHearsNothingOnACleanRound() =
        runBlocking {
            val h = host()
            val ns = "app/u/events"
            h.put(ns, randomUuid(), """{"v":1}""")
            val r = replica(ns)
            val changes = ArrayList<ChangeEvent>()
            r.onChange { changes += it }
            r.onConflict { fail("no conflict expected: $it") }
            r.syncNow()
            val remote = changes.singleOrNull { it.source == ChangeSource.REMOTE }
            assertNotNull(remote)
            assertEquals(1, remote.ids.size)
        }
}
