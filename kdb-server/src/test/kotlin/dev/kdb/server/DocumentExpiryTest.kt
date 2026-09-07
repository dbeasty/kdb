package dev.kdb.server

import dev.kdb.codec.KdbUuid
import dev.kdb.embed.openMemoryRuntime
import dev.kdb.policy.DocumentExpiryPolicy
import dev.kdb.schema.KdbSchema
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Layer 16 §9.5 (Component 72): documents whose expiry field is a timestamp in the past are hidden
 * from head reads between sweeps, and deleted by the sweeper.
 */
class DocumentExpiryTest {
    private val ns = "demo/expiry"
    private val now = 1_700_000_000_000L
    private val policy = DocumentExpiryPolicy("expiresAt", graceMillis = 0, sweepIntervalMillis = 50)

    private suspend fun newServer(
        expiry: DocumentExpiryPolicy? = policy,
        clock: () -> Long = { now },
        readOnly: Boolean = false,
    ): KdbServerRuntime {
        val runtime = openMemoryRuntime("demo", ns, KdbSchema.NONE)
        if (expiry != null) {
            val current = runtime.policyRegistry.get(ns)
            runtime.policyRegistry.put(current.copy(namespaceId = ns, documentExpiry = expiry))
        }
        return KdbServerRuntime(runtime, nowMillis = clock, readOnly = readOnly)
    }

    /** Guards: the predicate accepts both accepted timestamp forms and nothing else - an RFC 3339
     * string and epoch millis expire; a non-timestamp value (or an absent field) never does. */
    @Test
    fun expiryPredicateAcceptsRfc3339AndEpochMillisOnly() {
        assertTrue(isDocumentExpired("""{"expiresAt":"2023-01-01T00:00:00Z"}""", policy, now))
        assertTrue(isDocumentExpired("""{"expiresAt":${now - 1}}""", policy, now))
        assertFalse(isDocumentExpired("""{"expiresAt":"2099-01-01T00:00:00Z"}""", policy, now))
        assertFalse(isDocumentExpired("""{"expiresAt":${now + 1}}""", policy, now))
        // Never expires: not a timestamp at all, or no such field.
        assertFalse(isDocumentExpired("""{"expiresAt":"soon"}""", policy, now))
        assertFalse(isDocumentExpired("""{"expiresAt":true}""", policy, now))
        assertFalse(isDocumentExpired("""{"expiresAt":{"at":1}}""", policy, now))
        assertFalse(isDocumentExpired("""{"v":1}""", policy, now))
    }

    /** Guards: the grace window keeps a just-expired document visible until grace has passed. */
    @Test
    fun graceDelaysExpiry() {
        val graced = DocumentExpiryPolicy("expiresAt", graceMillis = 10_000)
        assertFalse(isDocumentExpired("""{"expiresAt":${now - 5_000}}""", graced, now))
        assertTrue(isDocumentExpired("""{"expiresAt":${now - 15_000}}""", graced, now))
    }

    /** Guards: an expired document reads as absent through the runtime's head point lookup, while a
     * live one and a non-timestamp one still read back. */
    @Test
    fun getDocumentHidesExpiredDocumentsAtHead() =
        runTest {
            val server = newServer()
            val dead = KdbUuid.random()
            val live = KdbUuid.random()
            val never = KdbUuid.random()
            server.upsert(ns, dead, """{"expiresAt":${now - 1}}""")
            server.upsert(ns, live, """{"expiresAt":${now + 60_000}}""")
            server.upsert(ns, never, """{"expiresAt":"not a time"}""")

            assertNull(server.getDocument(ns, dead).first, "an expired document must read as absent at head")
            assertNotNull(server.getDocument(ns, live).first)
            assertNotNull(server.getDocument(ns, never).first)
        }

    /** Guards: historical reads are untouched - the same document is visible at the commit that wrote
     * it even though it is hidden at head. */
    @Test
    fun historicalReadsIgnoreExpiry() =
        runTest {
            val server = newServer()
            val dead = KdbUuid.random()
            val commit = server.upsert(ns, dead, """{"expiresAt":${now - 1}}""")
            val tree = server.runtime.dag.getCommitOrThrow(commit.hash).documentTreeHash
            val filtered = ExpiryFilteringStorageAdapter(server.runtime.storage, server.runtime.dag, policy, { now })
            // At head (the same commit here) it is hidden...
            assertNull(filtered.getDocument(ns, dead, tree))
            // ...but once head has moved on, that older tree is history and reads unfiltered.
            server.upsert(ns, KdbUuid.random(), """{"v":1}""")
            assertNotNull(filtered.getDocument(ns, dead, tree), "historical reads must not apply expiry")
        }

    /** Guards: the head scan the SQL engine uses skips expired documents; a historical scan does not. */
    @Test
    fun scanAtHeadSkipsExpiredDocuments() =
        runTest {
            val server = newServer()
            server.upsert(ns, KdbUuid.random(), """{"expiresAt":${now - 1}}""")
            server.upsert(ns, KdbUuid.random(), """{"expiresAt":${now + 60_000}}""")
            val filtered = ExpiryFilteringStorageAdapter(server.runtime.storage, server.runtime.dag, policy, { now })
            val head = server.runtime.dag.head()
            val tree = server.runtime.dag.getCommitOrThrow(head).documentTreeHash

            var seen = 0
            filtered.scanDocuments(ns, tree, 256) { batch -> seen += batch.size }
            assertEquals(1, seen, "the head scan must skip the expired document")

            var unfiltered = 0
            server.runtime.storage.scanDocuments(ns, tree, 256) { batch -> unfiltered += batch.size }
            assertEquals(2, unfiltered)
        }

    /** Guards: one sweeper pass deletes exactly the expired documents, and the deletion is real -
     * they are gone from the underlying storage at the new head, not merely hidden. */
    @Test
    fun sweeperDeletesExpiredDocumentsAndReadsConfirmIt() =
        runTest {
            val server = newServer()
            val dead = KdbUuid.random()
            val live = KdbUuid.random()
            server.upsert(ns, dead, """{"expiresAt":${now - 1}}""")
            server.upsert(ns, live, """{"expiresAt":${now + 60_000}}""")

            assertEquals(1, server.sweepExpiredOnce(ns))
            val head = server.runtime.dag.head()
            val commit = server.runtime.dag.getCommitOrThrow(head)
            assertEquals("expiry sweep", commit.message)
            assertEquals(SERVER_SYSTEM_NODE_ID, commit.authorNodeId)
            assertNull(server.runtime.storage.getDocument(ns, dead, commit.documentTreeHash))
            assertNotNull(server.runtime.storage.getDocument(ns, live, commit.documentTreeHash))
            // A second pass has nothing left to do.
            assertEquals(0, server.sweepExpiredOnce(ns))
        }

    /** Guards: a read-only runtime never sweeps, and a namespace without an expiry policy never does. */
    @Test
    fun readOnlyRuntimesAndUnconfiguredNamespacesNeverSweep() =
        runTest {
            val readOnly = newServer(readOnly = true)
            readOnly.upsert(ns, KdbUuid.random(), """{"expiresAt":${now - 1}}""")
            assertEquals(0, readOnly.sweepExpiredOnce(ns))
            readOnly.start(ns)
            assertFalse(readOnly.expirySweeperActive, "a read-only runtime must not start the sweeper")

            val unconfigured = newServer(expiry = null)
            unconfigured.upsert(ns, KdbUuid.random(), """{"expiresAt":${now - 1}}""")
            assertEquals(0, unconfigured.sweepExpiredOnce(ns))
            unconfigured.start(ns)
            assertFalse(unconfigured.expirySweeperActive)
        }

    /** Guards: the sweeper is a coroutine owned by the runtime - started by start(), stopped by
     * close() (and by the last release()). */
    @Test
    fun sweeperIsCancelledOnClose() =
        runTest {
            val server = newServer()
            server.start(ns)
            assertTrue(server.expirySweeperActive, "start() must launch the sweeper when a policy is set")
            server.close()
            assertFalse(server.expirySweeperActive, "close() must cancel the sweeper")

            val released = newServer()
            released.start(ns)
            assertTrue(released.expirySweeperActive)
            released.release()
            assertFalse(released.expirySweeperActive, "the last release() must cancel the sweeper too")
        }
}
