package dev.kdb.server

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CompactionSafetyException
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.embed.openMemoryRuntime
import dev.kdb.error.ConflictException
import dev.kdb.query.hybrid.ReadConsistency
import dev.kdb.schema.KdbSchema
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Mirrors go/kdb/server/retention_test.go and go/kdb/server/backoff_test.go - see
 * [dev.kdb.dag.CommitDag.pin] and [conflictRetryAfterMs] for what these close.
 */
class RetentionAndBackoffTest {
    private val ns = "app/data"

    private suspend fun newServer(): KdbServerRuntime =
        KdbServerRuntime(openMemoryRuntime("demo", ns, KdbSchema.NONE))

    /** Commits one write and returns the new head, so a test can push a previously-pinned
     * commit into the interior of the history where compaction is allowed to reclaim it. */
    private suspend fun advance(server: KdbServerRuntime): KdbHash {
        server.upsert(ns, KdbUuid.random(), """{"v":1}""")
        return server.runtime.dag.head()
    }

    // ---- Retention: a SNAPSHOT session's read pin blocks squash ----

    @Test
    fun snapshotSessionPinBlocksSquash() =
        runTest {
            val server = newServer()
            val sessions = SessionManager(server)

            val pinnedHead = advance(server)
            val session = sessions.begin(ns, ReadConsistency.SNAPSHOT)
            assertTrue(
                server.runtime.dag.isPinned(pinnedHead),
                "a SNAPSHOT session did not pin the commit it reads at",
            )

            // Two more commits, so the pinned one is interior history - exactly what
            // compaction targets.
            advance(server)
            advance(server)

            assertFailsWith<CompactionSafetyException> {
                server.runtime.dag.squash(listOf(pinnedHead), pinnedHead, dev.kdb.document.DocumentTree.EMPTY, null)
            }

            // Ending the session releases it - a pin is held for the length of a transaction,
            // not forever.
            sessions.end(session.id.value)
            assertFalse(server.runtime.dag.isPinned(pinnedHead))
            server.runtime.dag.squash(listOf(pinnedHead), pinnedHead, dev.kdb.document.DocumentTree.EMPTY, null)
        }

    /** The pin follows the transaction, not the session: committing starts a new transaction at
     * a new commit, and the version the previous one was reading at must stop being held.
     * Otherwise a long-lived session accumulates a pin per commit and compaction never
     * reclaims anything. */
    @Test
    fun snapshotPinMovesAtTransactionBoundary() =
        runTest {
            val server = newServer()
            val sessions = SessionManager(server)

            val first = advance(server)
            val session = sessions.begin(ns, ReadConsistency.SNAPSHOT)
            assertTrue(server.runtime.dag.isPinned(first))

            val second = advance(server)
            session.pinRelease?.invoke()
            session.pinRelease = server.runtime.dag.pin(second)
            session.readPin = second

            assertFalse(server.runtime.dag.isPinned(first), "the previous transaction's pin outlived its transaction")
            assertTrue(server.runtime.dag.isPinned(second), "the new transaction did not pin its own read version")
            assertEquals(1, server.runtime.dag.pinnedCount())

            sessions.end(session.id.value)
            assertEquals(0, server.runtime.dag.pinnedCount())
        }

    /** READ_COMMITTED/READ_YOUR_WRITES follow the live head, which is a branch head and
     * therefore already a retention root - pinning it too would be a redundant hold. */
    @Test
    fun nonSnapshotSessionsDoNotPin() =
        runTest {
            val server = newServer()
            val sessions = SessionManager(server)
            sessions.begin(ns, ReadConsistency.READ_COMMITTED)
            sessions.begin(ns, ReadConsistency.READ_YOUR_WRITES)
            assertEquals(0, server.runtime.dag.pinnedCount())
        }

    /** An in-flight commit's base version is the other thing nothing rooted: it is resolved
     * before the writer enters writeCoordinator and not consulted until the commit runs, and
     * the engine throws if it was reclaimed in between. */
    @Test
    fun commitPinsItsBaseVersion() =
        runTest {
            val server = newServer()
            val base = advance(server)

            val tx =
                KdbTransaction(
                    id = KdbUuid.random(),
                    baseVersion = base,
                    operations = listOf(KdbOp.Write(KdbUuid.random(), """{"v":1}""")),
                    timestamp = KdbTimestamp.now(),
                    authorNodeId = KdbUuid.random(),
                )
            // No concurrency needed to observe the pin: commit() takes it before entering
            // writeCoordinator and releases it in a finally, so it must still be held for the
            // whole call and gone the instant it returns.
            server.commit(ns, tx)
            assertFalse(server.runtime.dag.isPinned(base), "the base-version pin outlived the commit")
        }

    // ---- Backoff: a lost race carries a usable, bounded, jittered retry-after ----

    @Test
    fun conflictCarriesRetryAfter() =
        runTest {
            val server = newServer()
            val docId = KdbUuid.random()
            server.upsert(ns, docId, """{"v":1}""")
            val stale = server.runtime.dag.head()
            server.upsert(ns, docId, """{"v":2}""")

            val tx =
                KdbTransaction(
                    id = KdbUuid.random(),
                    baseVersion = stale,
                    operations = listOf(KdbOp.Write(docId, """{"v":3}""")),
                    timestamp = KdbTimestamp.now(),
                    authorNodeId = KdbUuid.random(),
                )
            val e =
                assertFailsWith<ConflictException> {
                    server.commit(ns, tx)
                }
            // e itself carries no retry hint - that's computed at response-shaping time from
            // live write-coordinator pressure, mirroring Go's ConflictError.RetryAfterMs.
            val hint = server.conflictRetryAfterMs()
            assertTrue(hint in 2..250, "retry hint $hint outside the documented [2, 250] bound")
        }

    @Test
    fun conflictRetryAfterIsBoundedAndJittered() =
        runTest {
            val server = newServer()
            val floorOnly = (1..50).map { server.conflictRetryAfterMs() }
            assertTrue(floorOnly.all { it in 2..250 })

            // Force a non-zero mean service time so the ceiling can rise above the floor, then
            // check the draws are not all identical - identical delays reassemble the herd a
            // retry-after exists to break up. A real (not virtual) sleep: meanServiceTime is
            // measured off System.nanoTime, which runTest's virtual clock does not advance.
            server.writeCoordinator.run { Thread.sleep(20) }
            val seen = (1..400).map { server.conflictRetryAfterMs() }.toSet()
            assertTrue(seen.all { it in 2..250 })
            assertTrue(seen.size >= 5, "expected jittered hints, got ${seen.size} distinct values across 400 draws")
        }
}
