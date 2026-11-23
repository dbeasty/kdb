package dev.kdb.storage.wal

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class GroupCommitterTest {
    @Test
    fun allWaitersCoalesceIntoFewerSyncCalls() =
        runTest {
            val g = GroupCommitter()
            var syncCalls = 0
            val doSync: suspend () -> Unit = {
                syncCalls++
                delay(2)
            }

            coroutineScope {
                val jobs =
                    (1..200).map { i ->
                        async { g.syncTo(i.toLong(), doSync) }
                    }
                jobs.forEach { it.await() }
            }

            assertTrue(g.syncedSeqValue() >= 200, "expected syncedSeq >= 200, got ${g.syncedSeqValue()}")
            assertTrue(syncCalls in 1..199, "expected coalescing (<200 calls), got $syncCalls")
        }

    @Test
    fun alreadySyncedReturnsWithoutExtraSync() =
        runTest {
            val g = GroupCommitter()
            var calls = 0
            val doSync: suspend () -> Unit = { calls++ }

            g.syncTo(5, doSync)
            assertEquals(1, calls)

            g.syncTo(3, doSync)
            assertEquals(1, calls, "lower/equal seq must not trigger another physical sync")
        }

    @Test
    fun propagatesSyncError() =
        runTest {
            val g = GroupCommitter()
            val boom = IllegalStateException("boom")
            val doSync: suspend () -> Unit = { throw boom }

            coroutineScope {
                val jobs =
                    (1..10).map { i ->
                        async {
                            assertFailsWith<IllegalStateException> { g.syncTo(i.toLong(), doSync) }
                        }
                    }
                jobs.forEach { it.await() }
            }
            assertEquals(0, g.syncedSeqValue())
        }

    @Test
    fun everyWaiterSeesSyncedSeqCoveringItsOwnRequest() =
        runTest {
            val g = GroupCommitter()
            val doSync: suspend () -> Unit = { delay(1) }

            coroutineScope {
                val jobs =
                    (1..50).map { i ->
                        async {
                            g.syncTo(i.toLong(), doSync)
                            assertTrue(g.syncedSeqValue() >= i, "after syncTo($i), syncedSeq=${g.syncedSeqValue()}")
                        }
                    }
                jobs.forEach { it.await() }
            }
        }

    /**
     * The leader runs the fsync for every waiter queued behind it, so cancelling the leader (a
     * client hung up mid-commit) must not abort that shared work. It used to: the leader's
     * cancelled doSync threw CancellationException, which was handed to every other waiter in
     * the batch as its "sync error", and the leader then ran the next round's doSync in its
     * cancelled coroutine, failing those waiters too - a CancellationException out of syncTo for
     * a caller that was never cancelled and whose write was never made durable.
     */
    @Test
    fun cancellingTheLeaderDoesNotFailOtherWaiters() =
        runTest {
            val g = GroupCommitter()
            val gate = CompletableDeferred<Unit>()
            var syncCalls = 0
            val doSync: suspend () -> Unit = {
                syncCalls++
                gate.await()
            }
            val leader = launch { g.syncTo(1, doSync) }
            runCurrent() // leader is now inside doSync, waiting on the gate
            val follower = async { g.syncTo(2, doSync) }
            runCurrent() // follower is queued for the next round
            leader.cancel()
            runCurrent()
            gate.complete(Unit)
            follower.await() // must not throw
            assertTrue(g.syncedSeqValue() >= 2, "follower's write must be synced, syncedSeq=${g.syncedSeqValue()}")
            assertTrue(syncCalls >= 2, "the follower's round must have run doSync")
        }
}
