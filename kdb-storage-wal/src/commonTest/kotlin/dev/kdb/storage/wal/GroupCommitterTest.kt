package dev.kdb.storage.wal

import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
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
}
