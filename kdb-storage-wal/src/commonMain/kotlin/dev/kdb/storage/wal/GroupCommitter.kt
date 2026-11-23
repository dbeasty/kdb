package dev.kdb.storage.wal

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext

/**
 * Coalesces concurrent fsync requests into as few physical sync calls as
 * possible while guaranteeing [syncTo] only returns once every write up
 * to and including [seq] is durable. Mirrors go/kdb/storage/wal's
 * GroupCommitter - see its doc comment for the correctness argument
 * (waiters that join mid-flight are deferred to the next round rather
 * than folded into the in-flight one).
 *
 * Unlike the Go version this has no background goroutine: whichever
 * caller finds no round in flight becomes that round's leader and runs
 * doSync (possibly for several rounds, draining any waiters that arrived
 * meanwhile) inline as part of its own suspend call.
 */
public class GroupCommitter {
    private val mutex = Mutex()
    private var syncedSeq: Long = 0
    private var inFlight = false
    private val waiters = mutableListOf<Waiter>()

    private class Waiter(val seq: Long, val result: CompletableDeferred<Throwable?>)

    public suspend fun syncTo(seq: Long, doSync: suspend () -> Unit) {
        val result = CompletableDeferred<Throwable?>()
        var isLeader = false
        mutex.withLock {
            if (syncedSeq >= seq) {
                result.complete(null)
            } else {
                waiters.add(Waiter(seq, result))
                if (!inFlight) {
                    inFlight = true
                    isLeader = true
                }
            }
        }
        if (isLeader) {
            // The rounds sync every waiter queued behind this caller, not just its own write, so
            // the leader being cancelled must not abort them (Go runs them on a background
            // goroutine no caller can cancel). Otherwise a cancelled doSync's
            // CancellationException became every other waiter's "sync error", and a cancelled
            // mutex.lock could leave inFlight stuck true with waiters nobody would ever drain.
            withContext(NonCancellable) { runRounds(doSync) }
        }
        result.await()?.let { throw it }
    }

    private suspend fun runRounds(doSync: suspend () -> Unit) {
        while (true) {
            val batch = mutex.withLock {
                val snapshot = waiters.toList()
                waiters.clear()
                snapshot
            }
            if (batch.isEmpty()) {
                mutex.withLock { inFlight = false }
                return
            }

            val err: Throwable? =
                try {
                    doSync()
                    null
                } catch (e: Throwable) {
                    e
                }

            val maxSeq = batch.maxOf { it.seq }
            mutex.withLock {
                if (err == null && maxSeq > syncedSeq) syncedSeq = maxSeq
            }
            for (w in batch) w.result.complete(err)
        }
    }

    public suspend fun syncedSeqValue(): Long = mutex.withLock { syncedSeq }
}
