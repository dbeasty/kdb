package dev.kdb.server

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration
import kotlin.time.Duration.Companion.nanoseconds

/**
 * Serializes commits/replays against a shared server runtime, and doubles as the server's
 * contention sensor - mirrors Go's `writeGate` (`go/kdb/server/write_gate.go`), minus the
 * bounded-queue/deadline admission control that class also does (Kotlin's write path has no
 * equivalent to Go's BusyError/DeadlineExceededError yet; this only ports the measurement half).
 *
 * [queueDepth] and [meanServiceTime] are what let a conflict response size its retry-after from
 * live pressure instead of a guess (see `KdbServerRuntime.conflictRetryAfterMs`): the staleness
 * that causes a conflict is the queue itself - a transaction's base version is resolved before it
 * queues here, while the target head is resolved once it holds the lock, so a base ages by one
 * commit for every caller that drains ahead of it. This class is the only thing that can measure
 * that.
 */
public class WriteCoordinator {
    private val mutex = Mutex()
    private val queued = AtomicInteger(0)

    // Written only by the coroutine currently holding the lock - i.e. serialized with itself, the
    // same way Go's writeGate.serviceNanos is - so a plain load/store pair needs no CAS.
    private val serviceNanos = AtomicLong(0)

    public suspend fun <T> run(block: suspend () -> T): T {
        queued.incrementAndGet()
        try {
            return mutex.withLock {
                val start = System.nanoTime()
                try {
                    block()
                } finally {
                    observeService((System.nanoTime() - start).nanoseconds)
                }
            }
        } finally {
            queued.decrementAndGet()
        }
    }

    /** How many callers currently hold or are waiting for the lock, including the one running -
     * mirrors Go's `writeGate.queueDepth`. This is the staleness window: every commit that drains
     * ahead of a queued caller advances the head its base version was resolved against. */
    public fun queueDepth(): Int = queued.get()

    /** An exponentially-weighted mean of how long one call occupies the lock, or [Duration.ZERO]
     * before any call has completed. Mirrors Go's `writeGate.meanServiceTime` - see
     * [observeService] for the smoothing constant's reasoning. */
    public fun meanServiceTime(): Duration = serviceNanos.get().nanoseconds

    // Smooths by 1/8 of the gap per sample (shift of 3) - slow enough that one unusually long
    // commit does not spike every caller's backoff, fast enough to track a real change in
    // workload or disk behavior within a few dozen commits. Same constant as Go's writeGate.
    private fun observeService(sample: Duration) {
        val sampleNanos = sample.inWholeNanoseconds
        if (sampleNanos < 0) return
        val prev = serviceNanos.get()
        if (prev == 0L) {
            serviceNanos.set(sampleNanos)
            return
        }
        serviceNanos.addAndGet((sampleNanos - prev) shr 3)
    }
}
