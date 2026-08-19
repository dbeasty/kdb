package dev.kdb.storage.engine

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.time.Duration
import kotlin.time.TimeSource

/**
 * Minimal, dependency-free per-stage latency tracking for the write path.
 * Mirrors go/kdb/metrics so both implementations report the same stage
 * names for the Phase 0 baseline (see docs/benchmarks/phase0-baseline.md).
 */
public object StorageStage {
    public const val LOCK_WAIT: String = "lock_wait"
    public const val FSYNC_WAIT: String = "fsync_wait"
    public const val TREE_REBUILD: String = "tree_rebuild"
}

public data class StageSnapshot(
    val stage: String,
    val count: Long,
    val mean: Duration,
    val p50: Duration,
    val p99: Duration,
    val max: Duration,
)

private const val MAX_SAMPLES = 4096

private class StageData {
    var count: Long = 0
    var sumNanos: Long = 0
    val samples = LongArray(MAX_SAMPLES)
}

/** Records per-stage write-path latencies. Guarded by its own coroutine mutex, independent of any engine lock. */
public class StageRecorder {
    private val guard = Mutex()
    private val stages = mutableMapOf<String, StageData>()

    public suspend fun record(stage: String, duration: Duration) {
        val ns = duration.inWholeNanoseconds
        guard.withLock {
            val sd = stages.getOrPut(stage) { StageData() }
            sd.samples[(sd.count % MAX_SAMPLES).toInt()] = ns
            sd.sumNanos += ns
            sd.count += 1
        }
    }

    public suspend inline fun <T> track(stage: String, block: () -> T): T {
        val start = TimeSource.Monotonic.markNow()
        try {
            return block()
        } finally {
            record(stage, start.elapsedNow())
        }
    }

    public suspend fun snapshot(): List<StageSnapshot> =
        guard.withLock {
            stages.entries.sortedBy { it.key }.map { (name, sd) ->
                if (sd.count == 0L) {
                    StageSnapshot(name, 0, Duration.ZERO, Duration.ZERO, Duration.ZERO, Duration.ZERO)
                } else {
                    val n = minOf(sd.count, MAX_SAMPLES.toLong()).toInt()
                    val sorted = sd.samples.copyOf(n).also { it.sort() }
                    StageSnapshot(
                        stage = name,
                        count = sd.count,
                        mean = nanos(sd.sumNanos / sd.count),
                        p50 = nanosAt(sorted, 0.50),
                        p99 = nanosAt(sorted, 0.99),
                        max = nanosAt(sorted, 1.0),
                    )
                }
            }
        }

    public suspend fun reset() {
        guard.withLock { stages.clear() }
    }

    private fun nanos(value: Long): Duration = Duration.parse("${value}ns")

    private fun nanosAt(sorted: LongArray, p: Double): Duration {
        if (sorted.isEmpty()) return Duration.ZERO
        val idx = (kotlin.math.ceil(p * sorted.size).toInt() - 1).coerceIn(0, sorted.size - 1)
        return nanos(sorted[idx])
    }

    public companion object {
        public val Default: StageRecorder = StageRecorder()
    }
}
