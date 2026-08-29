package dev.kdb.storage.compaction

import dev.kdb.codec.KdbUuid
import dev.kdb.storage.PlatformIoShim
import dev.kdb.storage.sstable.SsTableHandle

public enum class StorageTierHint { HOT, WARM, COLD, ICE }

public enum class CompactionKind { SSTABLE_LEVEL, DELTA_ROLL }

public data class CompactionJob(
    val jobId: KdbUuid,
    val namespaceId: String,
    val kind: CompactionKind,
    val level: Int = 0,
    val inputSegmentIds: List<String> = emptyList(),
)

public data class CompactionResult(
    val jobId: KdbUuid,
    val bytesRead: Long,
    val bytesWritten: Long,
    val outputHandles: List<SsTableHandle> = emptyList(),
)

public interface CompactionPlanner {
    public fun plan(namespaceId: String, tier: StorageTierHint): List<CompactionJob>
}

public class DefaultCompactionPlanner : CompactionPlanner {
    override fun plan(namespaceId: String, tier: StorageTierHint): List<CompactionJob> = emptyList()
}

/**
 * Thrown by the compaction entry points below, which are not implemented.
 *
 * They are declared, and nothing in the tree calls them - see the doc comment on
 * [runSstableCompaction] for why they now refuse rather than run.
 */
public class CompactionNotImplementedException(
    message: String,
) : IllegalStateException(message)

/**
 * NOT IMPLEMENTED - always throws [CompactionNotImplementedException].
 *
 * This was a stub that looked like a working compaction and was not one: it opened a writer for
 * the next level, walked the input segment ids without ever reading a byte from them (building a
 * handle from an all-zero hash and discarding it, counting segments rather than bytes into
 * `bytesRead`), wrote out the resulting **empty** SSTable - and then deleted every input
 * segment. Calling it with real inputs destroyed them and replaced them with nothing.
 *
 * Nothing in the tree calls it, which is the only reason that has never happened. Rather than
 * leave a public function whose contract is "loses your data", it now fails loudly. Implementing
 * real SSTable compaction - a merge across the input segments' key ranges, tombstone handling,
 * and only then deleting inputs - is Phase 4 work, tracked in docs/kdb-finish-up-plan.md.
 */
@Suppress("UNUSED_PARAMETER")
public suspend fun runSstableCompaction(
    ioShim: PlatformIoShim,
    namespaceId: String,
    job: CompactionJob,
): CompactionResult =
    throw CompactionNotImplementedException(
        "SSTable compaction is not implemented: it would delete job ${job.jobId}'s " +
            "${job.inputSegmentIds.size} input segment(s) without merging their contents",
    )

/** NOT IMPLEMENTED - always throws [CompactionNotImplementedException]. */
@Suppress("UNUSED_PARAMETER")
public suspend fun runDeltaSegmentRoll(
    namespaceId: String,
): CompactionResult =
    throw CompactionNotImplementedException("delta segment roll is not implemented")

/** NOT IMPLEMENTED - always throws [CompactionNotImplementedException] via [runSstableCompaction]. */
public suspend fun runCompactionBatch(
    ioShim: PlatformIoShim,
    jobs: List<CompactionJob>,
): List<CompactionResult> = jobs.map { runSstableCompaction(ioShim, it.namespaceId, it) }
