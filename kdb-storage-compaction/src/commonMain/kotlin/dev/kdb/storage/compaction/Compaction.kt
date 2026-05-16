package dev.kdb.storage.compaction

import dev.kdb.codec.KdbUuid
import dev.kdb.storage.PlatformIoShim
import dev.kdb.storage.io.SegmentNameBuilder
import dev.kdb.storage.sstable.DefaultSsTableReader
import dev.kdb.storage.sstable.DefaultSsTableWriter
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

public suspend fun runSstableCompaction(
    ioShim: PlatformIoShim,
    namespaceId: String,
    job: CompactionJob,
): CompactionResult {
    val writer = DefaultSsTableWriter(ioShim, namespaceId, job.level + 1)
    var read = 0L
    for (id in job.inputSegmentIds) {
        val seg = SegmentNameBuilder.sstable(namespaceId, job.level, id)
        val handle = SsTableHandle(dev.kdb.codec.KdbHash.fromHex("0".repeat(64)), job.level, seg)
        read += 1
    }
    val out = writer.finish()
    for (id in job.inputSegmentIds) {
        ioShim.deleteSegment(SegmentNameBuilder.sstable(namespaceId, job.level, id))
    }
    return CompactionResult(job.jobId, read, 0, listOf(out))
}

public suspend fun runDeltaSegmentRoll(
    namespaceId: String,
): CompactionResult =
    CompactionResult(KdbUuid.random(), 0, 0)

public suspend fun runCompactionBatch(
    ioShim: PlatformIoShim,
    jobs: List<CompactionJob>,
): List<CompactionResult> = jobs.map { runSstableCompaction(ioShim, it.namespaceId, it) }
