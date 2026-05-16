package dev.kdb.storage.wal

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.error.KdbErrorCode
import dev.kdb.error.KdbException

public interface WriteAheadLog {
    public val walId: KdbUuid
    public val partitionKey: String
    public val lastSequence: Long
    public val activeSegmentSizeBytes: Long
    public suspend fun append(record: WalRecord): WalAppendResult
    public suspend fun appendBatch(records: List<WalRecord>): WalAppendResult
    public suspend fun sync()
    public suspend fun recover(handler: suspend (WalRecord) -> Unit): WalRecoverySummary
    public suspend fun truncate(truncateThroughSequence: Long)
    public suspend fun close()
}

public interface WriteAheadLogFactory {
    public suspend fun openOrCreate(
        partitionKey: String,
        config: dev.kdb.storage.StorageEngineConfig,
        ioShim: dev.kdb.storage.PlatformIoShim,
    ): WriteAheadLog

    public fun activeSegmentName(partitionKey: String, walId: KdbUuid): String
}

public interface WalSegmentCatalog {
    public suspend fun listSegments(partitionKey: String): List<WalSegmentInfo>
    public suspend fun deleteSegment(segmentName: String)
}

public data class WalSegmentInfo(
    val segmentName: String,
    val walId: KdbUuid,
    val firstSequence: Long,
    val lastSequence: Long,
    val sizeBytes: Long,
    val isActive: Boolean,
)

public data class WalRecord(
    val sequence: Long,
    val timestamp: KdbTimestamp,
    val kind: WalRecordKind,
    val payload: ByteArray,
) {
    override fun equals(other: Any?): Boolean =
        other is WalRecord &&
            sequence == other.sequence &&
            timestamp == other.timestamp &&
            kind == other.kind &&
            payload.contentEquals(other.payload)

    override fun hashCode(): Int = sequence.hashCode() xor payload.contentHashCode()
}

public sealed class WalRecordKind {
    public data object PutBlob : WalRecordKind()
    public data object DeleteBlob : WalRecordKind()
    public data object FlushCheckpoint : WalRecordKind()
    public data object Marker : WalRecordKind()
}

public data class WalPutBlob(val contentHash: KdbHash, val bytes: ByteArray) {
    override fun equals(other: Any?): Boolean = other is WalPutBlob && contentHash == other.contentHash && bytes.contentEquals(other.bytes)
    override fun hashCode(): Int = contentHash.hashCode() xor bytes.contentHashCode()
}

public data class WalFlushCheckpoint(
    val sstableFileId: KdbUuid,
    val minKey: KdbHash,
    val maxKey: KdbHash,
    val recordCount: Long,
    val fileSizeBytes: Long,
)

public data class WalAppendResult(val sequence: Long, val segmentOffset: Long, val segmentSizeAfterBytes: Long)

public data class WalRecoverySummary(
    val recordsReplayed: Long,
    val recordsSkippedCorrupt: Long,
    val lastSequence: Long,
    val segmentsScanned: Int,
)

public class WalCorruptionException(
    message: String,
    val partitionKey: String,
    val segmentName: String,
    val offset: Long,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.STORAGE_TIER_ERROR
}

public class WalClosedException(message: String, val partitionKey: String) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.STORAGE_TIER_ERROR
}
