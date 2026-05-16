package dev.kdb.stream

import dev.kdb.codec.KdbHash
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.error.ConflictReport
import dev.kdb.error.KdbErrorCode
import dev.kdb.error.KdbException
import dev.kdb.index.IndexHint
import dev.kdb.wire.PayloadEncoding
import kotlinx.coroutines.flow.SharedFlow

public enum class StreamClientMode {
    READ_ONLY,
    WRITE_BACK,
}

public data class StreamSessionConfig(
    val namespaceId: String,
    val nodeId: String,
    val headProvider: suspend () -> KdbHash,
)

public data class StreamSubscriberConfig(
    val namespaceId: String,
    val nodeId: String,
    val mode: StreamClientMode,
    val coordinatorUri: String = "memory://local",
    val resumeFrom: KdbHash? = null,
)

public data class PublishedCommit(
    val commitHash: KdbHash,
    val parentHash: KdbHash,
    val operations: List<KdbOp> = emptyList(),
    val indexHints: List<IndexHint> = emptyList(),
    val timestampMicros: Long,
)

public data class StreamConnection(
    val namespaceId: String,
    val mode: StreamClientMode,
    val position: () -> KdbHash?,
    val submitTransaction: suspend (KdbTransaction) -> ReplayResult,
    val tryPoll: () -> ByteArray? = { null },
)

public sealed class ReplayResult {
    public data class Applied(val commitHash: KdbHash) : ReplayResult()
    public data class Conflict(val report: ConflictReport) : ReplayResult()
    public data class Rejected(val reason: String) : ReplayResult()
}

public sealed class StreamEvent {
    public data class Connected(val negotiatedEncoding: PayloadEncoding) : StreamEvent()
    public data class DeltaReceived(val commitHash: KdbHash, val hintCount: Int) : StreamEvent()
    public data class PositionUpdated(val commitHash: KdbHash) : StreamEvent()
    public data class CompactionWarning(val boundary: KdbHash) : StreamEvent()
    public data class IceArchived(val originalHash: KdbHash, val location: String) : StreamEvent()
    public data class Disconnected(val cause: Throwable?) : StreamEvent()
    public data class Error(val throwable: Throwable) : StreamEvent()
}

public data class SubscriberState(
    val nodeId: String,
    val mode: StreamClientMode,
    val lastAck: KdbHash?,
)

public class StreamDesyncException(
    public val expectedParent: KdbHash,
    public val actualParent: KdbHash,
) : KdbException("stream desync: expected parent $expectedParent, got $actualParent") {
    override val code: KdbErrorCode get() = KdbErrorCode.KDB_DECODE_ERROR
}

public class StreamNotConnectedException : KdbException("stream not connected") {
    override val code: KdbErrorCode get() = KdbErrorCode.KDB_DECODE_ERROR
}
