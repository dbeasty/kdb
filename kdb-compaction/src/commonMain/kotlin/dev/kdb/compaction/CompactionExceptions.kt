package dev.kdb.compaction

import dev.kdb.codec.KdbHash
import dev.kdb.error.KdbErrorCode
import dev.kdb.error.KdbException

public class PeerCompactionRejectedException(
    public val namespaceId: String,
    public val boundary: KdbHash,
    public val rejected: Map<String, KdbHash>,
) : KdbException("peer compaction rejected for $namespaceId") {
    override val code: KdbErrorCode get() = KdbErrorCode.COMPACTION_BOUNDARY
}

public class SnapshotMaterializationException(
    public val commit: KdbHash,
    cause: Throwable? = null,
) : KdbException("failed to materialize snapshot at ${commit.toHex()}", cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.COMPACTION_BOUNDARY
}
