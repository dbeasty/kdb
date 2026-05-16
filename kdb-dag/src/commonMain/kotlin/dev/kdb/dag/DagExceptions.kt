package dev.kdb.dag

import dev.kdb.codec.KdbHash
import dev.kdb.error.KdbErrorCode
import dev.kdb.error.KdbException

public class DagConsistencyException(
    message: String,
    public val namespaceId: String,
    public val hash: KdbHash? = null,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.VERSION_NOT_FOUND
}

public class BranchNotFoundException(
    message: String,
    public val namespaceId: String,
    public val branchName: String,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.VERSION_NOT_FOUND
}

public class TagNotFoundException(
    message: String,
    public val namespaceId: String,
    public val tagName: String,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.VERSION_NOT_FOUND
}

public class CompactionSafetyException(
    message: String,
    public val namespaceId: String,
    public val blockerHash: KdbHash,
    public val reason: String,
) : KdbException(message) {
    override val code: KdbErrorCode get() = KdbErrorCode.COMPACTION_BOUNDARY
}
