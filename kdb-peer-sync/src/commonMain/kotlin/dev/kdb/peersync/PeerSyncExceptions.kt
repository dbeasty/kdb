package dev.kdb.peersync

import dev.kdb.error.KdbErrorCode
import dev.kdb.error.KdbException

public open class PeerSyncException(
    message: String,
    cause: Throwable? = null,
) : KdbException(message, cause) {
    override val code: KdbErrorCode get() = KdbErrorCode.UNSUPPORTED_PROTOCOL_VERSION
}

/** A peer refused a request: its PEER_ERROR, or a failed entry of a blob fetch or upload.
 * [errorCode] is Go's wire.ErrorCode string ([PeerErrorCodes]). */
public class RemotePeerException(
    public val errorCode: String,
    message: String,
) : PeerSyncException("peer sync: the peer answered $errorCode: $message")

/** A blob a peer sent does not match what was asked for - a chunk failing its Merkle proof, a
 * tree that does not hash to the blob's content hash. Nothing of it was stored. */
public class BlobIntegrityException(message: String) : PeerSyncException(message)

/** The peer's build cannot decode commits carrying attachments (it did not advertise
 * [SyncV2.CAP_ATTACHMENTS]); Go's AttachmentsUnsupportedError. */
public class AttachmentsUnsupportedException(
    public val namespace: String,
    public val commitHex: String,
) : PeerSyncException(
        "peer sync: namespace $namespace has attachments (commit $commitHex) and the peer's build does not support them - upgrade it",
    )
