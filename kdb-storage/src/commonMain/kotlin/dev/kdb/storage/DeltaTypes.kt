package dev.kdb.storage

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbDocument

/** Authorship envelope — present in every delta record. */
public data class DeltaAuthorshipEnvelope(
    val principal: String,
    val timestamp: KdbTimestamp,
    val rightsToken: String,
    val clientContext: String,
)

public data class DeltaRecord(
    val commitHash: KdbHash,
    val namespaceId: String,
    val authorship: DeltaAuthorshipEnvelope,
    /** Canonical Layer 0 bytes for the commit ([KdbCommit.toPayloadBytes]). */
    val commitPayload: ByteArray,
    val documentPatches: List<DocumentPatch>,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || this::class != other::class) return false

        other as DeltaRecord

        if (commitHash != other.commitHash) return false
        if (namespaceId != other.namespaceId) return false
        if (authorship != other.authorship) return false
        if (!commitPayload.contentEquals(other.commitPayload)) return false
        if (documentPatches != other.documentPatches) return false

        return true
    }

    override fun hashCode(): Int {
        var result = commitHash.hashCode()
        result = 31 * result + namespaceId.hashCode()
        result = 31 * result + authorship.hashCode()
        result = 31 * result + commitPayload.contentHashCode()
        result = 31 * result + documentPatches.hashCode()
        return result
    }
}

public data class DocumentPatch(
    val docId: KdbUuid,
    val before: KdbDocument?,
    val after: KdbDocument?,
    val contentHashAfter: KdbHash?,
)

public data class DeltaSegmentRef(
    val segmentId: KdbUuid,
    val namespaceId: String,
    val firstCommitHash: KdbHash,
    val lastCommitHash: KdbHash,
    val sizeBytes: Long,
    val compressionCodec: CompressionCodec,
)
