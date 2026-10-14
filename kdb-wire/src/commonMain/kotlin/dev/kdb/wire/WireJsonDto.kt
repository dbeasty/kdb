package dev.kdb.wire

import kotlinx.serialization.Serializable

@Serializable
internal data class OpDto(
    val kind: String,
    val docId: String? = null,
    val patch: String? = null,
    val replace: Boolean = false,
    val path: String? = null,
    val blobHashHex: String? = null,
    val migrationId: String? = null,
    val migrationPayload: String? = null,
    // attachmentPut / attachmentDelete: the attachment's id and name, and (put) its content hash
    // scheme, size and media type (the content hash travels in blobHashHex).
    val attachmentId: String? = null,
    val name: String? = null,
    val scheme: Int? = null,
    val size: Long? = null,
    val mime: String? = null,
)

@Serializable
internal data class IndexHintDto(
    val indexId: String,
    val fieldName: String,
    val indexType: String,
    val action: String,
    val docId: String,
    val key: String?,
    val commitHashHex: String,
)
