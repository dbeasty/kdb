package dev.kdb.wire

import kotlinx.serialization.Serializable

@Serializable
internal data class OpDto(
    val kind: String,
    val docId: String? = null,
    val patch: String? = null,
    val path: String? = null,
    val blobHashHex: String? = null,
    val migrationId: String? = null,
    val migrationPayload: String? = null,
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
