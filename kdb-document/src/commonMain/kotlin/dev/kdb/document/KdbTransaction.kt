package dev.kdb.document

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid

/**
 * In-flight transaction (not persisted until part of a [KdbCommit]).
 */
public data class KdbTransaction(
    val id: KdbUuid,
    val baseVersion: KdbHash,
    val operations: List<KdbOp>,
    val timestamp: KdbTimestamp,
    val authorNodeId: KdbUuid,
    val resultVersion: KdbHash? = null,
)
