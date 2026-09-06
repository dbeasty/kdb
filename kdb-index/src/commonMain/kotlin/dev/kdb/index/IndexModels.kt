package dev.kdb.index

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid

public enum class IndexType {
    HASH,
    BTREE,
    FULLTEXT,
    VECTOR,
}

public enum class IndexHintAction {
    PUT,
    DELETE,
}

/** Immutable description of one index (registry view). */
public data class IndexDescriptor(
    val indexId: KdbUuid,
    val namespaceId: String,
    val fieldName: String,
    val fields: List<String>,
    val type: IndexType,
    val unique: Boolean,
    val schemaVersion: Int,
    val createdAtHash: KdbHash,
    /**
     * Layer 16 index options: "index_name", "weights" ("title=3,description=1"), "dimensions",
     * "metric" (cosine|l2|inner_product), "m", "ef_construction", "ef_search".
     */
    val options: Map<String, String> = emptyMap(),
)

public data class IndexEntry(
    val docId: KdbUuid,
    val key: IndexKey,
    val commitHash: KdbHash,
)

public data class RankedResult(
    val docId: KdbUuid,
    val score: Float,
)

/** Pre-computed index update for replicated clients ([Component 8]). */
public data class IndexHint(
    val indexId: KdbUuid,
    val fieldName: String,
    val type: IndexType,
    val action: IndexHintAction,
    val docId: KdbUuid,
    val key: IndexKey?,
    val commitHash: KdbHash,
) {

    public companion object
}


public fun interface IndexStoreFactory {
    public fun create(descriptor: IndexDescriptor): IndexStore
}
