package dev.kdb.index

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid

/** Implemented by Layer 5 physical indexes; orchestrated here ([Component 8]). */
public interface IndexStore {
    public val descriptor: IndexDescriptor

    public suspend fun put(entry: IndexEntry)

    public suspend fun delete(
        docId: KdbUuid,
        atCommit: KdbHash,
    )

    public suspend fun bulkLoad(entries: List<IndexEntry>)

    public suspend fun lookup(
        key: IndexKey,
        atCommit: KdbHash? = null,
    ): List<KdbUuid>

    public suspend fun range(
        from: IndexKey?,
        to: IndexKey?,
        atCommit: KdbHash? = null,
        limit: Int = Int.MAX_VALUE,
        ascending: Boolean = true,
    ): List<KdbUuid>

    public suspend fun search(
        query: String,
        atCommit: KdbHash? = null,
        limit: Int = Int.MAX_VALUE,
    ): List<KdbUuid>

    public suspend fun nearestNeighbours(
        queryVector: FloatArray,
        k: Int,
        atCommit: KdbHash? = null,
    ): List<RankedResult>

    public suspend fun rebuild(entries: List<IndexEntry>)

    public suspend fun clear()

    public suspend fun isValid(atCommit: KdbHash): Boolean

    public suspend fun snapshot(): ByteArray

    public suspend fun restoreSnapshot(data: ByteArray)
}
