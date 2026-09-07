package dev.kdb.index.btree

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.index.IndexDescriptor
import dev.kdb.index.IndexEntry
import dev.kdb.index.IndexKey
import dev.kdb.index.IndexStore
import dev.kdb.index.IndexStoreFactory
import dev.kdb.index.IndexType
import dev.kdb.index.RankedResult
import dev.kdb.index.IndexTypeMismatchException
import dev.kdb.index.UniqueIndexViolationException
import dev.kdb.index.VersionedIndexEngine
import dev.kdb.storage.StorageAdapter

public class DefaultBtreeIndexStore(
    override val descriptor: IndexDescriptor,
    private val dag: CommitDag,
    @Suppress("UNUSED_PARAMETER") private val storage: StorageAdapter,
    private val engine: VersionedIndexEngine = VersionedIndexEngine(dag),
) : IndexStore {

    override suspend fun put(entry: IndexEntry) {
        if (descriptor.unique) {
            val existing = engine.lookup(entry.key, null)
            if (existing.any { it != entry.docId }) {
                throw UniqueIndexViolationException(
                    "unique index violation on ${descriptor.fieldName}",
                    descriptor.namespaceId,
                    descriptor.fieldName,
                    entry.key,
                    existing.first(),
                    entry.docId,
                )
            }
        }
        engine.put(entry)
    }

    override suspend fun delete(
        docId: KdbUuid,
        atCommit: KdbHash,
    ) {
        engine.delete(docId, atCommit)
    }

    override suspend fun bulkLoad(entries: List<IndexEntry>) {
        engine.bulkLoad(entries)
    }

    override suspend fun lookup(
        key: IndexKey,
        atCommit: KdbHash?,
    ): List<KdbUuid> = engine.lookup(key, atCommit)

    override suspend fun range(
        from: IndexKey?,
        to: IndexKey?,
        atCommit: KdbHash?,
        limit: Int,
        ascending: Boolean,
    ): List<KdbUuid> = engine.range(from, to, atCommit, limit, ascending)

    override suspend fun search(
        query: String,
        atCommit: KdbHash?,
        limit: Int,
    ): List<RankedResult> =
        throw IndexTypeMismatchException(
            "SEARCH not supported on BTREE",
            descriptor.fieldName,
            IndexType.FULLTEXT,
            IndexType.BTREE,
        )

    override suspend fun nearestNeighbours(
        queryVector: FloatArray,
        k: Int,
        atCommit: KdbHash?,
    ): List<RankedResult> =
        throw IndexTypeMismatchException(
            "VECTOR not supported on BTREE",
            descriptor.fieldName,
            IndexType.VECTOR,
            IndexType.BTREE,
        )

    override suspend fun rebuild(entries: List<IndexEntry>) {
        bulkLoad(entries)
    }

    override suspend fun clear() {
        engine.clear()
    }

    override suspend fun isValid(atCommit: KdbHash): Boolean = engine.isValid(atCommit)

    override suspend fun snapshot(): ByteArray = engine.snapshotBytes()

    override suspend fun restoreSnapshot(data: ByteArray) {
        engine.restoreSnapshotBytes(data)
    }
}

public fun btreeIndexStoreFactory(
    dag: CommitDag,
    storage: StorageAdapter,
): IndexStoreFactory =
    IndexStoreFactory { descriptor ->
        require(descriptor.type == IndexType.BTREE) {
            "BtreeIndexStoreFactory expected BTREE, got ${descriptor.type}"
        }
        DefaultBtreeIndexStore(descriptor, dag, storage)
    }
