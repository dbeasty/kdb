package dev.kdb.index

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbCommit
import dev.kdb.dag.CommitDag
import dev.kdb.schema.KdbSchema
import dev.kdb.storage.StorageAdapter

public data class SchemaSyncResult(
    val created: List<IndexDescriptor>,
    val removed: List<IndexDescriptor>,
    val unchanged: List<IndexDescriptor>,
    val rebuilding: List<IndexDescriptor>,
)

public interface IndexRegistry {

    val namespaceId: String

    val indexes: List<IndexStore>

    public fun get(
        fieldName: String,
        type: IndexType,
    ): IndexStore?

    public fun getById(indexId: KdbUuid): IndexStore?

    public suspend fun syncSchema(
        oldSchema: KdbSchema,
        newSchema: KdbSchema,
        storeFactory: IndexStoreFactory,
        dag: CommitDag,
        storage: StorageAdapter,
    ): SchemaSyncResult

    /** Registers an index declared via SQL `CREATE INDEX` (not from schema sync). */
    public suspend fun registerSqlIndex(
        descriptor: IndexDescriptor,
        storeFactory: IndexStoreFactory,
        dag: CommitDag,
        storage: StorageAdapter,
        schema: KdbSchema,
        sqlIndexName: String,
        rebuild: Boolean,
    ): SchemaSyncResult

    /** Drops an index created via `CREATE INDEX`; returns false if name unknown. */
    public suspend fun dropSqlIndex(
        namespaceId: String,
        sqlIndexName: String,
    ): Boolean
}

public interface IndexWriter {

    public suspend fun applyCommit(
        commit: KdbCommit,
        registry: IndexRegistry,
        storage: StorageAdapter,
        schema: KdbSchema,
    )

    public suspend fun rebuildAll(
        fromCommit: KdbHash,
        dag: CommitDag,
        registry: IndexRegistry,
        storage: StorageAdapter,
        schema: KdbSchema,
        onProgress: ((rebuilt: Int, total: Int) -> Unit)? = null,
    )
}

public interface IndexReader {

    public suspend fun lookupExact(
        registry: IndexRegistry,
        fieldName: String,
        key: IndexKey,
        atCommit: KdbHash? = null,
    ): List<KdbUuid>

    public suspend fun lookupRange(
        registry: IndexRegistry,
        fieldName: String,
        from: IndexKey?,
        to: IndexKey?,
        atCommit: KdbHash? = null,
        limit: Int = Int.MAX_VALUE,
        ascending: Boolean = true,
    ): List<KdbUuid>

    public suspend fun lookupFullText(
        registry: IndexRegistry,
        fieldName: String,
        query: String,
        atCommit: KdbHash? = null,
        limit: Int = Int.MAX_VALUE,
    ): List<RankedResult>

    public suspend fun lookupVector(
        registry: IndexRegistry,
        fieldName: String,
        queryVector: FloatArray,
        k: Int,
        atCommit: KdbHash? = null,
    ): List<RankedResult>
}

public interface IndexManager {
    val writer: IndexWriter
    val reader: IndexReader

    public fun registryFor(namespaceId: String): IndexRegistry

    /** Associates a namespace with its commit DAG so registries and readers can resolve HEAD. */
    public suspend fun bindNamespace(
        namespaceId: String,
        dag: CommitDag,
    )

    public suspend fun releaseRegistry(namespaceId: String)
}

public fun indexManager(storeFactory: IndexStoreFactory): IndexManager =
    DefaultIndexManager(storeFactory)
