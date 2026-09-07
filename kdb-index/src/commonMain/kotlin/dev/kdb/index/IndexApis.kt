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

    /** The store registered under a `CREATE INDEX` name, or null. */
    public fun getBySqlName(sqlIndexName: String): IndexStore?

    /** Every live descriptor (with SQL names) as a persistable catalog (Layer 16 §9.2). */
    public fun catalog(): IndexCatalog

    /**
     * Recreates the stores described by [catalog] (empty; call [restoreOrRebuild] afterwards).
     * Entries already registered under the same id are left alone. Returns the descriptors created.
     */
    public suspend fun loadCatalog(
        catalog: IndexCatalog,
        storeFactory: IndexStoreFactory,
    ): List<IndexDescriptor>

    /**
     * For every [DocumentIndexStore]: restore its snapshot, rebuilding from a scan at the DAG head
     * when the snapshot is missing or stale (§6.5, §10). HASH/BTREE stores are not touched — they
     * are rebuilt from the schema by [IndexWriter.rebuildAll].
     */
    public suspend fun restoreOrRebuild(
        dag: CommitDag,
        storage: StorageAdapter,
    ): List<IndexRestoreReport>

    /** Flushes every [DocumentIndexStore] snapshot (call on close). */
    public suspend fun flushAll()
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

/**
 * @param blobs where registries persist their catalog and where FULLTEXT/VECTOR stores created
 * through [storeFactory] should persist their snapshots; null means "never persist" (memory
 * runtimes). The catalog is saved after every [IndexRegistry.registerSqlIndex],
 * [IndexRegistry.dropSqlIndex] and [IndexRegistry.syncSchema].
 */
public fun indexManager(
    storeFactory: IndexStoreFactory,
    blobs: IndexBlobStore? = null,
): IndexManager = DefaultIndexManager(storeFactory, blobs)
