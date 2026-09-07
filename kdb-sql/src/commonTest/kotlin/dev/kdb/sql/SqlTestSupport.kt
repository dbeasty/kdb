package dev.kdb.sql

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.index.IndexDescriptor
import dev.kdb.index.IndexEntry
import dev.kdb.index.IndexKey
import dev.kdb.index.IndexManager
import dev.kdb.index.IndexRegistry
import dev.kdb.index.IndexStore
import dev.kdb.index.IndexStoreFactory
import dev.kdb.index.IndexType
import dev.kdb.index.RankedResult
import dev.kdb.index.compositeIndexStoreFactory
import dev.kdb.index.productionIndexManager
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.SchemaField
import dev.kdb.schema.isNone
import dev.kdb.storage.StorageAdapter
import dev.kdb.storage.mem.InMemoryStorageAdapter
import dev.kdb.transaction.ConflictPolicy
import dev.kdb.transaction.TransactionResult
import dev.kdb.transaction.transactionEngine

/**
 * In-memory SQL runtime for the Layer 16 suites: one namespace, `inMemoryCommitDag` +
 * `InMemoryStorageAdapter` + `productionIndexManager`, documents committed through the real
 * commit path and indexed through the index writer.
 */
internal class SqlTestRuntime(
    val ns: String,
    val dag: CommitDag,
    val storage: StorageAdapter,
    val manager: IndexManager,
    val registry: IndexRegistry,
    val engine: SqlEngine,
    var schema: KdbSchema,
) {
    fun ctx(parameters: List<SqlParameter> = emptyList()): QueryContext =
        QueryContext(ns, schema, parameters = parameters)

    suspend fun put(
        json: String,
        id: KdbUuid = KdbUuid.random(),
    ): KdbUuid {
        storage.putDocument(ns, KdbDocument(id, json))
        val parentCommit = dag.head()
        val parentTree = dag.getCommitOrThrow(parentCommit).documentTreeHash
        val tx =
            KdbTransaction(
                id = KdbUuid.random(),
                baseVersion = parentCommit,
                operations = listOf(KdbOp.Write(id, json)),
                timestamp = KdbTimestamp.now(),
                authorNodeId = KdbUuid.random(),
            )
        val tree = storage.commitTree(ns, parentTree)
        val commit = dag.appendCommit(tx, parentCommit, tree, schemaHash = null)
        manager.writer.applyCommit(commit, registry, storage, schema)
        return id
    }

    suspend fun query(
        sql: String,
        parameters: List<SqlParameter> = emptyList(),
    ): QueryResult = engine.execute(sql, ctx(parameters))

    suspend fun explain(sql: String): ExplainResult = engine.explain(sql, ctx())

    /** Runs DML and commits the produced operations, like the embedded/hybrid path does. */
    suspend fun dml(
        sql: String,
        parameters: List<SqlParameter> = emptyList(),
    ): DmlResult {
        val dml = engine.executeDml(sql, ctx(parameters))
        if (dml.operations.isEmpty()) return dml
        val tx =
            KdbTransaction(
                id = KdbUuid.random(),
                baseVersion = dag.head(),
                operations = dml.operations,
                timestamp = KdbTimestamp.now(),
                authorNodeId = KdbUuid.random(),
            )
        when (val result = transactionEngine(ConflictPolicy.STRICT).commit(tx, dag, storage, schema)) {
            is TransactionResult.Success ->
                if (!schema.isNone) manager.writer.applyCommit(result.commit, registry, storage, schema)
            is TransactionResult.Conflict -> error("transaction conflict")
            is TransactionResult.SchemaError -> error("schema rejection: ${result.violations}")
            is TransactionResult.Aborted -> error("transaction aborted: ${result.cause.message}")
        }
        return dml
    }

    suspend fun docJson(id: KdbUuid): String? {
        val tree = dag.getCommitOrThrow(dag.head()).documentTreeHash
        return storage.getDocument(ns, id, tree)?.json
    }
}

internal suspend fun schemalessRuntime(
    ns: String = "app/tasks",
    storeFactory: IndexStoreFactory? = null,
): SqlTestRuntime {
    val dag = inMemoryCommitDag(ns)
    val storage = InMemoryStorageAdapter()
    val manager = productionIndexManager(dag, storage)
    manager.bindNamespace(ns, dag)
    val factory = storeFactory ?: compositeIndexStoreFactory(dag, storage)
    val engine = sqlEngine(manager, storage, dag, indexStoreFactory = factory)
    return SqlTestRuntime(ns, dag, storage, manager, manager.registryFor(ns), engine, KdbSchema.NONE)
}

internal suspend fun schemaRuntime(
    fields: List<SchemaField>,
    ns: String = "app/users",
    storeFactory: IndexStoreFactory? = null,
): SqlTestRuntime {
    val dag = inMemoryCommitDag(ns)
    val storage = InMemoryStorageAdapter()
    val manager = productionIndexManager(dag, storage)
    manager.bindNamespace(ns, dag)
    val schema = KdbSchema.build(fields)
    val registry = manager.registryFor(ns)
    val factory = storeFactory ?: compositeIndexStoreFactory(dag, storage)
    registry.syncSchema(KdbSchema.NONE, schema, factory, dag, storage)
    val engine = sqlEngine(manager, storage, dag, indexStoreFactory = factory)
    return SqlTestRuntime(ns, dag, storage, manager, registry, engine, schema)
}

internal fun SqlCell.str(): String = (this as SqlCell.StringVal).value

internal fun SqlCell.long(): Long = (this as SqlCell.LongVal).value

internal fun SqlCell.dbl(): Double = (this as SqlCell.DoubleVal).value

internal fun QueryResult.column(i: Int): List<SqlCell> = rows.map { it.values[i] }

internal fun QueryResult.strings(i: Int = 0): List<String> = column(i).map { it.str() }

/**
 * A search index that answers from canned rankings. `MemoryIndexStore` throws on `search`, so the
 * SQL suites use this to prove the executor consults the index reader rather than the documents.
 * Records the last query/vector and limit it was asked for (depth rule, Layer 16 §9.1).
 */
internal class FakeSearchStore(
    override val descriptor: IndexDescriptor,
    private val textRankings: Map<String, List<RankedResult>> = emptyMap(),
    private val vectorRanking: List<RankedResult> = emptyList(),
) : IndexStore {
    var lastQuery: String? = null
    var lastLimit: Int? = null
    var lastVector: FloatArray? = null
    var lastK: Int? = null
    var searchCalls: Int = 0

    override suspend fun put(entry: IndexEntry) = Unit

    override suspend fun delete(
        docId: KdbUuid,
        atCommit: KdbHash,
    ) = Unit

    override suspend fun bulkLoad(entries: List<IndexEntry>) = Unit

    override suspend fun lookup(
        key: IndexKey,
        atCommit: KdbHash?,
    ): List<KdbUuid> = emptyList()

    override suspend fun range(
        from: IndexKey?,
        to: IndexKey?,
        atCommit: KdbHash?,
        limit: Int,
        ascending: Boolean,
    ): List<KdbUuid> = emptyList()

    override suspend fun search(
        query: String,
        atCommit: KdbHash?,
        limit: Int,
    ): List<RankedResult> {
        searchCalls++
        lastQuery = query
        lastLimit = limit
        val all = textRankings[query] ?: emptyList()
        return if (all.size > limit) all.take(limit) else all
    }

    override suspend fun nearestNeighbours(
        queryVector: FloatArray,
        k: Int,
        atCommit: KdbHash?,
    ): List<RankedResult> {
        lastVector = queryVector
        lastK = k
        return if (vectorRanking.size > k) vectorRanking.take(k) else vectorRanking
    }

    override suspend fun rebuild(entries: List<IndexEntry>) = Unit

    override suspend fun clear() = Unit

    override suspend fun isValid(atCommit: KdbHash): Boolean = true

    override suspend fun snapshot(): ByteArray = ByteArray(0)

    override suspend fun restoreSnapshot(data: ByteArray) = Unit
}

/**
 * Store factory that hands FULLTEXT/VECTOR descriptors to [FakeSearchStore] (remembering each
 * created store and the descriptor it received) and everything else to the production stores.
 */
internal class FakeSearchStoreFactory(
    dag: CommitDag,
    storage: StorageAdapter,
    private val textRankings: Map<String, List<RankedResult>> = emptyMap(),
    private val vectorRanking: List<RankedResult> = emptyList(),
) : IndexStoreFactory {
    private val production = compositeIndexStoreFactory(dag, storage)
    val created = mutableListOf<FakeSearchStore>()
    val descriptors = mutableListOf<IndexDescriptor>()

    override fun create(descriptor: IndexDescriptor): IndexStore {
        descriptors += descriptor
        return when (descriptor.type) {
            IndexType.FULLTEXT, IndexType.VECTOR ->
                FakeSearchStore(descriptor, textRankings, vectorRanking).also { created += it }
            else -> production.create(descriptor)
        }
    }

    fun store(type: IndexType): FakeSearchStore = created.first { it.descriptor.type == type }
}

internal fun ranked(vararg pairs: Pair<KdbUuid, Float>): List<RankedResult> = pairs.map { RankedResult(it.first, it.second) }
