package dev.kdb.embed

import dev.kdb.codec.KdbHash
import dev.kdb.dag.CommitDag
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.document.KdbCommit
import dev.kdb.index.IndexManager
import dev.kdb.index.productionIndexManager
import dev.kdb.policy.NamespacePolicyRegistry
import dev.kdb.policy.inMemoryNamespacePolicyRegistry
import dev.kdb.query.hybrid.HybridQueryEngine
import dev.kdb.query.hybrid.hybridQueryEngine
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.isNone
import dev.kdb.sql.SqlEngine
import dev.kdb.sql.sqlEngine
import dev.kdb.storage.StorageAdapter
import dev.kdb.storage.mem.InMemoryStorageAdapter
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

public class EmbeddedKdbRuntime(
    public val catalog: String,
    public val dag: CommitDag,
    public val storage: StorageAdapter,
    public val indexManager: IndexManager,
    public val hybrid: HybridQueryEngine,
    public val schema: KdbSchema,
    public val defaultNamespace: String,
    public val policyRegistry: NamespacePolicyRegistry,
    /** When set, [putJson] commits from this parent instead of [CommitDag.head]. */
    public var writeBaseVersion: KdbHash? = null,
) {
    // Component 44 (Layer 12): a single hook point every commit path shares -
    // EmbedWrites.commitViaEngine notifies through this after every successful commit,
    // regardless of whether the caller is kdb-server's KdbServerRuntime.commit() (the SQL wire
    // path) or kdb-jdbc's EmbeddedSqlSession (the embedded/local path) - both already hold the
    // same EmbeddedKdbRuntime instance when they share one process, so registering here
    // (rather than on either caller separately) is what actually satisfies "fires from every
    // commit path" instead of being bolted onto just one. Mutex-guarded mutable list matches
    // this codebase's existing idiom (see StreamBroadcastHub.subscribers) rather than
    // introducing a SharedFlow, which this codebase reserves for the stream-subscription
    // layer's own event bus.
    private val commitListenersMutex = Mutex()
    private val commitListeners = mutableListOf<suspend (namespaceId: String, commit: KdbCommit) -> Unit>()

    /** Registers a listener invoked after every successful commit through
     * [dev.kdb.embed.commitViaEngine], from any caller sharing this runtime instance. A listener
     * that throws is caught and does not fail the commit - a broken notification must never take
     * down the write path it's observing. */
    public suspend fun addCommitListener(listener: suspend (namespaceId: String, commit: KdbCommit) -> Unit) {
        commitListenersMutex.withLock { commitListeners.add(listener) }
    }

    internal suspend fun notifyCommit(
        namespaceId: String,
        commit: KdbCommit,
    ) {
        val listeners = commitListenersMutex.withLock { commitListeners.toList() }
        for (listener in listeners) {
            try {
                listener(namespaceId, commit)
            } catch (_: Exception) {
                // Intentionally swallowed - see addCommitListener's doc comment.
            }
        }
    }
}

public suspend fun openMemoryRuntime(
    catalog: String,
    namespaceId: String,
    schema: KdbSchema = KdbSchema.NONE,
): EmbeddedKdbRuntime {
    val dag = inMemoryCommitDag(namespaceId)
    val storage = InMemoryStorageAdapter()
    val indexManager = productionIndexManager(dag, storage)
    indexManager.bindNamespace(namespaceId, dag)
    val policies = inMemoryNamespacePolicyRegistry()
    val sql: SqlEngine = sqlEngine(indexManager, storage, dag)
    val hybrid = hybridQueryEngine(sql, dag, policies, indexManager, storage)
    val runtime =
        EmbeddedKdbRuntime(
            catalog = catalog,
            dag = dag,
            storage = storage,
            indexManager = indexManager,
            hybrid = hybrid,
            schema = schema,
            defaultNamespace = namespaceId,
            policyRegistry = policies,
        )
    if (!schema.isNone) {
        syncEmbedSchema(runtime, namespaceId, schema)
    }
    return runtime
}
