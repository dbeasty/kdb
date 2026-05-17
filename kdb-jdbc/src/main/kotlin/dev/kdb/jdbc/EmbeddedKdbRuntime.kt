package dev.kdb.jdbc

import dev.kdb.dag.CommitDag
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.index.IndexManager
import dev.kdb.index.productionIndexManager
import dev.kdb.policy.NamespacePolicyRegistry
import dev.kdb.policy.inMemoryNamespacePolicyRegistry
import dev.kdb.query.hybrid.HybridQueryEngine
import dev.kdb.query.hybrid.hybridQueryEngine
import dev.kdb.schema.KdbSchema
import dev.kdb.sql.SqlEngine
import dev.kdb.sql.sqlEngine
import dev.kdb.storage.StorageAdapter
import dev.kdb.storage.mem.InMemoryStorageAdapter
import kotlinx.coroutines.runBlocking

public class EmbeddedKdbRuntime(
    public val catalog: String,
    public val dag: CommitDag,
    public val storage: StorageAdapter,
    public val indexManager: IndexManager,
    public val hybrid: HybridQueryEngine,
    public val schema: KdbSchema,
    public val defaultNamespace: String,
    public val policyRegistry: NamespacePolicyRegistry,
)

public fun openMemoryRuntime(
    catalog: String,
    namespaceId: String,
    schema: KdbSchema = KdbSchema.NONE,
): EmbeddedKdbRuntime {
    val dag = inMemoryCommitDag(namespaceId)
    val storage = InMemoryStorageAdapter()
    val indexManager = productionIndexManager(dag, storage)
    runBlocking { indexManager.bindNamespace(namespaceId, dag) }
    val policies = inMemoryNamespacePolicyRegistry()
    val sql: SqlEngine = sqlEngine(indexManager, storage, dag)
    val hybrid = hybridQueryEngine(sql, dag, policies)
    return EmbeddedKdbRuntime(
        catalog = catalog,
        dag = dag,
        storage = storage,
        indexManager = indexManager,
        hybrid = hybrid,
        schema = schema,
        defaultNamespace = namespaceId,
        policyRegistry = policies,
    )
}
