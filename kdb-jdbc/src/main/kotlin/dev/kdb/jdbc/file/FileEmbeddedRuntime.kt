package dev.kdb.jdbc.file

import dev.kdb.dag.CommitDag
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.index.compositeIndexStoreFactory
import dev.kdb.index.productionIndexManager
import dev.kdb.embed.EmbeddedKdbRuntime
import dev.kdb.policy.inMemoryNamespacePolicyRegistry
import dev.kdb.query.hybrid.hybridQueryEngine
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.isNone
import dev.kdb.sql.sqlEngine
import dev.kdb.storage.StorageEngineConfig
import dev.kdb.storage.engine.DefaultStorageEngineFactory
import dev.kdb.storage.engine.ServerStorageEngine
import dev.kdb.storage.engine.StorageEngineTarget
import dev.kdb.storage.io.FileBackedPlatformIoShimFactory
import dev.kdb.storage.io.PlatformIoConfig
import kotlinx.coroutines.runBlocking

private const val DEFAULT_MEMORY_BUDGET: Long = 64L * 1024 * 1024

public fun openFileRuntime(
    dataRoot: String,
    catalog: String,
    namespaceId: String,
    schema: KdbSchema = KdbSchema.NONE,
): EmbeddedKdbRuntime {
    NamespacePaths.ensureDirs(dataRoot, namespaceId)
    val io =
        FileBackedPlatformIoShimFactory.open(
            PlatformIoConfig(
                rootDirectory = dataRoot,
                fsyncOnFlush = true,
            ),
        )
    val config =
        StorageEngineConfig(
            globalMemoryBudgetBytes = DEFAULT_MEMORY_BUDGET,
            ioShim = io,
        )
    val handle =
        runBlocking {
            DefaultStorageEngineFactory(StorageEngineTarget.SERVER).open(namespaceId, config)
        }
    runBlocking {
        (handle.adapter as? ServerStorageEngine)?.recoverBlobsFromWal()
    }
    val deltaWriter =
        handle.deltaWriter
            ?: error("SERVER storage engine requires delta writer for file mode")
    val baseDag = inMemoryCommitDag(namespaceId)
  runBlocking {
        DeltaNamespaceReplayer.replay(baseDag, handle.adapter, handle.deltaReader!!)
    }
    val dag: CommitDag =
        PersistingCommitDag(
            baseDag,
            DeltaCommitPersistence(namespaceId, deltaWriter),
        )
    val storage = handle.adapter
    val indexManager = productionIndexManager(dag, storage)
    runBlocking {
        indexManager.bindNamespace(namespaceId, dag)
        if (!schema.isNone) {
            indexManager.registryFor(namespaceId).syncSchema(
                KdbSchema.NONE,
                schema,
                compositeIndexStoreFactory(dag, storage),
                dag,
                storage,
            )
            indexManager.writer.rebuildAll(
                dag.head(),
                dag,
                indexManager.registryFor(namespaceId),
                storage,
                schema,
            )
        }
    }
    val policies = inMemoryNamespacePolicyRegistry()
    val sql = sqlEngine(indexManager, storage, dag)
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
