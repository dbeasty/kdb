package dev.kdb.jdbc.file

import dev.kdb.dag.CommitDag
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.index.IndexCatalog
import dev.kdb.index.compositeIndexStoreFactory
import dev.kdb.index.productionIndexManager
import dev.kdb.index.storageAdapterIndexBlobStore
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
    acquireDirectoryLock: Boolean = true,
    lockHolder: String = "kdb-jdbc",
): EmbeddedKdbRuntime {
    if (acquireDirectoryLock) {
        DataDirectoryLockRegistry.acquire(dataRoot, lockHolder)
    }
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
    // Layer 16 §6.5/§9.2: one blob store shared by the manager (catalog) and the store factory
    // (snapshots), with pointers that survive a restart - otherwise the snapshot bytes are on disk
    // under a name nothing can resolve, and every index is silently rebuilt by scan on each open.
    val blobs =
        storageAdapterIndexBlobStore(
            storage,
            FileIndexBlobPointers(NamespacePaths.indexPointersFile(dataRoot, namespaceId)),
        )
    val storeFactory = compositeIndexStoreFactory(dag, storage, blobs = blobs)
    val indexManager = productionIndexManager(dag, storage, blobs = blobs)
    runBlocking {
        indexManager.bindNamespace(namespaceId, dag)
        val registry = indexManager.registryFor(namespaceId)
        // Recreate the indexes this namespace had before the restart, then restore each snapshot -
        // a missing or stale one rebuilds from a scan at head (§6.5, §10).
        IndexCatalog.load(blobs, namespaceId)?.let { registry.loadCatalog(it, storeFactory) }
        registry.restoreOrRebuild(dag, storage)
        if (!schema.isNone) {
            registry.syncSchema(
                KdbSchema.NONE,
                schema,
                storeFactory,
                dag,
                storage,
            )
            indexManager.writer.rebuildAll(
                dag.head(),
                dag,
                registry,
                storage,
                schema,
            )
        }
    }
    val policies = inMemoryNamespacePolicyRegistry()
    val sql = sqlEngine(indexManager, storage, dag, indexStoreFactory = storeFactory)
    val hybrid = hybridQueryEngine(sql, dag, policies, indexManager, storage)
    return EmbeddedKdbRuntime(
        catalog = catalog,
        dag = dag,
        storage = storage,
        indexManager = indexManager,
        hybrid = hybrid,
        schema = schema,
        defaultNamespace = namespaceId,
        policyRegistry = policies,
        indexBlobs = blobs,
        indexStoreFactory = storeFactory,
    )
}
