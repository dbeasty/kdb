package dev.kdb.jdbc.file

import dev.kdb.codec.KdbHash
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
import dev.kdb.storage.DeltaSegmentReader
import dev.kdb.storage.PlatformIoShim
import dev.kdb.storage.StorageAdapter
import dev.kdb.storage.StorageEngineConfig
import dev.kdb.storage.delta.DefaultDeltaSegmentReader
import dev.kdb.storage.engine.DefaultStorageEngineFactory
import dev.kdb.storage.engine.ServerStorageEngine
import dev.kdb.storage.engine.StorageEngineTarget
import dev.kdb.storage.io.FileBackedPlatformIoShimFactory
import dev.kdb.storage.io.SegmentNameBuilder
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
    val baseDag = runBlocking { restoreNamespace(dataRoot, namespaceId, io, handle.adapter, handle.deltaReader!!) }
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
        attachmentShim = io,
    )
}

/**
 * Rebuilds the namespace's DAG and live state: from Go's checkpoint and the log after it when the
 * checkpoint is usable (CheckpointOpen), else by replaying the whole log - Go's restoreNamespace.
 * A full replay is refused, as Go refuses it, where it cannot rebuild the namespace: one with
 * shallow roots (state only the checkpoint holds) or a truncated log.
 */
private suspend fun restoreNamespace(
    dataRoot: String,
    namespaceId: String,
    io: PlatformIoShim,
    storage: StorageAdapter,
    reader: DeltaSegmentReader,
): CommitDag {
    val marker = NamespaceMarker.roots(dataRoot, namespaceId)
    fun hashes(hexes: List<String>): Set<KdbHash> = hexes.mapNotNull { runCatching { KdbHash.fromHex(it) }.getOrNull() }.toSet()
    val graft = hashes(marker.graftRoots)
    val decisions = CrossNamespaceDecisions(dataRoot)
    val engine = storage as? ServerStorageEngine
    val opener =
        if (engine != null && reader is DefaultDeltaSegmentReader) {
            CheckpointOpen(dataRoot, namespaceId, io, engine, reader, decisions, hashes(marker.shallowRoots), graft)
        } else {
            null
        }
    opener?.open()?.let { return it }
    val why = opener?.whyNot ?: "this storage engine cannot restore one"
    if (marker.shallowRoots.isNotEmpty()) {
        throw ShallowNamespaceException(namespaceId, marker.shallowRoots, "the checkpoint: $why")
    }
    val deltaPrefix = SegmentNameBuilder.namespacePrefix(namespaceId) + "delta/"
    val lowest =
        io.listSegments(namespaceId)
            .filter { it.startsWith(deltaPrefix) }
            .mapNotNull { SegmentNameBuilder.parseDeltaSequencedFileName(it.removePrefix(deltaPrefix)) }
            .minOrNull()
    if (lowest != null && lowest > 0) {
        // Sequences start at zero and are never reused: segments below this were deleted.
        throw TruncatedLogException(namespaceId, lowest, "the checkpoint: $why")
    }
    if (why != "there is none") {
        System.err.println("kdb: namespace $namespaceId: ignoring the checkpoint and replaying the log in full - $why")
    }
    val dag = inMemoryCommitDag(namespaceId)
    DeltaNamespaceReplayer.replay(dag, storage, reader, decisions, shallow = graft)
    return dag
}
