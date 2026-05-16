package dev.kdb.tier

import dev.kdb.codec.KdbHash
import dev.kdb.dag.CommitDag
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.document.DocumentTree
import dev.kdb.document.DocumentTreeWireType
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbDocumentWireRegistry
import dev.kdb.error.ArchiveRestoreException
import dev.kdb.error.VersionNotFoundException
import dev.kdb.policy.NamespacePolicy
import dev.kdb.schema.KdbSchema
import dev.kdb.storage.StorageAdapter
import dev.kdb.storage.manager.tier.DeltaLogTierRegistry
import dev.kdb.storage.manager.tier.SegmentTier
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

public interface StorageTierManager {
    public fun start()
    public fun stop()
    public suspend fun runCycle(namespaceId: String): TierCycleResult
    public suspend fun archiveCommit(request: ArchiveRequest): ArchiveResult
    public suspend fun restoreArchive(request: RestoreRequest): RestoreResult
    public suspend fun moveSegment(request: SegmentMoveRequest): SegmentMoveResult
}

public fun storageTierManager(
    dag: CommitDag,
    storage: StorageAdapter,
    tierRegistry: DeltaLogTierRegistry,
    policyProvider: suspend (String) -> NamespacePolicy,
    backends: TierBackendRegistry = inMemoryTierBackendRegistry(),
    bundleWriter: IceBundleWriter = DefaultIceBundleWriter(),
): StorageTierManager =
    DefaultStorageTierManager(dag, storage, tierRegistry, policyProvider, backends, bundleWriter)

internal class DefaultStorageTierManager(
    private val dag: CommitDag,
    @Suppress("UNUSED_PARAMETER") private val storage: StorageAdapter,
    private val tierRegistry: DeltaLogTierRegistry,
    private val policyProvider: suspend (String) -> NamespacePolicy,
    private val backends: TierBackendRegistry,
    private val bundleWriter: IceBundleWriter,
) : StorageTierManager {
    private var scope: CoroutineScope? = null
    private var signalJob: Job? = null
    private val mutex = Mutex()

    override fun start() {
        val sc = CoroutineScope(kotlinx.coroutines.Dispatchers.Default + Job())
        scope = sc
        signalJob =
            tierRegistry.tierSignals
                .onEach { signal ->
                    if (signal.to == SegmentTier.COLD) {
                        // v1: logical cold mark only for in-memory registry
                    }
                }.launchIn(sc)
    }

    override fun stop() {
        signalJob?.cancel()
        scope?.cancel()
        signalJob = null
        scope = null
    }

    override suspend fun runCycle(namespaceId: String): TierCycleResult =
        mutex.withLock {
            try {
                policyProvider(namespaceId)
            } catch (_: Throwable) {
                return TierCycleResult(0, 0, listOf(TierJobError("policy missing", false)))
            }
            TierCycleResult(segmentsMoved = 0, archivesStarted = 0, errors = emptyList())
        }

    override suspend fun archiveCommit(request: ArchiveRequest): ArchiveResult =
        mutex.withLock {
            val policy = policyProvider(request.namespaceId)
            request.tag?.let { tag ->
                val t = dag.getTag(tag) ?: throw VersionNotFoundException("tag missing", request.namespaceId, tag)
                require(t.commitHash == request.commitHash) { "tag does not point at commit" }
            }
            val backend = backends.get(request.targetBackendId)
            val artifact =
                bundleWriter.writeBundle(
                    dag,
                    request.commitHash,
                    request.namespaceId,
                    policy.schema,
                    backend,
                )
            val stub = dag.stubCommit(request.commitHash, artifact.location)
            ArchiveResult(artifact.location, stub, artifact.contentHash)
        }

    override suspend fun restoreArchive(request: RestoreRequest): RestoreResult {
        val backend = backends.get("default-ice")
        val writer = bundleWriter as DefaultIceBundleWriter
        val manifest =
            try {
                writer.readBundle(request.archiveLocation, backend, request.verifyBundle)
            } catch (e: Throwable) {
                throw ArchiveRestoreException("restore failed", request.archiveLocation, e)
            }
        val targetDag = inMemoryCommitDag(request.intoNamespaceId)
        val reg = KdbDocumentWireRegistry()
        val treeVal =
            dev.kdb.codec.KdbValue.decodeFromBytes(
                manifest.documentTreeBytes(),
                DocumentTreeWireType,
                reg,
            )
        val tree = DocumentTree.fromKdbValue(treeVal)
        targetDag.putDocumentTree(tree)
        val commitBytes = manifest.commitPayloadBytes()
        val commit = KdbCommit.fromPayloadBytes(commitBytes)
        targetDag.putCommit(commit, requireParents = false)
        targetDag.setHead("main", commit.hash)
        return RestoreResult(
            namespaceId = request.intoNamespaceId,
            headCommit = commit.hash,
            documentsImported = tree.entries.size,
        )
    }

    override suspend fun moveSegment(request: SegmentMoveRequest): SegmentMoveResult {
        val current = tierRegistry.tierOf(request.segmentId)
        if (current == request.toTier) {
            return SegmentMoveResult(0, null, null)
        }
        return SegmentMoveResult(0, null, "mem://${request.segmentId}")
    }
}
