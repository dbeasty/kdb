package dev.kdb.tier

import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
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
import dev.kdb.storage.PlatformIoShim
import dev.kdb.storage.StorageAdapter
import dev.kdb.storage.mem.InMemoryPlatformIoShim
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

    /** Walks HOT/WARM segments for [namespaceId] against policy age bands and demotes any that are due. */
    public suspend fun runCycle(namespaceId: String): TierCycleResult

    public suspend fun archiveCommit(request: ArchiveRequest): ArchiveResult

    public suspend fun restoreArchive(request: RestoreRequest): RestoreResult

    /** Moves one segment's bytes to [SegmentMoveRequest.toTier] regardless of age — explicit/manual path. */
    public suspend fun moveSegment(request: SegmentMoveRequest): SegmentMoveResult

    /** Reads a segment's bytes from wherever it currently lives (HOT/WARM/COLD). Throws for ICE — archived segments require [restoreArchive]. */
    public suspend fun readSegmentBytes(
        namespaceId: String,
        segmentId: KdbUuid,
    ): ByteArray
}

public fun storageTierManager(
    dag: CommitDag,
    storage: StorageAdapter,
    tierRegistry: DeltaLogTierRegistry,
    policyProvider: suspend (String) -> NamespacePolicy,
    backends: TierBackendRegistry = inMemoryTierBackendRegistry(),
    bundleWriter: IceBundleWriter = DefaultIceBundleWriter(),
    ioShim: PlatformIoShim = InMemoryPlatformIoShim(),
    clockMillis: () -> Long = { KdbTimestamp.now().epochMillis },
): StorageTierManager =
    DefaultStorageTierManager(dag, storage, tierRegistry, policyProvider, backends, bundleWriter, ioShim, clockMillis)

internal class DefaultStorageTierManager(
    private val dag: CommitDag,
    @Suppress("UNUSED_PARAMETER") private val storage: StorageAdapter,
    private val tierRegistry: DeltaLogTierRegistry,
    private val policyProvider: suspend (String) -> NamespacePolicy,
    private val backends: TierBackendRegistry,
    private val bundleWriter: IceBundleWriter,
    private val ioShim: PlatformIoShim,
    private val clockMillis: () -> Long,
) : StorageTierManager {
    private var scope: CoroutineScope? = null
    private var signalJob: Job? = null
    private val mutex = Mutex()

    /** Where a segment's bytes currently live once it has left HOT (HOT is derived from namespace+id instead). */
    private val segmentLocations = mutableMapOf<KdbUuid, String>()

    override fun start() {
        val sc = CoroutineScope(kotlinx.coroutines.Dispatchers.Default + Job())
        scope = sc
        signalJob =
            tierRegistry.tierSignals
                .onEach { }
                .launchIn(sc)
    }

    override fun stop() {
        signalJob?.cancel()
        scope?.cancel()
        signalJob = null
        scope = null
    }

    override suspend fun runCycle(namespaceId: String): TierCycleResult =
        mutex.withLock {
            val policy =
                try {
                    policyProvider(namespaceId)
                } catch (_: Throwable) {
                    return TierCycleResult(0, 0, listOf(TierJobError("policy missing", false)))
                }
            val now = clockMillis()
            var moved = 0
            val errors = mutableListOf<TierJobError>()

            suspend fun demoteDueSegments(
                fromTier: SegmentTier,
                toTier: SegmentTier,
                maxAgeMillis: Long,
            ) {
                for (segmentId in tierRegistry.segmentsInTier(namespaceId, fromTier)) {
                    val sealedAt = tierRegistry.sealedAtMillis(segmentId) ?: continue
                    if (now - sealedAt >= maxAgeMillis) {
                        try {
                            moveSegmentLocked(namespaceId, segmentId, toTier)
                            moved++
                        } catch (e: Throwable) {
                            errors += TierJobError(e.message ?: "move failed", retryable = true)
                        }
                    }
                }
            }

            demoteDueSegments(SegmentTier.HOT, SegmentTier.WARM, policy.tiers.hot.maxAgeMillis)
            demoteDueSegments(SegmentTier.WARM, SegmentTier.COLD, policy.tiers.warm.maxAgeMillis)

            TierCycleResult(segmentsMoved = moved, archivesStarted = 0, errors = errors)
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

    override suspend fun moveSegment(request: SegmentMoveRequest): SegmentMoveResult =
        mutex.withLock { moveSegmentLocked(request.namespaceId, request.segmentId, request.toTier) }

    override suspend fun readSegmentBytes(
        namespaceId: String,
        segmentId: KdbUuid,
    ): ByteArray =
        mutex.withLock {
            when (val tier = tierRegistry.tierOf(segmentId) ?: throw TierJobSkippedException(namespaceId, "unknown segment $segmentId")) {
                SegmentTier.HOT -> {
                    val ref = tierRegistry.refOf(segmentId) ?: throw TierJobSkippedException(namespaceId, "no ref for $segmentId")
                    HotSegmentAccess.readBytes(ioShim, namespaceId, ref)
                }
                SegmentTier.ICE ->
                    throw TierJobSkippedException(namespaceId, "segment $segmentId archived to ice; call restoreArchive")
                else -> {
                    val loc = segmentLocations[segmentId] ?: throw TierJobSkippedException(namespaceId, "no location for $segmentId")
                    backends.get(backendIdFor(tier)).get(loc)
                }
            }
        }

    /** Caller must hold [mutex]. */
    private suspend fun moveSegmentLocked(
        namespaceId: String,
        segmentId: KdbUuid,
        toTier: SegmentTier,
    ): SegmentMoveResult {
        val currentTier =
            tierRegistry.tierOf(segmentId) ?: throw TierJobSkippedException(namespaceId, "unknown segment $segmentId")
        if (currentTier == toTier) return SegmentMoveResult(0, null, null)
        val ref = tierRegistry.refOf(segmentId) ?: throw TierJobSkippedException(namespaceId, "no segment ref for $segmentId")

        val bytes: ByteArray
        val sourcePath: String
        if (currentTier == SegmentTier.HOT) {
            bytes = HotSegmentAccess.readBytes(ioShim, namespaceId, ref)
            sourcePath = "hot://$namespaceId/$segmentId"
        } else {
            val loc = segmentLocations[segmentId] ?: throw TierJobSkippedException(namespaceId, "no location for $segmentId")
            bytes = backends.get(backendIdFor(currentTier)).get(loc)
            sourcePath = loc
        }

        val destBackend = backends.get(backendIdFor(toTier))
        val destLocation = destBackend.put("$namespaceId/$segmentId.seg", bytes)

        if (currentTier == SegmentTier.HOT) {
            HotSegmentAccess.delete(ioShim, namespaceId, segmentId)
        } else {
            backends.get(backendIdFor(currentTier)).delete(segmentLocations.getValue(segmentId))
        }
        segmentLocations[segmentId] = destLocation
        tierRegistry.setTier(segmentId, toTier)

        return SegmentMoveResult(bytes.size.toLong(), sourcePath, destLocation)
    }

    private fun backendIdFor(tier: SegmentTier): String =
        when (tier) {
            SegmentTier.WARM -> "default-warm"
            SegmentTier.COLD -> "default-cold"
            SegmentTier.ICE -> "default-ice"
            SegmentTier.HOT -> error("HOT segments have no backend; read via ioShim directly")
        }
}
