package dev.kdb.compute.jvm

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.compute.ComputeAdapter
import dev.kdb.compute.ComputeAdapterCapabilities
import dev.kdb.compute.ComputeBackend
import dev.kdb.compute.GpuSegmentHandle
import dev.kdb.compute.GpuSegmentIngestRequest
import dev.kdb.compute.GpuVectorSearchRequest
import dev.kdb.error.ComputeUnavailableException
import dev.kdb.index.RankedResult
import dev.kdb.index.vector.VectorMetric
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.math.sqrt

internal class CpuComputeAdapter(
    private val config: JvmComputeConfig,
) : ComputeAdapter {
    override val capabilities: ComputeAdapterCapabilities =
        ComputeAdapterCapabilities(
            supportsVectorSearch = true,
            supportsDirectDeltaIngest = true,
            maxDimensions = 2048,
            maxBatchVectors = config.cpuThreads * 4096,
        )
    override val isAvailable: Boolean = true
    override val backend: ComputeBackend = ComputeBackend.CPU

    private val mutex = Mutex()
    private val segments = mutableMapOf<Long, SegmentVectors>()
    private val vectors = mutableMapOf<KdbUuid, FloatArray>()
    private var nextHandle = 1L
    private var shutdown = false

    override suspend fun ingestDeltaSegment(request: GpuSegmentIngestRequest): GpuSegmentHandle {
        if (shutdown) throw ComputeUnavailableException("adapter shut down")
        return mutex.withLock {
            val handleId = nextHandle++
            segments[handleId] = SegmentVectors(request.segment.segmentId, emptyList())
            GpuSegmentHandle(request.segment.segmentId, ComputeBackend.CPU, handleId)
        }
    }

    override suspend fun releaseSegment(handle: GpuSegmentHandle) {
        mutex.withLock {
            segments.remove(handle.nativeHandle)
        }
    }

    override suspend fun vectorNearestNeighbours(request: GpuVectorSearchRequest): List<RankedResult> {
        if (shutdown) throw ComputeUnavailableException("adapter shut down")
        if (request.queryVector.size != request.dimensions) {
            throw IllegalArgumentException(
                "query dimension ${request.queryVector.size} != ${request.dimensions}",
            )
        }
        return mutex.withLock {
            val candidates =
                request.candidateDocIds?.mapNotNull { id ->
                    vectors[id]?.let { id to it }
                }
                    ?: vectors.map { it.key to it.value }
            candidates
                .map { (id, vec) -> RankedResult(id, score(request.queryVector, vec, request.metric)) }
                .sortedByDescending { it.score }
                .take(request.k.coerceAtLeast(0))
        }
    }

    override suspend fun shutdown() {
        mutex.withLock {
            shutdown = true
            segments.clear()
            vectors.clear()
        }
    }

    /** Test / integration hook to register vectors for CPU search. */
    internal suspend fun registerVector(docId: KdbUuid, embedding: FloatArray) {
        mutex.withLock {
            vectors[docId] = embedding.copyOf()
        }
    }

    private data class SegmentVectors(val segmentId: KdbUuid, val entries: List<Pair<KdbUuid, FloatArray>>)

    private fun score(
        query: FloatArray,
        vector: FloatArray,
        metric: VectorMetric,
    ): Float =
        when (metric) {
            VectorMetric.COSINE -> cosineSimilarity(query, vector)
            VectorMetric.L2 -> {
                val d = l2(query, vector)
                (1.0 / (1.0 + d)).toFloat()
            }
            VectorMetric.INNER_PRODUCT -> {
                var dot = 0f
                for (i in query.indices) dot += query[i] * vector[i]
                dot
            }
        }

    private fun cosineSimilarity(a: FloatArray, b: FloatArray): Float {
        var dot = 0f
        var na = 0f
        var nb = 0f
        for (i in a.indices) {
            dot += a[i] * b[i]
            na += a[i] * a[i]
            nb += b[i] * b[i]
        }
        val denom = sqrt(na) * sqrt(nb)
        return if (denom == 0f) 0f else dot / denom
    }

    private fun l2(a: FloatArray, b: FloatArray): Float {
        var sum = 0f
        for (i in a.indices) {
            val d = a[i] - b[i]
            sum += d * d
        }
        return sqrt(sum)
    }
}

public data class JvmComputeConfig(
    val preferBackend: ComputeBackend? = null,
    val cudaDeviceIndex: Int = 0,
    val enableVulkan: Boolean = true,
    val cpuThreads: Int = Runtime.getRuntime().availableProcessors(),
)

public data class ComputeAdapterInfo(
    val backend: ComputeBackend,
    val deviceName: String?,
    val totalVramBytes: Long?,
)

public fun createJvmComputeAdapter(config: JvmComputeConfig = JvmComputeConfig()): ComputeAdapter {
    val preferred = config.preferBackend
    if (preferred == ComputeBackend.CUDA || preferred == ComputeBackend.VULKAN) {
        return CpuComputeAdapter(config)
    }
    return CpuComputeAdapter(config)
}

public fun probeComputeAdapter(): ComputeAdapterInfo {
    val adapter = createJvmComputeAdapter()
    return ComputeAdapterInfo(
        backend = adapter.backend,
        deviceName = adapter.backend.name,
        totalVramBytes = null,
    )
}

internal fun createCpuComputeAdapterForTests(config: JvmComputeConfig = JvmComputeConfig()): CpuComputeAdapter =
    CpuComputeAdapter(config)
