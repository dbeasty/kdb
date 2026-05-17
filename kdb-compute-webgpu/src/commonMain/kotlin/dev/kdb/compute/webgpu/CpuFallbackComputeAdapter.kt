package dev.kdb.compute.webgpu

import dev.kdb.codec.KdbUuid
import dev.kdb.index.vector.VectorMetric
import dev.kdb.compute.ComputeAdapter
import dev.kdb.compute.ComputeAdapterCapabilities
import dev.kdb.compute.ComputeBackend
import dev.kdb.compute.GpuSegmentHandle
import dev.kdb.compute.GpuSegmentIngestRequest
import dev.kdb.compute.GpuVectorSearchRequest
import dev.kdb.error.ComputeUnavailableException
import dev.kdb.index.RankedResult
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.math.sqrt

/** CPU fallback used when WebGPU is unavailable (headless CI, Safari, etc.). */
internal class CpuFallbackComputeAdapter : ComputeAdapter {
    override val capabilities: ComputeAdapterCapabilities =
        ComputeAdapterCapabilities(
            supportsVectorSearch = true,
            supportsDirectDeltaIngest = true,
            maxDimensions = 2048,
            maxBatchVectors = 65_536,
        )
    override val isAvailable: Boolean = true
    override val backend: ComputeBackend = ComputeBackend.CPU

    private val mutex = Mutex()
    private val vectors = mutableMapOf<KdbUuid, FloatArray>()
    private var handles = 0L
    private var shutdown = false

    override suspend fun ingestDeltaSegment(request: GpuSegmentIngestRequest): GpuSegmentHandle {
        if (shutdown) throw ComputeUnavailableException("shut down")
        return mutex.withLock {
            GpuSegmentHandle(request.segment.segmentId, ComputeBackend.CPU, ++handles)
        }
    }

    override suspend fun releaseSegment(handle: GpuSegmentHandle) {}

    override suspend fun vectorNearestNeighbours(request: GpuVectorSearchRequest): List<RankedResult> {
        if (request.queryVector.size != request.dimensions) {
            throw IllegalArgumentException("dimension mismatch")
        }
        return mutex.withLock {
            vectors
                .map { (id, vec) -> RankedResult(id, score(request.queryVector, vec, request.metric)) }
                .sortedByDescending { it.score }
                .take(request.k)
        }
    }

    override suspend fun shutdown() {
        mutex.withLock {
            shutdown = true
            vectors.clear()
        }
    }

    internal suspend fun register(docId: KdbUuid, embedding: FloatArray) {
        mutex.withLock { vectors[docId] = embedding.copyOf() }
    }

    private fun score(query: FloatArray, vector: FloatArray, metric: VectorMetric): Float =
        when (metric) {
            VectorMetric.COSINE -> {
                var dot = 0f
                var na = 0f
                var nb = 0f
                for (i in query.indices) {
                    dot += query[i] * vector[i]
                    na += query[i] * vector[i]
                    nb += vector[i] * vector[i]
                }
                val d = sqrt(na) * sqrt(nb)
                if (d == 0f) 0f else dot / d
            }
            VectorMetric.L2 -> {
                var sum = 0f
                for (i in query.indices) {
                    val diff = query[i] - vector[i]
                    sum += diff * diff
                }
                (1.0 / (1.0 + sqrt(sum))).toFloat()
            }
            VectorMetric.INNER_PRODUCT -> {
                var dot = 0f
                for (i in query.indices) dot += query[i] * vector[i]
                dot
            }
        }
}
