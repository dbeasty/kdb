package dev.kdb.compute

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.index.RankedResult
import dev.kdb.index.vector.VectorMetric
import dev.kdb.storage.DeltaSegmentRef

public interface ComputeAdapter {
    public val capabilities: ComputeAdapterCapabilities
    public val isAvailable: Boolean
    public val backend: ComputeBackend

    public suspend fun ingestDeltaSegment(request: GpuSegmentIngestRequest): GpuSegmentHandle

    public suspend fun releaseSegment(handle: GpuSegmentHandle)

    public suspend fun vectorNearestNeighbours(request: GpuVectorSearchRequest): List<RankedResult>

    public suspend fun shutdown()
}

public data class ComputeAdapterCapabilities(
    val supportsVectorSearch: Boolean,
    val supportsDirectDeltaIngest: Boolean,
    val maxDimensions: Int,
    val maxBatchVectors: Int,
)

public enum class ComputeBackend {
    CPU,
    CUDA,
    VULKAN,
    WEBGPU,
}

public data class GpuVectorSearchRequest(
    val namespaceId: String,
    val queryVector: FloatArray,
    val dimensions: Int,
    val metric: VectorMetric,
    val k: Int,
    val candidateDocIds: List<KdbUuid>? = null,
    val atCommit: KdbHash,
)

public data class GpuSegmentIngestRequest(
    val segment: DeltaSegmentRef,
    val compressedBytes: ByteArray,
)

public data class GpuSegmentHandle(
    val segmentId: KdbUuid,
    val backend: ComputeBackend,
    val nativeHandle: Long,
)
