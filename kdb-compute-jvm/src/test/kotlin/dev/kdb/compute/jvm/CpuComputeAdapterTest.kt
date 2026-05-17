package dev.kdb.compute.jvm

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.compute.ComputeBackend
import dev.kdb.compute.GpuVectorSearchRequest
import dev.kdb.index.vector.VectorMetric
import dev.kdb.storage.DeltaSegmentRef
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class CpuComputeAdapterTest {
    @Test
    fun probe_cpuAlwaysAvailable() {
        val info = probeComputeAdapter()
        assertEquals(ComputeBackend.CPU, info.backend)
        val adapter = createJvmComputeAdapter()
        assertTrue(adapter.isAvailable)
    }

    @Test
    fun vectorSearch_topK() =
        runTest {
            val adapter = createCpuComputeAdapterForTests()
            val a = KdbUuid.random()
            val b = KdbUuid.random()
            adapter.registerVector(a, floatArrayOf(1f, 0f))
            adapter.registerVector(b, floatArrayOf(0.9f, 0.1f))
            val results =
                adapter.vectorNearestNeighbours(
                    GpuVectorSearchRequest(
                        namespaceId = "ns",
                        queryVector = floatArrayOf(1f, 0f),
                        dimensions = 2,
                        metric = VectorMetric.COSINE,
                        k = 1,
                        atCommit = KdbHash.fromHex("00".repeat(32)),
                    ),
                )
            assertEquals(1, results.size)
            assertEquals(a, results.single().docId)
            adapter.shutdown()
        }

    @Test
    fun shutdown_idempotent() =
        runTest {
            val adapter = createJvmComputeAdapter()
            adapter.shutdown()
            adapter.shutdown()
        }

    @Test
    fun ingestAndRelease() =
        runTest {
            val adapter = createJvmComputeAdapter()
            val hash = KdbHash.fromHex("11".repeat(32))
            val seg =
                DeltaSegmentRef(
                    segmentId = KdbUuid.random(),
                    namespaceId = "ns",
                    firstCommitHash = hash,
                    lastCommitHash = hash,
                    sizeBytes = 100,
                    compressionCodec = dev.kdb.storage.CompressionCodec.NONE,
                )
            val handle =
                adapter.ingestDeltaSegment(
                    dev.kdb.compute.GpuSegmentIngestRequest(seg, ByteArray(0)),
                )
            adapter.releaseSegment(handle)
            adapter.shutdown()
        }
}
