package dev.kdb.compute.webgpu

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.compute.GpuVectorSearchRequest
import dev.kdb.index.vector.VectorMetric
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class WebGpuComputeTest {
    @Test
    fun cpuFallback_availableInHeadless() {
        val adapter = createWebGpuComputeAdapterOrCpuFallback()
        assertNotNull(adapter)
        assertEquals(true, adapter.isAvailable)
    }

    @Test
    fun vectorSearch_emptyWhenNoVectors() =
        runTest {
            val adapter = createWebGpuComputeAdapterOrCpuFallback()
            val results =
                adapter.vectorNearestNeighbours(
                    GpuVectorSearchRequest(
                        namespaceId = "ns",
                        queryVector = floatArrayOf(1f, 0f, 0f),
                        dimensions = 3,
                        metric = VectorMetric.COSINE,
                        k = 1,
                        atCommit = KdbHash.fromHex("00".repeat(32)),
                    ),
                )
            assertEquals(0, results.size)
        }
}
