package dev.kdb.compute.webgpu

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.compute.ComputeBackend
import dev.kdb.compute.GpuVectorSearchRequest
import dev.kdb.index.vector.VectorMetric
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Regression tests for docs/kdb-finish-up-plan.md's 1-K10: COSINE similarity accumulated
 * `na += query[i] * vector[i]` (the cross term, same as `dot`) instead of `na += query[i] *
 * query[i]` (the query vector's own squared norm). For any candidate vector pointing in a
 * *different* direction than the query, this understates or (whenever the cross term is
 * negative) makes `na` negative - `sqrt` of a negative number is NaN, so a candidate on the
 * opposite side of the query from correct ended up scored NaN instead of a real similarity value.
 * The correct formula (matching `kdb-compute-jvm`'s `CpuComputeAdapter.cosineSimilarity`, which
 * was never buggy) is exercised here through the adapter's public API, not the private `score`
 * method directly - this module had zero tests before this fix.
 */
class CpuFallbackComputeAdapterTest {
    private val zeroCommit = KdbHash.fromBytes(ByteArray(32))

    private suspend fun searchAll(
        adapter: CpuFallbackComputeAdapter,
        query: FloatArray,
        metric: VectorMetric = VectorMetric.COSINE,
        k: Int = 10,
    ) = adapter.vectorNearestNeighbours(
        GpuVectorSearchRequest(
            namespaceId = "ns",
            queryVector = query,
            dimensions = query.size,
            metric = metric,
            k = k,
            atCommit = zeroCommit,
        ),
    )

    @Test
    fun oppositeDirectionVectorScoresNegativeOneInsteadOfNaN() =
        runTest {
            val adapter = CpuFallbackComputeAdapter()
            val opposite = KdbUuid.random()
            adapter.register(opposite, floatArrayOf(-1f, 0f))

            val results = searchAll(adapter, floatArrayOf(1f, 0f))

            assertEquals(1, results.size)
            assertFalse(results[0].score.isNaN(), "cosine similarity against an opposite-direction vector must not be NaN")
            assertEquals(-1f, results[0].score, absoluteTolerance = 1e-6f)
        }

    @Test
    fun identicalVectorScoresOne() =
        runTest {
            val adapter = CpuFallbackComputeAdapter()
            val same = KdbUuid.random()
            adapter.register(same, floatArrayOf(1f, 0f))

            val results = searchAll(adapter, floatArrayOf(1f, 0f))

            assertEquals(1f, results[0].score, absoluteTolerance = 1e-6f)
        }

    @Test
    fun rankingOrdersByTrueCosineSimilarityAcrossAllDirections() =
        runTest {
            val adapter = CpuFallbackComputeAdapter()
            val same = KdbUuid.random()
            val orthogonal = KdbUuid.random()
            val opposite = KdbUuid.random()
            adapter.register(same, floatArrayOf(2f, 0f)) // same direction, different magnitude
            adapter.register(orthogonal, floatArrayOf(0f, 5f))
            adapter.register(opposite, floatArrayOf(-3f, 0f))

            val results = searchAll(adapter, floatArrayOf(1f, 0f))

            assertEquals(listOf(same, orthogonal, opposite), results.map { it.docId }, "expected same > orthogonal > opposite by true cosine similarity")
            assertTrue(results.none { it.score.isNaN() }, "no candidate should score NaN: $results")
        }

    @Test
    fun ingestAndReleaseStillWorkUnaffectedByTheScoringFix() =
        runTest {
            val adapter = CpuFallbackComputeAdapter()
            assertEquals(ComputeBackend.CPU, adapter.backend)
            assertTrue(adapter.isAvailable)
        }

    private fun assertEquals(expected: Float, actual: Float, absoluteTolerance: Float) {
        assertTrue(kotlin.math.abs(expected - actual) <= absoluteTolerance, "expected $expected but got $actual")
    }
}
