package dev.kdb.storage.compaction

import dev.kdb.codec.KdbUuid
import dev.kdb.storage.mem.InMemoryPlatformIoShim
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * This module had no tests. Most of it is data model; the parts that ran were a stub that looked
 * like a working compaction and was not one - it never read the input segments, wrote an empty
 * SSTable, and then deleted the inputs. These pin the refusal that replaced it, so nobody wires
 * it up believing it works, and cover the model types that are real.
 */
class CompactionTest {
    private fun job(vararg inputs: String) =
        CompactionJob(
            jobId = KdbUuid.random(),
            namespaceId = "app/data",
            kind = CompactionKind.SSTABLE_LEVEL,
            level = 0,
            inputSegmentIds = inputs.toList(),
        )

    @Test
    fun sstableCompactionRefusesRatherThanDeletingItsInputs() =
        runTest {
            val shim = InMemoryPlatformIoShim()
            val ex =
                assertFailsWith<CompactionNotImplementedException> {
                    runSstableCompaction(shim, "app/data", job("seg-a", "seg-b"))
                }
            assertTrue(
                ex.message!!.contains("not implemented"),
                "the error should say plainly that it is not implemented: ${ex.message}",
            )
            assertTrue(
                ex.message!!.contains("2 input segment"),
                "the error should name how many segments it declined to destroy: ${ex.message}",
            )
        }

    /** The refusal must come before any I/O, so nothing is deleted on the way to failing. */
    @Test
    fun sstableCompactionTouchesNothingBeforeRefusing() =
        runTest {
            val shim = InMemoryPlatformIoShim()
            shim.appendToSegment("app/data/sstable/L0/seg-a", byteArrayOf(1, 2, 3))
            shim.sealSegment("app/data/sstable/L0/seg-a")
            val before = shim.listSegments("app/data")

            assertFailsWith<CompactionNotImplementedException> {
                runSstableCompaction(shim, "app/data", job("seg-a"))
            }

            assertEquals(before, shim.listSegments("app/data"), "a segment was touched before refusing")
        }

    @Test
    fun deltaSegmentRollRefuses() =
        runTest {
            assertFailsWith<CompactionNotImplementedException> { runDeltaSegmentRoll("app/data") }
        }

    @Test
    fun batchRefusesOnItsFirstJob() =
        runTest {
            val shim = InMemoryPlatformIoShim()
            assertFailsWith<CompactionNotImplementedException> {
                runCompactionBatch(shim, listOf(job("seg-a"), job("seg-b")))
            }
        }

    /** An empty batch has nothing to refuse, so it is simply empty. */
    @Test
    fun emptyBatchIsEmpty() =
        runTest {
            assertTrue(runCompactionBatch(InMemoryPlatformIoShim(), emptyList()).isEmpty())
        }

    /**
     * The planner schedules nothing for any tier. That is the current state, not an oversight of
     * this test: with it returning nothing, the unimplemented runners above are unreachable in
     * practice, which is the only reason the old stub never destroyed anyone's segments.
     */
    @Test
    fun defaultPlannerSchedulesNothingForAnyTier() {
        val planner = DefaultCompactionPlanner()
        for (tier in StorageTierHint.entries) {
            assertTrue(
                planner.plan("app/data", tier).isEmpty(),
                "planner scheduled work for $tier while the runners are unimplemented",
            )
        }
    }

    @Test
    fun jobAndResultCarryTheirFields() {
        val id = KdbUuid.random()
        val j = CompactionJob(id, "app/data", CompactionKind.DELTA_ROLL, level = 2,
            inputSegmentIds = listOf("a", "b"))
        assertEquals(id, j.jobId)
        assertEquals(CompactionKind.DELTA_ROLL, j.kind)
        assertEquals(2, j.level)
        assertEquals(listOf("a", "b"), j.inputSegmentIds)

        // The defaults are the ones a caller gets when it omits them.
        val minimal = CompactionJob(id, "app/data", CompactionKind.SSTABLE_LEVEL)
        assertEquals(0, minimal.level)
        assertTrue(minimal.inputSegmentIds.isEmpty())

        val r = CompactionResult(id, bytesRead = 10, bytesWritten = 20)
        assertEquals(10, r.bytesRead)
        assertEquals(20, r.bytesWritten)
        assertTrue(r.outputHandles.isEmpty())
    }
}
