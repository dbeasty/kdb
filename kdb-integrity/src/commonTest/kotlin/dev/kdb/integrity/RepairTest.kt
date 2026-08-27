package dev.kdb.integrity

import dev.kdb.storage.CompressionCodec
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.test.runTest

class RepairTest {
    @Test
    fun tornTailTruncatesAndQuarantines() =
        runTest {
            val shim = newTestShim()
            val ns = "ns1"
            val commits = buildChain(2, ns)
            val full = rawFrame(commits[0])
            val torn = rawFrame(commits[1]).copyOfRange(0, 10)
            appendSegment(shim, ns, 0, full, torn)

            val opts = Options(Level.L1, CompressionCodec.NONE)
            val report = verify(shim, ns, opts)
            val result = repair(shim, report, opts)
            assertEquals(1, result.steps.size)
            assertEquals(Action.TRUNCATED_TORN_TAIL, result.steps[0].action)
            assertTrue(result.steps[0].quarantineName.isNotEmpty())

            val quarantined = shim.readFromSegment(result.steps[0].quarantineName, 0, 1 shl 20)
            assertTrue(quarantined.contentEquals(torn), "quarantined bytes must be exactly the removed torn tail")

            val report2 = verify(shim, ns, opts)
            assertTrue(report2.isClean, "expected clean report after repair, got ${report2.findings}")
            assertEquals(1, report2.segments[0].frameCount)
        }

    @Test
    fun repairIsIdempotent() =
        runTest {
            val shim = newTestShim()
            val ns = "ns1"
            val commits = buildChain(2, ns)
            appendSegment(shim, ns, 0, rawFrame(commits[0]), rawFrame(commits[1]).copyOfRange(0, 10))

            val opts = Options(Level.L1, CompressionCodec.NONE)
            repair(shim, verify(shim, ns, opts), opts)

            val result2 = repair(shim, verify(shim, ns, opts), opts)
            assertEquals(0, result2.steps.size, "expected no-op on an already-repaired report, got ${result2.steps}")
        }

    @Test
    fun repairMidLogRemovesOnlyProvenSafeFrame() =
        runTest {
            val shim = newTestShim()
            val ns = "ns1"
            val c0 = buildCommit(ns, null)
            val c1 = buildCommit(ns, c0.hash) // kept: precedes the corrupt frame in its segment
            val c1b = buildCommit(ns, c0.hash) // corrupted: a dead end nothing else references
            val c2 = buildCommit(ns, c0.hash) // independent, in a later segment

            appendSegment(shim, ns, 0, rawFrame(c0))
            appendSegment(shim, ns, 1, rawFrame(c1), flippedFrame(c1b))
            appendSegment(shim, ns, 2, rawFrame(c2))

            val opts = Options(Level.L1, CompressionCodec.NONE)
            val report = verify(shim, ns, opts)
            assertTrue(hasFinding(report, Classification.MID_LOG_CORRUPTION, 1), "expected mid_log_corruption at segment 1, got ${report.findings}")

            val result = repair(shim, report, opts)
            assertEquals(1, result.steps.size)
            assertEquals(Action.REWROTE_SEGMENT_PREFIX, result.steps[0].action)

            val report2 = verify(shim, ns, Options(Level.L2, CompressionCodec.NONE))
            assertTrue(report2.isClean, "expected clean report after safe repair, got ${report2.findings}")
        }

    @Test
    fun repairRefusesWhenClosureBreaks() =
        runTest {
            val shim = newTestShim()
            val ns = "ns1"
            val c0 = buildCommit(ns, null)
            val c1 = buildCommit(ns, c0.hash) // corrupted, alone in its segment
            val c2 = buildCommit(ns, c1.hash) // depends on the corrupted commit

            appendSegment(shim, ns, 0, rawFrame(c0))
            appendSegment(shim, ns, 1, flippedFrame(c1))
            appendSegment(shim, ns, 2, rawFrame(c2))

            val opts = Options(Level.L1, CompressionCodec.NONE)
            val report = verify(shim, ns, opts)

            val name = "ns/$ns/delta/00000000000000000001.seg"
            val before = shim.readFromSegment(name, 0, 1 shl 20)

            val result = repair(shim, report, opts)
            assertEquals(1, result.steps.size)
            assertEquals(Action.REFUSED, result.steps[0].action)
            assertEquals(listOf(c1.hash.toHex()), result.steps[0].missingHashes)

            val after = shim.readFromSegment(name, 0, 1 shl 20)
            assertTrue(before.contentEquals(after), "a refused repair must not touch the segment")
        }
}
