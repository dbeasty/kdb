package dev.kdb.integrity

import dev.kdb.storage.CompressionCodec
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlinx.coroutines.test.runTest

class VerifyTest {
    @Test
    fun cleanLogPassesAllLevels() =
        runTest {
            val shim = newTestShim()
            val ns = "ns1"
            val commits = buildChain(3, ns)
            appendSegment(shim, ns, 0, rawFrame(commits[0]), rawFrame(commits[1]), rawFrame(commits[2]))

            val report = verify(shim, ns, Options(Level.L2, CompressionCodec.NONE))
            assertTrue(report.isClean, "expected clean report, got ${report.findings}")
            assertEquals(1, report.segments.size)
            assertEquals(3, report.segments[0].frameCount)
        }

    @Test
    fun detectsMidLogCorruption() =
        runTest {
            val shim = newTestShim()
            val ns = "ns1"
            val commits = buildChain(3, ns) // c0 <- c1 <- c2
            appendSegment(shim, ns, 0, rawFrame(commits[0]), flippedFrame(commits[1]))
            appendSegment(shim, ns, 1, rawFrame(commits[2]))

            val report = verify(shim, ns, Options(Level.L2, CompressionCodec.NONE))
            assertFalse(report.isClean)
            assertTrue(hasFinding(report, Classification.MID_LOG_CORRUPTION, 0), "expected mid_log_corruption at segment 0, got ${report.findings}")
            assertTrue(
                hasFindingWithHash(report, Classification.MISSING_PARENT, commits[1].hash.toHex()),
                "expected missing_parent for ${commits[1].hash.toHex()}, got ${report.findings}",
            )
        }

    @Test
    fun crcMismatchOnLastSegmentIsTornTail() =
        runTest {
            val shim = newTestShim()
            val ns = "ns1"
            val commits = buildChain(2, ns)
            // Single segment, so any corruption in it is corruption in the last segment -
            // kdb-spec-layer13 §4.3 classifies this as a torn tail regardless of *why* the CRC
            // failed, not just outright truncation.
            appendSegment(shim, ns, 0, rawFrame(commits[0]), flippedFrame(commits[1]))

            val report = verify(shim, ns, Options(Level.L1, CompressionCodec.NONE))
            assertEquals(1, report.findings.size)
            assertEquals(Classification.TORN_TAIL, report.findings[0].classification)
        }

    @Test
    fun detectsTornTail() =
        runTest {
            val shim = newTestShim()
            val ns = "ns1"
            val commits = buildChain(2, ns)
            val full = rawFrame(commits[0])
            val torn = rawFrame(commits[1]).copyOfRange(0, 5) // cut short: declared length no longer fits
            appendSegment(shim, ns, 0, full, torn)

            val report = verify(shim, ns, Options(Level.L2, CompressionCodec.NONE))
            assertEquals(1, report.findings.size)
            val f = report.findings[0]
            assertEquals(Classification.TORN_TAIL, f.classification)
            assertEquals(full.size, f.offset)
            assertEquals(1, report.segments[0].frameCount)
        }

    @Test
    fun detectsSequenceGap() =
        runTest {
            val shim = newTestShim()
            val ns = "ns1"
            val commits = buildChain(2, ns)
            appendSegment(shim, ns, 0, rawFrame(commits[0]))
            appendSegment(shim, ns, 2, rawFrame(commits[1])) // sequence 1 is missing

            val report = verify(shim, ns, Options(Level.L1, CompressionCodec.NONE))
            assertTrue(hasFinding(report, Classification.SEQUENCE_GAP, 2), "expected sequence_gap at segment 2, got ${report.findings}")
        }

    @Test
    fun neverMutatesOnDisk() =
        runTest {
            val shim = newTestShim()
            val ns = "ns1"
            val commits = buildChain(2, ns)
            appendSegment(shim, ns, 0, rawFrame(commits[0]), rawFrame(commits[1]))

            val name = "ns/$ns/delta/00000000000000000000.seg"
            val before = shim.readFromSegment(name, 0, 1 shl 20)
            verify(shim, ns, Options(Level.L2, CompressionCodec.NONE))
            val after = shim.readFromSegment(name, 0, 1 shl 20)
            assertTrue(before.contentEquals(after), "verify must not mutate segment bytes")
        }
}

internal fun hasFinding(report: Report, classification: Classification, segment: Long): Boolean =
    report.findings.any { it.classification == classification && it.segment == segment }

internal fun hasFindingWithHash(report: Report, classification: Classification, hash: String): Boolean =
    report.findings.any { it.classification == classification && it.commitHash == hash }
