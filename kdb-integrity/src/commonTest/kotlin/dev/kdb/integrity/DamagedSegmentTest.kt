package dev.kdb.integrity

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.test.runTest

/**
 * N5: verify and repair apply the rule replay does. A torn tail is bytes at the very end of a
 * segment with nothing intact after them; damage that intact frames follow is mid-log corruption
 * even in the newest segment; zero padding after the last frame is no finding. Mirrors
 * go/kdb/integrity/damaged_segment_test.go.
 */
class DamagedSegmentTest {
    private fun garbledHeader(f: ByteArray): ByteArray = f.copyOf().also { it[0] = (it[0].toInt() xor 0xFF).toByte() }

    @Test
    fun damageWithIntactFramesAfterItIsNeverATornTail() =
        runTest {
            for (kind in listOf("crc", "magic")) {
                val shim = newTestShim()
                val ns = "ns1"
                val commits = buildChain(3, ns)
                val damaged = if (kind == "magic") garbledHeader(rawFrame(commits[1])) else flippedFrame(commits[1])
                appendSegment(shim, ns, 0, rawFrame(commits[0]), damaged, rawFrame(commits[2]))

                val report = verify(shim, ns, Options(Level.L1))
                assertEquals(1, report.findings.size, "$kind: ${report.findings}")
                val f = report.findings[0]
                assertEquals(Classification.MID_LOG_CORRUPTION, f.classification, "$kind: $f")
                assertEquals(rawFrame(commits[0]).size, f.offset, "$kind: $f")
                assertTrue(f.detail.contains("intact frames follow"), "$kind: ${f.detail}")
            }
        }

    @Test
    fun zeroPaddingAfterTheLastFrameIsClean() =
        runTest {
            val shim = newTestShim()
            val ns = "ns1"
            val commits = buildChain(4, ns)
            val pad = ByteArray(64 shl 10)
            appendSegment(shim, ns, 0, rawFrame(commits[0]), rawFrame(commits[1]), pad)
            appendSegment(shim, ns, 1, rawFrame(commits[2]), rawFrame(commits[3]), pad)

            val report = verify(shim, ns, Options(Level.L2))
            assertTrue(report.isClean, "a preallocated segment's zero padding is not damage: ${report.findings}")
        }

    @Test
    fun repairRefusesToDropIntactCommitsAfterDamage() =
        runTest {
            for (kind in listOf("crc", "magic")) {
                val shim = newTestShim()
                val ns = "ns1"
                val commits = buildChain(3, ns)
                val damaged = if (kind == "magic") garbledHeader(rawFrame(commits[1])) else flippedFrame(commits[1])
                appendSegment(shim, ns, 0, rawFrame(commits[0]), damaged, rawFrame(commits[2]))
                val name = "ns/$ns/delta/00000000000000000000.seg"
                val before = shim.readFromSegment(name, 0, 1 shl 20)

                val opts = Options(Level.L1)
                val result = repair(shim, verify(shim, ns, opts), opts)
                assertTrue(result.steps.any { it.action == Action.REFUSED }, "$kind: ${result.steps}")
                assertTrue(
                    result.steps.any { commits[2].hash.toHex() in it.missingHashes },
                    "$kind: the refusal should name the commit it would drop: ${result.steps}",
                )
                assertTrue(before.contentEquals(shim.readFromSegment(name, 0, 1 shl 20)), "$kind: a refused repair must not touch the segment")
            }
        }
}
