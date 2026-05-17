package dev.kdb.transport.core

import dev.kdb.error.ConnectionClosedException
import dev.kdb.wire.FrameTooLargeException
import dev.kdb.wire.WireDecodeException
import dev.kdb.wire.validateFrameLength
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class FrameStreamReaderTest {
  private fun frame(payloadSize: Int): ByteArray {
        val total = 12 + payloadSize
        validateFrameLength(total)
        val out = ByteArray(total)
        out[0] = (total and 0xFF).toByte()
        out[1] = ((total shr 8) and 0xFF).toByte()
        out[2] = ((total shr 16) and 0xFF).toByte()
        out[3] = ((total shr 24) and 0xFF).toByte()
        return out
    }

    @Test
    fun framer_singleFrame() {
        val f = frame(4)
        val reader = FrameStreamReader()
        val out = reader.feed(f)
        assertEquals(1, out.size)
        assertTrue(out[0].contentEquals(f))
    }

    @Test
    fun framer_splitHeader() {
        val f = frame(0)
        val reader = FrameStreamReader()
        assertTrue(reader.feed(f.copyOfRange(0, 2)).isEmpty())
        val out = reader.feed(f.copyOfRange(2, f.size))
        assertEquals(1, out.size)
    }

    @Test
    fun framer_backToBack() {
        val f1 = frame(0)
        val f2 = frame(8)
        val reader = FrameStreamReader()
        val combined = f1 + f2
        val out = reader.feed(combined)
        assertEquals(2, out.size)
        assertTrue(out[0].contentEquals(f1))
        assertTrue(out[1].contentEquals(f2))
    }

    @Test
    fun rejectZeroLength() {
        val reader = FrameStreamReader()
        assertFailsWith<FrameTooLargeException> {
            reader.feed(byteArrayOf(0, 0, 0, 0))
        }
    }

    @Test
    fun rejectOversizedLength() {
        val reader = FrameStreamReader()
        val bad = ByteArray(4)
        val huge = 20 * 1024 * 1024
        bad[0] = (huge and 0xFF).toByte()
        bad[1] = ((huge shr 8) and 0xFF).toByte()
        bad[2] = ((huge shr 16) and 0xFF).toByte()
        bad[3] = ((huge shr 24) and 0xFF).toByte()
        assertFailsWith<FrameTooLargeException> {
            reader.feed(bad)
        }
    }

    @Test
    fun eofMidFrame() =
        runTest {
            val partial = byteArrayOf(12, 0, 0, 0)
            var fed = false
            assertFailsWith<ConnectionClosedException> {
                FrameFramer.readFrame(16 * 1024 * 1024) {
                    if (!fed) {
                        fed = true
                        partial
                    } else {
                        null
                    }
                }
            }
        }

    @Test
    fun validateOutgoing_rejectsMismatch() {
        val f = frame(0)
        f[0] = 99
        assertFailsWith<IllegalArgumentException> {
            FrameStreamWriter.validateOutgoing(f, 16 * 1024 * 1024)
        }
    }

    @Test
    fun readFrameViaFramer() =
        runTest {
            val f = frame(2)
            var idx = 0
            val result =
                FrameFramer.readFrame(16 * 1024 * 1024) {
                    if (idx >= f.size) null
                    else {
                        val chunk = f.copyOfRange(idx, minOf(idx + 3, f.size))
                        idx += chunk.size
                        chunk
                    }
                }
            assertTrue(result != null)
            assertTrue(result!!.contentEquals(f))
        }

    @Test
    fun readFrameCleanEof() =
        runTest {
            assertNull(FrameFramer.readFrame(16 * 1024 * 1024) { null })
        }
}
