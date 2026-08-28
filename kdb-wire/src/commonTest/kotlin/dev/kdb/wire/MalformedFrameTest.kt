package dev.kdb.wire

import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * A frame's length prefix is attacker-controlled: it arrives whole over WebSocket (unlike the
 * stream reader, nothing has checked it against the buffer it came in) and can be truncated in a
 * capture file read by tooling. Every malformed shape has to end as a [WireDecodeException] the
 * caller can handle, not as an IndexOutOfBoundsException out of copyOfRange.
 *
 * Mirrors go/kdb/wire/malformed_frame_test.go - the two decoders read the same bytes and must
 * reject the same inputs.
 */
class MalformedFrameTest {
    private val codec = defaultWireCodec()

    /**
     * A syntactically valid 12-byte header in a buffer of [bufLen] bytes, whose length prefix
     * declares [declaredLen]. When the two disagree the frame is malformed exactly the way a
     * hostile or corrupt peer produces.
     */
    private fun framePrefix(
        bufLen: Int,
        declaredLen: Int,
        typeCode: Int = WireMessageType.HANDSHAKE.code.toInt(),
    ): ByteArray {
        val buf = ByteArray(bufLen)
        writeInt32Le(buf, 0, declaredLen)
        if (bufLen >= 12) {
            writeInt16Le(buf, 4, typeCode)
            writeInt16Le(buf, 6, KDB_WIRE_PROTOCOL_VERSION)
            writeInt32Le(buf, 8, 7)
        }
        return buf
    }

    private fun writeInt32Le(
        buf: ByteArray,
        offset: Int,
        value: Int,
    ) {
        buf[offset] = (value and 0xFF).toByte()
        buf[offset + 1] = ((value shr 8) and 0xFF).toByte()
        buf[offset + 2] = ((value shr 16) and 0xFF).toByte()
        buf[offset + 3] = ((value shr 24) and 0xFF).toByte()
    }

    private fun writeInt16Le(
        buf: ByteArray,
        offset: Int,
        value: Int,
    ) {
        buf[offset] = (value and 0xFF).toByte()
        buf[offset + 1] = ((value shr 8) and 0xFF).toByte()
    }

    @Test
    fun decodeRejectsDeclaredLengthLongerThanBuffer() {
        val ex = assertFailsWith<WireDecodeException> { codec.decode(framePrefix(20, 1000)) }
        assertTrue(
            ex.message!!.contains("declared length"),
            "unexpected message: ${ex.message}",
        )
    }

    @Test
    fun decodeHeaderRejectsDeclaredLengthLongerThanBuffer() {
        assertFailsWith<WireDecodeException> { codec.decodeHeader(framePrefix(20, 1000)) }
        // One byte short is still short: the boundary is exact, not approximate.
        assertFailsWith<WireDecodeException> { codec.decodeHeader(framePrefix(19, 20)) }
    }

    @Test
    fun decodeHeaderAcceptsExactAndOverlongBuffers() {
        val header = codec.decodeHeader(framePrefix(13, 13))
        kotlin.test.assertEquals(1, header.payloadLength)
        // A buffer longer than its declared length is legal for decodeHeader - the frame readers
        // hand over exact buffers, but nothing about the header itself is wrong.
        codec.decodeHeader(framePrefix(64, 13))
    }

    @Test
    fun decodeRejectsEmptyDeclaredPayload() {
        assertFailsWith<WireDecodeException> { codec.decode(framePrefix(64, 12)) }
    }

    @Test
    fun decodeHeaderRejectsUnknownMessageType() {
        assertFailsWith<WireDecodeException> {
            codec.decodeHeader(framePrefix(32, 32, typeCode = 0x7FFF))
        }
    }

    /**
     * The floor on a declared frame length is the header's own size. This was 8 while
     * FRAME_HEADER_SIZE has been 12, so lengths 8..11 - which Go has always rejected - were
     * accepted here, and then produced a negative payload length downstream.
     */
    @Test
    fun validateFrameLengthFloorIsTheHeaderSize() {
        for (tooSmall in listOf(0, 1, 7, 8, 9, 10, 11)) {
            assertFailsWith<FrameTooLargeException>("length $tooSmall was accepted") {
                validateFrameLength(tooSmall)
            }
        }
        validateFrameLength(12) // the smallest frame that can exist: header, no payload
        assertFailsWith<FrameTooLargeException> {
            validateFrameLength(DEFAULT_MAX_FRAME_BYTES + 1)
        }
    }

    @Test
    fun decodeHeaderRejectsNegativeDeclaredLength() {
        assertFailsWith<FrameTooLargeException> { codec.decodeHeader(framePrefix(32, -1)) }
    }
}

/**
 * The commit count in a CommitPush payload is peer-controlled and was handed straight to
 * ArrayList as a capacity, so four bytes could ask for a two-billion-element backing array.
 * Mirrors go/kdb/wire's TestDecodeCommitsDoesNotAllocateFromDeclaredCount.
 */
class CommitPushCodecMalformedTest {
    @Test
    fun decodeCommitsRejectsCountLargerThanPayloadCanHold() {
        // count = 0x7FFFFFFF with no commit bodies behind it
        val claimsTwoBillion = byteArrayOf(0xff.toByte(), 0xff.toByte(), 0xff.toByte(), 0x7f)
        assertFailsWith<IllegalArgumentException> {
            CommitPushCodec.decodeCommits(claimsTwoBillion)
        }
    }

    @Test
    fun decodeCommitsRejectsNegativeCount() {
        // A length prefix with the high bit set reads back as a negative Int, which used to take
        // out ArrayList's own argument check rather than producing a decode error.
        val negative = byteArrayOf(0xff.toByte(), 0xff.toByte(), 0xff.toByte(), 0xff.toByte())
        assertFailsWith<IllegalArgumentException> { CommitPushCodec.decodeCommits(negative) }
    }

    @Test
    fun decodeCommitsAcceptsPayloadTooShortForACount() {
        // Fewer than four bytes is "no commits", not an error - matching Go's DecodeCommits.
        kotlin.test.assertEquals(0, CommitPushCodec.decodeCommits(byteArrayOf(1, 2)).size)
    }

    @Test
    fun decodeCommitsAcceptsAnEmptyList() {
        val encoded = CommitPushCodec.encodeCommits(emptyList())
        kotlin.test.assertEquals(0, CommitPushCodec.decodeCommits(encoded).size)
    }
}
