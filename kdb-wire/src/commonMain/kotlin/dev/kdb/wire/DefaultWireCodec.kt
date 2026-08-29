package dev.kdb.wire

import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

internal class DefaultWireCodec(override val encoding: PayloadEncoding) : WireCodec {
    private val json = Json { ignoreUnknownKeys = true }

    override fun encode(message: WireMessage): ByteArray {
        val body = encodePayloadBody(message)
        val header =
            WireHeader(
                messageType = message.header.messageType,
                protocolVersion = message.header.protocolVersion,
                correlationId = message.header.correlationId,
                payloadLength = 1 + body.size,
            )
        return encodeFrameOnly(header, byteArrayOf(encodingTag(encoding)) + body)
    }

    override fun decode(frame: ByteArray): WireMessage {
        val header = decodeHeader(frame)
        val payloadOffset = FRAME_HEADER_SIZE
        if (frame.size < payloadOffset + 1) {
            throw WireDecodeException("frame too short for payload")
        }
        // A frame declaring an empty payload while carrying trailing bytes gets past the check
        // above and would ask copyOfRange for a range whose start is past its end.
        if (header.payloadLength < 1) {
            throw WireDecodeException("frame declares an empty payload")
        }
        val tag = frame[payloadOffset].toInt()
        val body = frame.copyOfRange(payloadOffset + 1, payloadOffset + header.payloadLength)
        // v1: KDB_BINARY (0) and JSON (1) both use UTF-8 JSON envelope for debug/interop.
        if (tag != 0 && tag != 1) {
            throw WireDecodeException("unsupported encoding tag: $tag")
        }
        val env = json.decodeFromString<WirePayloadEnvelope>(body.decodeToString())
        return env.toMessage(header)
    }

    override fun encodeFrameOnly(header: WireHeader, payload: ByteArray): ByteArray {
        val totalLength = FRAME_HEADER_SIZE + payload.size
        validateFrameLength(totalLength, DEFAULT_MAX_FRAME_BYTES)
        val out = ByteArray(totalLength)
        writeInt32Le(out, 0, totalLength)
        writeInt16Le(out, 4, header.messageType.code.toInt())
        writeInt16Le(out, 6, header.protocolVersion)
        writeInt32Le(out, 8, header.correlationId)
        payload.copyInto(out, FRAME_HEADER_SIZE)
        return out
    }

    override fun decodeHeader(frame: ByteArray): WireHeader {
        if (frame.size < FRAME_HEADER_SIZE) {
            throw WireDecodeException("frame shorter than header")
        }
        val frameLength = readInt32Le(frame, 0)
        validateFrameLength(frameLength, DEFAULT_MAX_FRAME_BYTES)
        // The declared length has to be checked against the buffer we were actually handed, not
        // only against the protocol maximum. payloadLength is derived from frameLength and
        // decode() slices the payload out with it, so a frame whose prefix claims more bytes
        // than it carries threw IndexOutOfBoundsException out of copyOfRange instead of a
        // WireDecodeException the caller can handle. The stream framing reader never produces
        // such a buffer (it only emits a frame once frameLength bytes have arrived), but a
        // WebSocket message is delivered whole and unvalidated, and a captured frame read from a
        // file can be truncated by whatever wrote it. Go's DecodeHeader enforces the same bound.
        if (frame.size < frameLength) {
            throw WireDecodeException("frame shorter than its declared length")
        }
        val typeCode = readInt16Le(frame, 4).toShort()
        val messageType =
            WireMessageType.fromCode(typeCode)
                ?: throw WireDecodeException("unknown message type: $typeCode")
        val protocolVersion = readInt16Le(frame, 6)
        val correlationId = readInt32Le(frame, 8)
        val payloadLength = frameLength - FRAME_HEADER_SIZE
        return WireHeader(messageType, protocolVersion, correlationId, payloadLength)
    }

    private fun encodePayloadBody(message: WireMessage): ByteArray {
        val env = message.toEnvelope()
        return json.encodeToString(env).encodeToByteArray()
    }

    private fun encodingTag(enc: PayloadEncoding): Byte =
        when (enc) {
            PayloadEncoding.KDB_BINARY -> 0
            PayloadEncoding.JSON -> 1
        }
}

internal const val FRAME_HEADER_SIZE: Int = 12

private fun writeInt32Le(buf: ByteArray, offset: Int, value: Int) {
    buf[offset] = (value and 0xFF).toByte()
    buf[offset + 1] = ((value shr 8) and 0xFF).toByte()
    buf[offset + 2] = ((value shr 16) and 0xFF).toByte()
    buf[offset + 3] = ((value shr 24) and 0xFF).toByte()
}

private fun writeInt16Le(buf: ByteArray, offset: Int, value: Int) {
    buf[offset] = (value and 0xFF).toByte()
    buf[offset + 1] = ((value shr 8) and 0xFF).toByte()
}

private fun readInt32Le(buf: ByteArray, offset: Int): Int =
    (buf[offset].toInt() and 0xFF) or
        ((buf[offset + 1].toInt() and 0xFF) shl 8) or
        ((buf[offset + 2].toInt() and 0xFF) shl 16) or
        ((buf[offset + 3].toInt() and 0xFF) shl 24)

private fun readInt16Le(buf: ByteArray, offset: Int): Int =
    (buf[offset].toInt() and 0xFF) or ((buf[offset + 1].toInt() and 0xFF) shl 8)
