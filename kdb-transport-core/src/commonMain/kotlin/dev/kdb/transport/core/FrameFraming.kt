package dev.kdb.transport.core

import dev.kdb.error.ConnectionClosedException
import dev.kdb.wire.FrameTooLargeException
import dev.kdb.wire.WireDecodeException
import dev.kdb.wire.validateFrameLength

internal fun readInt32Le(buf: ByteArray, offset: Int): Int =
    (buf[offset].toInt() and 0xFF) or
        ((buf[offset + 1].toInt() and 0xFF) shl 8) or
        ((buf[offset + 2].toInt() and 0xFF) shl 16) or
        ((buf[offset + 3].toInt() and 0xFF) shl 24)

/**
 * Reassembles length-prefixed wire frames from a byte stream (TCP/WebSocket binary).
 * [frameLength] is the total frame size including the 4-byte LE length prefix.
 */
public class FrameStreamReader(
    private val maxFrameBytes: Int = dev.kdb.wire.DEFAULT_MAX_FRAME_BYTES,
) {
    private val pending = mutableListOf<Byte>()

    public fun feed(chunk: ByteArray): List<ByteArray> {
        if (chunk.isNotEmpty()) {
            pending.addAll(chunk.toList())
        }
        return drainCompleteFrames()
    }

    public fun feed(chunk: ByteArray, offset: Int, length: Int): List<ByteArray> {
        if (length > 0) {
            for (i in offset until offset + length) {
                pending.add(chunk[i])
            }
        }
        return drainCompleteFrames()
    }

    public fun reset() {
        pending.clear()
    }

    public val bufferedBytes: Int get() = pending.size

    private fun drainCompleteFrames(): List<ByteArray> {
        val out = mutableListOf<ByteArray>()
        while (true) {
            if (pending.size < 4) break
            val header = pending.take(4).toByteArray()
            val frameLength =
                try {
                    val len = readInt32Le(header, 0)
                    validateFrameLength(len, maxFrameBytes)
                    len
                } catch (e: FrameTooLargeException) {
                    pending.clear()
                    throw e
                } catch (e: WireDecodeException) {
                    pending.clear()
                    throw e
                }
            if (pending.size < frameLength) break
            val frame = ByteArray(frameLength)
            for (i in 0 until frameLength) {
                frame[i] = pending.removeAt(0)
            }
            out += frame
        }
        return out
    }
}

public object FrameStreamWriter {
    public fun validateOutgoing(frame: ByteArray, maxFrameBytes: Int) {
        require(frame.size >= 4) { "frame shorter than length prefix" }
        val length = readInt32Le(frame, 0)
        validateFrameLength(length, maxFrameBytes)
        require(length == frame.size) { "frame length prefix $length does not match buffer size ${frame.size}" }
    }
}

public object FrameFramer {
    /**
     * Reads one complete frame from [readChunk]. Returns null at clean EOF with no partial frame.
     */
    public suspend fun readFrame(
        maxFrameBytes: Int,
        readChunk: suspend () -> ByteArray?,
    ): ByteArray? {
        val reader = FrameStreamReader(maxFrameBytes)
        while (true) {
            val frames = reader.feed(ByteArray(0))
            if (frames.isNotEmpty()) return frames.first()
            val chunk = readChunk() ?: break
            if (chunk.isEmpty()) break
            val complete = reader.feed(chunk)
            if (complete.isNotEmpty()) return complete.first()
        }
        if (reader.bufferedBytes > 0) {
            throw ConnectionClosedException("EOF before full frame (${reader.bufferedBytes} bytes buffered)")
        }
        return null
    }
}
