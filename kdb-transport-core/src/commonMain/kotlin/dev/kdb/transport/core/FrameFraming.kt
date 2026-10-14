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
    // The bytes received and not yet framed are buf[start, end). A byte array, not a list of
    // boxed bytes taken one at a time from the front: that made a frame of n bytes cost O(n^2),
    // so a multi-megabyte frame (a peer-sync blob page) never finished arriving.
    private var buf = ByteArray(0)
    private var start = 0
    private var end = 0

    public fun feed(chunk: ByteArray): List<ByteArray> = feed(chunk, 0, chunk.size)

    public fun feed(chunk: ByteArray, offset: Int, length: Int): List<ByteArray> {
        if (length > 0) {
            if (end + length > buf.size) {
                val live = end - start
                if (live + length > buf.size) {
                    val grown = ByteArray(maxOf(live + length, buf.size * 2, 8192))
                    buf.copyInto(grown, 0, start, end)
                    buf = grown
                } else {
                    buf.copyInto(buf, 0, start, end)
                }
                start = 0
                end = live
            }
            chunk.copyInto(buf, end, offset, offset + length)
            end += length
        }
        return drainCompleteFrames()
    }

    public fun reset() {
        start = 0
        end = 0
    }

    public val bufferedBytes: Int get() = end - start

    private fun drainCompleteFrames(): List<ByteArray> {
        val out = mutableListOf<ByteArray>()
        while (end - start >= 4) {
            val frameLength =
                try {
                    val len = readInt32Le(buf, start)
                    validateFrameLength(len, maxFrameBytes)
                    len
                } catch (e: FrameTooLargeException) {
                    reset()
                    throw e
                } catch (e: WireDecodeException) {
                    reset()
                    throw e
                }
            if (end - start < frameLength) break
            out += buf.copyOfRange(start, start + frameLength)
            start += frameLength
        }
        if (start == end) reset()
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
