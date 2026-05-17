package dev.kdb.transport.ws

import dev.kdb.error.TransportException
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.security.SecureRandom
import java.util.Base64

internal object WebSocketFraming {
    fun readHttpHeaders(input: BufferedInputStream): HttpHeaderBlock {
        val lines = mutableListOf<String>()
        val buf = StringBuilder()
        while (true) {
            val b = input.read()
            if (b == -1) break
            if (b == '\r'.code) {
                val next = input.read()
                if (next == '\n'.code) {
                    if (buf.isEmpty()) break
                    lines += buf.toString()
                    buf.clear()
                }
            } else {
                buf.append(b.toChar())
            }
        }
        return HttpHeaderBlock(lines)
    }

    fun websocketAccept(key: String): String {
        val digest = MessageDigest.getInstance("SHA-1")
        digest.update((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").toByteArray(StandardCharsets.US_ASCII))
        return Base64.getEncoder().encodeToString(digest.digest())
    }

    fun readFrame(
        input: BufferedInputStream,
        output: BufferedOutputStream,
    ): ByteArray? {
        while (true) {
            val b0 = input.read()
            if (b0 == -1) return null
            val opcode = b0 and 0x0F
            if (opcode == 0x8) return null
            val b1 = input.read()
            if (b1 == -1) return null
            val masked = (b1 and 0x80) != 0
            var len = (b1 and 0x7F).toLong()
            when (len) {
                126L -> len = ((input.read() shl 8) or input.read()).toLong()
                127L -> {
                    len = 0
                    repeat(8) { len = (len shl 8) or input.read().toLong() }
                }
            }
            val mask = if (masked) ByteArray(4) { input.read().toByte() } else null
            val data = ByteArray(len.toInt())
            var read = 0
            while (read < data.size) {
                val n = input.read(data, read, data.size - read)
                if (n == -1) return null
                read += n
            }
            if (mask != null) {
                for (i in data.indices) {
                    data[i] = (data[i].toInt() xor mask[i % 4].toInt()).toByte()
                }
            }
            when (opcode) {
                0x2 -> return data
                0x9 -> {
                    writeControlFrame(output, 0xA, data)
                    continue
                }
                0xA -> continue
                else -> throw TransportException("unsupported WebSocket opcode $opcode in v1")
            }
        }
    }

    fun writeBinaryFrame(
        output: BufferedOutputStream,
        payload: ByteArray,
        maskOutbound: Boolean,
    ) {
        output.write(0x82)
        val maskKey = if (maskOutbound) ByteArray(4).also { SecureRandom().nextBytes(it) } else null
        writePayloadLength(output, payload.size, maskOutbound)
        if (maskKey != null) {
            output.write(maskKey)
            for (i in payload.indices) {
                output.write(payload[i].toInt() xor maskKey[i % 4].toInt())
            }
        } else {
            output.write(payload)
        }
    }

    fun writeControlFrame(
        output: BufferedOutputStream,
        opcode: Int,
        payload: ByteArray,
    ) {
        output.write(0x80 or opcode)
        writePayloadLength(output, payload.size, masked = false)
        output.write(payload)
        output.flush()
    }

    private fun writePayloadLength(
        output: BufferedOutputStream,
        size: Int,
        masked: Boolean,
    ) {
        val maskBit = if (masked) 0x80 else 0
        when {
            size < 126 -> output.write(maskBit or size)
            size < 65536 -> {
                output.write(maskBit or 126)
                output.write((size shr 8) and 0xFF)
                output.write(size and 0xFF)
            }
            else -> {
                output.write(maskBit or 127)
                val len = size.toLong()
                for (i in 7 downTo 0) {
                    output.write(((len shr (8 * i)) and 0xFF).toInt())
                }
            }
        }
    }
}

internal class HttpHeaderBlock(
    val lines: List<String>,
) {
    val headers: Map<String, String> =
        lines.drop(1)
            .mapNotNull { line ->
                val idx = line.indexOf(':')
                if (idx <= 0) null else line.substring(0, idx).trim().lowercase() to line.substring(idx + 1).trim()
            }.toMap()

    fun line(index: Int): String = lines[index]
}
