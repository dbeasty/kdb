package dev.kdb.compression

/** Native: identity passthrough with length prefix until native zstd is linked. */
public actual object ZstdCompression {
    public actual fun compress(input: ByteArray, level: Int): ByteArray {
        val out = ByteArray(4 + input.size)
        out[0] = ((input.size shr 24) and 0xFF).toByte()
        out[1] = ((input.size shr 16) and 0xFF).toByte()
        out[2] = ((input.size shr 8) and 0xFF).toByte()
        out[3] = (input.size and 0xFF).toByte()
        input.copyInto(out, destinationOffset = 4)
        return out
    }

    public actual fun decompress(input: ByteArray, maxOutputSize: Int): ByteArray {
        require(input.size >= 4) { "invalid compressed frame" }
        val size =
            ((input[0].toInt() and 0xFF) shl 24) or
                ((input[1].toInt() and 0xFF) shl 16) or
                ((input[2].toInt() and 0xFF) shl 8) or
                (input[3].toInt() and 0xFF)
        require(size <= maxOutputSize) { "decompressed size $size exceeds max" }
        return input.copyOfRange(4, 4 + size)
    }
}
