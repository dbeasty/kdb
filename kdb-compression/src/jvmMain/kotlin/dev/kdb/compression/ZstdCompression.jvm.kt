package dev.kdb.compression

import com.github.luben.zstd.Zstd

public actual object ZstdCompression {
    public actual fun compress(input: ByteArray, level: Int): ByteArray = Zstd.compress(input, level)

    public actual fun decompress(input: ByteArray, maxOutputSize: Int): ByteArray {
        val size = Zstd.decompressedSize(input).toInt()
        if (size > 0) {
            require(size <= maxOutputSize) { "decompressed size $size exceeds max $maxOutputSize" }
            return Zstd.decompress(input, size)
        }
        // The frame does not declare its content size. Perfectly legal zstd - streaming
        // encoders, and notably Go's klauspost EncodeAll (the other KDB implementation), emit
        // such frames - and every KDB on-disk format that stores compressed bytes records the
        // uncompressed length right next to them, which is exactly what maxOutputSize is.
        // Requiring the frame to duplicate that length made every Go-written segment look
        // corrupt here: the delta replayer then swallowed the whole segment as a "torn tail"
        // and a Go-written data directory opened as silently empty on the JVM.
        val out = ByteArray(maxOutputSize)
        val written = Zstd.decompress(out, input).toInt()
        require(written in 0..maxOutputSize) { "decompressed $written bytes, expected at most $maxOutputSize" }
        return if (written == out.size) out else out.copyOf(written)
    }
}
