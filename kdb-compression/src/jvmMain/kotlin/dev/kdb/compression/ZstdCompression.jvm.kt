package dev.kdb.compression

import com.github.luben.zstd.Zstd

public actual object ZstdCompression {
    public actual fun compress(input: ByteArray, level: Int): ByteArray = Zstd.compress(input, level)

    public actual fun decompress(input: ByteArray, maxOutputSize: Int): ByteArray {
        val size = Zstd.decompressedSize(input).toInt()
        require(size >= 0 && size <= maxOutputSize) { "decompressed size $size exceeds max $maxOutputSize" }
        return Zstd.decompress(input, size)
    }
}
