package dev.kdb.compression

/** Kotlin/JS: the pure-Kotlin codec in commonMain (roadmap R1). See [ZstdCompression]. */
public actual object ZstdCompression {
    public actual val canCompress: Boolean = true
    public actual val canDecompress: Boolean = true

    public actual fun compress(input: ByteArray, level: Int): ByteArray = PureKotlinZstd.compress(input)

    public actual fun decompress(input: ByteArray, maxOutputSize: Int): ByteArray =
        PureKotlinZstd.decompress(input, maxOutputSize)
}
