package dev.kdb.compression

/** The pure-Kotlin codec behind ZstdCompression on Kotlin/JS and Kotlin/Native. */
internal object PureKotlinZstd {
    fun compress(input: ByteArray): ByteArray = ZstdEncoder.compress(input)

    fun decompress(input: ByteArray, maxOutputSize: Int): ByteArray {
        require(maxOutputSize >= 0) { "maxOutputSize must not be negative" }
        return ZstdDecoder.decompress(input, maxOutputSize)
    }
}
