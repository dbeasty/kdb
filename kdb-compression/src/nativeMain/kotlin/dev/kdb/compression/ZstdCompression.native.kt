package dev.kdb.compression

/** Native: no zstd codec is linked yet (roadmap R1). See [ZstdCompression]. */
public actual object ZstdCompression {
    public actual val isAvailable: Boolean = false

    public actual fun compress(input: ByteArray, level: Int): ByteArray =
        throw ZstdUnavailableException("Kotlin/Native")

    public actual fun decompress(input: ByteArray, maxOutputSize: Int): ByteArray =
        throw ZstdUnavailableException("Kotlin/Native")
}
