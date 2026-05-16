package dev.kdb.compression

/** Multiplatform ZSTD compress/decompress (JVM uses zstd-jni; other targets use identity wrapper). */
public expect object ZstdCompression {
    public fun compress(input: ByteArray, level: Int = 3): ByteArray
    public fun decompress(input: ByteArray, maxOutputSize: Int = 64 * 1024 * 1024): ByteArray
}
