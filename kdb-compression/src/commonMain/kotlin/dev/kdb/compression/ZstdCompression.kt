package dev.kdb.compression

/**
 * Multiplatform ZSTD compress/decompress.
 *
 * Only the JVM has a real zstd (zstd-jni). On JS and Native no codec is linked yet
 * (docs/kdb-open-issues-plan.md, roadmap R1), so [isAvailable] is false there and both
 * [compress] and [decompress] throw [ZstdUnavailableException]. Writers of on-disk formats that
 * carry a codec byte (SSTable blocks, delta pages) must check [isAvailable] and record "no
 * compression" instead of calling [compress] when it is false.
 *
 * These targets used to "compress" by prefixing the raw bytes with a 4-byte length. The writers
 * tagged that output as ZSTD, so every block a JS or Native build wrote claimed to be zstd and
 * could not be read by Go or the JVM. A real zstd frame from Go or the JVM was misread here as a
 * length prefix. Failing loudly is correct in both directions.
 */
public expect object ZstdCompression {
    /** True when this platform has a real zstd codec. False on JS and Native. */
    public val isAvailable: Boolean

    public fun compress(input: ByteArray, level: Int = 3): ByteArray

    public fun decompress(input: ByteArray, maxOutputSize: Int = 64 * 1024 * 1024): ByteArray
}

/** Thrown by [ZstdCompression] on a platform where [ZstdCompression.isAvailable] is false. */
public class ZstdUnavailableException(platform: String) :
    UnsupportedOperationException(
        "zstd is not supported on this platform ($platform): this build cannot read zstd-compressed " +
            "data written by the JVM or Go, and writes uncompressed blocks instead",
    )
