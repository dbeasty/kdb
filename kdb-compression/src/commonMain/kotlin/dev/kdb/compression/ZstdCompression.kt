package dev.kdb.compression

/**
 * Multiplatform ZSTD compress/decompress.
 *
 * The JVM uses zstd-jni (libzstd). Kotlin/JS and Kotlin/Native use the pure-Kotlin [ZstdDecoder]
 * and [ZstdEncoder] in commonMain (docs/kdb-open-issues-plan.md, roadmap R1): the decoder reads
 * every frame Go and the JVM write; the encoder writes real zstd frames that Go and the JVM read,
 * at a lower ratio than libzstd (raw literals, predefined tables - see [ZstdEncoder]).
 *
 * Writers of on-disk formats that carry a codec byte (SSTable blocks, delta pages, ice bundles)
 * must check [canCompress] and record "no compression" when it is false; readers check
 * [canDecompress]. Both are true on every current target. They are separate because the two
 * directions are independent: a target can read zstd without being able to write it, and before
 * R1 the JS and Native builds tagged length-prefixed copies as zstd, which nothing could read.
 */
public expect object ZstdCompression {
    /** True when [compress] writes real zstd frames on this platform. */
    public val canCompress: Boolean

    /** True when [decompress] reads real zstd frames on this platform. */
    public val canDecompress: Boolean

    /** [level] is honoured by zstd-jni on the JVM; the pure-Kotlin encoder has one level. */
    public fun compress(input: ByteArray, level: Int = 3): ByteArray

    /**
     * Decodes one or more concatenated zstd frames (an empty [input] decodes to empty output: Go's
     * klauspost EncodeAll emits zero bytes for an empty payload). Throws [ZstdFormatException] for
     * data that is not valid zstd - truncated, corrupted, failing its checksum or declared size -
     * or that decodes to more than [maxOutputSize] bytes.
     */
    public fun decompress(input: ByteArray, maxOutputSize: Int = 64 * 1024 * 1024): ByteArray
}

/**
 * [ZstdCompression.decompress] was given bytes it cannot decode to a result it can vouch for. An
 * [IllegalArgumentException], so callers that already treat a bad block as bad input keep doing so.
 */
public class ZstdFormatException(message: String, cause: Throwable? = null) : IllegalArgumentException(message, cause)
