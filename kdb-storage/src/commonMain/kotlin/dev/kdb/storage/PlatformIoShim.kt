package dev.kdb.storage

/**
 * Platform I/O shim — the only expect/actual boundary advertised for the storage stack ([Component 9] §F).
 *
 * Implementations belong in Layer 4a (JVM/native I/O); for tests consider [dev.kdb.storage.mem.InMemoryPlatformIoShim] on JVM.
 */
public expect interface PlatformIoShim {

    public suspend fun appendToSegment(
        segmentName: String,
        bytes: ByteArray,
    ): Long

    public suspend fun readFromSegment(
        segmentName: String,
        offset: Long,
        length: Int,
    ): ByteArray

    public suspend fun flushSegment(segmentName: String)

    public suspend fun sealSegment(segmentName: String)

    public suspend fun listSegments(namespaceId: String): List<String>

    public suspend fun deleteSegment(segmentName: String)

    public suspend fun availableBytes(): Long

    public suspend fun readSnapshot(key: String): ByteArray?

    public suspend fun writeSnapshot(
        key: String,
        data: ByteArray,
    )

    public suspend fun deleteSnapshot(key: String)
}
