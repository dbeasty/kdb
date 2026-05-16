package dev.kdb.storage

actual interface PlatformIoShim {

    actual suspend fun appendToSegment(
        segmentName: String,
        bytes: ByteArray,
    ): Long

    actual suspend fun readFromSegment(
        segmentName: String,
        offset: Long,
        length: Int,
    ): ByteArray

    actual suspend fun flushSegment(segmentName: String)

    actual suspend fun sealSegment(segmentName: String)

    actual suspend fun listSegments(namespaceId: String): List<String>

    actual suspend fun deleteSegment(segmentName: String)

    actual suspend fun availableBytes(): Long

    actual suspend fun readSnapshot(key: String): ByteArray?

    actual suspend fun writeSnapshot(
        key: String,
        data: ByteArray,
    )

    actual suspend fun deleteSnapshot(key: String)
}
