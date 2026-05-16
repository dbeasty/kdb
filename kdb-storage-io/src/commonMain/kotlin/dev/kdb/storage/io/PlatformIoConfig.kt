package dev.kdb.storage.io

public data class PlatformIoConfig(
    val rootDirectory: String? = null,
    val fsyncOnFlush: Boolean = true,
    val maxAppendBytes: Int = 16 * 1024 * 1024,
)

public data class SegmentHealthReport(
    val segmentName: String,
    val sizeBytes: Long,
    val readable: Boolean,
    val error: String? = null,
)
