package dev.kdb.tier

import dev.kdb.codec.KdbUuid
import dev.kdb.storage.DeltaSegmentRef
import dev.kdb.storage.PlatformIoShim
import dev.kdb.storage.io.SegmentNameBuilder

/** Reads/deletes the raw sealed segment file for a HOT segment — same naming convention the delta log writer uses. */
internal object HotSegmentAccess {
    suspend fun readBytes(
        ioShim: PlatformIoShim,
        namespaceId: String,
        ref: DeltaSegmentRef,
    ): ByteArray {
        val name = SegmentNameBuilder.delta(namespaceId, ref.segmentId.toString())
        return ioShim.readFromSegment(name, offset = 0, length = ref.sizeBytes.toInt())
    }

    suspend fun delete(
        ioShim: PlatformIoShim,
        namespaceId: String,
        segmentId: KdbUuid,
    ) {
        ioShim.deleteSegment(SegmentNameBuilder.delta(namespaceId, segmentId.toString()))
    }
}
