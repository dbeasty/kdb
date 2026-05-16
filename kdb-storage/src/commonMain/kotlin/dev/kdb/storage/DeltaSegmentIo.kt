package dev.kdb.storage

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid

/** Append-only delta log writer ([Layer 4a]); interface contract only ([Component 9]). */
public interface DeltaSegmentWriter {

    val namespaceId: String
    val segmentId: KdbUuid

    public suspend fun append(record: DeltaRecord): Long

    public suspend fun flush()

    public suspend fun seal(): DeltaSegmentRef

    val currentSizeBytes: Long

    val isSealed: Boolean
}

/** Read sealed delta segments ([Layer 4a]). */
public interface DeltaSegmentReader {

    val namespaceId: String

    public suspend fun readAll(segment: DeltaSegmentRef): List<DeltaRecord>

    public suspend fun readRange(
        segment: DeltaSegmentRef,
        sinceCommit: KdbHash,
        untilCommit: KdbHash,
    ): List<DeltaRecord>

    public suspend fun listSegments(): List<DeltaSegmentRef>
}
