package dev.kdb.storage.delta

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.compression.Crc32
import dev.kdb.compression.ZstdCompression
import dev.kdb.storage.CompressionCodec
import dev.kdb.storage.DeltaAuthorshipEnvelope
import dev.kdb.storage.DeltaDebugHook
import dev.kdb.storage.DeltaRecord
import dev.kdb.storage.DeltaSegmentReader
import dev.kdb.storage.DeltaSegmentRef
import dev.kdb.storage.DeltaSegmentWriter
import dev.kdb.storage.NoOpDeltaDebugHook
import dev.kdb.storage.PlatformIoShim
import dev.kdb.storage.StorageEngineConfig
import dev.kdb.storage.io.SegmentNameBuilder
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

public class DefaultDeltaSegmentWriter(
    override val namespaceId: String,
    override val segmentId: KdbUuid,
    private val ioShim: PlatformIoShim,
    private val config: StorageEngineConfig,
    private val debugHook: DeltaDebugHook = NoOpDeltaDebugHook,
) : DeltaSegmentWriter {
    private val mutex = Mutex()
    private val segmentName = SegmentNameBuilder.delta(namespaceId, segmentId.toString())
    private var sizeBytes = 0L
    private var sealed = false
    private var firstCommit: KdbHash? = null
    private var lastCommit: KdbHash? = null

    override val currentSizeBytes: Long get() = sizeBytes
    override val isSealed: Boolean get() = sealed

    override suspend fun append(record: DeltaRecord): Long =
        mutex.withLock {
            check(!sealed) { "segment sealed" }
            if (firstCommit == null) firstCommit = record.commitHash
            lastCommit = record.commitHash
            val payload = record.commitPayload
            val frame = DeltaPageCodec.frame(payload, config.compressionCodec)
            val offset = sizeBytes
            sizeBytes = ioShim.appendToSegment(segmentName, frame)
            debugHook.onAppend(record, segmentId, offset)
            offset
        }

    override suspend fun flush() {
        ioShim.flushSegment(segmentName)
    }

    override suspend fun seal(): DeltaSegmentRef {
        mutex.withLock {
            check(!sealed)
            sealed = true
            ioShim.sealSegment(segmentName)
        }
        val zero = KdbHash.fromBytes(ByteArray(32))
        return DeltaSegmentRef(
            segmentId = segmentId,
            namespaceId = namespaceId,
            firstCommitHash = firstCommit ?: zero,
            lastCommitHash = lastCommit ?: zero,
            sizeBytes = sizeBytes,
            compressionCodec = config.compressionCodec,
        )
    }
}

internal object DeltaPageCodec {
    fun frame(payload: ByteArray, codec: CompressionCodec): ByteArray {
        val body =
            when (codec) {
                CompressionCodec.NONE -> payload
                CompressionCodec.ZSTD -> ZstdCompression.compress(payload)
            }
        val out = ByteArray(16 + body.size)
        out[0] = 0x4B; out[1] = 0x44; out[2] = 0x42; out[3] = 0x50
        writeInt(out, 4, body.size)
        writeInt(out, 8, payload.size)
        writeInt(out, 12, Crc32.of(body))
        body.copyInto(out, 16)
        return out
    }

    fun parse(frame: ByteArray, codec: CompressionCodec): ByteArray {
        val body = frame.copyOfRange(16, frame.size)
        return when (codec) {
            CompressionCodec.NONE -> body
            CompressionCodec.ZSTD -> ZstdCompression.decompress(body, readInt(frame, 8) + 1024)
        }
    }

    private fun writeInt(a: ByteArray, o: Int, v: Int) {
        a[o] = (v ushr 24).toByte(); a[o + 1] = (v ushr 16).toByte()
        a[o + 2] = (v ushr 8).toByte(); a[o + 3] = v.toByte()
    }

    private fun readInt(a: ByteArray, o: Int): Int =
        ((a[o].toInt() and 0xFF) shl 24) or ((a[o + 1].toInt() and 0xFF) shl 16) or
            ((a[o + 2].toInt() and 0xFF) shl 8) or (a[o + 3].toInt() and 0xFF)
}

public class DefaultDeltaSegmentReader(
    override val namespaceId: String,
    private val ioShim: PlatformIoShim,
    private val config: StorageEngineConfig,
) : DeltaSegmentReader {
    override suspend fun readAll(segment: DeltaSegmentRef): List<DeltaRecord> {
        val segmentName = SegmentNameBuilder.delta(segment.namespaceId, segment.segmentId.toString())
        val bytes = readFullSegment(segmentName, segment.sizeBytes)
        return DeltaSegmentScanner.scanSegmentBytes(bytes, segment.compressionCodec).map { scanned ->
            DeltaRecord(
                commitHash = scanned.commitHash,
                namespaceId = segment.namespaceId,
                authorship =
                    DeltaAuthorshipEnvelope(
                        principal = "unknown",
                        timestamp = scanned.commit.timestamp,
                        rightsToken = "",
                        clientContext = "",
                    ),
                commitPayload = scanned.commit.toPayloadBytes(),
                documentPatches = emptyList(),
            )
        }
    }

    override suspend fun readRange(
        segment: DeltaSegmentRef,
        sinceCommit: KdbHash,
        untilCommit: KdbHash,
    ): List<DeltaRecord> {
        val all = readAll(segment)
        var pastSince = false
        return all.filter { record ->
            if (record.commitHash == sinceCommit) pastSince = true
            pastSince && record.commitHash != untilCommit
        }
    }

    override suspend fun listSegments(): List<DeltaSegmentRef> {
        val names =
            ioShim.listSegments(namespaceId).filter {
                it.startsWith(SegmentNameBuilder.namespacePrefix(namespaceId) + "delta/")
            }
        return names.mapNotNull { name -> scanSegmentRef(name) }
    }

    private suspend fun readFullSegment(segmentName: String, sizeBytes: Long): ByteArray {
        if (sizeBytes <= 0) return byteArrayOf()
        val len = sizeBytes.coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
        return ioShim.readFromSegment(segmentName, 0, len)
    }

    private suspend fun scanSegmentRef(segmentName: String): DeltaSegmentRef? {
        val segmentIdStr = segmentName.substringAfterLast('/')
        val segmentId =
            try {
                KdbUuid.fromString(segmentIdStr)
            } catch (_: Exception) {
                return null
            }
        val bytes =
            try {
                readEntireSegment(segmentName)
            } catch (_: Exception) {
                return null
            }
        val scanned = DeltaSegmentScanner.scanSegmentBytes(bytes, config.compressionCodec)
        val zero = KdbHash.fromBytes(ByteArray(32))
        if (scanned.isEmpty()) {
            return DeltaSegmentRef(
                segmentId = segmentId,
                namespaceId = namespaceId,
                firstCommitHash = zero,
                lastCommitHash = zero,
                sizeBytes = bytes.size.toLong(),
                compressionCodec = config.compressionCodec,
            )
        }
        return DeltaSegmentRef(
            segmentId = segmentId,
            namespaceId = namespaceId,
            firstCommitHash = scanned.first().commitHash,
            lastCommitHash = scanned.last().commitHash,
            sizeBytes = bytes.size.toLong(),
            compressionCodec = config.compressionCodec,
        )
    }

    private suspend fun readEntireSegment(segmentName: String): ByteArray =
        ioShim.readFromSegment(segmentName, 0, Int.MAX_VALUE / 4)
}

public class DeltaSegmentFactory(
    private val config: StorageEngineConfig,
    private val debugHook: DeltaDebugHook = NoOpDeltaDebugHook,
) {
    public fun openWriter(namespaceId: String): DefaultDeltaSegmentWriter =
        DefaultDeltaSegmentWriter(namespaceId, KdbUuid.random(), config.ioShim, config, debugHook)

    public fun openReader(namespaceId: String): DefaultDeltaSegmentReader =
        DefaultDeltaSegmentReader(namespaceId, config.ioShim, config)
}
