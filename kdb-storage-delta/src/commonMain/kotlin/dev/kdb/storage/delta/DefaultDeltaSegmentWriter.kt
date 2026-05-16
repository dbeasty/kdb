package dev.kdb.storage.delta

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.compression.Crc32
import dev.kdb.compression.ZstdCompression
import dev.kdb.storage.CompressionCodec
import dev.kdb.storage.DeltaRecord
import dev.kdb.storage.DeltaSegmentReader
import dev.kdb.storage.DeltaSegmentRef
import dev.kdb.storage.DeltaSegmentWriter
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
        val zero = KdbHash.fromHex("0".repeat(64))
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
) : DeltaSegmentReader {
    override suspend fun readAll(segment: DeltaSegmentRef): List<DeltaRecord> = emptyList()

    override suspend fun readRange(
        segment: DeltaSegmentRef,
        sinceCommit: KdbHash,
        untilCommit: KdbHash,
    ): List<DeltaRecord> = emptyList()

    override suspend fun listSegments(): List<DeltaSegmentRef> = emptyList()
}

public class DeltaSegmentFactory(
    private val config: StorageEngineConfig,
) {
    public fun openWriter(namespaceId: String): DefaultDeltaSegmentWriter =
        DefaultDeltaSegmentWriter(namespaceId, KdbUuid.random(), config.ioShim, config)

    public fun openReader(namespaceId: String): DefaultDeltaSegmentReader =
        DefaultDeltaSegmentReader(namespaceId, config.ioShim)
}
