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
    /**
     * This segment's position in namespace-wide commit order - see
     * [DeltaSegmentRef.sequenceNumber]'s doc comment. Determines the
     * segment's file name (and therefore its replay order), not
     * [segmentId]. Callers should obtain this from
     * [DeltaSegmentFactory.openWriter], which assigns it by scanning
     * existing segments for this namespace.
     */
    private val sequenceNumber: Long,
    private val ioShim: PlatformIoShim,
    private val config: StorageEngineConfig,
    private val debugHook: DeltaDebugHook = NoOpDeltaDebugHook,
) : DeltaSegmentWriter {
    private val mutex = Mutex()
    private val segmentName = SegmentNameBuilder.deltaSequenced(namespaceId, sequenceNumber)
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
            sequenceNumber = sequenceNumber,
        )
    }
}

/**
 * KDBP page frame, v2 - must stay byte-identical to Go's
 * `go/kdb/storage/delta/page_codec.go`:
 *
 * ```
 *  0..3   magic 'KDBP'
 *  4      version   u8  (= PAGE_FORMAT_VERSION)
 *  5      codec     u8  (CODEC_NONE | CODEC_ZSTD)
 *  6..7   reserved  u16 (zero)
 *  8..11  compressed length   u32 (big-endian, body only)
 * 12..15  uncompressed length u32 (big-endian)
 * 16..19  crc32 of body       u32 (big-endian)
 * ```
 *
 * v1 was 16 bytes and carried no codec, so a reader had to be told out of band
 * which codec a segment was written with: changing the configured codec made
 * existing segments unreadable, and verification could not tell a codec
 * mismatch from real corruption.
 */
internal object DeltaPageCodec {
    const val FRAME_HEADER_SIZE: Int = 20
    const val PAGE_FORMAT_VERSION: Byte = 2
    const val CODEC_NONE: Byte = 0
    const val CODEC_ZSTD: Byte = 1

    /**
     * The stable on-disk id for [codec]. Spelled out rather than derived from
     * the enum's ordinal so the enum can be reordered or extended without
     * changing what already-written segments mean.
     */
    private fun codecId(codec: CompressionCodec): Byte =
        when (codec) {
            CompressionCodec.NONE -> CODEC_NONE
            CompressionCodec.ZSTD -> CODEC_ZSTD
        }

    fun frame(payload: ByteArray, codec: CompressionCodec): ByteArray {
        val id = codecId(codec)
        val body =
            when (id) {
                CODEC_NONE -> payload
                else -> ZstdCompression.compress(payload)
            }
        val out = ByteArray(FRAME_HEADER_SIZE + body.size)
        out[0] = 0x4B; out[1] = 0x44; out[2] = 0x42; out[3] = 0x50
        out[4] = PAGE_FORMAT_VERSION
        out[5] = id
        out[6] = 0; out[7] = 0
        writeInt(out, 8, body.size)
        writeInt(out, 12, payload.size)
        writeInt(out, 16, Crc32.of(body))
        body.copyInto(out, FRAME_HEADER_SIZE)
        return out
    }

    /** Decodes one whole frame using the codec the frame itself records. */
    fun parse(frame: ByteArray): ByteArray {
        require(frame.size >= FRAME_HEADER_SIZE) {
            "delta page: frame shorter than its $FRAME_HEADER_SIZE-byte header"
        }
        val version = frame[4]
        require(version == PAGE_FORMAT_VERSION) {
            "delta page: unsupported frame version $version (this build writes and reads v$PAGE_FORMAT_VERSION)"
        }
        val body = frame.copyOfRange(FRAME_HEADER_SIZE, frame.size)
        // Exactly the recorded size, not a padded bound: the header says how
        // many bytes this frame decodes to, so anything else is corruption.
        val uncompressed = readInt(frame, 12)
        return when (frame[5]) {
            CODEC_NONE -> body
            CODEC_ZSTD -> ZstdCompression.decompress(body, uncompressed)
            else -> throw IllegalArgumentException("delta page: unknown codec id ${frame[5]} in frame")
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

/**
 * A data directory whose delta segments include at least one pre-Layer-13
 * random-name segment. Sorting those by name is not sorting by commit
 * order, which is exactly the bug that made a multi-segment namespace
 * permanently unopenable (kdb-spec-layer13 Component 47 §4.1, §2.1) -
 * rather than guess at their true order, this is thrown so the caller can
 * run the repair path instead.
 */
public class LegacySegmentFormatException(
    public val namespaceId: String,
    public val names: List<String>,
) : Exception(
        "kdb: namespace '$namespaceId' has ${names.size} delta segment(s) in the pre-Layer-13 " +
            "random-name format, whose on-disk order cannot be trusted as commit order - migrate this " +
            "namespace before opening it (see kdb-spec-layer13-resource-governance.md §4.1)",
    )

public class DefaultDeltaSegmentReader(
    override val namespaceId: String,
    private val ioShim: PlatformIoShim,
    private val config: StorageEngineConfig,
) : DeltaSegmentReader {
    override suspend fun readAll(segment: DeltaSegmentRef): List<DeltaRecord> {
        val segmentName = SegmentNameBuilder.deltaSequenced(segment.namespaceId, segment.sequenceNumber)
        val bytes = readFullSegment(segmentName, segment.sizeBytes)
        // A CorruptFrameException propagates as-is (with partialCommits already populated) -
        // the caller (embed-level replay) decides torn-tail tolerance from it; this method has
        // no context (is this the most recently written segment?) to make that call itself.
        val scanned = DeltaSegmentScanner.scanSegmentBytes(bytes)
        return scanned.map { scannedToRecord(it, segment) }
    }

    private fun scannedToRecord(scanned: DeltaSegmentScanner.ScannedCommit, segment: DeltaSegmentRef) =
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

    /**
     * Returns this namespace's delta segments **in sequence (commit)
     * order** - the names shim.listSegments gives back already sort that
     * way, because delta segment file names are zero-padded decimal
     * sequence numbers (see SegmentNameBuilder.deltaSequenced), which
     * sort lexicographically the same as numerically. Callers (see
     * DeltaNamespaceReplayer) must preserve this order rather than
     * re-sorting by segmentId - that was exactly the bug Component 47
     * fixes (kdb-spec-layer13 §4.1).
     *
     * @throws LegacySegmentFormatException if a pre-Layer-13 random-name
     *   segment is present.
     */
    override suspend fun listSegments(): List<DeltaSegmentRef> {
        val prefix = SegmentNameBuilder.namespacePrefix(namespaceId) + "delta/"
        val names = ioShim.listSegments(namespaceId).filter { it.startsWith(prefix) }
        val legacy = mutableListOf<String>()
        val out = mutableListOf<DeltaSegmentRef>()
        for (name in names) {
            val fileName = name.removePrefix(prefix)
            val seq = SegmentNameBuilder.parseDeltaSequencedFileName(fileName)
            if (seq == null) {
                legacy += name
                continue
            }
            scanSegmentRef(name, seq)?.let { out += it }
        }
        if (legacy.isNotEmpty()) throw LegacySegmentFormatException(namespaceId, legacy)
        return out
    }

    private suspend fun readFullSegment(segmentName: String, sizeBytes: Long): ByteArray {
        if (sizeBytes <= 0) return byteArrayOf()
        val len = sizeBytes.coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
        return ioShim.readFromSegment(segmentName, 0, len)
    }

    private suspend fun scanSegmentRef(segmentName: String, seq: Long): DeltaSegmentRef? {
        val bytes =
            try {
                readEntireSegment(segmentName)
            } catch (_: Exception) {
                return null
            }
        val scanned =
            try {
                DeltaSegmentScanner.scanSegmentBytes(bytes)
            } catch (e: DeltaSegmentScanner.CorruptFrameException) {
                // Building a ref only needs first/last commit hash - a torn tail just means the
                // last commit is whatever scanned cleanly, same as a fully-clean scan would see
                // if the corrupt frame simply wasn't there yet. The actual torn-tail-vs-real-
                // corruption decision belongs to the replayer, not this ref-listing helper.
                e.partialCommits
            }
        val zero = KdbHash.fromBytes(ByteArray(32))
        val segmentId = deterministicSegmentId(namespaceId, seq)
        if (scanned.isEmpty()) {
            return DeltaSegmentRef(
                segmentId = segmentId,
                namespaceId = namespaceId,
                firstCommitHash = zero,
                lastCommitHash = zero,
                sizeBytes = bytes.size.toLong(),
                compressionCodec = config.compressionCodec,
                sequenceNumber = seq,
            )
        }
        return DeltaSegmentRef(
            segmentId = segmentId,
            namespaceId = namespaceId,
            firstCommitHash = scanned.first().commitHash,
            lastCommitHash = scanned.last().commitHash,
            sizeBytes = bytes.size.toLong(),
            compressionCodec = config.compressionCodec,
            sequenceNumber = seq,
        )
    }

    /**
     * The segment's true identity - the random [KdbUuid] a [DefaultDeltaSegmentWriter] picks at
     * creation time - is never persisted to the segment file itself, so a later scan (e.g. after
     * a process restart) has no way to recover it. Previously this minted a fresh
     * [KdbUuid.random] on every single scan instead, which - since consumers like
     * `DeltaLogTierRegistry` key their state by `segmentId` - silently discarded that state on
     * every rescan. [sequenceNumber] *is* stable across scans (it's parsed straight from the
     * file name) and already unique within a namespace, so derive a deterministic id from it
     * instead: same (namespaceId, sequenceNumber) always maps to the same id, and different
     * sequence numbers within one namespace never collide (namespaceId only needs to keep
     * different namespaces' identically-numbered segments apart, so a checksum of it is enough).
     */
    private fun deterministicSegmentId(namespaceId: String, sequenceNumber: Long): KdbUuid {
        val namespaceCrc = Crc32.of(namespaceId.encodeToByteArray()).toLong() and 0xFFFFFFFFL
        return KdbUuid(msb = namespaceCrc, lsb = sequenceNumber)
    }

    private suspend fun readEntireSegment(segmentName: String): ByteArray =
        ioShim.readFromSegment(segmentName, 0, 1 shl 28)
}

public class DeltaSegmentFactory(
    private val config: StorageEngineConfig,
    private val debugHook: DeltaDebugHook = NoOpDeltaDebugHook,
) {
    /**
     * Opens a writer for namespaceId's next delta segment: the sequence
     * number one past the highest existing sequenced segment (0 if none
     * exist yet). Always starts a *new* segment rather than resuming a
     * previous run's last (possibly unsealed) one - see the Go side's
     * Factory.OpenWriter doc comment for why (kdb-spec-layer13 §4.1).
     *
     * @throws LegacySegmentFormatException, without opening anything, if
     *   any pre-Layer-13 random-name segment is present.
     */
    public suspend fun openWriter(namespaceId: String): DefaultDeltaSegmentWriter {
        val (nextSeq, legacy) = scanExistingDeltaSequence(namespaceId)
        if (legacy.isNotEmpty()) throw LegacySegmentFormatException(namespaceId, legacy)
        return DefaultDeltaSegmentWriter(namespaceId, KdbUuid.random(), nextSeq, config.ioShim, config, debugHook)
    }

    public fun openReader(namespaceId: String): DefaultDeltaSegmentReader =
        DefaultDeltaSegmentReader(namespaceId, config.ioShim, config)

    private suspend fun scanExistingDeltaSequence(namespaceId: String): Pair<Long, List<String>> {
        val prefix = SegmentNameBuilder.namespacePrefix(namespaceId) + "delta/"
        val names = config.ioShim.listSegments(namespaceId).filter { it.startsWith(prefix) }
        var maxSeq = -1L
        val legacy = mutableListOf<String>()
        for (name in names) {
            val fileName = name.removePrefix(prefix)
            val seq = SegmentNameBuilder.parseDeltaSequencedFileName(fileName)
            if (seq == null) {
                legacy += name
            } else if (seq > maxSeq) {
                maxSeq = seq
            }
        }
        return Pair(maxSeq + 1, legacy)
    }
}
