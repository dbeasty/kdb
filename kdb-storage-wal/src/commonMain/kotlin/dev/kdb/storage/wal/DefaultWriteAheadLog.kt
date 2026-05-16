package dev.kdb.storage.wal

import dev.kdb.codec.KdbUuid
import dev.kdb.storage.PlatformIoShim
import dev.kdb.storage.StorageEngineConfig
import dev.kdb.storage.io.SegmentNameBuilder
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

public class DefaultWriteAheadLog internal constructor(
    override val walId: KdbUuid,
    override val partitionKey: String,
    private val segmentName: String,
    private val ioShim: PlatformIoShim,
    private val walMaxSegmentBytes: Long,
    private val skipCorrupt: Boolean,
) : WriteAheadLog {
    private val mutex = Mutex()
    private var sequenceCounter: Long = 0
    private var segmentSize: Long = 0
    private var closed = false

    override val lastSequence: Long get() = sequenceCounter
    override val activeSegmentSizeBytes: Long get() = segmentSize

    override suspend fun append(record: WalRecord): WalAppendResult =
        mutex.withLock {
            checkOpen()
            val seq = ++sequenceCounter
            val rec = record.copy(sequence = seq)
            val bytes = WalCodec.encodeRecord(rec)
            val offset = segmentSize
            val newSize = ioShim.appendToSegment(segmentName, bytes)
            segmentSize = newSize
            WalAppendResult(seq, offset, newSize)
        }

    override suspend fun appendBatch(records: List<WalRecord>): WalAppendResult =
        mutex.withLock {
            checkOpen()
            var last = WalAppendResult(0, segmentSize, segmentSize)
            for (r in records) {
                val seq = ++sequenceCounter
                val rec = r.copy(sequence = seq)
                val bytes = WalCodec.encodeRecord(rec)
                val off = segmentSize
                val newSize = ioShim.appendToSegment(segmentName, bytes)
                segmentSize = newSize
                last = WalAppendResult(seq, off, newSize)
            }
            last
        }

    override suspend fun sync() {
        ioShim.flushSegment(segmentName)
    }

    override suspend fun recover(handler: suspend (WalRecord) -> Unit): WalRecoverySummary {
        val bytes = readFullSegment()
        val records = WalCodec.decodeRecords(bytes, partitionKey, segmentName, skipCorrupt)
        var replayed = 0L
        var maxSeq = 0L
        for (r in records.sortedBy { it.sequence }) {
            handler(r)
            replayed++
            if (r.sequence > maxSeq) maxSeq = r.sequence
        }
        sequenceCounter = maxSeq
        segmentSize = bytes.size.toLong()
        return WalRecoverySummary(replayed, 0, maxSeq, 1)
    }

    override suspend fun truncate(truncateThroughSequence: Long) {
        mutex.withLock {
            if (truncateThroughSequence >= sequenceCounter) {
                ioShim.deleteSegment(segmentName)
                segmentSize = 0
            }
        }
    }

    override suspend fun close() {
        mutex.withLock { closed = true }
    }

    private fun checkOpen() {
        if (closed) throw WalClosedException("WAL closed", partitionKey)
    }

    private suspend fun readFullSegment(): ByteArray {
        val chunk = 64 * 1024
        val out = mutableListOf<Byte>()
        var off = 0L
        while (true) {
            val part = ioShim.readFromSegment(segmentName, off, chunk)
            if (part.isEmpty()) break
            part.forEach { out.add(it) }
            off += part.size
            if (part.size < chunk) break
        }
        return out.toByteArray()
    }
}

public class DefaultWriteAheadLogFactory(
    private val walMaxSegmentBytes: Long = 64L * 1024 * 1024,
    private val skipCorrupt: Boolean = false,
) : WriteAheadLogFactory {
    override suspend fun openOrCreate(
        partitionKey: String,
        config: StorageEngineConfig,
        ioShim: PlatformIoShim,
    ): WriteAheadLog {
        val walId = KdbUuid.random()
        val name = activeSegmentName(partitionKey, walId)
        ioShim.appendToSegment(name, byteArrayOf())
        return DefaultWriteAheadLog(
            walId,
            partitionKey,
            name,
            ioShim,
            walMaxSegmentBytes,
            skipCorrupt,
        )
    }

    override fun activeSegmentName(partitionKey: String, walId: KdbUuid): String =
        SegmentNameBuilder.wal(partitionKey, walId.toString())
}
