package dev.kdb.storage.wal

import dev.kdb.codec.KdbUuid
import dev.kdb.storage.PlatformIoShim
import dev.kdb.storage.StorageEngineConfig
import dev.kdb.storage.io.SegmentNameBuilder
import kotlinx.coroutines.CompletableDeferred
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

    // Group commit: concurrent sync() callers that arrive while a flush is already in flight
    // piggyback on that single flushSegment() call instead of each paying a full fsync
    // themselves -- N concurrent durable writes cost ~1 fsync instead of N under load, the
    // same technique real WAL implementations (Postgres, SQLite WAL mode) use.
    //
    // Correctness requires more than "is a flush currently running": a caller may only join an
    // in-flight round if its own append is guaranteed to have completed *before* that round's
    // flushSegment() call started -- otherwise it could return from sync() believing its write
    // is durable when the running fsync never covered it. append() is fully serialized by
    // [mutex], so sequence numbers give a real happens-before order: the leader snapshots the
    // highest sequence appended at the moment it starts flushing, and only a caller whose own
    // sequence is <= that snapshot may join. Anyone else waits for the round to clear and
    // retries (as either a fresh leader or a joiner of a newer, covering round).
    private val syncGate = Mutex()
    private var inFlightSync: CompletableDeferred<Unit>? = null
    private var inFlightTargetSeq: Long = -1

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
        val mySeq = mutex.withLock { sequenceCounter }
        while (true) {
            lateinit var round: CompletableDeferred<Unit>
            var isLeader = false
            var covered = false
            syncGate.withLock {
                val existing = inFlightSync
                when {
                    existing == null -> {
                        round = CompletableDeferred()
                        inFlightSync = round
                        inFlightTargetSeq = mySeq
                        isLeader = true
                        covered = true
                    }
                    mySeq <= inFlightTargetSeq -> {
                        round = existing
                        covered = true
                    }
                    else -> {
                        // A round is running but started before my append landed; it can't
                        // cover me. Wait for it to clear, then loop -- I'll either become the
                        // leader of a fresh round or join one whose target has moved past mySeq.
                        round = existing
                        covered = false
                    }
                }
            }
            if (isLeader) {
                val outcome =
                    try {
                        ioShim.flushSegment(segmentName)
                        null
                    } catch (e: Throwable) {
                        e
                    }
                // Clear the slot *before* completing the deferred: otherwise a caller could
                // observe inFlightSync still pointing at this (already-finished) round between
                // complete() and the clear, and wrongly treat it as still-joinable.
                syncGate.withLock { if (inFlightSync === round) inFlightSync = null }
                if (outcome == null) round.complete(Unit) else round.completeExceptionally(outcome)
                round.await()
                return
            }
            round.await()
            if (covered) return
        }
    }

    override suspend fun recover(handler: suspend (WalRecord) -> Unit): WalRecoverySummary {
        val bytes = readFullSegment()
        val decoded = WalCodec.decodeRecords(bytes, partitionKey, segmentName, skipCorrupt)
        var replayed = 0L
        var maxSeq = 0L
        for (r in decoded.records.sortedBy { it.sequence }) {
            handler(r)
            replayed++
            if (r.sequence > maxSeq) maxSeq = r.sequence
        }
        // sequenceCounter/segmentSize are also touched by append()/truncate() under [mutex] -
        // mutate them under the same lock rather than racing a concurrent caller of those.
        mutex.withLock {
            sequenceCounter = maxSeq
            segmentSize = bytes.size.toLong()
        }
        return WalRecoverySummary(replayed, decoded.skippedCorrupt, maxSeq, 1)
    }

    override suspend fun truncate(truncateThroughSequence: Long) {
        mutex.withLock {
            if (truncateThroughSequence >= sequenceCounter) {
                ioShim.deleteSegment(segmentName)
                segmentSize = 0
                return@withLock
            }
            if (truncateThroughSequence <= 0) return@withLock
            // Partial truncate: the segment is append-only with no in-place trim, so rewrite it
            // from scratch keeping only records past the requested sequence. Previously this
            // branch didn't exist at all - any partial truncateThroughSequence silently did
            // nothing, so the WAL never shrank until every last record was superseded.
            val bytes = readFullSegment()
            val decoded = WalCodec.decodeRecords(bytes, partitionKey, segmentName, skipCorrupt)
            val kept = decoded.records.filter { it.sequence > truncateThroughSequence }
            val encoded = kept.map { WalCodec.encodeRecord(it) }
            val rewritten = ByteArray(encoded.sumOf { it.size })
            var pos = 0
            for (e in encoded) {
                e.copyInto(rewritten, pos)
                pos += e.size
            }
            ioShim.deleteSegment(segmentName)
            if (rewritten.isNotEmpty()) ioShim.appendToSegment(segmentName, rewritten)
            segmentSize = rewritten.size.toLong()
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
        val parts = mutableListOf<ByteArray>()
        var total = 0
        var off = 0L
        while (true) {
            val part = ioShim.readFromSegment(segmentName, off, chunk)
            if (part.isEmpty()) break
            parts += part
            total += part.size
            off += part.size
            if (part.size < chunk) break
        }
        val out = ByteArray(total)
        var pos = 0
        for (part in parts) {
            part.copyInto(out, destinationOffset = pos)
            pos += part.size
        }
        return out
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
        val existing =
            ioShim.listSegments(partitionKey).filter { segment ->
                segment.contains("/wal/")
            }
        if (existing.isNotEmpty()) {
            val name = existing.max()
            val walId = KdbUuid.fromString(name.substringAfterLast('/'))
            return DefaultWriteAheadLog(
                walId,
                partitionKey,
                name,
                ioShim,
                walMaxSegmentBytes,
                skipCorrupt,
            )
        }
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
