package dev.kdb.storage.wal

import dev.kdb.codec.KdbUuid
import dev.kdb.storage.PlatformIoShim
import dev.kdb.storage.StorageEngineConfig
import dev.kdb.storage.io.SegmentNameBuilder
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * One file in a WAL's segment chain. Every segment of one WAL shares the [walId]; the file name
 * carries the sequence its records start at, which is what orders the chain.
 */
internal data class WalSegment(val name: String, val firstSequence: Long)

/**
 * Names the segment that starts at [firstSequence]. The sequence is zero-padded to 20 digits so
 * lexicographic order (what every [PlatformIoShim.listSegments] gives back) equals numeric
 * order, matching the convention [SegmentNameBuilder.deltaSequencedFileName] already uses. Must
 * stay byte-identical to Go's `rotatedSegmentName` - a mixed Go/Kotlin deployment shares one
 * data directory, and a name either side cannot parse is a directory it cannot open.
 */
internal fun rotatedSegmentName(partitionKey: String, walId: KdbUuid, firstSequence: Long): String =
    SegmentNameBuilder.wal(partitionKey, "$walId." + firstSequence.toString().padStart(20, '0'))

/**
 * Splits a WAL segment file name into its walId and the sequence its records start at, or null
 * for anything that is not a WAL segment name. A name with no sequence suffix is a WAL's first
 * segment (and is also what every pre-rotation WAL wrote), so it starts at sequence 1.
 *
 * Mirrors Go's `parseWalFileName`. Kotlin previously fed the whole file name straight to
 * [KdbUuid.fromString], which threw on any rotated name Go had written: a Go-written data
 * directory became unopenable on the JVM the moment its WAL rotated once.
 */
internal fun parseWalFileName(fileName: String): Pair<String, Long>? {
    val dot = fileName.indexOf('.')
    if (dot < 0) return if (fileName.isEmpty()) null else Pair(fileName, 1L)
    val id = fileName.substring(0, dot)
    if (id.isEmpty()) return null
    val seq = fileName.substring(dot + 1).toLongOrNull() ?: return null
    if (seq < 1) return null
    return Pair(id, seq)
}

/**
 * Groups WAL segment names by walId and returns the newest group's segments in chain order.
 * Only one WAL per partition is ever active; anything left over from an earlier walId is
 * ignored, as it was before rotation existed. Mirrors Go's `latestWalChain`.
 */
internal fun latestWalChain(segmentNames: List<String>): Pair<List<WalSegment>, String?> {
    val groups = mutableMapOf<String, MutableList<WalSegment>>()
    for (name in segmentNames) {
        if (!name.contains("/wal/")) continue
        val fileName = name.substringAfterLast('/')
        val parsed = parseWalFileName(fileName) ?: continue
        groups.getOrPut(parsed.first) { mutableListOf() }.add(WalSegment(name, parsed.second))
    }
    val newest = groups.keys.maxOrNull() ?: return Pair(emptyList(), null)
    return Pair(groups.getValue(newest).sortedBy { it.firstSequence }, newest)
}

public class DefaultWriteAheadLog internal constructor(
    override val walId: KdbUuid,
    override val partitionKey: String,
    initialSegments: List<WalSegment>,
    private val ioShim: PlatformIoShim,
    private val walMaxSegmentBytes: Long,
    private val skipCorrupt: Boolean,
    initialSegmentSize: Long = 0,
) : WriteAheadLog {
    private val mutex = Mutex()
    private val segments = initialSegments.toMutableList()
    private var segmentName: String = initialSegments.last().name
    private var sequenceCounter: Long = 0
    private var segmentSize: Long = initialSegmentSize
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

    /** The segment appends currently land in. */
    public val activeSegmentName: String get() = segmentName

    /** The WAL's segment chain in sequence order, active segment last. */
    public val segmentNames: List<String> get() = segments.map { it.name }

    override suspend fun append(record: WalRecord): WalAppendResult =
        mutex.withLock {
            checkOpen()
            val seq = sequenceCounter + 1
            val rec = record.copy(sequence = seq)
            val bytes = WalCodec.encodeRecord(rec)
            rotateIfFullLocked(bytes.size.toLong(), seq)
            val offset = segmentSize
            val newSize = ioShim.appendToSegment(segmentName, bytes)
            sequenceCounter = seq
            segmentSize = newSize
            WalAppendResult(seq, offset, newSize)
        }

    override suspend fun appendBatch(records: List<WalRecord>): WalAppendResult =
        mutex.withLock {
            checkOpen()
            val encoded =
                records.mapIndexed { i, r ->
                    WalCodec.encodeRecord(r.copy(sequence = sequenceCounter + 1 + i))
                }
            // Rotate once for the whole batch rather than mid-way through it, so a batch always
            // lands in a single segment and its records stay contiguous on disk.
            rotateIfFullLocked(encoded.sumOf { it.size }.toLong(), sequenceCounter + 1)
            var last = WalAppendResult(sequenceCounter, segmentSize, segmentSize)
            for (bytes in encoded) {
                val seq = sequenceCounter + 1
                val off = segmentSize
                val newSize = ioShim.appendToSegment(segmentName, bytes)
                sequenceCounter = seq
                segmentSize = newSize
                last = WalAppendResult(seq, off, newSize)
            }
            last
        }

    /**
     * Seals the active segment and opens a new one when the incoming write would push it past
     * [walMaxSegmentBytes]. A write larger than the cap gets a segment to itself rather than
     * being rejected. Before this existed, [walMaxSegmentBytes] was accepted by the constructor
     * and never read: one segment grew without limit for the lifetime of the partition, and Go -
     * which does rotate - wrote chains this side could not even name-parse.
     */
    private suspend fun rotateIfFullLocked(incomingBytes: Long, firstSequence: Long) {
        if (walMaxSegmentBytes <= 0 || segmentSize == 0L) return
        if (segmentSize + incomingBytes <= walMaxSegmentBytes) return
        val previous = segmentName
        val name = rotatedSegmentName(partitionKey, walId, firstSequence)
        ioShim.appendToSegment(name, byteArrayOf())
        // Flush before sealing: the segment is about to stop being written to, and a shim that
        // treats sealing as final would otherwise leave its tail unflushed.
        ioShim.flushSegment(previous)
        ioShim.sealSegment(previous)
        segments.add(WalSegment(name, firstSequence))
        segmentName = name
        segmentSize = 0
    }

    /** Flushes the active segment. Sealed segments were flushed as part of rotation. */
    override suspend fun sync() {
        val mySeq = mutex.withLock { sequenceCounter }
        while (true) {
            lateinit var round: CompletableDeferred<Unit>
            var isLeader = false
            var covered = false
            val target = mutex.withLock { segmentName }
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
                        ioShim.flushSegment(target)
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

    /**
     * Replays **every** segment of the chain, oldest first. This used to read only the single
     * segment the factory happened to pick, so on a rotated chain - the only kind Go writes -
     * every record before the newest segment was silently dropped on recovery.
     */
    override suspend fun recover(handler: suspend (WalRecord) -> Unit): WalRecoverySummary {
        val chain = mutex.withLock { segments.toList() }
        var replayed = 0L
        var skipped = 0L
        var maxSeq = 0L
        var scanned = 0
        var activeSize = 0L
        for ((i, seg) in chain.withIndex()) {
            val bytes = readFullSegment(seg.name)
            if (i == chain.size - 1) activeSize = bytes.size.toLong()
            scanned++
            val decoded = WalCodec.decodeRecords(bytes, partitionKey, seg.name, skipCorrupt)
            skipped += decoded.skippedCorrupt
            for (r in decoded.records.sortedBy { it.sequence }) {
                handler(r)
                replayed++
                if (r.sequence > maxSeq) maxSeq = r.sequence
            }
        }
        // sequenceCounter/segmentSize are also touched by append()/truncate() under [mutex] -
        // mutate them under the same lock rather than racing a concurrent caller of those.
        mutex.withLock {
            if (maxSeq > sequenceCounter) sequenceCounter = maxSeq
            segmentSize = activeSize
        }
        return WalRecoverySummary(replayed, skipped, maxSeq, scanned)
    }

    /**
     * Drops WAL bytes already reflected elsewhere: every sealed segment whose records all fall
     * at or below [truncateThroughSequence] is deleted, and the active segment is emptied when
     * it too is fully covered. When the cut falls *inside* the active segment, that segment is
     * rewritten keeping only the records past the cut - the log is append-only with no in-place
     * trim, so a rewrite is the only way to reclaim anything, and without it a partial truncate
     * silently did nothing until every last record was superseded. The sequence counter is
     * preserved in all three cases, so appends continue where they left off.
     *
     * Go's `Truncate` implements the same three cases; the two must agree, or a partition
     * truncated by one runtime looks like it still owes records to the other.
     */
    override suspend fun truncate(truncateThroughSequence: Long) {
        mutex.withLock {
            checkOpen()
            val kept = mutableListOf<WalSegment>()
            for (i in 0 until segments.size - 1) {
                // A sealed segment's last sequence is one below where its successor starts.
                if (segments[i + 1].firstSequence - 1 <= truncateThroughSequence) {
                    ioShim.deleteSegment(segments[i].name)
                    continue
                }
                kept.add(segments[i])
            }
            var active = segments.last()
            if (truncateThroughSequence >= sequenceCounter) {
                ioShim.deleteSegment(active.name)
                ioShim.appendToSegment(active.name, byteArrayOf())
                active = active.copy(firstSequence = sequenceCounter + 1)
                segmentSize = 0
            } else if (truncateThroughSequence >= active.firstSequence) {
                val bytes = readFullSegment(active.name)
                val decoded = WalCodec.decodeRecords(bytes, partitionKey, active.name, skipCorrupt)
                val rewritten =
                    decoded.records
                        .filter { it.sequence > truncateThroughSequence }
                        .fold(ByteArray(0)) { acc, r -> acc + WalCodec.encodeRecord(r) }
                ioShim.deleteSegment(active.name)
                ioShim.appendToSegment(active.name, rewritten)
                active = active.copy(firstSequence = truncateThroughSequence + 1)
                segmentSize = rewritten.size.toLong()
            }
            kept.add(active)
            segments.clear()
            segments.addAll(kept)
            segmentName = active.name
        }
    }

    override suspend fun close() {
        mutex.withLock { closed = true }
    }

    private fun checkOpen() {
        if (closed) throw WalClosedException("WAL closed", partitionKey)
    }

    private suspend fun readFullSegment(name: String): ByteArray {
        val chunk = 64 * 1024
        val parts = mutableListOf<ByteArray>()
        var total = 0
        var off = 0L
        while (true) {
            val part = ioShim.readFromSegment(name, off, chunk)
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
        val (chain, walIdStr) = latestWalChain(ioShim.listSegments(partitionKey))
        if (chain.isEmpty() || walIdStr == null) {
            val walId = KdbUuid.random()
            val name = activeSegmentName(partitionKey, walId)
            ioShim.appendToSegment(name, byteArrayOf())
            return DefaultWriteAheadLog(
                walId,
                partitionKey,
                listOf(WalSegment(name, 1)),
                ioShim,
                walMaxSegmentBytes,
                skipCorrupt,
            )
        }
        val walId = KdbUuid.fromString(walIdStr)
        // The active segment's size decides when the next append rotates, so it has to be known
        // before any append - not only after a recover() pass, which is the one place that used
        // to set it (leaving every offset and every size check wrong on a re-opened WAL).
        val activeSize = segmentSizeOf(ioShim, chain.last().name)
        return DefaultWriteAheadLog(
            walId,
            partitionKey,
            chain,
            ioShim,
            walMaxSegmentBytes,
            skipCorrupt,
            activeSize,
        )
    }

    override fun activeSegmentName(partitionKey: String, walId: KdbUuid): String =
        SegmentNameBuilder.wal(partitionKey, walId.toString())

    private suspend fun segmentSizeOf(ioShim: PlatformIoShim, name: String): Long {
        val chunk = 64 * 1024
        var total = 0L
        while (true) {
            val part = ioShim.readFromSegment(name, total, chunk)
            if (part.isEmpty()) return total
            total += part.size
            if (part.size < chunk) return total
        }
    }
}
