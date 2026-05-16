package dev.kdb.index

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

private sealed interface MemEvent {
    val seq: Long

    data class Put(
        override val seq: Long,
        val entry: IndexEntry,
    ) : MemEvent

    data class Delete(
        override val seq: Long,
        val docId: KdbUuid,
        val atCommit: KdbHash,
    ) : MemEvent
}

/** Chronological replay index for correctness tests ([Component 8]). */
public class MemoryIndexStore(
    override val descriptor: IndexDescriptor,
    private val dag: CommitDag,
) : IndexStore {

    private val mutex = Mutex()
    private val log = mutableListOf<MemEvent>()
    private var seqCounter = 0L

    override suspend fun put(entry: IndexEntry) {
        mutex.withLock {
            seqCounter++
            log.add(MemEvent.Put(seqCounter, entry))
        }
    }

    override suspend fun delete(
        docId: KdbUuid,
        atCommit: KdbHash,
    ) {
        mutex.withLock {
            seqCounter++
            log.add(MemEvent.Delete(seqCounter, docId, atCommit))
        }
    }

    override suspend fun bulkLoad(entries: List<IndexEntry>) {
        mutex.withLock {
            clearLocked()
            for (e in entries) {
                seqCounter++
                log.add(MemEvent.Put(seqCounter, e))
            }
        }
    }

    override suspend fun rebuild(entries: List<IndexEntry>) {
        bulkLoad(entries)
    }

    override suspend fun lookup(
        key: IndexKey,
        atCommit: KdbHash?,
    ): List<KdbUuid> {
        require(descriptor.type == IndexType.HASH || descriptor.type == IndexType.BTREE)
        return replayBuckets(cutoff(atCommit))[key]?.toList() ?: emptyList()
    }

    override suspend fun range(
        from: IndexKey?,
        to: IndexKey?,
        atCommit: KdbHash?,
        limit: Int,
        ascending: Boolean,
    ): List<KdbUuid> {
        require(descriptor.type == IndexType.HASH || descriptor.type == IndexType.BTREE)
        val buckets = replayBuckets(cutoff(atCommit))
        val filtered =
            buckets.keys.filter { k ->
                (from == null || compareIndexKeys(k, from) >= 0) &&
                    (to == null || compareIndexKeys(k, to) <= 0)
            }
        val sorted =
            filtered.sortedWith { a, b ->
                compareIndexKeys(a, b)
            }
        val keys =
            if (ascending) {
                sorted
            } else {
                sorted.asReversed()
            }
        val out = LinkedHashSet<KdbUuid>()
        outer@
        for (k in keys) {
            for (doc in buckets[k] ?: continue) {
                out.add(doc)
                if (out.size >= limit) break@outer
            }
        }
        return out.toList()
    }

    override suspend fun search(
        query: String,
        atCommit: KdbHash?,
        limit: Int,
    ): List<KdbUuid> {
        if (descriptor.type != IndexType.FULLTEXT) {
            throw IndexTypeMismatchException(descriptor.fieldName, descriptor.fieldName, IndexType.FULLTEXT, descriptor.type)
        }
        throw UnsupportedOperationException()
    }

    override suspend fun nearestNeighbours(
        queryVector: FloatArray,
        k: Int,
        atCommit: KdbHash?,
    ): List<RankedResult> {
        if (descriptor.type != IndexType.VECTOR) {
            throw IndexTypeMismatchException(descriptor.fieldName, descriptor.fieldName, IndexType.VECTOR, descriptor.type)
        }
        throw UnsupportedOperationException()
    }

    override suspend fun clear() {
        mutex.withLock { clearLocked() }
    }

    override suspend fun isValid(atCommit: KdbHash): Boolean =
        mutex.withLock { dag.hasCommit(atCommit) }

    override suspend fun snapshot(): ByteArray =
        mutex.withLock { Snapshot.lines(log).joinToString("\n").encodeToByteArray() }

    override suspend fun restoreSnapshot(data: ByteArray) {
        mutex.withLock {
            val lines = data.decodeToString().lines().filter { it.isNotBlank() }
            clearLocked()
            Snapshot.parse(lines, this)
        }
    }

    private fun clearLocked() {
        log.clear()
        seqCounter = 0L
    }

    private suspend fun cutoff(atCommit: KdbHash?): KdbHash =
        atCommit ?: dag.head()

    private suspend fun replayBuckets(cutoffHash: KdbHash): MutableMap<IndexKey, LinkedHashSet<KdbUuid>> {
        val buckets = mutableMapOf<IndexKey, LinkedHashSet<KdbUuid>>()
        for (evt in log.sortedBy { it.seq }) {
            when (evt) {
                is MemEvent.Put -> {
                    if (!dag.isAncestor(evt.entry.commitHash, cutoffHash)) {
                        continue
                    }
                    buckets.getOrPut(evt.entry.key) { LinkedHashSet() }.add(evt.entry.docId)
                }

                is MemEvent.Delete -> {
                    if (!dag.isAncestor(evt.atCommit, cutoffHash)) {
                        continue
                    }
                    prune(buckets, evt.docId)
                }
            }
        }
        return buckets
    }

    private fun prune(
        buckets: MutableMap<IndexKey, LinkedHashSet<KdbUuid>>,
        docId: KdbUuid,
    ) {
        val dead = buckets.filterValues { ids -> ids.remove(docId); ids.isEmpty() }.keys
        dead.forEach { buckets.remove(it) }
    }

    internal fun ingestSnapshotPut(entry: IndexEntry) {
        seqCounter++
        log.add(MemEvent.Put(seqCounter, entry))
    }

    internal fun ingestSnapshotDelete(
        docId: KdbUuid,
        atCommit: KdbHash,
    ) {
        seqCounter++
        log.add(MemEvent.Delete(seqCounter, docId, atCommit))
    }
}

private object Snapshot {

    fun lines(log: List<MemEvent>): List<String> =
        log.sortedBy { it.seq }.map { evt ->
            when (evt) {
                is MemEvent.Put ->
                    "P|${evt.entry.docId}|${evt.entry.commitHash}|${KeyLine.encode(evt.entry.key)}"

                is MemEvent.Delete ->
                    "D|${evt.docId}|${evt.atCommit}"
            }
        }

    fun parse(lines: List<String>, into: MemoryIndexStore) {
        for (ln in lines) {
            val parts = ln.split('|', limit = 4)
            when (parts[0]) {
                "P" -> {
                    val doc = KdbUuid.fromString(parts[1])
                    val commit = KdbHash.fromHex(parts[2])
                    val key = KeyLine.decode(parts.getOrElse(3) { "NULL" })
                    into.ingestSnapshotPut(IndexEntry(doc, key, commit))
                }

                "D" -> {
                    require(parts.size >= 3) { "snapshot delete line malformed" }
                    val doc = KdbUuid.fromString(parts[1])
                    val commit = KdbHash.fromHex(parts[2])
                    into.ingestSnapshotDelete(doc, commit)
                }

                else -> error("snapshot line corrupt")
            }
        }
    }
}

private object KeyLine {
    fun encode(k: IndexKey): String =
        when (k) {
            IndexKey.NullKey -> "NULL"
            is IndexKey.BoolKey -> "B:${k.value}"
            is IndexKey.Int32Key -> "I:${k.value}"
            is IndexKey.Int64Key -> "L:${k.value}"
            is IndexKey.Float64Key -> "F:${k.value}"
            is IndexKey.TimestampKey -> "T:${k.epochMillis}"
            is IndexKey.StringKey -> "S:${escape(k.value)}"
            is IndexKey.UuidKey -> "U:${k.id}"
            is IndexKey.VectorKey -> throw IllegalArgumentException("VECTOR keys cannot be snapshotted in MemoryIndexStore")
            is IndexKey.CompositeKey -> throw IllegalArgumentException("COMPOSITE keys cannot be snapshotted yet")
        }

    fun decode(line: String): IndexKey =
        when {
            line == "NULL" -> IndexKey.NullKey
            line.startsWith("B:") -> IndexKey.BoolKey(line.removePrefix("B:").toBoolean())
            line.startsWith("I:") -> IndexKey.Int32Key(line.removePrefix("I:").toInt())
            line.startsWith("L:") -> IndexKey.Int64Key(line.removePrefix("L:").toLong())
            line.startsWith("F:") -> IndexKey.Float64Key(line.removePrefix("F:").toDouble())
            line.startsWith("T:") -> IndexKey.TimestampKey(line.removePrefix("T:").toLong())
            line.startsWith("S:") -> IndexKey.StringKey(unescape(line.removePrefix("S:")))
            line.startsWith("U:") -> IndexKey.UuidKey(KdbUuid.fromString(line.removePrefix("U:")))
            else -> IndexKey.NullKey
        }

    private fun escape(s: String): String =
        s.replace("\\", "\\\\").replace("|", "\\|")

    private fun unescape(s: String): String =
        buildString {
            var i = 0
            while (i < s.length) {
                if (s[i] == '\\' && i + 1 < s.length) {
                    append(s[i + 1])
                    i += 2
                } else {
                    append(s[i])
                    i++
                }
            }
        }
}
