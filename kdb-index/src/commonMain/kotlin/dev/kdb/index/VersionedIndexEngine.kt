package dev.kdb.index

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

internal sealed interface VersionedEvent {
    val seq: Long
}

internal data class VersionedPut(
    override val seq: Long,
    val entry: IndexEntry,
) : VersionedEvent

internal data class VersionedDelete(
    override val seq: Long,
    val docId: KdbUuid,
    val atCommit: KdbHash,
) : VersionedEvent

/**
 * Commit-ancestry-aware index replay shared by HASH and BTREE stores (Component 12).
 */
public class VersionedIndexEngine(
    private val dag: CommitDag,
) {
    private val mutex = Mutex()
    private val log = mutableListOf<VersionedEvent>()
    private var seqCounter = 0L

    public suspend fun put(entry: IndexEntry) {
        mutex.withLock {
            seqCounter++
            log.add(VersionedPut(seqCounter, entry))
        }
    }

    public suspend fun delete(
        docId: KdbUuid,
        atCommit: KdbHash,
    ) {
        mutex.withLock {
            seqCounter++
            log.add(VersionedDelete(seqCounter, docId, atCommit))
        }
    }

    public suspend fun bulkLoad(entries: List<IndexEntry>) {
        mutex.withLock {
            clearLocked()
            for (e in entries) {
                seqCounter++
                log.add(VersionedPut(seqCounter, e))
            }
        }
    }

    public suspend fun clear() {
        mutex.withLock { clearLocked() }
    }

    public suspend fun lookup(
        key: IndexKey,
        atCommit: KdbHash?,
    ): List<KdbUuid> {
        val buckets = replayBuckets(cutoff(atCommit))
        return buckets[key]?.toList() ?: emptyList()
    }

    public suspend fun range(
        from: IndexKey?,
        to: IndexKey?,
        atCommit: KdbHash?,
        limit: Int,
        ascending: Boolean,
    ): List<KdbUuid> {
        val buckets = replayBuckets(cutoff(atCommit))
        val filtered =
            buckets.keys.filter { k ->
                (from == null || compareIndexKeys(k, from) >= 0) &&
                    (to == null || compareIndexKeys(k, to) <= 0)
            }
        val sorted =
            if (ascending) {
                filtered.sortedWith { a, b -> compareIndexKeys(a, b) }
            } else {
                filtered.sortedWith { a, b -> compareIndexKeys(b, a) }
            }
        val out = LinkedHashSet<KdbUuid>()
        outer@
        for (k in sorted) {
            for (doc in buckets[k] ?: continue) {
                out.add(doc)
                if (out.size >= limit) break@outer
            }
        }
        return out.toList()
    }

    public suspend fun headBuckets(): Map<IndexKey, Set<KdbUuid>> =
        replayBuckets(cutoff(null)).mapValues { it.value.toSet() }

    public suspend fun isValid(atCommit: KdbHash): Boolean =
        mutex.withLock { dag.hasCommit(atCommit) }

    public suspend fun snapshotBytes(): ByteArray =
        mutex.withLock {
            VersionedIndexSnapshot.lines(log).joinToString("\n").encodeToByteArray()
        }

    public suspend fun restoreSnapshotBytes(data: ByteArray) {
        mutex.withLock {
            val lines = data.decodeToString().lines().filter { it.isNotBlank() }
            clearLocked()
            VersionedIndexSnapshot.parse(lines, this)
        }
    }

    internal fun ingestPut(entry: IndexEntry) {
        seqCounter++
        log.add(VersionedPut(seqCounter, entry))
    }

    internal fun ingestDelete(
        docId: KdbUuid,
        atCommit: KdbHash,
    ) {
        seqCounter++
        log.add(VersionedDelete(seqCounter, docId, atCommit))
    }

    private fun clearLocked() {
        log.clear()
        seqCounter = 0L
    }

    private suspend fun cutoff(atCommit: KdbHash?): KdbHash = atCommit ?: dag.head()

    private suspend fun replayBuckets(cutoffHash: KdbHash): MutableMap<IndexKey, LinkedHashSet<KdbUuid>> {
        val buckets = mutableMapOf<IndexKey, LinkedHashSet<KdbUuid>>()
        for (evt in log.sortedBy { it.seq }) {
            when (evt) {
                is VersionedPut -> {
                    if (!dag.isAncestor(evt.entry.commitHash, cutoffHash)) continue
                    buckets.getOrPut(evt.entry.key) { LinkedHashSet() }.add(evt.entry.docId)
                }
                is VersionedDelete -> {
                    if (!dag.isAncestor(evt.atCommit, cutoffHash)) continue
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
}

private object VersionedIndexSnapshot {
    fun lines(log: List<VersionedEvent>): List<String> =
        log.sortedBy { it.seq }.map { evt ->
            when (evt) {
                is VersionedPut ->
                    "P|${evt.entry.docId}|${evt.entry.commitHash}|${IndexKeyLine.encode(evt.entry.key)}"
                is VersionedDelete ->
                    "D|${evt.docId}|${evt.atCommit}"
            }
        }

    fun parse(
        lines: List<String>,
        into: VersionedIndexEngine,
    ) {
        for (ln in lines) {
            val parts = ln.split('|', limit = 4)
            when (parts[0]) {
                "P" -> {
                    val doc = KdbUuid.fromString(parts[1])
                    val commit = KdbHash.fromHex(parts[2])
                    val key = IndexKeyLine.decode(parts.getOrElse(3) { "NULL" })
                    into.ingestPut(IndexEntry(doc, key, commit))
                }
                "D" -> {
                    val doc = KdbUuid.fromString(parts[1])
                    val commit = KdbHash.fromHex(parts[2])
                    into.ingestDelete(doc, commit)
                }
                else -> error("snapshot line corrupt")
            }
        }
    }
}

internal object IndexKeyLine {
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
            is IndexKey.VectorKey -> throw IllegalArgumentException("VECTOR keys cannot be snapshotted in hash/btree")
            is IndexKey.CompositeKey ->
                "C:" + k.parts.joinToString(",") { encode(it) }
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
            line.startsWith("C:") ->
                IndexKey.CompositeKey(
                    line.removePrefix("C:").split(',').map { decode(it) },
                )
            else -> IndexKey.NullKey
        }

    private fun escape(s: String): String =
        s.replace("\\", "\\\\").replace("|", "\\|").replace(",", "\\,")

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
