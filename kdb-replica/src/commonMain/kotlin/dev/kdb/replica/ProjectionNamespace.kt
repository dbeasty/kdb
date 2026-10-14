package dev.kdb.replica

import dev.kdb.document.kdbSha256
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.Serializable

/** The local name Go gives a projection (peersync.ProjectionNamespace). */
public fun projectionNamespace(ns: String, filter: String): String =
    "$ns.projection-${kdbSha256(filter.trim().encodeToByteArray()).toHex().substring(0, 8)}"

/** One document of a write-back transaction (PROJECT_WRITE's writeBackDocDto). */
@Serializable
public data class WriteBackDoc(
    val docId: String,
    val body: String? = null,
    val deleted: Boolean = false,
    val baseHash: String? = null,
    /** The write left the body alone: only the attachments changed. */
    val keepBody: Boolean = false,
    /** [attachments] is the document's whole set after the write; [attachmentsBase] the set it replaced. */
    val attachmentsChanged: Boolean = false,
    val attachments: List<WireAttachment>? = null,
    /** The set hash of the attachments the replica held before the write; "" for none. */
    val attachmentsBase: String? = null,
)

/**
 * One document of a local write on a filtered namespace: [json] its new body, or null to delete it
 * - unless [keepBody], when the body stays as it is. [attachments], when given, is its whole
 * attachment set after the write.
 */
public data class LocalWrite(
    val docId: String,
    val json: String?,
    val keepBody: Boolean = false,
    val attachments: List<ReplicaAttachment>? = null,
)

/** A write-back transaction queued for the source, with the tx id fixed when it was written. */
@Serializable
public data class OutboxEntry(val seq: Long, val txId: String, val docs: List<WriteBackDoc>)

/** A source document's state as PROJECT_WRITE_RESULT reports it, attachments included. */
public data class CurrentDoc(
    val docId: String,
    val body: String?,
    val absent: Boolean,
    val attachments: List<WireAttachment> = emptyList(),
)

/** A transfer in progress: resumed with the same from, at and cursor. */
@Serializable
public data class ProjectionTransfer(val from: String, val at: String, val after: String)

@Serializable
public data class ProjectionMeta(
    val source: String,
    val filter: String,
    val writeBack: Boolean,
    /** The source commit the last complete transfer reached; "" before the first. */
    val lastSource: String = "",
    val transfer: ProjectionTransfer? = null,
    val nextSeq: Long = 1,
)

/** A PROJECT_PAGE. */
public data class ProjectPage(
    val atHex: String,
    val reset: Boolean,
    val writes: List<ReplicaDocument>,
    val deletes: List<String>,
    val next: String,
    val done: Boolean,
    /** Every written document's whole attachment set at [atHex]; a written document absent has none. */
    val attachments: Map<String, List<WireAttachment>> = emptyMap(),
)

public class ReadOnlyProjectionException(message: String) : RuntimeException(message)

/**
 * A filtered namespace (docs/kdb-client-replica-plan.md §4.2): the documents of a source namespace
 * that match a KDB-SQL filter and that the principal may read, kept by PROJECT_FETCH. With write
 * back, local writes go to the source by PROJECT_WRITE.
 *
 * Unlike a full namespace it holds no commits: the host decides what the replica may see, so the
 * replica cannot verify source commits it only partly receives (plan D3). It keeps the documents,
 * the last complete source commit, any transfer in progress, and an outbox.
 *
 * - Pages apply atomically with the transfer's cursor, so an interrupted transfer resumes. Every
 *   page carries final states.
 * - A reset (the host could not relate the replica's position to its head) replaces the
 *   projection. Each page covers the ids from the cursor to its `next`, and local documents in
 *   that range that the page does not deliver are removed.
 * - Local writes apply at once (optimistic) and queue in the outbox, oldest first. Each entry has
 *   a transaction id fixed when it was written, so a resend after a lost reply is recognised at
 *   the source rather than applied twice. Its base hash is the document's content hash before the
 *   write, as the source will hold it once the entries ahead of it are applied.
 * - A document with a write still in the outbox keeps its local state against incoming pages,
 *   until the source decides. A conflicting or refused write puts the source's state back.
 * - Each document's attachment set (refs; bytes are fetched on read at the last source commit)
 *   comes with every page that writes it. A write may change a document's attachments: the entry
 *   then carries the document's whole set after it, and the hash of the set it replaced (the
 *   source applies it only if it still holds that set). An attachment-only write keeps the body
 *   (keepBody) and is still guarded by its base hash. The blobs go up before the entry (Sync.kt).
 */
public class ProjectionNamespace private constructor(
    private val store: ReplicaStore,
    @kotlin.concurrent.Volatile private var meta: ProjectionMeta,
) {
    /** The source namespace's name. */
    public val name: String = meta.source
    internal val key: String = encodeKeyComponent(projectionNamespace(meta.source, meta.filter))
    private val lock = Mutex()

    public companion object {
        public fun metaKey(source: String, filter: String): String =
            "m/proj/${encodeKeyComponent(projectionNamespace(source, filter))}"

        public suspend fun open(store: ReplicaStore, source: String, filter: String, writeBack: Boolean): ProjectionNamespace {
            val key = metaKey(source, filter)
            val raw = store.get(key)
            val meta: ProjectionMeta
            if (raw != null) {
                meta = metaJson.decodeFromString(ProjectionMeta.serializer(), raw.decodeToString()).copy(writeBack = writeBack)
            } else {
                meta = ProjectionMeta(source = source, filter = filter.trim(), writeBack = writeBack)
                store.apply(Batch().apply { put(key, encodeMeta(meta)) })
            }
            return ProjectionNamespace(store, meta)
        }

        private fun encodeMeta(m: ProjectionMeta) = metaJson.encodeToString(ProjectionMeta.serializer(), m).encodeToByteArray()
    }

    public val filter: String get() = meta.filter
    public val writeBack: Boolean get() = meta.writeBack
    public val lastSource: String get() = meta.lastSource
    public val transfer: ProjectionTransfer? get() = meta.transfer

    private fun docKey(id: String) = "d/$key/${id.lowercase()}"

    private fun pendingKey(id: String) = "p/$key/${id.lowercase()}"

    private fun attachKey(id: String) = attachmentSetKey(key, id)

    /** The document's attachments as of the last transfer (refs; bytes are fetched on read), in id order. */
    public suspend fun attachments(id: String): List<ReplicaAttachment> = attachmentsOf(store, key, id)

    private fun outboxKey(seq: Long) = "o/$key/${seq.toString().padStart(16, '0')}"

    private suspend fun commit(batch: Batch, m: ProjectionMeta) {
        batch[metaKey(meta.source, meta.filter)] = encodeMeta(m)
        store.apply(batch)
        meta = m
    }

    public suspend fun get(id: String): String? = store.get(docKey(id))?.decodeToString()

    public suspend fun scan(after: String? = null, before: String? = null, limit: Int = Int.MAX_VALUE): List<ReplicaDocument> {
        val base = "d/$key/"
        val start = if (after != null) base + after.lowercase() + "\u0000" else base
        val end = if (before != null) base + before.lowercase() else prefixEnd(base)
        return store.scan(start, end, limit).map { (k, v) -> ReplicaDocument(k.substring(base.length), v.decodeToString()) }
    }

    private suspend fun pendingCount(id: String): Int = store.get(pendingKey(id))?.decodeToString()?.toInt() ?: 0

    /** Writes locally and queues the write for the source, as one transaction. A null json deletes. */
    public suspend fun write(docs: List<Pair<String, String?>>, txId: String): OutboxEntry =
        writeDocs(docs.map { (id, json) -> LocalWrite(id, json) }, txId)

    /** Writes locally - bodies and attachment sets - and queues the write for the source, as one transaction. */
    public suspend fun writeDocs(docs: List<LocalWrite>, txId: String): OutboxEntry =
        lock.withLock {
            if (!meta.writeBack) {
                throw ReadOnlyProjectionException("kdb replica: $name is filtered without write-back, so it is read-only here")
            }
            val batch = Batch()
            val counts = LinkedHashMap<String, Int>()
            val out = ArrayList<WriteBackDoc>()
            // A document written twice in one call: its second base is the first write's result.
            val staged = HashMap<String, String?>()
            val stagedSets = HashMap<String, List<ReplicaAttachment>>()
            for (d in docs) {
                val id = d.docId.lowercase()
                val before = if (staged.containsKey(id)) staged[id] else get(id)
                val baseHash = before?.let { contentHash(id, it) }
                val json = if (d.keepBody) before else d.json
                var wb: WriteBackDoc
                if (d.keepBody) {
                    if (before == null) throw IllegalArgumentException("kdb replica: $name/$id is not here to attach to")
                    wb = WriteBackDoc(docId = id, keepBody = true, baseHash = baseHash)
                } else if (json == null) {
                    wb = WriteBackDoc(docId = id, deleted = true, baseHash = baseHash)
                    batch[docKey(id)] = null
                    batch[attachKey(id)] = null // a deleted document keeps no attachments
                    stagedSets[id] = emptyList()
                } else {
                    wb = WriteBackDoc(docId = id, body = json, baseHash = baseHash)
                    batch[docKey(id)] = json.encodeToByteArray()
                }
                if (d.attachments != null && json != null) {
                    val prev = stagedSets[id] ?: attachments(id)
                    val next = sortAttachments(d.attachments)
                    wb =
                        wb.copy(
                            attachmentsChanged = true,
                            attachments = next.map { it.toWire() },
                            attachmentsBase = baseSetHash(prev),
                        )
                    stagedSets[id] = next
                    batch[attachKey(id)] = if (next.isEmpty()) null else encodeAttachmentSet(next)
                }
                out += wb
                staged[id] = json
                counts[id] = (counts[id] ?: pendingCount(id)) + 1
            }
            val entry = OutboxEntry(meta.nextSeq, txId, out)
            for ((id, n) in counts) batch[pendingKey(id)] = n.toString().encodeToByteArray()
            batch[outboxKey(entry.seq)] = metaJson.encodeToString(OutboxEntry.serializer(), entry).encodeToByteArray()
            commit(batch, meta.copy(nextSeq = entry.seq + 1))
            entry
        }

    /** The outbox, oldest first. */
    public suspend fun outbox(): List<OutboxEntry> {
        val base = "o/$key/"
        return store.scan(base, prefixEnd(base)).map { (_, v) -> metaJson.decodeFromString(OutboxEntry.serializer(), v.decodeToString()) }
    }

    public suspend fun outboxSize(): Int = outbox().size

    /**
     * Records the source's decision on [entry]. Applied ([current] null): the entry leaves the
     * outbox. Not applied: its documents go back to the source's state ([current]), and the entry
     * leaves the outbox.
     */
    public suspend fun resolve(entry: OutboxEntry, current: List<CurrentDoc>?) {
        lock.withLock {
            val batch = Batch()
            batch[outboxKey(entry.seq)] = null
            val counts = LinkedHashMap<String, Int>()
            for (d in entry.docs) counts[d.docId] = (counts[d.docId] ?: pendingCount(d.docId)) - 1
            for ((id, n) in counts) batch[pendingKey(id)] = if (n > 0) n.toString().encodeToByteArray() else null
            for (c in current.orEmpty()) {
                val gone = c.absent || c.body == null
                batch[docKey(c.docId)] = if (gone) null else c.body!!.encodeToByteArray()
                // The source's attachments go back with its body (bytes fetched on read).
                val set = if (gone) emptyList() else c.attachments.toReplicaSet()
                batch[attachKey(c.docId)] = if (set.isEmpty()) null else encodeAttachmentSet(set)
            }
            commit(batch, meta)
        }
    }

    /** The transfer to resume, or a new one from the last complete source commit. */
    public fun beginTransfer(): ProjectionTransfer = meta.transfer ?: ProjectionTransfer(meta.lastSource, "", "")

    /** Applies one page of a transfer from [from], with its cursor, atomically. Returns changed ids. */
    public suspend fun applyPage(from: String, after: String, page: ProjectPage): List<String> =
        lock.withLock {
            val batch = Batch()
            val changed = ArrayList<String>()
            val delivered = HashSet<String>()
            val listed = page.attachments.mapKeys { it.key.lowercase() }
            for (w in page.writes) {
                val id = w.id.lowercase()
                delivered.add(id)
                if (pendingCount(id) > 0) continue
                // Every written document comes with its whole attachment set; absent means none.
                val set = listed[id].toReplicaSet()
                if (get(id) != w.json || attachments(id) != set) changed.add(id)
                batch[docKey(id)] = w.json.encodeToByteArray()
                batch[attachKey(id)] = if (set.isEmpty()) null else encodeAttachmentSet(set)
            }
            for (raw in page.deletes) {
                val id = raw.lowercase()
                if (pendingCount(id) > 0) continue
                if (get(id) != null) changed.add(id)
                batch[docKey(id)] = null
                batch[attachKey(id)] = null
            }
            if (page.reset) {
                // The page stands for every id after the cursor up to its next (to the end when
                // done): what it does not deliver there is no longer in the projection.
                val base = "d/$key/"
                val start = if (after.isNotEmpty()) base + after + "\u0000" else base
                val end = if (page.done) prefixEnd(base) else base + page.next + "\u0000"
                for ((k, _) in store.scan(start, end)) {
                    val id = k.substring(base.length)
                    if (id in delivered || pendingCount(id) > 0) continue
                    batch[k] = null
                    batch[attachKey(id)] = null
                    changed.add(id)
                }
            }
            val next =
                if (page.done) {
                    meta.copy(lastSource = page.atHex, transfer = null)
                } else {
                    meta.copy(transfer = ProjectionTransfer(from, page.atHex, page.next))
                }
            commit(batch, next)
            changed
        }
}
