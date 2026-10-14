package dev.kdb.replica

import dev.kdb.codec.KdbHash
import dev.kdb.document.ATTACHMENT_HASH_SCHEME_CHUNKED_1
import dev.kdb.document.Attachment
import dev.kdb.document.AttachmentException
import dev.kdb.document.AttachmentRef
import dev.kdb.document.AttachmentSet
import dev.kdb.document.AttachmentTable
import dev.kdb.document.KdbOp
import dev.kdb.document.validateAttachmentName
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.ListSerializer

/**
 * Attachments (docs/kdb-attachments-plan.md; replica side in docs/kdb-client-replica-plan.md §9),
 * a port of packages/kdb-replica/src/attachments.ts.
 *
 * A record has a set of named attachments. A commit carries attachment ops and the root of its
 * attachment table: a second trie, hashed like the document trie, mapping each document id to the
 * hash of its attachment set. The replica keeps that table in the store (beside the document trie,
 * node by node, so it never has to be resident) and verifies every root it is given, as it does
 * tree hashes. The bytes themselves are fetched lazily, by BLOB_FETCH ([BlobStore]).
 *
 * The hashes are kdb-document's ([AttachmentSet.hash], [AttachmentRef.hash], the scheme-1 content
 * hash), which are Go's byte for byte; only the table's storage is the replica's own.
 */

/** One attachment of a document, as the replica holds and reports it. */
@Serializable
public data class ReplicaAttachment(
    /** The attachment's permanent id (lowercase UUID). */
    val id: String,
    val name: String,
    val scheme: Int = ATTACHMENT_HASH_SCHEME_CHUNKED_1,
    /** Hex of the content hash. */
    val contentHash: String,
    val size: Long,
    val mime: String = "",
)

/** An attachment as PROJECT_WRITE, PROJECT_PAGE and TREE_NODES list it (Go's wire.TreeAttachment). */
@Serializable
public data class WireAttachment(
    val id: String,
    val name: String,
    val hash: String,
    val size: Long,
    val mime: String = "",
)

internal fun ReplicaAttachment.toWire(): WireAttachment = WireAttachment(id, name, contentHash, size, mime)

internal fun WireAttachment.toReplica(): ReplicaAttachment =
    ReplicaAttachment(id.lowercase(), name, ATTACHMENT_HASH_SCHEME_CHUNKED_1, hash.lowercase(), size, mime)

internal fun List<WireAttachment>?.toReplicaSet(): List<ReplicaAttachment> = sortAttachments(orEmpty().map { it.toReplica() })

/** Sorted by id: unsigned byte order, which for lowercase canonical UUID strings is string order. */
internal fun sortAttachments(set: List<ReplicaAttachment>): List<ReplicaAttachment> = set.sortedBy { it.id }

internal fun ReplicaAttachment.toDocument(): Attachment =
    Attachment(parseUuid(id), name, AttachmentRef(scheme, KdbHash.fromBytes(hex32(contentHash)), size, mime))

internal fun Attachment.toReplica(): ReplicaAttachment =
    ReplicaAttachment(id.toString(), name, scheme, contentHash.toHex(), size, mime)

/** The set hash (kdb-document's [AttachmentSet.hash], Go's SetHash) of a document's attachments. */
public fun attachmentSetHash(set: List<ReplicaAttachment>): String = AttachmentSet.of(set.map { it.toDocument() }).hash().toHex()

/** The hash a write-back names a document's attachment set by: "" for none. */
internal fun baseSetHash(set: List<ReplicaAttachment>): String = if (set.isEmpty()) "" else attachmentSetHash(set)

private val setSerializer = ListSerializer(ReplicaAttachment.serializer())

internal fun encodeAttachmentSet(set: List<ReplicaAttachment>): ByteArray =
    metaJson.encodeToString(setSerializer, set).encodeToByteArray()

internal fun decodeAttachmentSet(b: ByteArray): List<ReplicaAttachment> = metaJson.decodeFromString(setSerializer, b.decodeToString())

/** The table's trie lives beside the document trie; "%att" cannot occur in an encoded name. */
internal fun attachmentTrieKey(nsKey: String): String = "$nsKey%att"

internal fun attachmentSetKey(nsKey: String, docId: String): String = "a/$nsKey/${docId.lowercase()}"

/** A document's attachments as the store holds them. */
internal suspend fun attachmentsOf(store: ReplicaStore, nsKey: String, docId: String): List<ReplicaAttachment> =
    store.get(attachmentSetKey(nsKey, docId))?.let(::decodeAttachmentSet).orEmpty()

/** The attachment table's current root, null when empty (as a commit states it). */
internal suspend fun attachmentRootOf(store: ReplicaStore, nsKey: String): String? =
    TrieReader(store, attachmentTrieKey(nsKey)).rootHash().takeUnless { it == ZERO_HEX }

private val ZERO_HEX = "0".repeat(64)

/**
 * An edit of one namespace's attachment table: ops applied in order with Go's
 * AttachmentTable.Apply semantics (a put adds or replaces by id and refuses a name another
 * attachment of the document has; a delete removes one; a document delete removes them all), then
 * the trie updated and the root computed. Writes reach the store only through [writeTo].
 */
internal class AttachmentEdit(private val store: ReplicaStore, private val nsKey: String) {
    private val sets = LinkedHashMap<String, List<ReplicaAttachment>>()
    private var trie: TrieEdit? = null

    private suspend fun load(docId: String): List<ReplicaAttachment> {
        val id = docId.lowercase()
        return sets[id] ?: attachmentsOf(store, nsKey, id).also { sets[id] = it }
    }

    /** Applies the attachment effect of [ops], in order; other ops are ignored. */
    suspend fun apply(ops: Iterable<KdbOp>) {
        for (op in ops) {
            when (op) {
                is KdbOp.AttachmentPut -> {
                    if (op.scheme != ATTACHMENT_HASH_SCHEME_CHUNKED_1) {
                        throw AttachmentException("unknown content hash scheme ${op.scheme}")
                    }
                    validateAttachmentName(op.name)
                    if (op.size < 0) throw AttachmentException("attachment ${op.attachmentId}: negative size ${op.size}")
                    val doc = op.docId.toString()
                    val attId = op.attachmentId.toString()
                    val set = load(doc)
                    val clash = set.firstOrNull { it.name == op.name && it.id != attId }
                    if (clash != null) {
                        throw AttachmentException("attachment name \"${op.name}\" is already used by attachment ${clash.id}")
                    }
                    val a = ReplicaAttachment(attId, op.name, op.scheme, op.contentHash.toHex(), op.size, op.mime)
                    sets[doc] = sortAttachments(set.filter { it.id != attId } + a)
                }
                is KdbOp.AttachmentDelete -> {
                    val doc = op.docId.toString()
                    val attId = op.attachmentId.toString()
                    sets[doc] = load(doc).filter { it.id != attId }
                }
                is KdbOp.Delete -> {
                    val doc = op.docId.toString()
                    load(doc)
                    sets[doc] = emptyList()
                }
                else -> {}
            }
        }
    }

    /** Replaces a document's whole set (a snapshot or a diff that states final sets). */
    fun set(docId: String, set: List<ReplicaAttachment>) {
        sets[docId.lowercase()] = sortAttachments(set)
    }

    /** Every document the store holds a set for: what a whole-table replacement must clear. */
    suspend fun heldDocuments(): List<String> {
        val base = "a/$nsKey/"
        return store.scan(base, prefixEnd(base)).map { it.first.substring(base.length) }
    }

    /** The table's root after the edit: null when the table is empty, as a commit states it. */
    suspend fun finish(): String? {
        val t = TrieEdit(store, attachmentTrieKey(nsKey))
        for ((id, set) in sets) {
            if (set.isEmpty()) t.delete(id) else t.set(id, attachmentSetHash(set))
        }
        trie = t
        return t.finish().takeUnless { it == ZERO_HEX }
    }

    fun writeTo(batch: Batch) {
        val t = trie ?: error("kdb replica: finish the attachment edit before writing it")
        t.writeTo(batch)
        for ((id, set) in sets) batch[attachmentSetKey(nsKey, id)] = if (set.isEmpty()) null else encodeAttachmentSet(set)
    }
}

/**
 * The first attachment table of a packed list (go/kdb/document AppendPackedAttachmentTables), as a
 * snapshot's last page carries the snapshot commit's; parsed by kdb-document's
 * [AttachmentTable.parsePacked].
 */
internal fun parsePackedAttachmentTable(packed: ByteArray): Map<String, List<ReplicaAttachment>> {
    val table = AttachmentTable.parsePacked(packed).firstOrNull() ?: return emptyMap()
    val out = LinkedHashMap<String, MutableList<ReplicaAttachment>>()
    for ((doc, a) in table.entries()) out.getOrPut(doc.toString()) { ArrayList() }.add(a.toReplica())
    return out.mapValues { sortAttachments(it.value) }
}

/** Which of a namespace's two tries [ReplicaNamespace.treeNode] reads. */
public enum class TreeKind { DOCUMENTS, ATTACHMENTS }

/** A commit's attachment root is not the table the replica built (or could build) for it. */
public class AttachmentRootMismatchException(
    public val commit: String,
    public val declared: String?,
    public val built: String?,
) : RuntimeException("kdb replica: commit $commit declares attachment root ${declared ?: "(none)"}, the replica has ${built ?: "(none)"}")
