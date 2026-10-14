package dev.kdb.document

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid

/**
 * Attachments: binary attachments on a document (docs/kdb-attachments-plan.md). A mirror of
 * go/kdb/document/attachment_table.go - every hash here must match Go's byte for byte.
 *
 * A document has any number of attachments, each with a permanent ID and a name unique within the
 * document. A namespace's attachments at a commit are an [AttachmentTable]: a [DocumentTree] (the
 * same trie, so the same hashing as Go's) mapping each document id to its [AttachmentSet]'s hash.
 * Its [AttachmentTable.root] is the commit's `attachmentTreeHash`.
 */
public const val MAX_ATTACHMENT_NAME_BYTES: Int = 255

/** An attachment rule was broken: a bad name, a duplicate name, a missing id, an unknown scheme. */
public class AttachmentException(
    message: String,
) : IllegalArgumentException(message)

/** Why [name] is not a valid attachment name (1 to 255 bytes of UTF-8, no control characters), or
 * null when it is. */
public fun attachmentNameProblem(name: String): String? {
    if (name.isEmpty()) return "attachment name is empty"
    var i = 0
    while (i < name.length) {
        val c = name[i]
        if (c.isHighSurrogate()) {
            if (i + 1 >= name.length || !name[i + 1].isLowSurrogate()) return "attachment name is not valid UTF-8"
            i += 2
            continue
        }
        if (c.isLowSurrogate()) return "attachment name is not valid UTF-8"
        if (c.code < 0x20 || c.code == 0x7f) return "attachment name contains a control character"
        i++
    }
    val bytes = name.encodeToByteArray().size
    if (bytes > MAX_ATTACHMENT_NAME_BYTES) return "attachment name is $bytes bytes, over $MAX_ATTACHMENT_NAME_BYTES"
    return null
}

/** Throws [AttachmentException] unless [name] is a valid attachment name. */
public fun validateAttachmentName(name: String) {
    attachmentNameProblem(name)?.let { throw AttachmentException(it) }
}

/** One attachment's bytes: how its content hash was computed, the hash, its length, its media type. */
public data class AttachmentRef(
    val scheme: Int,
    val contentHash: KdbHash,
    val size: Long,
    val mime: String,
) {
    /** SHA-256('A' | scheme | contentHash | be64 size | be32 len(mime) | mime). */
    public fun hash(): KdbHash {
        val m = mime.encodeToByteArray()
        val b = ByteArray(2 + 32 + 8 + 4 + m.size)
        b[0] = 'A'.code.toByte()
        b[1] = scheme.toByte()
        contentHash.bytes.copyInto(b, 2)
        putBe64(b, 34, size)
        putBe32(b, 42, m.size)
        m.copyInto(b, 46)
        return KdbHash(kdbSha256(b))
    }
}

/** One attachment of a document: its permanent [id], its [name] and its bytes' [ref]. */
public data class Attachment(
    val id: KdbUuid,
    val name: String,
    val ref: AttachmentRef,
) {
    val scheme: Int get() = ref.scheme
    val contentHash: KdbHash get() = ref.contentHash
    val size: Long get() = ref.size
    val mime: String get() = ref.mime
}

/** Unsigned (msb, lsb) order - Go's uuidLess, and the trie's nibble order. */
public val ATTACHMENT_ID_ORDER: Comparator<KdbUuid> =
    Comparator { a, b ->
        val c = a.msb.toULong().compareTo(b.msb.toULong())
        if (c != 0) c else a.lsb.toULong().compareTo(b.lsb.toULong())
    }

/** One document's attachments, sorted by ID. Immutable. */
public class AttachmentSet internal constructor(
    public val attachments: List<Attachment>,
) : List<Attachment> by attachments {
    /** SHA-256 of 'S' then, per attachment in ID order: id (16, big-endian) | be32 len(name) | name |
     * ref hash. */
    public fun hash(): KdbHash {
        val parts = ArrayList<ByteArray>(attachments.size)
        var total = 1
        for (a in attachments) {
            val n = a.name.encodeToByteArray()
            val b = ByteArray(16 + 4 + n.size + 32)
            putBe64(b, 0, a.id.msb)
            putBe64(b, 8, a.id.lsb)
            putBe32(b, 16, n.size)
            n.copyInto(b, 20)
            a.ref.hash().bytes.copyInto(b, 20 + n.size)
            parts.add(b)
            total += b.size
        }
        val out = ByteArray(total)
        out[0] = 'S'.code.toByte()
        var off = 1
        for (p in parts) {
            p.copyInto(out, off)
            off += p.size
        }
        return KdbHash(kdbSha256(out))
    }

    private fun find(id: KdbUuid): Int = attachments.binarySearch { ATTACHMENT_ID_ORDER.compare(it.id, id) }

    /** The attachment with [id]. */
    public fun byId(id: KdbUuid): Attachment? = find(id).takeIf { it >= 0 }?.let { attachments[it] }

    /** The attachment called [name]. */
    public fun byName(name: String): Attachment? = attachments.firstOrNull { it.name == name }

    /** This set with [a] set, replacing the attachment of a.id; a name another attachment already
     * has is an error. */
    internal fun with(a: Attachment): AttachmentSet {
        val other = byName(a.name)
        if (other != null && other.id != a.id) {
            throw AttachmentException("attachment name \"${a.name}\" is already used by attachment ${other.id}")
        }
        val i = find(a.id)
        val out = ArrayList<Attachment>(attachments.size + 1)
        if (i >= 0) {
            out.addAll(attachments)
            out[i] = a
        } else {
            val at = -i - 1
            out.addAll(attachments.subList(0, at))
            out.add(a)
            out.addAll(attachments.subList(at, attachments.size))
        }
        return AttachmentSet(out)
    }

    /** This set without [id], or null when it was not there. */
    internal fun without(id: KdbUuid): AttachmentSet? {
        val i = find(id)
        if (i < 0) return null
        return AttachmentSet(attachments.filterIndexed { j, _ -> j != i })
    }

    override fun equals(other: Any?): Boolean = other is AttachmentSet && other.attachments == attachments

    override fun hashCode(): Int = attachments.hashCode()

    override fun toString(): String = attachments.toString()

    public companion object {
        public val EMPTY: AttachmentSet = AttachmentSet(emptyList())

        /** A set of [attachments] (any order); duplicate IDs or names are an error. */
        public fun of(attachments: Iterable<Attachment>): AttachmentSet {
            var s = EMPTY
            for (a in attachments) {
                if (s.byId(a.id) != null) throw AttachmentException("attachment ${a.id} appears twice")
                s = s.with(a)
            }
            return s
        }
    }
}

/**
 * A namespace's attachments at one commit. Immutable: [put], [delete], [deleteAll] and [apply]
 * return a new table and leave this one as it was.
 */
public class AttachmentTable private constructor(
    private val tree: DocumentTree, // docId -> AttachmentSet.hash()
    // docId -> its set, shared between versions of the table (UuidHamt): a change copies one path,
    // so a cached table costs what changed, not every document again. Go keeps a set pool by hash
    // instead, spilled to the blob store; nothing here grows with history, so none is needed.
    private val sets: UuidHamt<AttachmentSet>,
    /** The number of attachments, across all documents. */
    public val size: Int,
) {
    /** The number of documents with at least one attachment. */
    public val documents: Int get() = sets.size

    /** The commit's attachmentTreeHash for this table: null when it is empty, so a commit with no
     * attachments carries no field and hashes as it always did. */
    public val root: KdbHash? get() = if (sets.size == 0) null else tree.treeHash

    /** The documents with attachments. */
    public val documentIds: Set<KdbUuid> get() = sets.keys()

    /** [docId]'s attachments, in ID order (empty when it has none). */
    public fun list(docId: KdbUuid): AttachmentSet = sets[docId] ?: AttachmentSet.EMPTY

    public fun count(docId: KdbUuid): Int = list(docId).size

    public fun get(
        docId: KdbUuid,
        id: KdbUuid,
    ): Attachment? = sets[docId]?.byId(id)

    public fun getByName(
        docId: KdbUuid,
        name: String,
    ): Attachment? = sets[docId]?.byName(name)

    private fun setDoc(
        docId: KdbUuid,
        before: AttachmentSet,
        after: AttachmentSet,
    ): AttachmentTable =
        if (after.isEmpty()) {
            AttachmentTable(tree.without(docId), sets.remove(docId), size - before.size)
        } else {
            AttachmentTable(tree.with(docId, after.hash()), sets.put(docId, after), size - before.size + after.size)
        }

    /** The table with [docId]'s attachment a.id set to [a] - created, or replaced (bytes and name). */
    public fun put(
        docId: KdbUuid,
        a: Attachment,
    ): AttachmentTable {
        if (a.id == NIL_UUID) throw AttachmentException("attachment of $docId has no id")
        validateAttachmentName(a.name)
        if (a.size < 0) throw AttachmentException("attachment ${a.id} of $docId: negative size ${a.size}")
        if (a.scheme != ATTACHMENT_HASH_SCHEME_CHUNKED_1) {
            throw AttachmentException("attachment ${a.id} of $docId: unknown content hash scheme ${a.scheme}")
        }
        val before = list(docId)
        return setDoc(docId, before, before.with(a))
    }

    /** The table without [docId]'s attachment [id]; deleting one that is not there is a no-op. */
    public fun delete(
        docId: KdbUuid,
        id: KdbUuid,
    ): AttachmentTable {
        val before = list(docId)
        val after = before.without(id) ?: return this
        return setDoc(docId, before, after)
    }

    /** The table without any of [docId]'s attachments. */
    public fun deleteAll(docId: KdbUuid): AttachmentTable {
        val before = list(docId)
        if (before.isEmpty()) return this
        return setDoc(docId, before, AttachmentSet.EMPTY)
    }

    /** Every (docId, attachment), documents in unsigned id order (the trie's), each in ID order. */
    public fun entries(): Sequence<Pair<KdbUuid, Attachment>> =
        sets.keys().sortedWith(ATTACHMENT_ID_ORDER).asSequence().flatMap { d -> sets[d]!!.asSequence().map { d to it } }

    /**
     * The table after [ops]: an AttachmentPut creates or replaces one attachment, an
     * AttachmentDelete removes one, a document Delete removes all the document's. Other ops do not
     * touch it. Order matters as written.
     */
    public fun apply(ops: List<KdbOp>): AttachmentTable {
        var t = this
        for (op in ops) {
            t =
                when (op) {
                    is KdbOp.AttachmentPut ->
                        t.put(op.docId, Attachment(op.attachmentId, op.name, AttachmentRef(op.scheme, op.contentHash, op.size, op.mime)))
                    is KdbOp.AttachmentDelete -> t.delete(op.docId, op.attachmentId)
                    is KdbOp.Delete -> t.deleteAll(op.docId)
                    else -> t
                }
        }
        return t
    }

    override fun equals(other: Any?): Boolean = other is AttachmentTable && other.root == root

    override fun hashCode(): Int = root?.hashCode() ?: 0

    override fun toString(): String = "AttachmentTable(root=${root?.toHex()}, size=$size, documents=$documents)"

    public companion object {
        public val EMPTY: AttachmentTable = AttachmentTable(DocumentTree.EMPTY, UuidHamt.empty(), 0)

        /**
         * Packs tables for a checkpoint (Go's AppendPackedAttachmentTables): a uvarint count, then per
         * table a uvarint attachment count and its attachments (doc id 16 | attachment id 16 | name
         * length uvarint | name | scheme 1 | content hash 32 | size u64 big-endian | mime length
         * uvarint | mime). Empty tables are skipped.
         */
        public fun pack(tables: List<AttachmentTable>): ByteArray {
            val out = ByteBuf()
            val nonEmpty = tables.filter { it.size > 0 }
            out.uvarint(nonEmpty.size.toLong())
            for (t in nonEmpty) {
                out.uvarint(t.size.toLong())
                for ((doc, a) in t.entries()) {
                    out.be64(doc.msb)
                    out.be64(doc.lsb)
                    out.be64(a.id.msb)
                    out.be64(a.id.lsb)
                    val n = a.name.encodeToByteArray()
                    out.uvarint(n.size.toLong())
                    out.bytes(n)
                    out.byte(a.scheme)
                    out.bytes(a.contentHash.bytes)
                    out.be64(a.size)
                    val m = a.mime.encodeToByteArray()
                    out.uvarint(m.size.toLong())
                    out.bytes(m)
                }
            }
            return out.toByteArray()
        }

        /** Reverses [pack]. */
        public fun parsePacked(packed: ByteArray): List<AttachmentTable> {
            if (packed.isEmpty()) return emptyList()
            val r = ByteReader(packed)
            val count = r.uvarint()
            val out = ArrayList<AttachmentTable>()
            for (i in 0 until count) {
                val entries = r.uvarint()
                val byDoc = LinkedHashMap<KdbUuid, AttachmentSet>()
                for (j in 0 until entries) {
                    val doc = KdbUuid(r.be64(), r.be64())
                    val attId = KdbUuid(r.be64(), r.be64())
                    val name = r.string()
                    val scheme = r.byte()
                    val hash = KdbHash(r.bytes(32))
                    val size = r.be64()
                    val mime = r.string()
                    attachmentNameProblem(name)?.let { throw AttachmentException("attachment tables: $it") }
                    val set = byDoc[doc] ?: AttachmentSet.EMPTY
                    byDoc[doc] = set.with(Attachment(attId, name, AttachmentRef(scheme, hash, size, mime)))
                }
                var t = EMPTY
                for ((doc, set) in byDoc) t = t.setDoc(doc, AttachmentSet.EMPTY, set)
                out.add(t)
            }
            if (!r.atEnd()) throw AttachmentException("attachment tables: ${r.remaining()} trailing bytes")
            return out
        }
    }
}

/** Whether applying [ops] could change a table: an attachment op, or a document delete when the
 * table has entries. */
public fun touchesAttachments(
    ops: List<KdbOp>,
    tableNonEmpty: Boolean,
): Boolean =
    ops.any {
        it is KdbOp.AttachmentPut || it is KdbOp.AttachmentDelete || (it is KdbOp.Delete && tableNonEmpty)
    }

/** Whether [ops] carry an attachment op at all. */
public fun hasAttachmentOps(ops: List<KdbOp>): Boolean = ops.any { it is KdbOp.AttachmentPut || it is KdbOp.AttachmentDelete }

private val NIL_UUID = KdbUuid(0, 0)

private fun putBe64(
    b: ByteArray,
    off: Int,
    v: Long,
) {
    for (i in 0 until 8) b[off + i] = (v ushr (56 - 8 * i)).toByte()
}

private fun putBe32(
    b: ByteArray,
    off: Int,
    v: Int,
) {
    for (i in 0 until 4) b[off + i] = (v ushr (24 - 8 * i)).toByte()
}

private class ByteBuf {
    private var buf = ByteArray(256)
    private var n = 0

    private fun ensure(k: Int) {
        if (n + k <= buf.size) return
        var cap = buf.size * 2
        while (cap < n + k) cap *= 2
        buf = buf.copyOf(cap)
    }

    fun byte(v: Int) {
        ensure(1)
        buf[n++] = v.toByte()
    }

    fun bytes(b: ByteArray) {
        ensure(b.size)
        b.copyInto(buf, n)
        n += b.size
    }

    fun be64(v: Long) {
        ensure(8)
        putBe64(buf, n, v)
        n += 8
    }

    fun uvarint(v: Long) {
        var x = v
        while ((x and 0x7fL.inv()) != 0L) {
            byte(((x and 0x7f) or 0x80).toInt())
            x = x ushr 7
        }
        byte(x.toInt())
    }

    fun toByteArray(): ByteArray = buf.copyOf(n)
}

private class ByteReader(
    private val b: ByteArray,
) {
    private var off = 0

    fun atEnd(): Boolean = off == b.size

    fun remaining(): Int = b.size - off

    private fun need(k: Int) {
        if (b.size - off < k) throw AttachmentException("attachment tables: truncated at $off")
    }

    fun uvarint(): Long {
        var x = 0L
        var shift = 0
        while (true) {
            need(1)
            val c = b[off++].toInt() and 0xff
            if (shift == 63 && c > 1) throw AttachmentException("attachment tables: malformed count at $off")
            x = x or ((c and 0x7f).toLong() shl shift)
            if (c < 0x80) return x
            shift += 7
            if (shift > 63) throw AttachmentException("attachment tables: malformed count at $off")
        }
    }

    fun byte(): Int {
        need(1)
        return b[off++].toInt() and 0xff
    }

    fun bytes(k: Int): ByteArray {
        need(k)
        val out = b.copyOfRange(off, off + k)
        off += k
        return out
    }

    fun be64(): Long {
        need(8)
        var v = 0L
        for (i in 0 until 8) v = (v shl 8) or (b[off + i].toLong() and 0xff)
        off += 8
        return v
    }

    fun string(): String {
        val l = uvarint()
        if (l < 0 || l > remaining()) throw AttachmentException("attachment tables: string truncated at $off")
        return bytes(l.toInt()).decodeToString()
    }
}
