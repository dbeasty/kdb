package dev.kdb.dag

import dev.kdb.codec.KdbUuid
import dev.kdb.document.ATTACHMENT_ID_ORDER
import dev.kdb.document.Attachment
import dev.kdb.document.AttachmentException
import dev.kdb.document.AttachmentSet
import dev.kdb.document.AttachmentTable
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbOp
import dev.kdb.document.MAX_ATTACHMENT_NAME_BYTES

/*
 * Merging attachment tables - a port of go/kdb/dag/attachment_merge.go, which must stay exact: the
 * merge ops are part of the merge commit, so any difference is a different commit hash.
 *
 * A commit's table is its first parent's with its operations applied, so a two-parent commit
 * carries the attachment operations that turn its first parent's table into a three-way merge of
 * both parents' tables against their merge base (the first by hash; an empty table when there is
 * none). Per attachment ID:
 *   - a side that left an attachment as the base had it takes the other side's version;
 *   - both sides changing it differently: a version beats a removal, and between two versions the
 *     greater (content hash bytes, name, size, mime) wins;
 *   - two attachments of a document ending with one name keep it on the smaller ID; the other is
 *     renamed "<name>~<first 8 hex of its ID>".
 * A document absent from the merged document tree keeps none; a document the commit's own
 * attachment ops touch is left to them. Ops: documents in ID order, per document deletes (removals
 * and renames) then puts in ID order; appended after the commit's own ops.
 */

/** The attachment ops a two-parent commit needs beyond [ownOps]. */
internal fun mergeAttachmentOps(
    ownOps: List<KdbOp>,
    base: AttachmentTable,
    ours: AttachmentTable,
    theirs: AttachmentTable,
    tree: DocumentTree,
): List<KdbOp> {
    val decided = mutableSetOf<KdbUuid>()
    for (op in ownOps) {
        when (op) {
            is KdbOp.AttachmentPut -> decided += op.docId
            is KdbOp.AttachmentDelete -> decided += op.docId
            else -> Unit
        }
    }
    val docs =
        (ours.documentIds + theirs.documentIds)
            .filter { ours.list(it) != theirs.list(it) && it !in decided }
            .sortedWith(ATTACHMENT_ID_ORDER)
    val ops = mutableListOf<KdbOp>()
    for (doc in docs) {
        val merged =
            if (tree.contains(doc)) mergeAttachmentSets(base.list(doc), ours.list(doc), theirs.list(doc)) else emptyList()
        ops += attachmentSetOps(doc, ours.list(doc), merged)
    }
    return ops
}

private fun compareBytes(
    a: ByteArray,
    b: ByteArray,
): Int {
    for (i in 0 until minOf(a.size, b.size)) {
        val c = (a[i].toInt() and 0xff) - (b[i].toInt() and 0xff)
        if (c != 0) return c
    }
    return a.size - b.size
}

/** Go's string order: byte-wise over UTF-8. */
private fun compareUtf8(
    a: String,
    b: String,
): Int = compareBytes(a.encodeToByteArray(), b.encodeToByteArray())

/** Orders two conflicting versions deterministically (Go's attachmentGreater). */
private fun attachmentGreater(
    a: Attachment,
    b: Attachment,
): Boolean {
    compareBytes(a.contentHash.bytes, b.contentHash.bytes).let { if (it != 0) return it > 0 }
    if (a.name != b.name) return compareUtf8(a.name, b.name) > 0
    if (a.size != b.size) return a.size > b.size
    return compareUtf8(a.mime, b.mime) > 0
}

/** Merges one document's attachments three-way. */
internal fun mergeAttachmentSets(
    base: AttachmentSet,
    ours: AttachmentSet,
    theirs: AttachmentSet,
): List<Attachment> {
    val b = base.associateBy { it.id }
    val o = ours.associateBy { it.id }
    val t = theirs.associateBy { it.id }
    val out = mutableListOf<Attachment>()
    for (id in b.keys + o.keys + t.keys) {
        val bv = b[id]
        val ov = o[id]
        val tv = t[id]
        val keep: Attachment? =
            when {
                ov == tv -> ov
                ov == bv -> tv // ours left it alone: theirs decides
                tv == bv -> ov
                ov == null -> tv // a version beats a removal
                tv == null -> ov
                attachmentGreater(ov, tv) -> ov
                else -> tv
            }
        if (keep != null) out += keep
    }
    // Names unique: the smaller ID keeps a contested name.
    out.sortWith { x, y -> ATTACHMENT_ID_ORDER.compare(x.id, y.id) }
    val taken = mutableSetOf<String>()
    for (i in out.indices) {
        if (out[i].name in taken) {
            val suffix = "~" + out[i].id.toString().substring(0, 8)
            var name = out[i].name
            val nameBytes = name.encodeToByteArray()
            if (nameBytes.size + suffix.length > MAX_ATTACHMENT_NAME_BYTES) {
                // Go cuts bytes; a cut inside a character is invalid UTF-8, which Go's table refuses.
                name =
                    try {
                        nameBytes.copyOf(MAX_ATTACHMENT_NAME_BYTES - suffix.length).decodeToString(throwOnInvalidSequence = true)
                    } catch (e: CharacterCodingException) {
                        throw AttachmentException("attachment name is not valid UTF-8")
                    }
            }
            out[i] = out[i].copy(name = name + suffix)
        }
        taken += out[i].name
    }
    return out
}

/** The ops that turn [doc]'s attachments from [before] into [after]: removals and renames first (as
 * deletes), so no put lands on a name still taken, then the puts, in ID order. */
internal fun attachmentSetOps(
    doc: KdbUuid,
    before: AttachmentSet,
    after: List<Attachment>,
): List<KdbOp> {
    val prev = before.associateBy { it.id }
    val next = after.associateBy { it.id }
    val dels = mutableListOf<KdbOp>()
    for (a in before) {
        val n = next[a.id]
        if (n == null || n.name != a.name) dels += KdbOp.AttachmentDelete(doc, a.id)
    }
    val puts = mutableListOf<KdbOp>()
    for (a in after.sortedWith { x, y -> ATTACHMENT_ID_ORDER.compare(x.id, y.id) }) {
        if (prev[a.id] == a) continue
        puts += KdbOp.AttachmentPut(doc, a.id, a.name, a.scheme, a.contentHash, a.size, a.mime)
    }
    return dels + puts
}
