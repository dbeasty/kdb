package dev.kdb.transaction

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.document.AttachmentException
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.document.attachmentNameProblem
import dev.kdb.document.hasAttachmentOps
import dev.kdb.error.ConflictOperationType
import dev.kdb.error.FieldViolation
import dev.kdb.error.ViolationType
import dev.kdb.storage.StorageAdapter

/*
 * Attachment rules (docs/kdb-attachments-plan.md; Go: go/kdb/transaction/attachments.go):
 *   - an AttachmentPut needs an attachment ID, a valid name unique within its document, its blob
 *     already stored, and its document to exist once the transaction has applied (stored before,
 *     or written in it - not deleted after);
 *   - a change to one attachment conflicts with a concurrent change to that same attachment (its
 *     ID), and with a concurrent delete of the document; a document delete conflicts with a
 *     concurrent change to any of its attachments; changes to different attachments of a document
 *     do not conflict, and a JSON-only write conflicts with none.
 */

/**
 * The optional capability preflight uses to ask whether an attachment blob is stored without
 * reading it - an attachment can be gigabytes. Passed to [transactionEngine], or implemented by the
 * [StorageAdapter]; without either, preflight asks [StorageAdapter.readBlob].
 */
public fun interface BlobPresence {
    public suspend fun hasBlob(contentHash: KdbHash): Boolean
}

internal suspend fun hasAttachmentBlob(
    presence: BlobPresence?,
    storage: StorageAdapter,
    h: KdbHash,
): Boolean {
    val p = presence ?: (storage as? BlobPresence)
    return try {
        if (p != null) p.hasBlob(h) else storage.readBlob(h) != null
    } catch (_: Exception) {
        false
    }
}

private fun attachmentViolation(
    index: Int,
    op: KdbOp,
    detail: String,
): OperationViolation =
    OperationViolation(index, op, listOf(FieldViolation("_attachment", ViolationType.CUSTOM_CONSTRAINT, detail)))

internal suspend fun preflightAttachments(
    tx: KdbTransaction,
    namespaceId: String,
    dag: CommitDag,
    storage: StorageAdapter,
    presence: BlobPresence?,
    targetTreeHash: KdbHash,
    anchorCommit: KdbHash,
): List<OperationViolation> {
    if (!hasAttachmentOps(tx.operations)) return emptyList()
    // A document's presence after the transaction: the target tree, then each op in order.
    val present = mutableMapOf<KdbUuid, Boolean>()
    for (op in tx.operations) {
        when (op) {
            is KdbOp.Write -> present[op.docId] = true
            is KdbOp.Delete -> present[op.docId] = false
            else -> Unit
        }
    }
    suspend fun presentAt(id: KdbUuid): Boolean = present[id] ?: (storage.getDocument(namespaceId, id, targetTreeHash) != null)
    val out = mutableListOf<OperationViolation>()
    for ((index, op) in tx.operations.withIndex()) {
        if (op !is KdbOp.AttachmentPut) continue
        val detail =
            when {
                op.attachmentId == KdbUuid(0, 0) -> "attachment has no id"
                attachmentNameProblem(op.name) != null -> attachmentNameProblem(op.name)
                op.size < 0 -> "attachment size is negative"
                !presentAt(op.docId) -> "no such document: an attachment needs its document"
                !hasAttachmentBlob(presence, storage, op.contentHash) -> "attachment blob not stored: ${op.contentHash.toHex()}"
                else -> null
            }
        if (detail != null) out += attachmentViolation(index, op, detail)
    }
    if (out.isNotEmpty()) return out
    // The rules a table enforces itself - a name unique within its document - checked by applying
    // the operations to the table they land on.
    var table =
        try {
            dag.attachmentTableAt(anchorCommit)
        } catch (e: Exception) {
            return listOf(attachmentViolation(0, tx.operations[0], e.message ?: "attachment table unavailable"))
        }
    for ((index, op) in tx.operations.withIndex()) {
        try {
            table = table.apply(listOf(op))
        } catch (e: AttachmentException) {
            return listOf(attachmentViolation(index, op, e.message ?: "attachment rejected"))
        }
    }
    return emptyList()
}

/** Compares each document the transaction's attachment operations (and document deletes) touch
 * between the transaction's base and the commit it lands on. */
internal suspend fun detectAttachmentConflicts(
    tx: KdbTransaction,
    namespaceId: String,
    dag: CommitDag,
    storage: StorageAdapter,
    baseTreeHash: KdbHash,
    targetTreeHash: KdbHash,
    anchorCommit: KdbHash,
): List<OperationConflict> {
    val touches = hasAttachmentOps(tx.operations)
    if (!touches && tx.operations.none { it is KdbOp.Delete }) return emptyList()
    if (tx.baseVersion == anchorCommit) return emptyList() // nothing landed in between
    val base = dag.attachmentTableAt(tx.baseVersion)
    val target = dag.attachmentTableAt(anchorCommit)
    if (base.size == 0 && target.size == 0 && !touches) return emptyList()

    fun attachmentChanged(
        doc: KdbUuid,
        att: KdbUuid,
    ): Boolean = base.get(doc, att) != target.get(doc, att)

    fun documentChanged(doc: KdbUuid): Boolean = base.list(doc).hash() != target.list(doc).hash()
    val out = mutableListOf<OperationConflict>()
    for ((index, op) in tx.operations.withIndex()) {
        val (id, changed) =
            when (op) {
                is KdbOp.AttachmentPut -> op.docId to attachmentChanged(op.docId, op.attachmentId)
                is KdbOp.AttachmentDelete -> op.docId to attachmentChanged(op.docId, op.attachmentId)
                is KdbOp.Delete -> op.docId to documentChanged(op.docId)
                else -> continue
            }
        val baseDoc = storage.getDocument(namespaceId, id, baseTreeHash)
        val existingDoc = storage.getDocument(namespaceId, id, targetTreeHash)
        when {
            // The attachment moved under us - for an attachment op, or for a delete that would take
            // attachments the caller has not seen.
            changed ->
                out += OperationConflict(index, op, ConflictOperationType.CONCURRENT_WRITE, existingDoc, null, baseDoc)
            // The document was deleted concurrently: its attachment cannot be set or removed.
            op !is KdbOp.Delete && baseDoc != null && existingDoc == null ->
                out += OperationConflict(index, op, ConflictOperationType.WRITE_DELETE, null, null, baseDoc)
        }
    }
    return out
}

/** The document an operation addresses, for conflict reports. */
internal fun KdbOp.addressedDocId(): KdbUuid? =
    when (this) {
        is KdbOp.Write -> docId
        is KdbOp.Delete -> docId
        is KdbOp.AttachmentPut -> docId
        is KdbOp.AttachmentDelete -> docId
        else -> null
    }
