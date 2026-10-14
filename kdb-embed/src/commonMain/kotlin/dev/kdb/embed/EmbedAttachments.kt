package dev.kdb.embed

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.TraversalEntry
import dev.kdb.document.ATTACHMENT_HASH_SCHEME_CHUNKED_1
import dev.kdb.document.Attachment
import dev.kdb.document.AttachmentRef
import dev.kdb.document.AttachmentSet
import dev.kdb.document.AttachmentTable
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.document.validateAttachmentName
import dev.kdb.transaction.BlobPresence
import dev.kdb.transaction.transactionEngine
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.time.Duration

/*
 * Attachments: binary attachments on documents (docs/kdb-attachments-plan.md) - the Kotlin
 * counterpart of go/kdb/embed/attachments.go. A document has any number; each has a permanent ID and
 * a name unique within the document, and either selects it ([AttachmentSelector]). The bytes live in
 * the namespace's [AttachmentBlobStore]; a commit records which document has which bytes
 * (KdbOp.AttachmentPut), atomically and versioned like everything else, so an as-of read returns the
 * attachment as of that commit.
 */

/** How much attachment history a runtime keeps. */
public enum class AttachmentHistory {
    /** Every version a retained commit references (the default). */
    ALL,

    /** Only what branch and tag heads reference; older versions go at the next [compactAttachments]. */
    LATEST,
}

/** The document has no such attachment (at the commit asked about). */
public class NoSuchAttachmentException(
    message: String,
) : NoSuchElementException(message)

/** A commit references an attachment version whose bytes are no longer kept. */
public class AttachmentNotRetainedException(
    message: String,
) : NoSuchElementException(message)

/** An attachment was written to a document that does not exist. */
public class NoSuchDocumentException(
    message: String,
) : NoSuchElementException(message)

/** One attachment of a document. */
public data class AttachmentInfo(
    val docId: KdbUuid,
    val id: KdbUuid,
    val name: String,
    val contentHash: KdbHash,
    val size: Long,
    val mime: String,
)

private fun Attachment.info(docId: KdbUuid) = AttachmentInfo(docId, id, name, contentHash, size, mime)

/** Picks one attachment of a document, by ID or by name. */
public sealed class AttachmentSelector {
    public data class ById(
        val id: KdbUuid,
    ) : AttachmentSelector()

    public data class ByName(
        val name: String,
    ) : AttachmentSelector()

    internal fun find(set: AttachmentSet): Attachment? =
        when (this) {
            is ById -> set.byId(id)
            is ByName -> set.byName(name)
        }

    override fun toString(): String =
        when (this) {
            is ById -> id.toString()
            is ByName -> "\"$name\""
        }
}

/**
 * Which attachment [putAttachment] writes:
 *  - [id] set: that attachment - replaced if the document has it, created with that ID if not;
 *  - [name] set, no id: the attachment of that name is replaced (keeping its ID), or a new one is
 *    created under the name with a fresh ID;
 *  - neither: a new attachment, with a fresh ID and that ID as its name.
 * [name] alongside an [id] renames it; left null it keeps its name (or, when new, takes the ID's).
 */
public data class PutAttachmentOptions(
    val id: KdbUuid? = null,
    val name: String? = null,
    val mime: String = "",
)

/** What [compactAttachments] did. */
public data class AttachmentMaintenance(
    val removed: Int,
)

/** A pinned, verifying read of one attachment version, with what it is. Close the [reader]. */
public data class AttachmentRead(
    val info: AttachmentInfo,
    val reader: AttachmentReader,
)

internal class AttachmentState {
    val mutex = Mutex()
    val stores = mutableMapOf<String, AttachmentBlobStore>()
    var history = AttachmentHistory.ALL
    var maxBytes = 0L
    var uploadGrace = DEFAULT_ATTACHMENT_UPLOAD_GRACE
}

/** Chooses how much attachment history this runtime keeps. */
public suspend fun EmbeddedKdbRuntime.setAttachmentHistory(history: AttachmentHistory) {
    attachments.mutex.withLock { attachments.history = history }
}

/** Caps one attachment's size for stores opened afterwards; 0 is no limit. */
public suspend fun EmbeddedKdbRuntime.setAttachmentMaxBytes(maxBytes: Long) {
    attachments.mutex.withLock { attachments.maxBytes = maxBytes }
}

/**
 * How long, for stores opened afterwards, an uploaded attachment no commit references yet is kept
 * by [compactAttachments]: the longest an upload may wait for its commit. Default
 * [DEFAULT_ATTACHMENT_UPLOAD_GRACE]; not positive restores it. The hold ends as soon as a commit
 * referencing the attachment is durable.
 */
public suspend fun EmbeddedKdbRuntime.setAttachmentUploadGrace(grace: Duration) {
    attachments.mutex.withLock { attachments.uploadGrace = grace }
}

/** [namespaceId]'s attachment store, opened on first use. */
public suspend fun EmbeddedKdbRuntime.attachmentStore(namespaceId: String = defaultNamespace): AttachmentBlobStore =
    attachments.mutex.withLock {
        attachments.stores.getOrPut(namespaceId) {
            AttachmentBlobStore(attachmentShim, namespaceId, attachments.maxBytes, attachments.uploadGrace)
        }
    }

/** Ends the upload hold on the blobs [commit] puts, once it is durable (commitViaEngine). A
 * failure only leaves the hold to [compactAttachments], which ends it on finding the blob referenced. */
internal suspend fun EmbeddedKdbRuntime.attachmentsCommitted(
    namespaceId: String,
    commit: KdbCommit,
) {
    val hashes = commit.operations.mapNotNull { (it as? KdbOp.AttachmentPut)?.contentHash }
    if (hashes.isNotEmpty()) runCatching { attachmentStore(namespaceId).committed(hashes) }
}

/** The attachment table at [commit] (null: main's head). */
public suspend fun EmbeddedKdbRuntime.attachmentTableAt(commit: KdbHash? = null): AttachmentTable = dag.attachmentTableAt(commit ?: dag.head())

/**
 * Stores [source]'s bytes as one of [docId]'s attachments - which one, [options] says - and commits
 * it. The bytes are durable before the commit that references them is. The document's JSON, its
 * content hash and its other attachments are untouched. Returns the commit and the attachment.
 */
public suspend fun EmbeddedKdbRuntime.putAttachment(
    namespaceId: String,
    docId: KdbUuid,
    source: AttachmentSource,
    options: PutAttachmentOptions = PutAttachmentOptions(),
): Pair<KdbCommit, AttachmentInfo> {
    options.name?.let { validateAttachmentName(it) }
    val blob = attachmentStore(namespaceId).put(source)
    val ref = AttachmentRef(ATTACHMENT_HASH_SCHEME_CHUNKED_1, blob.hash, blob.size, options.mime)
    return putAttachmentRef(namespaceId, docId, options.id, options.name, ref)
}

/** Gives one of [docId]'s attachments a new name, keeping its ID and bytes. */
public suspend fun EmbeddedKdbRuntime.renameAttachment(
    namespaceId: String,
    docId: KdbUuid,
    selector: AttachmentSelector,
    newName: String,
): Pair<KdbCommit, AttachmentInfo> {
    validateAttachmentName(newName)
    val a =
        selector.find(attachmentTableAt().list(docId))
            ?: throw NoSuchAttachmentException("no such attachment: $selector of $docId")
    return putAttachmentRef(namespaceId, docId, a.id, newName, a.ref)
}

private suspend fun EmbeddedKdbRuntime.putAttachmentRef(
    namespaceId: String,
    docId: KdbUuid,
    id: KdbUuid?,
    name: String?,
    ref: AttachmentRef,
): Pair<KdbCommit, AttachmentInfo> {
    lateinit var put: KdbOp.AttachmentPut
    val commit =
        commitAttachmentOps(namespaceId, docId) { set ->
            val existing =
                when {
                    id != null -> set.byId(id)
                    name != null -> set.byName(name)
                    else -> null
                }
            val attId = existing?.id ?: id ?: KdbUuid.random()
            val attName = name ?: existing?.name ?: attId.toString()
            put = KdbOp.AttachmentPut(docId, attId, attName, ref.scheme, ref.contentHash, ref.size, ref.mime)
            listOf(put)
        }
    return commit to AttachmentInfo(docId, put.attachmentId, put.name, put.contentHash, put.size, put.mime)
}

/** Removes one of [docId]'s attachments, leaving the document and its others. */
public suspend fun EmbeddedKdbRuntime.deleteAttachment(
    namespaceId: String,
    docId: KdbUuid,
    selector: AttachmentSelector,
): KdbCommit =
    commitAttachmentOps(namespaceId, docId) { set ->
        val a = selector.find(set) ?: throw NoSuchAttachmentException("no such attachment: $selector of $docId")
        listOf(KdbOp.AttachmentDelete(docId, a.id))
    }

/** Commits on main's head the ops [plan] derives from [docId]'s attachments there, after checking
 * the document exists. Goes through the transaction engine, so preflight and conflict rules apply. */
private suspend fun EmbeddedKdbRuntime.commitAttachmentOps(
    namespaceId: String,
    docId: KdbUuid,
    plan: (AttachmentSet) -> List<KdbOp>,
): KdbCommit {
    val head = dag.head()
    val headCommit = dag.getCommitOrThrow(head)
    storage.getDocument(namespaceId, docId, headCommit.documentTreeHash)
        ?: throw NoSuchDocumentException("no such document: $docId")
    val table = dag.attachmentTableAt(head)
    val ops = plan(table.list(docId))
    table.apply(ops) // the table checks what it can (a name another attachment has) before anything commits
    val store = attachmentStore(namespaceId)
    val engine =
        transactionEngine(
            policyRegistry.get(namespaceId).conflict,
            blobPresence = BlobPresence { store.has(it) },
        )
    val tx = KdbTransaction(KdbUuid.random(), head, ops, KdbTimestamp.now(), KdbUuid.random())
    return commitViaEngine(this, namespaceId, tx, engine = engine, targetHead = head)
}

/**
 * Opens one of [docId]'s attachments as of [asOf] (null: main's head). The reader streams the bytes
 * chunk by chunk, each checked against the content hash, and is pinned to this version until closed.
 */
public suspend fun EmbeddedKdbRuntime.getAttachment(
    namespaceId: String,
    docId: KdbUuid,
    selector: AttachmentSelector,
    asOf: KdbHash? = null,
): AttachmentRead {
    val info = attachmentInfoAt(namespaceId, docId, selector, asOf)
    val reader =
        try {
            attachmentStore(namespaceId).open(info.contentHash)
        } catch (_: AttachmentBlobNotFoundException) {
            throw AttachmentNotRetainedException("attachment version is no longer retained: ${info.contentHash.toHex()} of $docId")
        }
    return AttachmentRead(info, reader)
}

/** Describes one of [docId]'s attachments at [commit] (null: main's head). */
public suspend fun EmbeddedKdbRuntime.attachmentInfoAt(
    namespaceId: String,
    docId: KdbUuid,
    selector: AttachmentSelector,
    commit: KdbHash? = null,
): AttachmentInfo {
    val a =
        selector.find(attachmentTableAt(commit).list(docId))
            ?: throw NoSuchAttachmentException("no such attachment: $selector of $docId")
    return a.info(docId)
}

/** [docId]'s attachments at [commit] (null: main's head), sorted by name. */
public suspend fun EmbeddedKdbRuntime.listAttachments(
    namespaceId: String,
    docId: KdbUuid,
    commit: KdbHash? = null,
): List<AttachmentInfo> = attachmentTableAt(commit).list(docId).map { it.info(docId) }.sortedBy { it.name }

/** How many attachments [docId] has at [commit] (null: main's head). */
public suspend fun EmbeddedKdbRuntime.countAttachments(
    namespaceId: String,
    docId: KdbUuid,
    commit: KdbHash? = null,
): Int = attachmentTableAt(commit).count(docId)

private suspend fun EmbeddedKdbRuntime.heads(): List<KdbHash> = dag.listBranches().map { it.headHash } + dag.listTags().map { it.commitHash }

/** Every content hash a branch or tag head's attachment table references. */
private suspend fun EmbeddedKdbRuntime.headAttachmentHashes(): MutableSet<KdbHash> {
    val keep = mutableSetOf<KdbHash>()
    for (h in heads()) {
        for ((_, a) in dag.attachmentTableAt(h).entries()) keep += a.contentHash
    }
    return keep
}

/** Every content hash any commit reachable from a branch or tag references. */
private suspend fun EmbeddedKdbRuntime.referencedAttachmentHashes(): Set<KdbHash> {
    val keep = headAttachmentHashes()
    val seen = mutableSetOf<KdbHash>()
    for (head in heads()) {
        for (e in dag.walk(head)) {
            val c = (e as? TraversalEntry.Full)?.commit ?: continue
            if (!seen.add(c.hash) || c.attachmentTreeHash == null) continue
            for (op in c.operations) if (op is KdbOp.AttachmentPut) keep += op.contentHash
        }
    }
    return keep
}

/**
 * Deletes the blobs nothing keeps: under [AttachmentHistory.LATEST] anything a branch or tag head
 * does not reference, under [AttachmentHistory.ALL] anything no reachable commit references (an
 * upload never committed) - but neither while it is within its upload grace
 * ([setAttachmentUploadGrace]), its commit perhaps still to come. A blob being read is removed
 * when its reader closes. Safe at any time.
 */
public suspend fun EmbeddedKdbRuntime.compactAttachments(namespaceId: String = defaultNamespace): AttachmentMaintenance {
    val history = attachments.mutex.withLock { attachments.history }
    val keep = if (history == AttachmentHistory.LATEST) headAttachmentHashes() else referencedAttachmentHashes()
    return AttachmentMaintenance(attachmentStore(namespaceId).collect { it in keep })
}
