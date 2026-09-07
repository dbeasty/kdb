package dev.kdb.server

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.document.KdbDocument
import dev.kdb.json.JsonValue
import dev.kdb.json.kdbJsonGet
import dev.kdb.policy.DocumentExpiryPolicy
import dev.kdb.storage.StorageAdapter
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

/**
 * Layer 16 §9.5 read-side expiry predicate: true when the value at `$.<fieldPath>` is a timestamp
 * `<= nowMillis - graceMillis`. An RFC 3339 string or a number of epoch milliseconds are timestamps;
 * anything else (absent, null, unparsable, boolean, object) means "never expires".
 */
public fun isDocumentExpired(
    json: String,
    policy: DocumentExpiryPolicy,
    nowMillis: Long,
): Boolean {
    val raw =
        try {
            kdbJsonGet(json, "$." + policy.fieldPath)
        } catch (_: Throwable) {
            return false
        }
    val ts = expiryTimestampMillis(raw) ?: return false
    return ts <= nowMillis - policy.graceMillis
}

private fun expiryTimestampMillis(v: JsonValue?): Long? =
    when (v) {
        is JsonValue.JInt -> v.value
        is JsonValue.JNumber -> if (v.value.isFinite()) v.value.toLong() else null
        is JsonValue.JString ->
            try {
                OffsetDateTime.parse(v.value, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant().toEpochMilli()
            } catch (_: Throwable) {
                null
            }
        else -> null
    }

/**
 * A [StorageAdapter] that hides expired documents from reads at the current head (the head commit's
 * document tree, or the head commit hash itself - callers use both spellings) and passes every
 * historical read through untouched. This is how the SQL/hybrid engine - which kdb-server cannot
 * change - honours expiry between sweeps: [KdbServerRuntime.hybridFor] builds an engine over this
 * adapter for a namespace whose policy declares `documentExpiry`. Writes delegate unchanged.
 */
public class ExpiryFilteringStorageAdapter(
    private val delegate: StorageAdapter,
    private val dag: CommitDag,
    private val policy: DocumentExpiryPolicy,
    private val nowMillis: () -> Long,
) : StorageAdapter by delegate {

    private suspend fun isHeadRead(atCommit: KdbHash): Boolean {
        val head = dag.head()
        if (atCommit == head) return true
        val commit = dag.getCommitOrThrow(head)
        return atCommit == commit.documentTreeHash
    }

    private fun expired(doc: KdbDocument): Boolean = isDocumentExpired(doc.json, policy, nowMillis())

    override suspend fun getDocument(
        namespaceId: String,
        docId: KdbUuid,
        atCommit: KdbHash,
    ): KdbDocument? {
        val doc = delegate.getDocument(namespaceId, docId, atCommit) ?: return null
        return if (isHeadRead(atCommit) && expired(doc)) null else doc
    }

    override suspend fun getDocumentOrThrow(
        namespaceId: String,
        docId: KdbUuid,
        atCommit: KdbHash,
    ): KdbDocument {
        val doc = delegate.getDocumentOrThrow(namespaceId, docId, atCommit)
        if (isHeadRead(atCommit) && expired(doc)) {
            throw dev.kdb.storage.DocumentNotFoundException("missing document $docId", namespaceId, docId, atCommit)
        }
        return doc
    }

    override suspend fun getDocuments(
        namespaceId: String,
        docIds: List<KdbUuid>,
        atCommit: KdbHash,
    ): List<KdbDocument?> {
        val docs = delegate.getDocuments(namespaceId, docIds, atCommit)
        if (!isHeadRead(atCommit)) return docs
        return docs.map { d -> if (d != null && expired(d)) null else d }
    }

    override suspend fun scanDocuments(
        namespaceId: String,
        atCommit: KdbHash,
        batchSize: Int,
        onBatch: suspend (List<KdbDocument>) -> Unit,
    ) {
        if (!isHeadRead(atCommit)) {
            delegate.scanDocuments(namespaceId, atCommit, batchSize, onBatch)
            return
        }
        delegate.scanDocuments(namespaceId, atCommit, batchSize) { batch ->
            val live = batch.filterNot { expired(it) }
            if (live.isNotEmpty()) onBatch(live)
        }
    }
}
