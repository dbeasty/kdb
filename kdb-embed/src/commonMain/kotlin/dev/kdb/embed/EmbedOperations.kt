package dev.kdb.embed

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.dag.TraversalEntry
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.document.resolveDocumentId
import dev.kdb.index.IndexManager
import dev.kdb.index.compositeIndexStoreFactory
import dev.kdb.query.hybrid.HybridQueryRequest
import dev.kdb.query.hybrid.HybridQueryResult
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.isNone
import dev.kdb.sql.SqlCell
import dev.kdb.storage.StorageAdapter
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive

public suspend fun syncEmbedSchema(
    runtime: EmbeddedKdbRuntime,
    namespaceId: String = runtime.defaultNamespace,
    schema: KdbSchema = runtime.schema,
) {
    syncEmbedSchema(
        runtime = runtime,
        indexManager = runtime.indexManager,
        namespaceId = namespaceId,
        dag = runtime.dag,
        storage = runtime.storage,
        schema = schema,
    )
}

internal suspend fun syncEmbedSchema(
    runtime: EmbeddedKdbRuntime?,
    indexManager: IndexManager,
    namespaceId: String,
    dag: CommitDag,
    storage: StorageAdapter,
    schema: KdbSchema,
) {
    if (schema.isNone) return
    indexManager.registryFor(namespaceId).syncSchema(
        KdbSchema.NONE,
        schema,
        // The runtime's own factory when there is one: every store must share the runtime's single
        // IndexBlobStore, or snapshots land somewhere the catalog reload will never look.
        runtime?.indexStoreFactory ?: compositeIndexStoreFactory(dag, storage),
        dag,
        storage,
    )
    runtime?.let {
        if (it.schema.isNone && !schema.isNone) {
            // schema applied on open
        }
    }
}

/** Replays commit operations into storage and indexes (after peer sync). */
public suspend fun materializeCommitHistory(
    runtime: EmbeddedKdbRuntime,
    namespaceId: String,
    schema: KdbSchema = runtime.schema,
) {
    val head = runtime.dag.head()
    val commits =
        runtime.dag
            .walk(head)
            .filterIsInstance<TraversalEntry.Full>()
            .map { it.commit }
            .reversed()
    if (commits.isEmpty()) {
        val commit = runtime.dag.getCommitOrThrow(head)
        if (commit.operations.isNotEmpty()) {
            materializeSingleCommit(runtime, namespaceId, schema, commit)
        }
        return
    }
    for (commit in commits) {
        materializeSingleCommit(runtime, namespaceId, schema, commit)
    }
}

/** Applies one commit's document ops to storage and SQL indexes. */
public suspend fun materializeCommit(
    runtime: EmbeddedKdbRuntime,
    namespaceId: String,
    commit: dev.kdb.document.KdbCommit,
    schema: KdbSchema = runtime.schema,
) {
    materializeSingleCommit(runtime, namespaceId, schema, commit)
}

private suspend fun materializeSingleCommit(
    runtime: EmbeddedKdbRuntime,
    namespaceId: String,
    schema: KdbSchema,
    commit: dev.kdb.document.KdbCommit,
) {
    val parent = commit.parentHashes.firstOrNull() ?: return
    val parentTree = runtime.dag.getCommitOrThrow(parent).documentTreeHash
    for (op in commit.operations) {
        when (op) {
            is KdbOp.Write ->
                runtime.storage.putDocument(namespaceId, KdbDocument(op.docId, op.patch))
            is KdbOp.Delete -> runtime.storage.deleteDocument(namespaceId, op.docId)
            is KdbOp.FileWrite, is KdbOp.SchemaMigration -> {}
        }
    }
    runtime.storage.commitTree(namespaceId, parentTree)
    // See commitViaEngine: document indexes exist on schemaless namespaces too (§9.2).
    runtime.indexManager.writer.applyCommit(
        commit,
        runtime.indexManager.registryFor(namespaceId),
        runtime.storage,
        schema,
    )
}

public suspend fun putJson(
    runtime: EmbeddedKdbRuntime,
    namespaceId: String,
    json: String,
    schema: KdbSchema = runtime.schema,
): String {
    // Layer 16 §9.4: the body is stored byte-exact; a supplied top-level `id` is the identity (UUID
    // directly, any other non-empty string via the derived id), otherwise a random one is minted.
    val docId = resolveDocumentId(json).id
    val doc = KdbDocument(docId, json)
    val parent = runtime.writeBaseVersion ?: runtime.dag.head()
    val parentCommit = runtime.dag.getCommitOrThrow(parent)
    materializeCommit(runtime, namespaceId, parentCommit, schema)
    val tx =
        KdbTransaction(
            KdbUuid.random(),
            parent,
            listOf(KdbOp.Write(doc.id, doc.json)),
            KdbTimestamp.now(),
            KdbUuid.random(),
        )
    commitViaEngine(runtime, namespaceId, tx, schema, targetHead = parent)
    return docId.toString()
}

public suspend fun getJson(
    runtime: EmbeddedKdbRuntime,
    namespaceId: String,
    docId: String,
): String = getJsonAtCommit(runtime, namespaceId, docId, runtime.dag.head())

public suspend fun getJsonAtCommit(
    runtime: EmbeddedKdbRuntime,
    namespaceId: String,
    docId: String,
    atCommit: KdbHash,
): String {
    val id = KdbUuid.fromString(docId)
    val commit = runtime.dag.getCommitOrThrow(atCommit)
    val doc =
        runtime.storage.getDocument(namespaceId, id, commit.documentTreeHash)
            ?: throw IllegalArgumentException("document not found: $docId at ${atCommit.toHex()}")
    return doc.json
}

public suspend fun querySql(
    runtime: EmbeddedKdbRuntime,
    namespaceId: String,
    sql: String,
    schema: KdbSchema = runtime.schema,
): QueryResultJson {
    val result =
        runtime.hybrid.execute(
            sql,
            HybridQueryRequest(namespaceId, schema),
        )
    return result.toQueryResultJson()
}

public fun HybridQueryResult.toQueryResultJson(): QueryResultJson =
    QueryResultJson(
        columns = result.columns.map { it.name },
        rows = result.rows.map { row -> row.values.map { it.toJsonElement() } },
        resolvedCommit = resolvedCommit.toHex(),
        readOnly = readOnly,
    )

private fun SqlCell?.toJsonElement(): JsonElement =
    when (this) {
        null, SqlCell.Null -> JsonNull
        is SqlCell.StringVal -> JsonPrimitive(value)
        is SqlCell.LongVal -> JsonPrimitive(value)
        is SqlCell.DoubleVal -> JsonPrimitive(value)
        is SqlCell.BoolVal -> JsonPrimitive(value)
        is SqlCell.JsonVal -> Json.parseToJsonElement(json)
    }

@Serializable
public data class QueryResultJson(
    val columns: List<String>,
    val rows: List<List<JsonElement>>,
    val resolvedCommit: String,
    val readOnly: Boolean,
)

public fun QueryResultJson.toJsonString(): String =
    Json.encodeToString(QueryResultJson.serializer(), this)
