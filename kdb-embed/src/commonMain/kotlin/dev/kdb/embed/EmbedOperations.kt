package dev.kdb.embed

import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.dag.TraversalEntry
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
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
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

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
        compositeIndexStoreFactory(dag, storage),
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
    if (!schema.isNone) {
        runtime.indexManager.writer.applyCommit(
            commit,
            runtime.indexManager.registryFor(namespaceId),
            runtime.storage,
            schema,
        )
    }
}

public suspend fun putJson(
    runtime: EmbeddedKdbRuntime,
    namespaceId: String,
    json: String,
    schema: KdbSchema = runtime.schema,
): String {
    val element = Json.parseToJsonElement(json).jsonObject
    val docId =
        element["id"]?.jsonPrimitive?.content?.let { KdbUuid.fromString(it) }
            ?: KdbUuid.random()
    val doc = KdbDocument(docId, json)
    runtime.storage.putDocument(namespaceId, doc)
    val parent = runtime.dag.head()
    val parentTree = runtime.dag.getCommitOrThrow(parent).documentTreeHash
    val tree = runtime.storage.commitTree(namespaceId, parentTree)
    val tx =
        KdbTransaction(
            KdbUuid.random(),
            parent,
            listOf(KdbOp.Write(doc.id, doc.json)),
            KdbTimestamp.now(),
            KdbUuid.random(),
        )
    val commit = runtime.dag.appendCommit(tx, parent, tree, null)
    if (!schema.isNone) {
        runtime.indexManager.writer.applyCommit(
            commit,
            runtime.indexManager.registryFor(namespaceId),
            runtime.storage,
            schema,
        )
    }
    return docId.toString()
}

public suspend fun getJson(
    runtime: EmbeddedKdbRuntime,
    namespaceId: String,
    docId: String,
): String {
    val id = KdbUuid.fromString(docId)
    val head = runtime.dag.head()
    val doc =
        runtime.storage.getDocument(namespaceId, id, head)
            ?: throw IllegalArgumentException("document not found: $docId")
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
