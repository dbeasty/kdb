package dev.kdb.index

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.json.JsonValue
import dev.kdb.json.kdbJsonGet
import dev.kdb.dag.CommitDag
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.SchemaField
import dev.kdb.storage.StorageAdapter
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

public fun memoryIndexStoreFactory(dag: CommitDag): IndexStoreFactory =
    IndexStoreFactory { descriptor -> MemoryIndexStore(descriptor, dag) }

internal class DefaultIndexRegistry(
    override val namespaceId: String,
    internal val backingDag: CommitDag,
) : IndexRegistry {

    private val lock = Mutex()
    private val byKey = mutableMapOf<Pair<String, IndexType>, IndexStore>()
    private val byId = mutableMapOf<KdbUuid, Pair<IndexDescriptor, IndexStore>>()

    override val indexes: List<IndexStore>
        get() = byId.values.map { (_, store) -> store }

    override fun get(
        fieldName: String,
        type: IndexType,
    ): IndexStore? = byKey[fieldName to type]

    override fun getById(indexId: KdbUuid): IndexStore? = byId[indexId]?.second

    override suspend fun syncSchema(
        oldSchema: KdbSchema,
        newSchema: KdbSchema,
        storeFactory: IndexStoreFactory,
        dag: CommitDag,
        @Suppress("UNUSED_PARAMETER") storage: StorageAdapter,
    ): SchemaSyncResult {
        val headSnapshot = dag.head()
        return lock.withLock {
            val removedDesc = mutableListOf<IndexDescriptor>()
            val createdDesc = mutableListOf<IndexDescriptor>()
            val unchangedDesc = mutableListOf<IndexDescriptor>()

            val oldIx = oldSchema.indexedFields().associateBy { it.name }
            val newIx = newSchema.indexedFields().associateBy { it.name }

            fun drop(field: SchemaField) {
                val type = inferIndexType(field.type)
                val pair = field.name to type
                val store = byKey.remove(pair) ?: return
                val desc = store.descriptor
                byId.remove(desc.indexId)
                removedDesc += desc
            }

            fun add(field: SchemaField, version: Int) {
                val type = inferIndexType(field.type)
                val descriptor =
                    IndexDescriptor(
                        indexId = KdbUuid.random(),
                        namespaceId = namespaceId,
                        fieldName = field.name,
                        fields = listOf(field.name),
                        type = type,
                        unique = field.unique,
                        schemaVersion = version,
                        createdAtHash = headSnapshot,
                    )
                val store = storeFactory.create(descriptor)
                byKey[field.name to type] = store
                byId[descriptor.indexId] = descriptor to store
                createdDesc += descriptor
            }

            for ((name, field) in oldIx) {
                if (!newIx.containsKey(name)) drop(field)
            }

            for ((name, field) in newIx) {
                val previous = oldIx[name]
                if (previous == null) {
                    add(field, newSchema.version)
                    continue
                }

                val sameShape =
                    previous.type == field.type &&
                        previous.unique == field.unique &&
                        inferIndexType(previous.type) == inferIndexType(field.type)

                if (sameShape) {
                    unchangedDesc += descriptorFor(field.name)
                } else {
                    drop(previous)
                    add(field, newSchema.version)
                }
            }

            SchemaSyncResult(
                created = createdDesc.distinct(),
                removed = removedDesc.distinct(),
                unchanged = unchangedDesc.distinct(),
                rebuilding = createdDesc,
            )
        }
    }

    private fun descriptorFor(fieldName: String): IndexDescriptor =
        indexes.firstOrNull { it.descriptor.fieldName == fieldName }?.descriptor
            ?: throw IllegalStateException("No live index backing $fieldName")
}

internal class DefaultIndexWriter(
    private val hintSink: MutableList<IndexHint>?,
) : IndexWriter {

    override suspend fun applyCommit(
        commit: KdbCommit,
        registry: IndexRegistry,
        storage: StorageAdapter,
        schema: KdbSchema,
    ) {
        hintSink?.clear()
        val ns = commit.namespaceId
        val treeHash = commit.documentTreeHash
        if (!schema.indexedFields().any()) return

        for (op in commit.operations) {
            when (op) {
                is KdbOp.Write -> {
                    val doc = storage.getDocument(ns, op.docId, treeHash) ?: continue
                    for ((fieldName, store, descriptor, field) in matchingStores(registry, schema)) {
                        val raw =
                            jsonAt(doc.json, "$.$fieldName")
                        if (!shouldIndex(raw)) continue
                        val key = indexKeyFromJsonValue(raw, field.type)
                        if (key === IndexKey.NullKey) continue
                        store.put(IndexEntry(op.docId, key, commit.hash))
                        recordHint(descriptor, fieldName, IndexHintAction.PUT, op.docId, key, commit.hash)
                    }
                }

                is KdbOp.Delete ->
                    registry.indexes.forEach { idx ->
                        val desc = idx.descriptor
                        idx.delete(op.docId, commit.hash)
                        recordHint(desc, desc.fieldName, IndexHintAction.DELETE, op.docId, null, commit.hash)
                    }

                else -> Unit
            }
        }
    }

    override suspend fun rebuildAll(
        fromCommit: KdbHash,
        dag: CommitDag,
        registry: IndexRegistry,
        storage: StorageAdapter,
        schema: KdbSchema,
        onProgress: ((rebuilt: Int, total: Int) -> Unit)?,
    ) {
        val ns = dag.namespaceId
        val docTreeHash = dag.getCommitOrThrow(fromCommit).documentTreeHash
        val docs = mutableListOf<KdbDocument>()
        storage.scanDocuments(ns, docTreeHash, batchSize = 256) { docs += it }

        registry.indexes.forEach { idx -> idx.clear() }

        var rebuilt = 0
        val totalDocs = docs.size.coerceAtLeast(1)
        if (!schema.indexedFields().any()) {
            return
        }
        for (doc in docs) {
            for ((_, store, descriptor, field) in matchingStores(registry, schema)) {
                val raw = jsonAt(doc.json, "$.${descriptor.fieldName}")
                if (!shouldIndex(raw)) continue
                val key = indexKeyFromJsonValue(raw, field.type)
                if (key === IndexKey.NullKey) continue
                store.put(IndexEntry(doc.id, key, fromCommit))
            }
            rebuilt++
            onProgress?.invoke(rebuilt, totalDocs)
        }
    }

    private fun recordHint(
        descriptor: IndexDescriptor,
        fieldName: String,
        action: IndexHintAction,
        docId: KdbUuid,
        key: IndexKey?,
        commitHash: KdbHash,
    ) {
        hintSink?.add(
            IndexHint(descriptor.indexId, fieldName, descriptor.type, action, docId, key, commitHash),
        )
    }

    private fun jsonAt(
        doc: String,
        path: String,
    ): JsonValue? =
        try {
            kdbJsonGet(doc, path)
        } catch (_: Throwable) {
            null
        }

    private fun shouldIndex(raw: JsonValue?): Boolean =
        raw != null && raw !== JsonValue.JNull

    private data class Match(
        val fieldName: String,
        val store: IndexStore,
        val descriptor: IndexDescriptor,
        val field: SchemaField,
    )

    private fun matchingStores(
        registry: IndexRegistry,
        schema: KdbSchema,
    ): List<Match> {
        val out = mutableListOf<Match>()
        for ((name, field) in schema.fieldsByName) {
            if (!field.indexed) continue
            val type = inferIndexType(field.type)
            val idx = registry.get(name, type) ?: continue
            out += Match(name, idx, idx.descriptor, field)
        }
        return out
    }
}

internal class DefaultIndexReader(
    private val dagFor: suspend (String) -> CommitDag,
) : IndexReader {

    private suspend fun resolveHead(
        registry: IndexRegistry,
        at: KdbHash?,
    ): KdbHash = at ?: dagFor(registry.namespaceId).head()

    private fun indexesFor(registry: IndexRegistry, fieldName: String): List<IndexStore> =
        registry.indexes.filter { it.descriptor.fieldName == fieldName }.ifEmpty {
            throw IndexNotFoundException("no index backing $fieldName", registry.namespaceId, fieldName, IndexType.HASH)
        }

    override suspend fun lookupExact(
        registry: IndexRegistry,
        fieldName: String,
        key: IndexKey,
        atCommit: KdbHash?,
    ): List<KdbUuid> {
        val rows = indexesFor(registry, fieldName)
        val preferred =
            rows.minWithOrNull(
                    compareBy<IndexStore>(
                        {
                            when (it.descriptor.type) {
                                IndexType.HASH -> 0
                                IndexType.BTREE -> 1
                                else -> 2
                            }
                        },
                        { it.descriptor.indexId.toString() },
                    ),
                )
                ?: rows.first()
        val target = resolveHead(registry, atCommit)
        return preferred.lookup(key, target)
    }

    override suspend fun lookupRange(
        registry: IndexRegistry,
        fieldName: String,
        from: IndexKey?,
        to: IndexKey?,
        atCommit: KdbHash?,
        limit: Int,
        ascending: Boolean,
    ): List<KdbUuid> {
        val candidates = indexesFor(registry, fieldName)
        val btree = candidates.firstOrNull { it.descriptor.type == IndexType.BTREE }
        val store =
            btree
                ?: throw IndexTypeMismatchException(
                    "RANGE requires btree",
                    fieldName,
                    IndexType.BTREE,
                    candidates.first().descriptor.type,
                )

        val target = resolveHead(registry, atCommit)
        return store.range(from, to, target, limit, ascending)
    }

    override suspend fun lookupFullText(
        registry: IndexRegistry,
        fieldName: String,
        query: String,
        atCommit: KdbHash?,
        limit: Int,
    ): List<KdbUuid> {
        val store =
            registry.get(fieldName, IndexType.FULLTEXT)
                ?: throw IndexNotFoundException("FULLTEXT missing", registry.namespaceId, fieldName, IndexType.FULLTEXT)
        val target = resolveHead(registry, atCommit)
        return store.search(query, target, limit)
    }

    override suspend fun lookupVector(
        registry: IndexRegistry,
        fieldName: String,
        queryVector: FloatArray,
        k: Int,
        atCommit: KdbHash?,
    ): List<RankedResult> {
        val store =
            registry.get(fieldName, IndexType.VECTOR)
                ?: throw IndexNotFoundException("VECTOR missing", registry.namespaceId, fieldName, IndexType.VECTOR)
        val target = resolveHead(registry, atCommit)
        return store.nearestNeighbours(queryVector, k, target)
    }
}


internal class DefaultIndexManager(
    @Suppress("UNUSED_PARAMETER") private val storeFactory: IndexStoreFactory,
) : IndexManager {

    private val lock = Mutex()
    private val dags = mutableMapOf<String, CommitDag>()
    private val namespaces = mutableMapOf<String, DefaultIndexRegistry>()
    private val hints = mutableListOf<IndexHint>()

    override val writer: IndexWriter = DefaultIndexWriter(hints)
    override val reader: IndexReader =
        DefaultIndexReader { ns ->
            lock.withLock {
                dags[ns]
                    ?: throw IllegalArgumentException("namespace $ns is not bound to a DAG")
            }
        }

    internal fun drainHints(): List<IndexHint> = hints.toList()

    override suspend fun bindNamespace(
        namespaceId: String,
        dag: CommitDag,
    ): Unit =
        lock.withLock {
            require(namespaceId == dag.namespaceId) { "namespace id mismatch against DAG pointer" }
            val prev = dags[namespaceId]
            require(prev == null || prev == dag) { "DAG rebound for namespace $namespaceId — release first" }
            dags[namespaceId] = dag
            namespaces.getOrPut(namespaceId) {
                DefaultIndexRegistry(namespaceId, dag)
            }
            Unit
        }

    override fun registryFor(namespaceId: String): IndexRegistry =
        namespaces[namespaceId] ?: error("Call bindNamespace($namespaceId, dag) before registryFor(...)")

    override suspend fun releaseRegistry(namespaceId: String) {
        lock.withLock {
            namespaces.remove(namespaceId)
            dags.remove(namespaceId)
        }
    }
}
