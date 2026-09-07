package dev.kdb.index

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.json.JsonValue
import dev.kdb.schema.KdbSchema
import dev.kdb.storage.StorageAdapter
import dev.kdb.storage.mem.InMemoryStorageAdapter
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * The commit path and registry wiring for document-level indexes (Components 64/65 through §10):
 * FULLTEXT and VECTOR stores are fed whole documents, validated before anything is mutated, and
 * restored or rebuilt at bind time.
 */
class DocumentIndexWiringTest {

    // ------------------------------------------------------------------ fake store

    /** Records what the commit path hands it, and fails validation for a marked document. */
    private class RecordingDocumentStore(
        override val descriptor: IndexDescriptor,
        private val rejectMarker: String? = null,
        private val blobs: IndexBlobStore = InMemoryIndexBlobStore(),
        private val head: suspend () -> KdbHash,
    ) : DocumentIndexStore {
        val puts = mutableListOf<Triple<KdbUuid, KdbHash, String>>()
        val deletes = mutableListOf<Pair<KdbUuid, KdbHash>>()
        var flushes = 0
        var cleared = 0
        var snapshotHeadHex: String? = null

        override fun validateDocument(
            docId: KdbUuid,
            json: String,
        ) {
            if (rejectMarker != null && json.contains(rejectMarker)) {
                throw IllegalStateException("document $docId rejected by validation")
            }
        }

        override suspend fun putDocument(
            docId: KdbUuid,
            commitHash: KdbHash,
            json: String,
        ) {
            validateDocument(docId, json)
            puts += Triple(docId, commitHash, json)
        }

        override suspend fun flush() {
            flushes++
            snapshotHeadHex = head().toHex()
            blobs.write(indexSnapshotBlobKey(descriptor.indexId), (snapshotHeadHex ?: "").encodeToByteArray())
        }

        override suspend fun restoreFromStorage(): SnapshotRestoreResult {
            val bytes =
                blobs.read(indexSnapshotBlobKey(descriptor.indexId))
                    ?: return SnapshotRestoreResult(SnapshotRestoreStatus.MISSING, null)
            val stored = bytes.decodeToString()
            val current = head().toHex()
            return if (stored == current) {
                SnapshotRestoreResult(SnapshotRestoreStatus.RESTORED, stored)
            } else {
                SnapshotRestoreResult(SnapshotRestoreStatus.STALE, stored)
            }
        }

        override suspend fun put(entry: IndexEntry) {
            val key = entry.key as IndexKey.StringKey
            putDocument(entry.docId, entry.commitHash, key.value)
        }

        override suspend fun delete(
            docId: KdbUuid,
            atCommit: KdbHash,
        ) {
            deletes += docId to atCommit
        }

        override suspend fun bulkLoad(entries: List<IndexEntry>) {
            for (e in entries) put(e)
        }

        override suspend fun rebuild(entries: List<IndexEntry>) = bulkLoad(entries)

        override suspend fun clear() {
            cleared++
            puts.clear()
            deletes.clear()
        }

        override suspend fun lookup(
            key: IndexKey,
            atCommit: KdbHash?,
        ): List<KdbUuid> = emptyList()

        override suspend fun range(
            from: IndexKey?,
            to: IndexKey?,
            atCommit: KdbHash?,
            limit: Int,
            ascending: Boolean,
        ): List<KdbUuid> = emptyList()

        override suspend fun search(
            query: String,
            atCommit: KdbHash?,
            limit: Int,
        ): List<RankedResult> = puts.map { RankedResult(it.first, 1f) }

        override suspend fun nearestNeighbours(
            queryVector: FloatArray,
            k: Int,
            atCommit: KdbHash?,
        ): List<RankedResult> = emptyList()

        override suspend fun isValid(atCommit: KdbHash): Boolean = true

        override suspend fun snapshot(): ByteArray = ByteArray(0)

        override suspend fun restoreSnapshot(data: ByteArray) = Unit
    }

    // ------------------------------------------------------------------ fixtures

    private val docA = KdbUuid.fromString("aaaaaaaa-0000-4000-8000-000000000001")
    private val docB = KdbUuid.fromString("bbbbbbbb-0000-4000-8000-000000000002")

    private fun descriptor(
        namespaceId: String,
        type: IndexType = IndexType.FULLTEXT,
        field: String = "title",
    ) = IndexDescriptor(
        indexId = KdbUuid.random(),
        namespaceId = namespaceId,
        fieldName = field,
        fields = listOf(field),
        type = type,
        unique = false,
        schemaVersion = 1,
        createdAtHash = KdbHash.fromBytes(ByteArray(32)),
    )

    /** Commits [ops] against [storage], returning the new commit. */
    private suspend fun commitDocuments(
        dag: CommitDag,
        storage: StorageAdapter,
        documents: List<KdbDocument>,
        deletes: List<KdbUuid> = emptyList(),
    ) = run {
        val parent = dag.head()
        for (d in documents) storage.putDocument(dag.namespaceId, d)
        val parentTree = dag.getCommitOrThrow(parent).documentTreeHash
        val tree = storage.commitTree(dag.namespaceId, parentTree)
        dag.putDocumentTree(tree)
        val ops = documents.map { KdbOp.Write(it.id, it.json) } + deletes.map { KdbOp.Delete(it) }
        val tx =
            KdbTransaction(
                id = KdbUuid.random(),
                baseVersion = parent,
                operations = ops,
                timestamp = KdbTimestamp.now(),
                authorNodeId = KdbUuid.random(),
            )
        dag.appendCommit(tx, parent, tree, null)
    }

    private fun document(
        id: KdbUuid,
        json: String,
    ) = KdbDocument(id, json)

    // ------------------------------------------------------------------ tests

    /** Guards §10: a WriteOp reaches a document store as putDocument with the whole JSON. */
    @Test
    fun applyCommitFeedsDocumentStoresWholeDocuments() =
        runTest {
            val ns = "ns/wire-put"
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val store = RecordingDocumentStore(descriptor(ns)) { dag.head() }
            val manager = indexManager(IndexStoreFactory { store })
            manager.bindNamespace(ns, dag)
            val registry = manager.registryFor(ns)
            registry.loadCatalog(IndexCatalog(ns, listOf(IndexCatalogEntry(store.descriptor, "t"))), IndexStoreFactory { store })

            val json = """{"title":"deploy staging","body":"notes"}"""
            val commit = commitDocuments(dag, storage, listOf(document(docA, json)))
            manager.writer.applyCommit(commit, registry, storage, KdbSchema.NONE)

            assertEquals(1, store.puts.size)
            assertEquals(docA, store.puts[0].first)
            assertEquals(commit.hash, store.puts[0].second)
            assertEquals(json, store.puts[0].third, "the store must see the whole document, not one field")
        }

    /** Guards §10: a DeleteOp reaches a document store as a tombstone at the new commit. */
    @Test
    fun applyCommitTombstonesDeletedDocuments() =
        runTest {
            val ns = "ns/wire-delete"
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val store = RecordingDocumentStore(descriptor(ns)) { dag.head() }
            val manager = indexManager(IndexStoreFactory { store })
            manager.bindNamespace(ns, dag)
            val registry = manager.registryFor(ns)
            registry.loadCatalog(IndexCatalog(ns, listOf(IndexCatalogEntry(store.descriptor, "t"))), IndexStoreFactory { store })

            commitDocuments(dag, storage, listOf(document(docA, """{"title":"deploy"}""")))
                .let { manager.writer.applyCommit(it, registry, storage, KdbSchema.NONE) }
            val second = commitDocuments(dag, storage, emptyList(), deletes = listOf(docA))
            manager.writer.applyCommit(second, registry, storage, KdbSchema.NONE)

            assertEquals(listOf(docA to second.hash), store.deletes)
        }

    /**
     * Guards §10's "nothing is half-applied": when one document of a commit fails validation the
     * exception surfaces before any store has been mutated, even by the documents that preceded it.
     */
    @Test
    fun validationFailureRejectsTheCommitBeforeAnyStoreIsMutated() =
        runTest {
            val ns = "ns/wire-validate"
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val store = RecordingDocumentStore(descriptor(ns), rejectMarker = "BAD") { dag.head() }
            val manager = indexManager(IndexStoreFactory { store })
            manager.bindNamespace(ns, dag)
            val registry = manager.registryFor(ns)
            registry.loadCatalog(IndexCatalog(ns, listOf(IndexCatalogEntry(store.descriptor, "t"))), IndexStoreFactory { store })

            val commit =
                commitDocuments(
                    dag,
                    storage,
                    listOf(
                        document(docA, """{"title":"fine"}"""),
                        document(docB, """{"title":"BAD"}"""),
                    ),
                )

            assertFailsWith<IllegalStateException> {
                manager.writer.applyCommit(commit, registry, storage, KdbSchema.NONE)
            }
            assertTrue(store.puts.isEmpty(), "the good document must not have been indexed either")
        }

    /** Guards the rebuild helper: every document at the tree is re-fed, after a clear. */
    @Test
    fun rebuildFromScanRefeedsEveryDocument() =
        runTest {
            val ns = "ns/wire-rebuild"
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val store = RecordingDocumentStore(descriptor(ns)) { dag.head() }
            commitDocuments(
                dag,
                storage,
                listOf(document(docA, """{"title":"a"}"""), document(docB, """{"title":"b"}""")),
            )

            val scanned = rebuildFromScan(store, storage, dag)

            assertEquals(2, scanned)
            assertEquals(setOf(docA, docB), store.puts.map { it.first }.toSet())
            assertEquals(1, store.cleared, "a rebuild starts from an empty store")
        }

    /** Guards §6.5 bind-time recovery: a missing snapshot rebuilds from scan and then flushes. */
    @Test
    fun restoreOrRebuildRebuildsWhenTheSnapshotIsMissing() =
        runTest {
            val ns = "ns/wire-restore-missing"
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val store = RecordingDocumentStore(descriptor(ns)) { dag.head() }
            commitDocuments(dag, storage, listOf(document(docA, """{"title":"a"}""")))

            val report = restoreOrRebuild(store, storage, dag)

            assertEquals(SnapshotRestoreStatus.MISSING, report.restore.status)
            assertTrue(report.rebuilt)
            assertEquals(1, report.documentsScanned)
            assertEquals(1, store.flushes, "a rebuilt index snapshots itself so the next open is cheap")
        }

    /** Guards §6.5: a current snapshot is used as is, with no scan. */
    @Test
    fun restoreOrRebuildUsesACurrentSnapshot() =
        runTest {
            val ns = "ns/wire-restore-current"
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val blobs = InMemoryIndexBlobStore()
            val store = RecordingDocumentStore(descriptor(ns), blobs = blobs) { dag.head() }
            commitDocuments(dag, storage, listOf(document(docA, """{"title":"a"}""")))
            store.flush()

            val fresh = RecordingDocumentStore(store.descriptor, blobs = blobs) { dag.head() }
            val report = restoreOrRebuild(fresh, storage, dag)

            assertEquals(SnapshotRestoreStatus.RESTORED, report.restore.status)
            assertTrue(!report.rebuilt)
            assertTrue(fresh.puts.isEmpty(), "a current snapshot means no rescan")
        }

    /** Guards the registry driving restore/rebuild for every document store it holds. */
    @Test
    fun registryRestoresOrRebuildsEveryDocumentStore() =
        runTest {
            val ns = "ns/wire-registry"
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val store = RecordingDocumentStore(descriptor(ns)) { dag.head() }
            val manager = indexManager(IndexStoreFactory { store })
            manager.bindNamespace(ns, dag)
            val registry = manager.registryFor(ns)
            registry.loadCatalog(IndexCatalog(ns, listOf(IndexCatalogEntry(store.descriptor, "t"))), IndexStoreFactory { store })
            commitDocuments(dag, storage, listOf(document(docA, """{"title":"a"}""")))

            val reports = registry.restoreOrRebuild(dag, storage)

            assertEquals(1, reports.size)
            assertTrue(reports[0].rebuilt)
            assertEquals(setOf(docA), store.puts.map { it.first }.toSet())
        }

    /** Guards the registry exposing SQL-named indexes and its catalog view for persistence. */
    @Test
    fun registryExposesSqlNamesAndCatalog() =
        runTest {
            val ns = "ns/wire-catalog"
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val blobs = InMemoryIndexBlobStore()
            val descriptor = descriptor(ns)
            val store = RecordingDocumentStore(descriptor) { dag.head() }
            val manager = indexManager(IndexStoreFactory { store }, blobs)
            manager.bindNamespace(ns, dag)
            val registry = manager.registryFor(ns)

            registry.registerSqlIndex(
                descriptor,
                IndexStoreFactory { store },
                dag,
                storage,
                KdbSchema.NONE,
                "tasks_text",
                rebuild = false,
            )

            assertEquals(store, registry.getBySqlName("tasks_text"))
            val catalog = registry.catalog()
            assertEquals(1, catalog.entries.size)
            assertEquals("tasks_text", catalog.entries[0].sqlIndexName)
            assertEquals(
                "tasks_text",
                catalog.entries[0].descriptor.options[INDEX_OPTION_INDEX_NAME],
                "the SQL name is recorded on the descriptor so the planner can resolve MATCH(name, …)",
            )
            // …and it was persisted for the next open.
            assertEquals(catalog, IndexCatalog.load(blobs, ns))
        }

    /** Guards §2 path evaluation: arrays traverse implicitly and the final array flattens. */
    @Test
    fun documentPathCandidatesFollowsArraysImplicitly() {
        val root =
            JsonValue.fromJsonString(
                """{"steps":[{"text":"one"},{"text":"two"}],"tags":["a","b"],"nested":{"x":1}}""",
            )
        assertEquals(
            listOf("one", "two"),
            documentPathCandidates(root, "steps.text").map { (it as JsonValue.JString).value },
        )
        assertEquals(
            listOf("a", "b"),
            documentPathCandidates(root, "tags").map { (it as JsonValue.JString).value },
        )
        // A vector field must stay one array value rather than becoming its elements.
        assertTrue(documentPathCandidates(root, "tags", flattenFinalArray = false).single() is JsonValue.JArray)
        assertTrue(documentPathCandidates(root, "absent").isEmpty())
        assertTrue(documentPathCandidates(root, "nested.absent").isEmpty())
    }
}
