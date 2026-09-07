package dev.kdb.integration

import dev.kdb.embed.putJson
import dev.kdb.index.IndexType
import dev.kdb.jdbc.file.FileIndexBlobPointers
import dev.kdb.jdbc.file.NamespacePaths
import dev.kdb.jdbc.file.openFileRuntime
import dev.kdb.query.hybrid.HybridQueryRequest
import dev.kdb.schema.KdbSchema
import kotlinx.coroutines.test.runTest
import kotlin.io.path.createTempDirectory
import kotlin.io.path.exists
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Layer 16 §6.5/§9.2: a file-backed runtime's index catalog and snapshots survive a restart.
 *
 * The snapshot *bytes* always did - they go to the content-addressed blob store - but the name
 * that finds them again did not, because StorageAdapterIndexBlobStore's pointer table defaults to
 * memory. openFileRuntime supplies a durable one; without it a restarted server has snapshots it
 * cannot name and silently rebuilds (or serves an empty index).
 */
class IndexDurabilityIntegrationTest {
    private val ns = "app/durable"

    /** Guards: the pointer table itself round-trips through the file, including after an overwrite. */
    @Test
    fun pointersSurviveReloadOfTheTable() =
        runTest {
            val dir = createTempDirectory("kdb-index-pointers")
            val file = dir.resolve("index-pointers.tsv")
            val a = dev.kdb.codec.KdbHash.fromHex("aa".repeat(32))
            val b = dev.kdb.codec.KdbHash.fromHex("bb".repeat(32))

            val first = FileIndexBlobPointers(file)
            first.put("index/x/snapshot", a)
            first.put("index/catalog/app/durable", b)
            assertTrue(file.exists())

            val reopened = FileIndexBlobPointers(file)
            assertEquals(a, reopened.get("index/x/snapshot"))
            assertEquals(b, reopened.get("index/catalog/app/durable"))

            reopened.put("index/x/snapshot", b)
            reopened.remove("index/catalog/app/durable")
            val third = FileIndexBlobPointers(file)
            assertEquals(b, third.get("index/x/snapshot"))
            assertEquals(null, third.get("index/catalog/app/durable"))
        }

    /** Guards: an index created in one process is present, and searchable, in the next - the catalog
     * is reloaded and its snapshot restored rather than the index coming back empty. */
    @Test
    fun aFullTextIndexIsReloadedAfterRestart() =
        runTest {
            val dir = createTempDirectory("kdb-index-durability").toString()
            val docId: String
            run {
                val runtime = openFileRuntime(dir, "app", ns, KdbSchema.NONE, acquireDirectoryLock = false)
                runtime.hybrid.execute(
                    "CREATE INDEX docs_text ON docs (title) USING FULLTEXT",
                    HybridQueryRequest(namespaceId = ns, schema = KdbSchema.NONE),
                )
                docId = putJson(runtime, ns, """{"title":"deploy staging tonight"}""")
                val registry = runtime.indexManager.registryFor(ns)
                assertNotNull(registry.getBySqlName("docs_text"), "the index must exist in the first process")
                val hits = runtime.indexManager.reader.lookupFullText(registry, "title", "deploy", null, 10)
                assertEquals(listOf(docId), hits.map { it.docId.toString() }, "index must be fed on the commit path")
                registry.flushAll()
            }

            // The pointer table is what makes the next line possible at all.
            assertTrue(NamespacePaths.indexPointersFile(dir, ns).exists(), "no durable pointer table was written")

            val reopened = openFileRuntime(dir, "app", ns, KdbSchema.NONE, acquireDirectoryLock = false)
            val registry = reopened.indexManager.registryFor(ns)
            val store = registry.getBySqlName("docs_text")
            assertNotNull(store, "the index catalog must be reloaded on open")
            assertEquals(IndexType.FULLTEXT, store!!.descriptor.type)
            val hits = reopened.indexManager.reader.lookupFullText(registry, "title", "deploy", null, 10)
            assertEquals(listOf(docId), hits.map { it.docId.toString() }, "the reloaded index must still find the document")
        }
}
