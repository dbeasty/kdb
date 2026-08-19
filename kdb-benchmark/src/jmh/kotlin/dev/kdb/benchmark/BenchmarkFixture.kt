package dev.kdb.benchmark

import dev.kdb.cli.CliConfig
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.index.compositeIndexStoreFactory
import dev.kdb.jdbc.EmbeddedKdbRuntime
import kotlin.io.path.ExperimentalPathApi
import dev.kdb.jdbc.file.openFileRuntime
import dev.kdb.jdbc.openMemoryRuntime
import dev.kdb.schema.KdbFieldType
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.SchemaField
import kotlinx.coroutines.runBlocking
import kotlin.io.path.createTempDirectory
import kotlin.io.path.deleteRecursively

object BenchmarkFixture {
    const val CATALOG: String = "bench"
    const val TABLE: String = "users"
    const val NAMESPACE_ID: String = "$CATALOG/$TABLE"

    val usersSchema: KdbSchema =
        KdbSchema.build(
            listOf(
                SchemaField("userId", KdbFieldType.StringType, required = true, indexed = true),
            ),
        )

    fun fileJdbcUrl(dataRoot: String): String = "jdbc:kdb:file://$dataRoot/$CATALOG/$TABLE"

    fun memoryJdbcUrl(): String = "jdbc:kdb:memory:///$NAMESPACE_ID"

    fun seedMemoryRuntime(docCount: Int): SeededMemory {
        val runtime = openMemoryRuntime(CATALOG, NAMESPACE_ID, usersSchema)
        val firstDocId = runBlocking { seedRuntime(runtime, NAMESPACE_ID, docCount) }
        return SeededMemory(runtime, firstDocId)
    }

    fun seedFileDataRoot(docCount: Int): SeededFile {
        val dataRoot = createTempDirectory("kdb-bench-file-").toString()
        val firstDocId =
            runBlocking {
                val runtime = openFileRuntime(dataRoot, CATALOG, NAMESPACE_ID, usersSchema)
                seedRuntime(runtime, NAMESPACE_ID, docCount)
            }
        return SeededFile(dataRoot, CliConfig(dataDir = dataRoot, quiet = true), firstDocId)
    }

    suspend fun seedRuntime(
        runtime: EmbeddedKdbRuntime,
        namespaceId: String,
        docCount: Int,
    ): String {
        val dag = runtime.dag
        val storage = runtime.storage
        val manager = runtime.indexManager
        manager.registryFor(namespaceId).syncSchema(
            KdbSchema.NONE,
            usersSchema,
            compositeIndexStoreFactory(dag, storage),
            dag,
            storage,
        )
        var firstId: KdbUuid? = null
        for (i in 0 until docCount) {
            val userId = if (i == 0) "u1" else "u$i"
            val doc = KdbDocument(KdbUuid.random(), """{"userId":"$userId"}""")
            if (firstId == null) firstId = doc.id
            storage.putDocument(namespaceId, doc)
            val parent = dag.head()
            val parentTree = dag.getCommitOrThrow(parent).documentTreeHash
            val tree = storage.commitTree(namespaceId, parentTree)
            val tx =
                KdbTransaction(
                    KdbUuid.random(),
                    parent,
                    listOf(KdbOp.Write(doc.id, doc.json)),
                    KdbTimestamp.now(),
                    KdbUuid.random(),
                )
            val commit = dag.appendCommit(tx, parent, tree, null)
            manager.writer.applyCommit(commit, manager.registryFor(namespaceId), storage, usersSchema)
        }
        return firstId?.toString() ?: ""
    }

    @OptIn(ExperimentalPathApi::class)
    fun removeDataRoot(dataRoot: String) {
        kotlin.io.path.Path(dataRoot).deleteRecursively()
    }

    data class SeededMemory(
        val runtime: EmbeddedKdbRuntime,
        val firstDocId: String,
    )

    data class SeededFile(
        val dataRoot: String,
        val config: CliConfig,
        val firstDocId: String,
    )
}
