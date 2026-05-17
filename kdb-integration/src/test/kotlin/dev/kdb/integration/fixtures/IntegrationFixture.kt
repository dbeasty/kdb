package dev.kdb.integration.fixtures

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.jdbc.EmbeddedKdbRuntime
import dev.kdb.jdbc.openMemoryRuntime

class IntegrationFixture(
    val namespaceId: String = "integration/test",
) {
    val runtime: EmbeddedKdbRuntime = openMemoryRuntime("integration", namespaceId)

    suspend fun writeJson(json: String): KdbUuid {
        val doc = KdbDocument(KdbUuid.random(), json)
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
        runtime.dag.appendCommit(tx, parent, tree, null)
        return doc.id
    }

    suspend fun head(): KdbHash = runtime.dag.head()
}

fun integrationFixture(namespaceId: String = "integration/test"): IntegrationFixture =
    IntegrationFixture(namespaceId)
