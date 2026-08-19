package dev.kdb.transaction

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.storage.mem.InMemoryStorageAdapter
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs

class AuthorizingTransactionEngineTest {
    private class DeniedException(msg: String) : RuntimeException(msg)

    @Test
    fun rejectsWriteWhenAuthorizerDenies() =
        runTest {
            val ns = "app/authz"
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val base = dag.head()
            val denyingEngine =
                authorizingTransactionEngine(transactionEngine(ConflictPolicy.STRICT), ns) { _, _ ->
                    throw DeniedException("no")
                }
            val docId = KdbUuid.random()
            val transaction = tx(base, KdbOp.Write(docId, """{"v":"a"}"""))

            assertFailsWith<DeniedException> {
                denyingEngine.commit(transaction, dag, storage)
            }
            assertEquals(base, dag.head(), "denied write must not advance the commit head")
        }

    @Test
    fun passesThroughWhenAuthorizerAllows() =
        runTest {
            val ns = "app/authz-ok"
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val base = dag.head()
            val checkedOps = mutableListOf<KdbOp>()
            val allowingEngine =
                authorizingTransactionEngine(transactionEngine(ConflictPolicy.STRICT), ns) { namespaceId, op ->
                    assertEquals(ns, namespaceId)
                    checkedOps += op
                }
            val docId = KdbUuid.random()
            val transaction = tx(base, KdbOp.Write(docId, """{"v":"a"}"""))

            val result = allowingEngine.commit(transaction, dag, storage)
            assertIs<TransactionResult.Success>(result)
            assertEquals(1, checkedOps.size)
        }

    private fun tx(
        base: KdbHash,
        vararg ops: KdbOp,
    ): KdbTransaction =
        KdbTransaction(
            id = KdbUuid.random(),
            baseVersion = base,
            operations = ops.toList(),
            timestamp = KdbTimestamp.now(),
            authorNodeId = KdbUuid.random(),
        )
}
