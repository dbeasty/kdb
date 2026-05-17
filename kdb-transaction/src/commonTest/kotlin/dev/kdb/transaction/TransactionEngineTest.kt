package dev.kdb.transaction

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.storage.mem.InMemoryStorageAdapter
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

class TransactionEngineTest {
    private val engine = transactionEngine(ConflictPolicy.STRICT)

    @Test
    fun commit_singleWrite_succeeds() =
        runTest {
            val ns = "app/tx"
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val base = dag.head()
            val doc = KdbDocument(KdbUuid.random(), """{"v":"a"}""")
            val tx = tx(base, KdbOp.Write(doc.id, doc.json))
            val result = engine.commit(tx, dag, storage)
            assertIs<TransactionResult.Success>(result)
            assertEquals(1, result.commit.operations.size)
        }

    @Test
    fun strict_conflict_onConcurrentWrite() =
        runTest {
            val ns = "app/conflict"
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val base = dag.head()
            val docId = KdbUuid.random()
            val tx1 = tx(base, KdbOp.Write(docId, """{"v":"1"}"""))
            assertIs<TransactionResult.Success>(engine.commit(tx1, dag, storage))
            val head = dag.head()
            val tx2a = tx(head, KdbOp.Write(docId, """{"v":"2a"}"""))
            val tx2b = tx(head, KdbOp.Write(docId, """{"v":"2b"}"""))
            assertIs<TransactionResult.Success>(engine.commit(tx2a, dag, storage))
            val conflict = engine.commit(tx2b, dag, storage)
            assertIs<TransactionResult.Conflict>(conflict)
        }

    @Test
    fun fileWrite_missingBlob_rejected() =
        runTest {
            val ns = "app/file"
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val base = dag.head()
            val missing =
                KdbHash.fromHex(
                    "0000000000000000000000000000000000000000000000000000000000000001",
                )
            val tx =
                tx(
                    base,
                    KdbOp.FileWrite("attachments/x", missing),
                )
            val result = engine.commit(tx, dag, storage)
            assertIs<TransactionResult.SchemaError>(result)
            assertTrue(result.violations.isNotEmpty())
        }

    @Test
    fun replay_idempotent() =
        runTest {
            val ns = "app/idempotent"
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val base = dag.head()
            val doc = KdbDocument(KdbUuid.random(), """{"v":"x"}""")
            val tx = tx(base, KdbOp.Write(doc.id, doc.json))
            val landed = engine.commit(tx, dag, storage) as TransactionResult.Success
            val first = engine.replay(tx, dag, storage, replayTarget = landed.commit.hash)
            val second = engine.replay(tx, dag, storage, replayTarget = landed.commit.hash)
            assertIs<TransactionResult.Success>(first)
            assertIs<TransactionResult.Success>(second)
            assertEquals(first.commit.hash, second.commit.hash)
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
