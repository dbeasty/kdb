package dev.kdb.transaction

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.storage.StorageAdapter
import dev.kdb.storage.mem.InMemoryStorageAdapter
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull
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

    // Guards the fix that replaced findExistingCommit's walk of up to 8192 commits of history
    // with an O(1) CommitDag.getCommitByTransactionId lookup (see that method's doc comment -
    // this was the dominant cost behind kdb-service getting OOM-killed under sustained write
    // load, docs/benchmarks/lightsail-sim/README.md; both implementations shared this exact
    // pattern). replay_idempotent above only covers "immediately again" with nothing in between;
    // a real caller replaying against a pinned target (e.g. resending a write-back transaction
    // after a dropped response) may do so long after other, unrelated commits landed.
    @Test
    fun replay_idempotent_acrossInterveningHistory() =
        runTest {
            val ns = "app/idempotent-long-history"
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val base = dag.head()
            val doc = KdbDocument(KdbUuid.random(), """{"v":"original"}""")
            val originalTx = tx(base, KdbOp.Write(doc.id, doc.json))
            val original = engine.commit(originalTx, dag, storage) as TransactionResult.Success

            var head = original.commit.hash
            repeat(50) { i ->
                val fillerDoc = KdbDocument(KdbUuid.random(), """{"v":"filler-$i"}""")
                val fillerTx = tx(head, KdbOp.Write(fillerDoc.id, fillerDoc.json))
                val res = engine.commit(fillerTx, dag, storage) as TransactionResult.Success
                head = res.commit.hash
            }

            val retry = engine.replay(originalTx, dag, storage, replayTarget = original.commit.parentHashes.single())
            assertIs<TransactionResult.Success>(retry)
            assertEquals(original.commit.hash, retry.commit.hash, "idempotent retry produced a different commit than the original")
            assertEquals(head, dag.head(), "main moved during an idempotent retry")
        }

    @Test
    fun commit_writeFailsMidTransaction_abortsAndRollsBackWithoutPartialWrites() =
        runTest {
            val ns = "app/rollback"
            val dag = inMemoryCommitDag(ns)
            val delegate = InMemoryStorageAdapter()
            val failOnDoc = KdbUuid.random()
            val storage = FailingStorageAdapter(delegate, failOnDoc)
            val base = dag.head()
            val okDoc = KdbDocument(KdbUuid.random(), """{"v":"ok"}""")
            val tx =
                tx(
                    base,
                    KdbOp.Write(okDoc.id, okDoc.json),
                    KdbOp.Write(failOnDoc, """{"v":"boom"}"""),
                )
            val result = engine.commit(tx, dag, storage)
            assertIs<TransactionResult.Aborted>(result)

            // Neither the doc written before the failing one, nor any tree
            // mutation, should be visible: the write phase is all-or-nothing.
            assertNull(delegate.getDocument(ns, okDoc.id, base))
            assertNull(delegate.getDocument(ns, failOnDoc, base))
            assertEquals(1, storage.discardPendingCalls)

            // The namespace is still usable for a subsequent, successful transaction.
            val retry = tx(base, KdbOp.Write(okDoc.id, okDoc.json))
            assertIs<TransactionResult.Success>(engine.commit(retry, dag, storage))
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

/** Delegates to [delegate] but throws on putDocument for [failOnDocId], to exercise write-phase rollback. */
private class FailingStorageAdapter(
    private val delegate: StorageAdapter,
    private val failOnDocId: KdbUuid,
) : StorageAdapter by delegate {
    var discardPendingCalls: Int = 0
        private set

    override suspend fun putDocument(namespaceId: String, document: KdbDocument) {
        if (document.id == failOnDocId) {
            throw RuntimeException("injected write failure")
        }
        delegate.putDocument(namespaceId, document)
    }

    override suspend fun discardPending(namespaceId: String) {
        discardPendingCalls++
        delegate.discardPending(namespaceId)
    }
}
