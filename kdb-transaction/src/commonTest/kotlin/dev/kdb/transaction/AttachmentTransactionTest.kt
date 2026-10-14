package dev.kdb.transaction

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.error.ConflictOperationType
import dev.kdb.storage.mem.InMemoryStorageAdapter
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/** Attachment preflight and conflict rules - go/kdb/transaction/attachments.go. */
class AttachmentTransactionTest {
    private val stored = mutableSetOf<KdbHash>()
    private val engine = transactionEngine(ConflictPolicy.STRICT, blobPresence = { it in stored })
    private val doc = KdbUuid(1, 1)
    private val attA = KdbUuid(10, 1)
    private val attB = KdbUuid(11, 1)

    private fun h(b: Int): KdbHash = KdbHash(ByteArray(32) { b.toByte() }).also { stored += it }

    private fun tx(
        base: KdbHash,
        vararg ops: KdbOp,
    ) = KdbTransaction(KdbUuid.random(), base, ops.toList(), KdbTimestamp.now(), KdbUuid.random())

    private fun put(
        id: KdbUuid,
        name: String,
        hash: KdbHash,
        d: KdbUuid = doc,
    ) = KdbOp.AttachmentPut(d, id, name, 1, hash, 3, "x/y")

    private suspend fun violation(
        dag: CommitDag,
        storage: InMemoryStorageAdapter,
        vararg ops: KdbOp,
    ): String {
        val r = engine.commit(tx(dag.head(), *ops), dag, storage)
        assertIs<TransactionResult.SchemaError>(r)
        return r.violations.single().violations.single().detail
    }

    @Test
    fun preflight() =
        runTest {
            val dag = inMemoryCommitDag("ns")
            val storage = InMemoryStorageAdapter()
            // The document must exist after the transaction: written in it is enough.
            val ok = engine.commit(tx(dag.head(), KdbOp.Write(doc, "{}"), put(attA, "a.png", h(1))), dag, storage)
            assertIs<TransactionResult.Success>(ok)
            assertEquals(1, dag.attachmentTableAt(ok.commit.hash).count(doc))
            assertTrue(ok.commit.attachmentTreeHash != null)

            assertTrue(violation(dag, storage, put(attB, "b", h(2), KdbUuid(9, 9))).startsWith("no such document"))
            assertTrue(violation(dag, storage, KdbOp.Delete(doc), put(attB, "b", h(2))).startsWith("no such document"))
            assertTrue(violation(dag, storage, put(KdbUuid(0, 0), "b", h(2))).contains("no id"))
            assertTrue(violation(dag, storage, put(attB, "", h(2))).contains("empty"))
            assertTrue(violation(dag, storage, put(attB, "b", KdbHash(ByteArray(32) { 77 }))).startsWith("attachment blob not stored"))
            assertTrue(violation(dag, storage, put(attB, "a.png", h(2))).contains("already used"))
            // A JSON-only write is untouched by all of it, and keeps the root.
            val json = engine.commit(tx(dag.head(), KdbOp.Write(doc, """{"x":1}""")), dag, storage)
            assertIs<TransactionResult.Success>(json)
            assertEquals(ok.commit.attachmentTreeHash, json.commit.attachmentTreeHash)
        }

    @Test
    fun conflictsArePerAttachment() =
        runTest {
            val dag = inMemoryCommitDag("ns")
            val storage = InMemoryStorageAdapter()
            val seed = engine.commit(tx(dag.head(), KdbOp.Write(doc, "{}"), put(attA, "a", h(1)), put(attB, "b", h(2))), dag, storage)
            assertIs<TransactionResult.Success>(seed)
            val base = seed.commit.hash
            // Someone replaces attA.
            assertIs<TransactionResult.Success>(engine.commit(tx(base, put(attA, "a", h(3))), dag, storage))
            // A concurrent change to attB, on the old base, does not conflict...
            assertIs<TransactionResult.Success>(engine.commit(tx(base, put(attB, "b", h(4))), dag, storage))
            // ...one to attA does,
            val c = engine.commit(tx(base, put(attA, "a", h(5))), dag, storage)
            assertIs<TransactionResult.Conflict>(c)
            assertEquals(ConflictOperationType.CONCURRENT_WRITE, c.conflictingOps.single().type)
            assertEquals(doc.toString(), c.report.conflicts.single().documentId)
            // ...and so does deleting the document whose attachments moved since the base.
            val d = engine.commit(tx(base, KdbOp.Delete(doc)), dag, storage)
            assertIs<TransactionResult.Conflict>(d)
            // A JSON-only write on the old base does not conflict with attachment changes.
            assertIs<TransactionResult.Success>(engine.commit(tx(base, KdbOp.Write(doc, """{"j":1}""")), dag, storage))
        }

    @Test
    fun anAttachmentChangeConflictsWithAConcurrentDocumentDelete() =
        runTest {
            val dag = inMemoryCommitDag("ns")
            val storage = InMemoryStorageAdapter()
            val seed = engine.commit(tx(dag.head(), KdbOp.Write(doc, "{}"), KdbOp.Write(KdbUuid(2, 2), "{}")), dag, storage)
            assertIs<TransactionResult.Success>(seed)
            val base = seed.commit.hash
            val del = engine.commit(tx(base, KdbOp.Delete(doc)), dag, storage)
            assertIs<TransactionResult.Success>(del)
            // The document is gone at the target, so preflight refuses before conflicts are looked at.
            val r = engine.commit(tx(base, put(attA, "a", h(1))), dag, storage)
            assertIs<TransactionResult.SchemaError>(r)
            // Deleting an attachment of a concurrently deleted document is a WRITE_DELETE conflict.
            val r2 = engine.commit(tx(base, KdbOp.AttachmentDelete(doc, attA)), dag, storage)
            assertIs<TransactionResult.Conflict>(r2)
            assertEquals(ConflictOperationType.WRITE_DELETE, r2.conflictingOps.single().type)
        }
}
