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
import kotlin.test.assertIs

/**
 * A merge re-applies the merged branch's field-level changes over the primary, not its whole stored
 * documents: a logged write is the whole document (operationsAsStored), and replaying that as a
 * patch overwrote every field the primary changed concurrently. The expected documents are the same
 * literals as Go's TestMergeAppliesTheBranchesFieldChanges, so both engines must produce them byte
 * for byte.
 */
class MergeFieldChangesTest {
    private class Case(
        val name: String,
        val base: String?,
        val primary: (KdbUuid) -> List<KdbOp>,
        val other: (KdbUuid) -> List<KdbOp>,
        val want: String?,
    )

    private val cases =
        listOf(
            Case("disjoint fields keep both sides", """{"a":1,"b":1}""", { listOf(KdbOp.Write(it, """{"b":3}""")) }, { listOf(KdbOp.Write(it, """{"a":2}""")) }, """{"a":2,"b":3}"""),
            Case(
                "a key the branch removed goes, the primary's change stays",
                """{"a":1,"b":1,"c":1}""",
                { listOf(KdbOp.Write(it, """{"c":2}""")) },
                { listOf(KdbOp.Write(it, """{"a":1,"c":1}""", replace = true)) },
                """{"a":1,"c":2}""",
            ),
            Case("the same field on both sides takes the branch's value", """{"a":1}""", { listOf(KdbOp.Write(it, """{"a":3}""")) }, { listOf(KdbOp.Write(it, """{"a":2}""")) }, """{"a":2}"""),
            Case(
                "a branch edit to a document the primary deleted keeps only the edit",
                """{"a":1,"b":1}""",
                { listOf(KdbOp.Delete(it)) },
                { listOf(KdbOp.Write(it, """{"a":2}""")) },
                """{"a":2}""",
            ),
            Case("a document the branch created arrives whole", null, { emptyList() }, { listOf(KdbOp.Write(it, """{"n":1,"m":"x"}""")) }, """{"n":1,"m":"x"}"""),
            Case("a document the branch deleted is gone", """{"a":1}""", { listOf(KdbOp.Write(it, """{"b":2}""")) }, { listOf(KdbOp.Delete(it)) }, null),
        )

    @Test
    fun mergeAppliesTheBranchesFieldChanges() =
        runTest {
            for (policy in listOf(ConflictPolicy.STRICT, ConflictPolicy.LAST_WRITE)) {
                for (c in cases) {
                    val ns = "app/merge-fields"
                    val dag = inMemoryCommitDag(ns)
                    val storage = InMemoryStorageAdapter()
                    val engine = transactionEngine(policy)
                    val id = KdbUuid.random()

                    // Replay onto an explicit target, so a second branch can be built off the same base.
                    suspend fun commitOn(
                        target: KdbHash,
                        ops: List<KdbOp>,
                    ): KdbHash {
                        val tx = KdbTransaction(KdbUuid.random(), target, ops, KdbTimestamp.now(), KdbUuid.random())
                        return assertIs<TransactionResult.Success>(engine.replay(tx, dag, storage, replayTarget = target)).commit.hash
                    }
                    var base = dag.head()
                    if (c.base != null) base = commitOn(base, listOf(KdbOp.Write(id, c.base)))
                    val left = c.primary(id).takeIf { it.isNotEmpty() }?.let { commitOn(base, it) } ?: base
                    val right = c.other(id).takeIf { it.isNotEmpty() }?.let { commitOn(base, it) } ?: base

                    val merged = assertIs<TransactionResult.Success>(engine.merge(left, right, dag, storage), "$policy ${c.name}")
                    assertEquals(c.want, storage.getDocument(ns, id, merged.commit.documentTreeHash)?.json, "$policy ${c.name}")
                }
            }
        }
}
