package dev.kdb.dag

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.AttachmentTable
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.document.attachmentContentHash
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

/**
 * The DAG derives every commit's attachment root from its first parent's table and its operations,
 * as Go's does: the same transactions appended to a fresh "app/t" DAG reach the commit hashes
 * go/kdb/dag/attachments_kotlin_fixture_test.go pins.
 */
class AttachmentDagTest {
    private val docA = KdbUuid.fromString("6f9619ff-8b86-d011-b42d-00cf4fc964ff")
    private val docB = KdbUuid.fromString("11111111-2222-4333-8444-555555555555")
    private val att1 = KdbUuid.fromString("0b6f3c1e-2a4d-4e8f-9a1b-3c5d7e9f1a2b")
    private val att2 = KdbUuid.fromString("ffffffff-0000-4000-8000-000000000001")
    private val author = KdbUuid.fromString("00000000-0000-4000-8000-0000000000bb")
    private val hello = attachmentContentHash("hello".encodeToByteArray())

    private fun filled(b: Int): KdbHash = KdbHash(ByteArray(32) { b.toByte() })

    private suspend fun CommitDag.append(
        id: String,
        millis: Long,
        ops: List<KdbOp>,
    ): KdbCommit {
        val head = head()
        return appendCommit(
            KdbTransaction(KdbUuid.fromString(id), head, ops, KdbTimestamp(millis), author),
            head,
            DocumentTree.EMPTY,
            null,
            "m",
        )
    }

    private suspend fun CommitDag.goFixtureChain(): List<KdbCommit> =
        listOf(
            append(
                "00000000-0000-4000-8000-000000000101",
                1750000000000,
                listOf(
                    KdbOp.Write(docA, """{"a":1}"""),
                    KdbOp.Write(docB, """{"b":2}"""),
                    KdbOp.AttachmentPut(docA, att1, "photo.png", 1, hello, 5, "image/png"),
                    KdbOp.AttachmentPut(docB, att2, "b.bin", 1, filled(0xab), 1234567, ""),
                ),
            ),
            append(
                "00000000-0000-4000-8000-000000000102",
                1750000000001,
                listOf(
                    KdbOp.AttachmentPut(docA, att1, "renamed.png", 1, hello, 5, "image/png"),
                    KdbOp.Delete(docB),
                ),
            ),
            append("00000000-0000-4000-8000-000000000103", 1750000000002, listOf(KdbOp.Write(docA, """{"a":2}"""))),
        )

    @Test
    fun appendedCommitsHashAsGosDo() =
        runTest {
            val dag = inMemoryCommitDag("app/t")
            val (c1, c2, c3) = dag.goFixtureChain()
            assertEquals("de6abf4c2f833d64af1691558fbf5ffe4720659252f8c644aaf1f56bc62f4238", c1.attachmentTreeHash?.toHex())
            assertEquals("268c9447c13c34dda56062ff8da6486bade8b62ff738549bcbd2a01e19197053", c1.hash.toHex())
            assertEquals("ba364b68a1ef152eff592288b3a7ae38619ce67b154f62f306159b6789e6b9e8", c2.attachmentTreeHash?.toHex())
            assertEquals("0d3070272cd0d28ccf4cc4ff480eafd374a7c35fa15e4c32f78e36b98b6cf2c8", c2.hash.toHex())
            assertEquals("e29ec7e15fd88bb85aaba751481a91472147e52455e6fb03aa1c43fe033b097e", c3.hash.toHex())
            assertEquals(c2.attachmentTreeHash, c3.attachmentTreeHash)

            val t1 = dag.attachmentTableAt(c1.hash)
            assertEquals(2, t1.size)
            assertEquals("photo.png", t1.get(docA, att1)?.name)
            val t3 = dag.attachmentTableAt(c3.hash)
            assertEquals("renamed.png", t3.get(docA, att1)?.name)
            assertEquals(0, t3.count(docB))
            assertEquals(AttachmentTable.EMPTY, dag.attachmentTableAt(dag.walk(c1.hash).last().let { (it as TraversalEntry.Full).commit.hash }))
        }

    @Test
    fun receivedCommitsAreVerified() =
        runTest {
            val source = inMemoryCommitDag("app/t")
            val chain = source.goFixtureChain()
            // Replicated in order onto another node, every commit's root checks out.
            val replica = inMemoryCommitDag("app/t")
            for (c in chain) replica.putCommit(c)
            assertEquals(source.attachmentTableAt(chain[2].hash).root, replica.attachmentTableAt(chain[2].hash).root)
            // A commit whose recorded root is not its parent's table plus its ops is refused.
            val forged =
                KdbCommit.build(
                    listOf(chain[2].hash), "app/t", KdbUuid.random(), KdbTimestamp(1750000000003), author,
                    listOf(KdbOp.AttachmentPut(docA, att2, "x", 1, filled(7), 7, "")), DocumentTree.EMPTY.treeHash, null, "",
                    attachmentTreeHash = filled(9),
                )
            assertFailsWith<DagConsistencyException> { replica.putCommit(forged) }
            // So is a root that changes under an op list touching no attachment.
            val drifted =
                KdbCommit.build(
                    listOf(chain[2].hash), "app/t", KdbUuid.random(), KdbTimestamp(1750000000003), author,
                    listOf(KdbOp.Write(docA, "{}")), DocumentTree.EMPTY.treeHash, null, "", attachmentTreeHash = null,
                )
            assertFailsWith<DagConsistencyException> { replica.putCommit(drifted) }
            // And a duplicate name within a document.
            val dup =
                KdbCommit.build(
                    listOf(chain[2].hash), "app/t", KdbUuid.random(), KdbTimestamp(1750000000003), author,
                    listOf(KdbOp.AttachmentPut(docA, att2, "renamed.png", 1, filled(7), 7, "")), DocumentTree.EMPTY.treeHash, null, "",
                    attachmentTreeHash = filled(9),
                )
            assertFailsWith<DagConsistencyException> { replica.putCommit(dup) }
        }

    @Test
    fun tablesRebuildPastTheCacheAndFromPackedSeeds() =
        runTest {
            val dag = inMemoryCommitDag("ns")
            val first =
                dag.append(
                    "00000000-0000-4000-8000-000000000001",
                    1,
                    listOf(KdbOp.Write(docA, "{}"), KdbOp.AttachmentPut(docA, att1, "v0", 1, filled(0), 0, "")),
                )
            var last = first
            for (i in 1..(ATTACHMENT_CACHE_SIZE + 10)) {
                last = dag.append(KdbUuid.random().toString(), 1L + i, listOf(KdbOp.AttachmentPut(docA, att1, "v$i", 1, filled(i), i.toLong(), "")))
            }
            assertEquals("v0", dag.attachmentTableAt(first.hash).get(docA, att1)?.name)
            assertEquals("v${ATTACHMENT_CACHE_SIZE + 10}", dag.attachmentTableAt(last.hash).get(docA, att1)?.name)
            val packed = dag.packAttachmentTables(listOf(first.hash, last.hash, last.hash))!!
            val tables = AttachmentTable.parsePacked(packed)
            assertEquals(2, tables.size)
            assertNull(inMemoryCommitDag("other").packAttachmentTables(listOf(inMemoryCommitDag("other").head())))
        }

    @Test
    fun squashRefusesAWindowWithAttachments() =
        runTest {
            val dag = inMemoryCommitDag("ns")
            val c1 = dag.append(KdbUuid.random().toString(), 1, listOf(KdbOp.Write(docA, "{}"), KdbOp.AttachmentPut(docA, att1, "a", 1, hello, 5, "")))
            val c2 = dag.append(KdbUuid.random().toString(), 2, listOf(KdbOp.Write(docA, "{}")))
            dag.append(KdbUuid.random().toString(), 3, listOf(KdbOp.Write(docA, "{}")))
            assertFailsWith<CompactionSafetyException> { dag.squash(listOf(c1.hash, c2.hash), dag.head(), DocumentTree.EMPTY, null) }
        }
}
