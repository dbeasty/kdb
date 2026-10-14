package dev.kdb.dag

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Merge commits merge their parents' attachment tables exactly as Go's do: the graph of
 * go/kdb/dag/attachment_merge_kotlin_fixture_test.go, built here, reaches the same merge commit
 * hashes in both parent orders.
 */
class AttachmentMergeTest {
    private val x = KdbUuid.fromString("6f9619ff-8b86-d011-b42d-00cf4fc964ff")
    private val y = KdbUuid.fromString("11111111-2222-4333-8444-555555555555")
    private val k0 = KdbUuid.fromString("00000000-0000-4000-8000-000000000009")
    private val k1 = KdbUuid.fromString("00000000-0000-4000-8000-00000000000a")
    private val k2 = KdbUuid.fromString("00000000-0000-4000-8000-00000000000b")
    private val k3 = KdbUuid.fromString("00000000-0000-4000-8000-00000000000c")
    private val k4 = KdbUuid.fromString("00000000-0000-4000-8000-00000000000d")
    private val author = KdbUuid.fromString("00000000-0000-4000-8000-0000000000bb")

    private fun fill(b: Int): KdbHash = KdbHash(ByteArray(32) { b.toByte() })

    private fun put(
        doc: KdbUuid,
        id: KdbUuid,
        name: String,
        content: Int,
    ) = KdbOp.AttachmentPut(doc, id, name, 1, fill(content), content.toLong(), "x/y")

    private fun tree(vararg ids: KdbUuid): DocumentTree = ids.fold(DocumentTree.EMPTY) { t, id -> t.with(id, fill(99)) }

    private class Builder(
        val dag: CommitDag,
        val author: KdbUuid,
    ) {
        var seq = 0

        suspend fun commit(
            parents: List<KdbHash>,
            tree: DocumentTree,
            vararg ops: KdbOp,
        ): KdbCommit {
            seq++
            val tx = KdbTransaction(KdbUuid(0x77, seq.toLong()), parents[0], ops.toList(), KdbTimestamp(1750000000000 + seq), author)
            return if (parents.size == 1) {
                dag.appendCommit(tx, parents[0], tree, null, "m")
            } else {
                dag.appendMergeCommit(tx, parents[0], parents[1], tree, null, "m")
            }
        }
    }

    @Test
    fun mergesHashAsGosDo() =
        runTest {
            val dag = inMemoryCommitDag("app/t")
            val b = Builder(dag, author)
            val genesis = dag.head()
            val base =
                b.commit(listOf(genesis), tree(x), KdbOp.Write(x, "{}"), put(x, k1, "keep", 1), put(x, k2, "both", 2), put(x, k3, "gone", 3))
            val ours = b.commit(listOf(base.hash), tree(x), put(x, k2, "both", 20), KdbOp.AttachmentDelete(x, k3), put(x, k4, "new", 40))
            val theirs = b.commit(listOf(base.hash), tree(x), put(x, k2, "both", 21), put(x, k3, "gone", 30), put(x, k0, "new", 50))
            val m1 = b.commit(listOf(ours.hash, theirs.hash), tree(x))
            val m2 = b.commit(listOf(theirs.hash, ours.hash), tree(x))
            assertEquals("f858638eca2a0b24de69b72439980592d035d9402f5aecc8536bf79bf2b08612", m1.hash.toHex())
            assertEquals("25881f5ae35922cfda5575f3835654a9fed0109d8a70e586acd2872ea886fbd9", m2.hash.toHex())
            assertEquals("4e6079dc728984919d9abf6e721e0933b43e3f7c8323b64e614c3f249c467b4c", m1.attachmentTreeHash?.toHex())
            assertEquals(m1.attachmentTreeHash, m2.attachmentTreeHash)
            val t = dag.attachmentTableAt(m1.hash)
            assertEquals(fill(1), t.getByName(x, "keep")?.contentHash)
            assertEquals(fill(30), t.getByName(x, "gone")?.contentHash) // a version beats a removal
            assertEquals(fill(50), t.getByName(x, "new")?.contentHash) // the smaller ID keeps the name
            assertEquals("new~" + k4.toString().substring(0, 8), t.get(x, k4)?.name)
            assertEquals(fill(21), t.get(x, k2)?.contentHash) // the greater content hash
            val m3 = b.commit(listOf(ours.hash, theirs.hash), tree(), KdbOp.Delete(x))
            assertEquals("e80ca239c4bc149b229fef1a38441dc7550e71260b4441d2b66f5fde2d609939", m3.hash.toHex())
            assertEquals(0, dag.attachmentTableAt(m3.hash).count(x))

            val rb = b.commit(listOf(genesis), tree(x, y), KdbOp.Write(x, "{}"), KdbOp.Write(y, "{}"))
            val l = b.commit(listOf(rb.hash), tree(x, y), KdbOp.Write(x, """{"a":1}"""), put(y, k2, "same", 7))
            val hc = b.commit(listOf(rb.hash), tree(x, y), put(x, k1, "d.txt", 1), put(y, k1, "same", 8))
            val r1 = b.commit(listOf(l.hash, hc.hash), tree(x, y), KdbOp.Write(x, """{"a":1}"""))
            val r2 = b.commit(listOf(hc.hash, l.hash), tree(x, y), KdbOp.Write(x, """{"a":1}"""))
            assertEquals("61729ba938295a1d5f6e8117db72aec8829fdbc437009a51c0376cb9ba809233", r1.hash.toHex())
            assertEquals("08e87ef107a6edd513cc0b6e69577c5a8bd515d8bd907db741f6718937631341", r2.hash.toHex())
            for (r in listOf(r1, r2)) {
                val rt = dag.attachmentTableAt(r.hash)
                assertEquals(fill(1), rt.getByName(x, "d.txt")?.contentHash)
                assertEquals(2, rt.count(y))
            }

            // A node receiving the merges derives and accepts the same roots.
            val replica = inMemoryCommitDag("app/t")
            for (c in listOf(base, ours, theirs, m1, m2, m3, rb, l, hc, r1, r2)) replica.putCommit(c)
            assertEquals(m1.attachmentTreeHash, replica.attachmentTableAt(m2.hash).root)
        }
}
