package dev.kdb.document

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Kotlin attachments against Go's pinned values: go/kdb/document/attachment_golden_test.go and
 * attachment_kotlin_fixture_test.go (which generates the fixture values below and pins them on the
 * Go side too - regenerate both together).
 */
class AttachmentParityTest {
    private fun hex(s: String): ByteArray = ByteArray(s.length / 2) { s.substring(it * 2, it * 2 + 2).toInt(16).toByte() }

    private fun toHex(b: ByteArray): String = b.joinToString("") { (it.toInt() and 0xFF).toString(16).padStart(2, '0') }

    private fun filled(b: Int): KdbHash = KdbHash(ByteArray(32) { b.toByte() })

    private fun fixtureData(): ByteArray = ByteArray(3 * ATTACHMENT_CHUNK_BYTES + 5) { (it * 7).toByte() }

    private val docA = KdbUuid.fromString("6f9619ff-8b86-d011-b42d-00cf4fc964ff")
    private val docB = KdbUuid.fromString("11111111-2222-4333-8444-555555555555")
    private val att1 = KdbUuid.fromString("0b6f3c1e-2a4d-4e8f-9a1b-3c5d7e9f1a2b")
    private val att2 = KdbUuid.fromString("ffffffff-0000-4000-8000-000000000001")
    private val att3 = KdbUuid.fromString("00000000-0000-4000-8000-0000000000aa")

    private fun fixtureOps(): List<KdbOp> =
        listOf(
            KdbOp.Write(docA, """{"a":1}"""),
            KdbOp.Write(docB, """{"b":2}"""),
            KdbOp.AttachmentPut(docA, att1, "photo.png", 1, attachmentContentHash("hello".encodeToByteArray()), 5, "image/png"),
            KdbOp.AttachmentPut(docA, att2, "é ☃.txt", 1, filled(0xab), 1234567, ""),
            KdbOp.AttachmentPut(docB, att3, "doc.pdf", 1, attachmentContentHash(ByteArray(0)), 0, "application/pdf"),
            KdbOp.AttachmentPut(docB, att1, "tmp", 1, filled(1), 1, "x/y"),
            KdbOp.AttachmentDelete(docB, att1),
        )

    @Test
    fun refSetAndHelloGoldens() {
        val r = AttachmentRef(1, filled(0xab), 1234567, "image/png")
        assertEquals("365f1bd0c0b790d6812664a83229589447f5a9a3156bfa137b65320621ff0a68", r.hash().toHex())
        val set = AttachmentSet.of(listOf(Attachment(KdbUuid(1, 2), "a.png", r)))
        assertEquals("dd3ca4b42f1408538c2e5a1c84c888b6670fb9da99b2029a80eb9c8d411630d4", set.hash().toHex())
        assertEquals(
            "cf1b6c96f0fcdb59fffdc5a353309421c73530cf0639f595d3a42546131c168b",
            attachmentContentHash("hello".encodeToByteArray()).toHex(),
        )
    }

    @Test
    fun multiChunkContentHashesMatchGo() {
        val data = fixtureData()
        assertEquals("c9b63a6ef1b604ed023c66d0ea3f9935caf4713028a5a0b06255522879842cf7", attachmentContentHash(data).toHex())
        assertEquals(
            "356146ad0b53c660daa6370d94ece3770d1add8fbbe7e4197408c9a6718e8e0f",
            attachmentContentHash(data.copyOf(3 * ATTACHMENT_CHUNK_BYTES)).toHex(),
        )
        assertEquals("c0e50f0d90d6e2fea1e6f53d4f758ed5fc9035a399a8eb215c29c6ff3ea05425", attachmentContentHash(ByteArray(0)).toHex())
        // The streaming hasher, fed in odd pieces, agrees.
        val h = AttachmentHasher()
        var off = 0
        var step = 1
        while (off < data.size) {
            val n = minOf(step, data.size - off)
            h.update(data, off, n)
            off += n
            step = step * 3 + 7
        }
        assertEquals(attachmentContentHash(data), h.finish())
        assertEquals(4, h.leafHashes.size)
        assertEquals(attachmentContentHash(ByteArray(0)), AttachmentHasher().finish())
    }

    @Test
    fun inclusionProofMatchesGo() {
        val data = fixtureData()
        val leaves = (0 until 4).map { i -> chunkLeafHash(data, i * ATTACHMENT_CHUNK_BYTES, minOf(ATTACHMENT_CHUNK_BYTES, data.size - i * ATTACHMENT_CHUNK_BYTES)) }
        val proof = merkleInclusionProof(leaves, 2)
        assertEquals(
            "52de9c7c5d4c7e62f1d725dcb1d56782cbe3e5826028d45ed1a081fc00e14eba08ca36f4851a8761dcec63ebaac67067e1749b7b3c3622e69332965d805d5bf1",
            proof.joinToString("") { it.toHex() },
        )
        assertTrue(verifyMerkleInclusion(leaves[2], 2, 4, proof, merkleTreeHash(leaves)))
        assertEquals(attachmentIdentity(merkleTreeHash(leaves), data.size.toLong()), attachmentContentHash(data))
    }

    @Test
    fun inclusionProofsForEverySize() {
        for (n in 1..17) {
            val leaves = (0 until n).map { chunkLeafHash(byteArrayOf(it.toByte(), n.toByte())) }
            val m = MerkleBuilder()
            leaves.forEach { m.add(it) }
            val tree = merkleTreeHash(leaves)
            assertEquals(tree, m.root(), "n=$n: streaming builder disagrees with the recursive tree hash")
            for (i in 0 until n) {
                val proof = merkleInclusionProof(leaves, i)
                assertTrue(verifyMerkleInclusion(leaves[i], i.toLong(), n.toLong(), proof, tree), "n=$n i=$i")
                assertFalse(verifyMerkleInclusion(chunkLeafHash("forged".encodeToByteArray()), i.toLong(), n.toLong(), proof, tree))
                if (n > 1) assertFalse(verifyMerkleInclusion(leaves[i], ((i + 1) % n).toLong(), n.toLong(), proof, tree))
            }
        }
    }

    @Test
    fun tableRootAndPackedBytesMatchGo() {
        val tab = AttachmentTable.EMPTY.apply(fixtureOps())
        assertEquals(3, tab.size)
        assertEquals(2, tab.documents)
        assertEquals("f5d1f686bf4e4507ef7881b95b6d250b9312adfcfe6bbf4bdfcbd7afaaf35712", tab.root?.toHex())
        val goPacked =
            "010311111111222243338444555555555555000000000000400080000000000000aa07646f632e70646601c0e50f0d90d6e2fea1e6f53d4f758ed5fc9035a399a8eb215c29c6ff3ea0542500000000000000000f6170706c69636174696f6e2f7064666f9619ff8b86d011b42d00cf4fc964ff0b6f3c1e2a4d4e8f9a1b3c5d7e9f1a2b0970686f746f2e706e6701cf1b6c96f0fcdb59fffdc5a353309421c73530cf0639f595d3a42546131c168b000000000000000509696d6167652f706e676f9619ff8b86d011b42d00cf4fc964ffffffffff0000400080000000000000010ac3a920e298832e74787401abababababababababababababababababababababababababababababababab000000000012d68700"
        assertEquals(goPacked, toHex(AttachmentTable.pack(listOf(tab, AttachmentTable.EMPTY))))
        val back = AttachmentTable.parsePacked(hex(goPacked))
        assertEquals(1, back.size)
        assertEquals(tab.root, back[0].root)
        assertEquals(3, back[0].size)
        assertFailsWith<AttachmentException> { AttachmentTable.parsePacked(hex(goPacked).copyOf(hex(goPacked).size - 2)) }
    }

    @Test
    fun commitWithDerivedRootHashesAsGo() {
        val ops = fixtureOps()
        val root = AttachmentTable.EMPTY.apply(ops).root
        val c =
            KdbCommit.build(
                parentHashes = listOf(filled(1)),
                namespaceId = "app/data",
                transactionId = docA,
                timestamp = KdbTimestamp(1750000000000, 5),
                authorNodeId = docA,
                operations = ops,
                documentTreeHash = filled(2),
                schemaHash = null,
                message = "m",
                attachmentTreeHash = root,
            )
        assertEquals("6078b2d1eb03b628bb2d9c304f38295adff19901bf3d1c8b8021f1a0975695a4", c.hash.toHex())
        assertEquals(c.hash, computeCommitHash(c))
        assertEquals(c, KdbCommit.fromPayloadBytes(c.toPayloadBytes()))
        val plain = KdbCommit.build(listOf(filled(1)), "app/data", docA, KdbTimestamp(1750000000000, 5), docA, ops, filled(2), null, "m")
        assertNotEquals(plain.hash, c.hash)
        assertEquals(plain.hash, KdbCommit.build(listOf(filled(1)), "app/data", docA, KdbTimestamp(1750000000000, 5), docA, ops, filled(2), null, "m", null).hash)
    }

    private fun att(
        id: Int,
        name: String,
        content: Int,
    ): Attachment = Attachment(KdbUuid(id.toLong(), 1), name, AttachmentRef(1, filled(content), content.toLong(), "x/y"))

    @Test
    fun severalPerDocument() {
        val doc = KdbUuid(1, 0)
        val other = KdbUuid(2, 0)
        var tab = AttachmentTable.EMPTY
        for (a in listOf(att(1, "a.png", 1), att(2, "b.pdf", 2), att(3, "c.txt", 3))) tab = tab.put(doc, a)
        tab = tab.put(other, att(1, "a.png", 9))
        assertEquals(3, tab.count(doc))
        assertEquals(4, tab.size)
        assertEquals(2, tab.documents)
        assertEquals(att(2, "", 0).id, tab.getByName(doc, "b.pdf")?.id)
        assertEquals("c.txt", tab.get(doc, att(3, "", 0).id)?.name)
        val replaced = tab.put(doc, att(2, "b-v2.pdf", 7))
        assertEquals("b-v2.pdf", replaced.get(doc, att(2, "", 0).id)?.name)
        assertEquals(filled(7), replaced.get(doc, att(2, "", 0).id)?.contentHash)
        assertNull(replaced.getByName(doc, "b.pdf"))
        assertEquals(filled(1), replaced.get(doc, att(1, "", 0).id)?.contentHash)
        assertFailsWith<AttachmentException> { tab.put(doc, att(4, "a.png", 4)) }
        assertFailsWith<AttachmentException> { tab.put(doc, Attachment(KdbUuid(0, 0), "x", att(0, "", 1).ref)) }
        // The lone surrogate is built at run time: a "\uD800" literal reaches Kotlin/JS already replaced by "?".
        for (bad in listOf("", "x".repeat(256), "a\nb", "a\u007fb", Char(0xD800).toString() + "x")) {
            assertFailsWith<AttachmentException>("name '$bad'") { tab.put(doc, att(5, bad, 5)) }
        }
        tab.put(doc, att(5, "é".repeat(127), 5)) // 254 bytes
        assertFailsWith<AttachmentException> { tab.put(doc, att(5, "é".repeat(128), 5)) } // 256 bytes
        val one = tab.delete(doc, att(1, "", 0).id)
        assertEquals(2, one.count(doc))
        val none = one.deleteAll(doc)
        assertEquals(0, none.count(doc))
        assertEquals(1, none.documents)
        var rev = AttachmentTable.EMPTY
        for (a in listOf(att(3, "c.txt", 3), att(1, "a.png", 1), att(2, "b.pdf", 2))) rev = rev.put(doc, a)
        rev = rev.put(other, att(1, "a.png", 9))
        assertEquals(tab.root, rev.root)
        assertNotEquals(tab.root, tab.put(doc, att(1, "renamed.png", 1)).root)
        assertNull(AttachmentTable.EMPTY.root)
    }

    @Test
    fun applyRules() {
        val a = KdbUuid(1, 0)
        val idA = KdbUuid(10, 0)
        val idB = KdbUuid(11, 0)

        fun put(
            id: KdbUuid,
            name: String,
            h: Int,
        ) = KdbOp.AttachmentPut(a, id, name, 1, filled(h), h.toLong(), "x")
        val t1 = AttachmentTable.EMPTY.apply(listOf(put(idA, "one", 1), put(idB, "two", 2), KdbOp.Write(a, "{}")))
        assertEquals(2, t1.count(a))
        val t2 = t1.apply(listOf(KdbOp.Delete(a)))
        assertEquals(0, t2.count(a))
        assertNull(t2.root)
        val t3 = t1.apply(listOf(KdbOp.AttachmentDelete(a, idA), put(idA, "again", 3)))
        assertEquals("again", t3.get(a, idA)?.name)
        assertEquals(filled(3), t3.get(a, idA)?.contentHash)
        assertFailsWith<AttachmentException> { AttachmentTable.EMPTY.apply(listOf(put(idA, "x", 1), put(idB, "x", 2))) }
        assertFailsWith<AttachmentException> { AttachmentTable.EMPTY.apply(listOf(put(idA, "x", 1).copy(scheme = 9))) }
        assertTrue(touchesAttachments(listOf(KdbOp.Delete(a)), true))
        assertFalse(touchesAttachments(listOf(KdbOp.Delete(a)), false))
    }
}
