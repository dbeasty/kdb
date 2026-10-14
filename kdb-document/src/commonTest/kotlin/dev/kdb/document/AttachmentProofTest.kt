package dev.kdb.document

import dev.kdb.codec.KdbHash
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * [MerkleTree] and [verifyMerkleInclusion] against go/kdb/document/attachment_proof_test.go: the
 * same roots and audit paths as the recursive RFC 6962 definitions, and the range-proof golden
 * vector Go, TS and Kotlin all reproduce.
 */
class AttachmentProofTest {
    @Test
    fun merkleTreeMatchesRfc6962() {
        for (n in 1..70) {
            val leaves = List(n) { chunkLeafHash(byteArrayOf(it.toByte(), n.toByte())) }
            val tree = MerkleTree(leaves)
            assertEquals(merkleTreeHash(leaves), tree.root(), "n=$n: root differs")
            assertEquals(n, tree.leaves)
            for (i in 0 until n) {
                val p = tree.proof(i)
                assertEquals(merkleInclusionProof(leaves, i), p, "n=$n leaf $i: path differs")
                assertTrue(verifyMerkleInclusion(leaves[i], i.toLong(), n.toLong(), p, tree.root()), "n=$n leaf $i: path does not verify")
                // The wrong leaf, index or count does not verify.
                assertFalse(verifyMerkleInclusion(chunkLeafHash(ByteArray(1)), i.toLong(), n.toLong(), p, tree.root()))
                if (n > 1) assertFalse(verifyMerkleInclusion(leaves[i], ((i + 1) % n).toLong(), n.toLong(), p, tree.root()))
                assertFalse(verifyMerkleInclusion(leaves[i], i.toLong(), n + 1L, p + p, tree.root()))
            }
        }
        assertEquals(chunkLeafHash(ByteArray(0)), MerkleTree(emptyList()).root(), "the empty tree is the tree of one empty chunk")
        assertEquals(1, MerkleTree(emptyList()).leaves)
        assertEquals(emptyList(), MerkleTree(emptyList()).proof(0))
    }

    /** Go's rangeProofGoldenBytes: 4 MiB + 5 bytes, byte i = i mod 251 - five chunks, the last 5 long. */
    private fun rangeProofGoldenBytes(): ByteArray = ByteArray(4 * ATTACHMENT_CHUNK_BYTES + 5) { (it % 251).toByte() }

    private val goldenHash = "1a57b9052a9bae5f2bbaa720791a30f99ea84caecaa0898551c84d1746d3ae27"
    private val goldenTree = "b6b9d1a58b2d9dc4d118b3347037b67ed5ba981c51684aad96da01b56091d758"
    private val goldenLeaf =
        listOf(
            "f4e53704c07aef05b5b12a89d6c1e54292fa7d9d8c3ee431c40b6f6ef6d19280",
            "23124c5e2cfe3dc827a1b85edc1a2da44b2797cfaccc2120830b0663a818c044",
            "eff5a4ce2d94f327c9dc3df962c7a293b9dd39731f7ad77cf9c000ae46893633",
            "4e4be56a1af3ce8199934fa45f00954b81a1789f1bdd37fd0c9a176bb84e9c61",
            "80c33e07ae030ed1db9a08ac474659b0bb3a4c92abf785750c4030adbc37c9b3",
        )
    private val goldenPath =
        listOf(
            listOf(
                "23124c5e2cfe3dc827a1b85edc1a2da44b2797cfaccc2120830b0663a818c044",
                "43d36e796ccb572050e71018e752c7b68495fdc5b025bf606d7d249205fc80fb",
                "80c33e07ae030ed1db9a08ac474659b0bb3a4c92abf785750c4030adbc37c9b3",
            ),
            listOf(
                "f4e53704c07aef05b5b12a89d6c1e54292fa7d9d8c3ee431c40b6f6ef6d19280",
                "43d36e796ccb572050e71018e752c7b68495fdc5b025bf606d7d249205fc80fb",
                "80c33e07ae030ed1db9a08ac474659b0bb3a4c92abf785750c4030adbc37c9b3",
            ),
            listOf(
                "4e4be56a1af3ce8199934fa45f00954b81a1789f1bdd37fd0c9a176bb84e9c61",
                "1661cb740559458c2a1dce898f8f510ea07d28501f983c733aec692565131eb7",
                "80c33e07ae030ed1db9a08ac474659b0bb3a4c92abf785750c4030adbc37c9b3",
            ),
            listOf(
                "eff5a4ce2d94f327c9dc3df962c7a293b9dd39731f7ad77cf9c000ae46893633",
                "1661cb740559458c2a1dce898f8f510ea07d28501f983c733aec692565131eb7",
                "80c33e07ae030ed1db9a08ac474659b0bb3a4c92abf785750c4030adbc37c9b3",
            ),
            listOf("b70f08b29c8bc80f288233ea601ba8396567682f18ca787267d5c695ec190f78"),
        )

    @Test
    fun rangeProofGoldens() {
        val data = rangeProofGoldenBytes()
        val leaves = ArrayList<KdbHash>()
        var off = 0
        while (off < data.size) {
            val end = minOf(off + ATTACHMENT_CHUNK_BYTES, data.size)
            leaves.add(chunkLeafHash(data, off, end - off))
            off = end
        }
        val tree = MerkleTree(leaves)
        assertEquals(goldenHash, attachmentContentHash(data).toHex())
        assertEquals(goldenTree, tree.root().toHex())
        assertEquals(attachmentContentHash(data), attachmentIdentity(tree.root(), data.size.toLong()))
        assertEquals(goldenLeaf, leaves.map { it.toHex() })
        for (i in leaves.indices) {
            assertEquals(goldenPath[i], tree.proof(i).map { it.toHex() }, "leaf $i path")
            assertTrue(verifyMerkleInclusion(leaves[i], i.toLong(), leaves.size.toLong(), tree.proof(i), tree.root()))
        }
        assertEquals(1, attachmentChunkCount(0))
        assertEquals(1, attachmentChunkCount(1))
        assertEquals(1, attachmentChunkCount(ATTACHMENT_CHUNK_BYTES.toLong()))
        assertEquals(2, attachmentChunkCount(ATTACHMENT_CHUNK_BYTES + 1L))
        assertEquals(5, attachmentChunkCount(data.size.toLong()))
    }
}
