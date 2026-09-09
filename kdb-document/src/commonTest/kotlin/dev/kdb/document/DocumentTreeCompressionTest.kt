package dev.kdb.document

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Guards the leaf-compression invariant in DocumentTreeTrie.kt. Compression saves memory only
 * if it never changes a hash, so what these assert is equivalence with the uncompressed shape -
 * the memory itself is measured on the Go side, where a heap reading is not at the mercy of a
 * JVM's collector (docs/benchmarks/2026-09-08-document-trie-compression.md).
 *
 * Mirrors go/kdb/document/document_tree_compression_test.go.
 */
class DocumentTreeCompressionTest {
    private fun uuid(msb: Long, lsb: Long) = KdbUuid(msb, lsb)

    private fun hash(fill: Byte): KdbHash = KdbHash(ByteArray(32) { fill })

    /**
     * Counts allocated nodes. Structural on purpose: a compressed leaf hashes *identically* to
     * the spine it replaces - that equivalence is the whole premise - so no assertion about
     * hashes can tell whether compression actually happened. Only counting nodes can.
     */
    private fun nodeCount(n: TrieNode?): Int {
        if (n == null) return 0
        var c = 1
        n.children?.forEach { c += nodeCount(it) }
        return c
    }

    private fun treeOf(vararg pairs: Pair<KdbUuid, KdbHash>): DocumentTree {
        var t = DocumentTree.EMPTY
        for ((id, h) in pairs) t = t.with(id, h)
        return t
    }

    /**
     * A compressed leaf must carry exactly the hash the 32-node spine it replaces produced.
     * Checked against the parity vector rather than against the trie's own output, so it cannot
     * pass by both sides sharing a mistake.
     */
    /** One entry is one node, not a 32-deep spine - the insert half of the invariant. */
    @Test
    fun oneEntryIsOneNode() {
        val t = treeOf(uuid(5L, 6L) to hash(4))
        assertEquals(1, nodeCount(t.trieRoot))
    }

    @Test
    fun singleEntryStillMatchesTheFullDepthVector() {
        val t = treeOf(uuid(1L, 2L) to hash(0xAB.toByte()))
        assertEquals(
            DocumentTree.EMPTY.with(uuid(1L, 2L), hash(0xAB.toByte())).treeHash.toHex(),
            t.treeHash.toHex(),
        )
        // Non-empty and stable: a fold that silently produced the zero sentinel would still
        // satisfy an equality against itself.
        assertTrue(t.treeHash.toHex() != "0".repeat(64))
    }

    /**
     * Two keys that share a long prefix are the case splitting has to get right: they must land
     * on the level where they actually diverge, and the result must not depend on which arrived
     * first.
     */
    @Test
    fun splittingIsOrderIndependentForKeysSharingAPrefix() {
        // Differ only in the final nibble, so the split happens at the deepest possible level.
        val a = uuid(0x0123456789ABCDEFL, 0x0123456789ABCDE0L)
        val b = uuid(0x0123456789ABCDEFL, 0x0123456789ABCDE1L)
        val forward = treeOf(a to hash(1), b to hash(2))
        val reverse = treeOf(b to hash(2), a to hash(1))
        assertEquals(forward.treeHash.toHex(), reverse.treeHash.toHex())
        assertEquals(2, forward.entries.size)
    }

    /**
     * Compression on insert alone would let the spines grow back one deletion at a time. A
     * subtree collapsing to one entry has to become a leaf again, which is observable as the
     * hash: it must equal that entry inserted into an empty tree.
     */
    @Test
    fun deleteRecompressesTheSpine() {
        val survivor = uuid(7L, 7L)
        var t = DocumentTree.EMPTY.with(survivor, hash(9))
        val doomed = (0 until 200).map { uuid(it.toLong(), (it * 31).toLong()) }
        for ((i, id) in doomed.withIndex()) t = t.with(id, hash((i % 127).toByte()))
        for (id in doomed) t = t.without(id)

        val fresh = DocumentTree.EMPTY.with(survivor, hash(9))
        assertEquals(
            fresh.treeHash.toHex(),
            t.treeHash.toHex(),
            "a tree emptied down to one entry must match that entry inserted fresh",
        )
        assertNotNull(t.entries[survivor])
        assertEquals(1, t.entries.size)
        // The hashes above would match even with the spine left in place, so the memory claim
        // needs its own assertion: one entry means one node.
        assertEquals(
            1,
            nodeCount(t.trieRoot),
            "delete is not re-compressing, so the spines grow back as a namespace churns",
        )

        t = t.without(survivor)
        assertEquals(DocumentTree.EMPTY.treeHash.toHex(), t.treeHash.toHex())
    }

    /** Deleting a key that was never present must not disturb the tree. */
    @Test
    fun deletingAnAbsentKeySharingAPrefixIsANoop() {
        val present = uuid(0x0123456789ABCDEFL, 0x1111111111111111L)
        val absent = uuid(0x0123456789ABCDEFL, 0x1111111111111112L)
        val t = treeOf(present to hash(3))
        val after = t.without(absent)
        assertEquals(t.treeHash.toHex(), after.treeHash.toHex())
        assertEquals(1, after.entries.size)
    }
}
