package dev.kdb.document

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * trieBuild sorts and builds each node once instead of inserting entry by entry. That is only an
 * optimisation if the result is the same trie: same hash, and the same shape node for node, so a
 * later with/without on a bulk-built tree behaves exactly as on an incrementally built one.
 */
class DocumentTreeBulkBuildTest {
    private fun insertBuilt(entries: Map<KdbUuid, KdbHash>): TrieNode? {
        var root: TrieNode? = null
        for ((id, h) in entries) root = trieInsert(root, id, h)
        return root
    }

    private fun assertSameShape(
        want: TrieNode?,
        got: TrieNode?,
        path: String,
    ) {
        if (want == null || got == null) {
            assertEquals(want == null, got == null, "presence at $path")
            return
        }
        assertTrue(want.hash.contentEquals(got.hash), "hash at $path")
        assertEquals(want.leaf?.uuid, got.leaf?.uuid, "leaf id at $path")
        assertEquals(want.leaf?.hash, got.leaf?.hash, "leaf content at $path")
        assertEquals(want.children == null, got.children == null, "children at $path")
        for (i in 0 until 16) assertSameShape(want.children?.get(i), got.children?.get(i), "$path${i.toString(16)}")
    }

    private fun sample(
        r: Random,
        n: Int,
    ): Map<KdbUuid, KdbHash> {
        val out = LinkedHashMap<KdbUuid, KdbHash>()
        fun put(id: KdbUuid) {
            out[id] = KdbHash(r.nextBytes(32))
        }
        if (n >= 8) {
            // Extremes, sign bits, and ids that share all but their last nibbles, so the deep
            // single-child chains a split builds are exercised too.
            put(KdbUuid(0, 0))
            put(KdbUuid(-1, -1))
            put(KdbUuid(Long.MIN_VALUE, 0))
            put(KdbUuid(Long.MAX_VALUE, -1))
            val base = r.nextLong()
            put(KdbUuid(base, 0x10))
            put(KdbUuid(base, 0x11))
            put(KdbUuid(base, 0x20))
        }
        while (out.size < n) put(KdbUuid(r.nextLong(), r.nextLong()))
        return out
    }

    @Test
    fun matchesInsertionNodeForNode() {
        val r = Random(11)
        for (n in listOf(0, 1, 2, 3, 15, 16, 17, 100, 2000, 4095, 4096, 9000)) {
            val entries = sample(r, n)
            val bulk = trieBuild(entries)
            assertSameShape(insertBuilt(entries), bulk, "n=$n:")
            assertEquals(trieTreeHash(insertBuilt(entries)), trieTreeHash(bulk))
        }
    }

    @Test
    fun editsOnABulkBuiltTreeMatch() {
        val r = Random(12)
        val entries = sample(r, 500)
        var bulk = DocumentTree.build(entries)
        var inc = DocumentTree.EMPTY
        for ((id, h) in entries) inc = inc.with(id, h)
        for (id in entries.keys.shuffled(r).take(200)) {
            bulk = bulk.without(id)
            inc = inc.without(id)
        }
        repeat(100) {
            val id = KdbUuid(r.nextLong(), r.nextLong())
            val h = KdbHash(r.nextBytes(32))
            bulk = bulk.with(id, h)
            inc = inc.with(id, h)
        }
        assertEquals(inc.treeHash, bulk.treeHash)
        assertSameShape(inc.trieRoot, bulk.trieRoot, "")
    }

    @Test
    fun buildingFromATreesOwnEntriesReusesItsTrie() {
        val tree = DocumentTree.build(sample(Random(13), 50))
        assertSame(tree.trieRoot, DocumentTree.build(tree.entries).trieRoot)
    }
}
