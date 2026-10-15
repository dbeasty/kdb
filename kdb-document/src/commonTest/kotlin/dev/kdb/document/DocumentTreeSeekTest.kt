package dev.kdb.document

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull

/**
 * DocumentTree.idsAfter must give exactly what sorting every id by its canonical string and dropping
 * those at or before the cursor gives, which is how SNAPSHOT_FETCH paged before it seeked.
 */
class DocumentTreeSeekTest {
    private fun hash(i: Int): KdbHash = KdbHash(ByteArray(32) { (i + it).toByte() })

    private fun reference(entries: Map<KdbUuid, KdbHash>, after: KdbUuid?): List<KdbUuid> {
        val from = after?.toString()
        return entries.keys.map { it to it.toString() }.sortedBy { it.second }.filter { from == null || it.second > from }.map { it.first }
    }

    private fun ids(tree: DocumentTree, after: KdbUuid?): List<KdbUuid> = tree.idsAfter(after).asSequence().toList()

    /** Random ids, plus ones that share long prefixes, sit at the extremes, and have the sign bits set. */
    private fun sampleIds(r: Random, n: Int): List<KdbUuid> {
        val out = LinkedHashSet<KdbUuid>()
        out += KdbUuid(0, 0)
        out += KdbUuid(-1, -1)
        out += KdbUuid(Long.MIN_VALUE, 0)
        out += KdbUuid(Long.MAX_VALUE, -1)
        out += KdbUuid(0, Long.MIN_VALUE)
        val base = r.nextLong()
        for (i in 0 until 16) out += KdbUuid(base, i.toLong())
        out += KdbUuid(base, -1)
        out += KdbUuid(base xor 1, 0)
        while (out.size < n) out += KdbUuid(r.nextLong(), r.nextLong())
        return out.toList()
    }

    private fun cursors(r: Random, present: List<KdbUuid>): List<KdbUuid?> {
        val out = ArrayList<KdbUuid?>()
        out += null
        out += present
        out += KdbUuid(0, 0)
        out += KdbUuid(-1, -1)
        out += KdbUuid(-1, -2)
        // Absent ids next to present ones exercise cursors that end on an empty slot or a foreign leaf.
        for (id in present.take(50)) {
            out += KdbUuid(id.msb, id.lsb + 1)
            out += KdbUuid(id.msb, id.lsb - 1)
            out += KdbUuid(id.msb xor 0x10, id.lsb)
        }
        repeat(100) { out += KdbUuid(r.nextLong(), r.nextLong()) }
        return out
    }

    @Test
    fun matchesSortedFilterForEveryCursor() {
        val r = Random(42)
        for (n in listOf(1, 2, 3, 17, 300)) {
            val all = sampleIds(r, maxOf(n, 0)).take(n)
            val entries = all.withIndex().associate { (i, id) -> id to hash(i) }
            val tree = DocumentTree.build(entries)
            for (c in cursors(r, all)) assertEquals(reference(entries, c), ids(tree, c), "n=$n cursor=$c")
        }
    }

    @Test
    fun followsIncrementalEdits() {
        val r = Random(7)
        val all = sampleIds(r, 400)
        var tree = DocumentTree.EMPTY
        val entries = LinkedHashMap<KdbUuid, KdbHash>()
        for ((i, id) in all.withIndex()) {
            tree = tree.with(id, hash(i))
            entries[id] = hash(i)
        }
        for (id in all.shuffled(r).take(250)) {
            tree = tree.without(id)
            entries.remove(id)
        }
        for (c in cursors(r, all)) assertEquals(reference(entries, c), ids(tree, c), "cursor=$c")
    }

    @Test
    fun emptyTreesAndTreesOverAPlainMap() {
        assertFalse(DocumentTree.EMPTY.idsAfter(null).hasNext())
        assertFalse(DocumentTree.EMPTY.idsAfter(KdbUuid(1, 2)).hasNext())
        val r = Random(3)
        val all = sampleIds(r, 60)
        val entries = all.withIndex().associate { (i, id) -> id to hash(i) }
        // A tree made with the constructor over a plain map builds its trie on first use.
        val bare = DocumentTree(DocumentTree.build(entries).treeHash, HashMap(entries))
        for (c in cursors(r, all)) assertEquals(reference(entries, c), ids(bare, c), "cursor=$c")
    }

    @Test
    fun iteratorIsLazyAndExhausts() {
        val all = sampleIds(Random(9), 50)
        val tree = DocumentTree.build(all.associateWith { hash(0) })
        assertNotNull(tree.trieRoot)
        val it = tree.idsAfter(null)
        var count = 0
        while (it.hasNext()) {
            assertEquals(it.hasNext(), true) // hasNext is idempotent
            it.next()
            count++
        }
        assertEquals(all.size, count)
        kotlin.test.assertFailsWith<NoSuchElementException> { it.next() }
    }
}
