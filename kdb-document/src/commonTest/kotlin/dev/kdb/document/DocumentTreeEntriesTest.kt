package dev.kdb.document

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * DocumentTree.entries is a map view over the trie rather than a copied map. These check it still
 * behaves as the plain map it replaced: same contents, same Map equality/hashCode, and a tree
 * hash identical to building from scratch however the tree was reached.
 */
class DocumentTreeEntriesTest {
    private val rnd = Random(7)

    private fun id() = KdbUuid(rnd.nextLong(), rnd.nextLong())

    private fun hash() = KdbHash(rnd.nextBytes(32))

    /** Applies a random sequence of puts, replaces and deletes to both a tree and a plain map. */
    private fun mirrored(ops: Int): Pair<DocumentTree, Map<KdbUuid, KdbHash>> {
        var t = DocumentTree.EMPTY
        val m = LinkedHashMap<KdbUuid, KdbHash>()
        val ids = ArrayList<KdbUuid>()
        repeat(ops) {
            when {
                ids.isEmpty() || rnd.nextInt(10) < 6 -> {
                    val k = id()
                    ids += k
                    val h = hash()
                    t = t.with(k, h)
                    m[k] = h
                }
                rnd.nextInt(2) == 0 -> {
                    val k = ids[rnd.nextInt(ids.size)]
                    val h = hash()
                    t = t.with(k, h)
                    m[k] = h
                }
                else -> {
                    val k = ids.removeAt(rnd.nextInt(ids.size))
                    t = t.without(k)
                    m.remove(k)
                }
            }
        }
        return t to m
    }

    @Test
    fun viewMatchesAPlainMap() {
        val (t, m) = mirrored(3000)
        assertEquals(m.size, t.size)
        assertEquals(m.size, t.entries.size)
        assertEquals(m, t.entries)
        assertEquals(t.entries, m)
        assertEquals(m.hashCode(), t.entries.hashCode())
        assertEquals(m.keys, t.entries.keys)
        for ((k, v) in m) {
            assertEquals(v, t.hashFor(k))
            assertTrue(t.contains(k))
        }
        assertNull(t.hashFor(id()))
        assertFalse(t.contains(id()))
        assertEquals(DocumentTree.build(m).treeHash, t.treeHash)
        assertEquals(DocumentTree.build(m), t)
    }

    @Test
    fun iterationIsInCanonicalIdOrder() {
        val (t, m) = mirrored(500)
        val ids = t.entries.keys.map { it.toString() }
        assertEquals(m.keys.map { it.toString() }.sorted(), ids)
    }

    @Test
    fun noOpChangesReturnTheSameTree() {
        val (t, _) = mirrored(200)
        val (k, v) = t.entries.entries.first()
        assertSame(t, t.with(k, v))
        assertSame(t, t.without(id()))
    }

    @Test
    fun deletingEverythingIsEmpty() {
        val (t0, m) = mirrored(300)
        var t = t0
        for (k in m.keys) t = t.without(k)
        assertEquals(0, t.size)
        assertTrue(t.entries.isEmpty())
        assertEquals(DocumentTree.EMPTY, t)
        assertEquals(DocumentTree.EMPTY.treeHash, t.treeHash)
    }

    @Test
    fun aTreeOverAForeignMapStaysCorrect() {
        val (t, m) = mirrored(400)
        val foreign = DocumentTree(t.treeHash, HashMap(m))
        assertEquals(t, foreign)
        val k = id()
        val h = hash()
        val next = foreign.with(k, h)
        assertEquals(t.with(k, h), next)
        assertEquals(m.size + 1, next.size)
        val copied = t.copy()
        assertEquals(t, copied)
        assertEquals(t.without(m.keys.first()).treeHash, copied.without(m.keys.first()).treeHash)
    }

    @Test
    fun buildDoesNotRetainTheCallersMap() {
        val m = LinkedHashMap<KdbUuid, KdbHash>()
        repeat(10) { m[id()] = hash() }
        val t = DocumentTree.build(m)
        m[id()] = hash()
        assertEquals(10, t.size)
    }
}
