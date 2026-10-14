package dev.kdb.document

import dev.kdb.codec.KdbUuid
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class UuidHamtTest {
    /** Random puts and removes agree with a plain map, and every earlier version stays as it was. */
    @Test
    fun agreesWithAMapAndKeepsEveryVersion() {
        val r = Random(7)
        var m = UuidHamt.empty<Int>()
        val model = HashMap<KdbUuid, Int>()
        val versions = ArrayList<Pair<UuidHamt<Int>, Map<KdbUuid, Int>>>()
        // A small key space so removes and replaces hit; some keys share a half to stress mixing.
        val keys = List(300) { KdbUuid(if (it % 3 == 0) 1L else r.nextLong(), it.toLong()) }
        repeat(5000) { step ->
            val k = keys[r.nextInt(keys.size)]
            if (r.nextInt(3) == 0) {
                m = m.remove(k)
                model.remove(k)
            } else {
                m = m.put(k, step)
                model[k] = step
            }
            if (step % 500 == 0) versions.add(m to HashMap(model))
        }
        for ((v, want) in versions + (m to model)) {
            assertEquals(want.size, v.size)
            for (k in keys) assertEquals(want[k], v[k])
            val seen = HashMap<KdbUuid, Int>()
            v.forEach { k, value -> seen[k] = value }
            assertEquals(want, seen)
        }
        var e = m
        for (k in keys) e = e.remove(k)
        assertEquals(0, e.size)
        assertNull(e[keys[0]])
    }

    /** A table's versions share their sets: each old one still lists what it had, and a table built
     * fresh from the last one's contents has its root. */
    @Test
    fun tableVersionsKeepTheirSets() {
        val att = KdbUuid(9, 9)
        val docs = List(50) { KdbUuid(it.toLong(), 1) }
        var t = AttachmentTable.EMPTY
        val old = ArrayList<Pair<AttachmentTable, String>>()
        for (i in 0 until 2000) {
            val doc = docs[i % docs.size]
            val ref = AttachmentRef(1, dev.kdb.codec.KdbHash(ByteArray(32) { i.toByte() }), i.toLong(), "x/y")
            t = t.put(doc, Attachment(att, "v$i", ref))
            if (i % 100 == 99) old.add(t to "v$i")
        }
        for ((tab, name) in old) {
            val doc = docs[name.drop(1).toInt() % docs.size]
            assertEquals(name, tab.get(doc, att)?.name)
            assertEquals(docs.size, tab.documents)
        }
        var fresh = AttachmentTable.EMPTY
        for ((doc, a) in t.entries().toList().reversed()) fresh = fresh.put(doc, a)
        assertEquals(t.root, fresh.root)
        assertEquals(t.size, fresh.size)
    }
}
