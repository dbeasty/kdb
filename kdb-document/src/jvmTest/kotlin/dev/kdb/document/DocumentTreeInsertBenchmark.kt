package dev.kdb.document

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import kotlin.random.Random
import kotlin.test.Test

/**
 * Per-insert cost of [DocumentTree.with] / [DocumentTree.without] against trees of growing size:
 * the cost ServerStorageEngine.commitTree pays once per committed put or delete. A persistent
 * tree keeps it flat as the tree grows; a copied map makes it grow with the tree.
 *
 * Opt-in (KDB_BENCH=1), since the 1M-document base alone takes tens of seconds to build:
 *
 *     KDB_BENCH=1 ./gradlew :kdb-document:jvmTest --tests '*DocumentTreeInsertBenchmark*' -i
 */
class DocumentTreeInsertBenchmark {
    private val sizes = listOf(1_000, 10_000, 100_000, 1_000_000)

    private companion object {
        const val MAX_OPS = 20_000
    }

    @Test
    fun insertCostBySize() {
        if (System.getenv("KDB_BENCH") != "1") return
        val rnd = Random(42)
        fun id() = KdbUuid(rnd.nextLong(), rnd.nextLong())
        fun hash() = KdbHash(rnd.nextBytes(32))

        for (n in sizes) {
            val m = LinkedHashMap<KdbUuid, KdbHash>(n * 2)
            repeat(n) { m[id()] = hash() }
            val base = DocumentTree.build(m)
            // Every timed insert is a new id: a repeat of an unchanged entry is a no-op, not an insert.
            val fresh = List(MAX_OPS) { id() to hash() }

            // Warm up, then time a fixed budget: enough ops to be stable at 1k, few enough that a
            // per-insert copy of a 1M-entry map still finishes.
            timeInserts(base, fresh.subList(0, MAX_OPS / 4), 200_000_000L)
            val (insOps, insNs) = timeInserts(base, fresh, 1_000_000_000L)
            val (delOps, delNs) = timeDeletes(base, m.keys.take(4096), 1_000_000_000L)
            println(
                "DocumentTreeInsertBenchmark n=$n " +
                    "with=${insNs / insOps} ns/op ($insOps ops) " +
                    "without=${delNs / delOps} ns/op ($delOps ops)",
            )
        }
    }

    private fun timeInserts(base: DocumentTree, fresh: List<Pair<KdbUuid, KdbHash>>, budgetNs: Long): Pair<Long, Long> {
        var t = base
        var ops = 0L
        val start = System.nanoTime()
        while (true) {
            val (id, h) = fresh[ops.toInt()]
            t = t.with(id, h)
            ops++
            val el = System.nanoTime() - start
            if (el >= budgetNs || ops >= fresh.size) return ops to el
        }
    }

    private fun timeDeletes(base: DocumentTree, existing: List<KdbUuid>, budgetNs: Long): Pair<Long, Long> {
        var t = base
        var ops = 0L
        val start = System.nanoTime()
        while (ops < existing.size) {
            t = t.without(existing[ops.toInt()])
            ops++
            val el = System.nanoTime() - start
            if (el >= budgetNs) return ops to el
        }
        return ops to (System.nanoTime() - start)
    }
}
