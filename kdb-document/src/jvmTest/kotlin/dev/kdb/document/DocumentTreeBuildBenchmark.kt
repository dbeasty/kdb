package dev.kdb.document

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import kotlin.random.Random
import kotlin.test.Test

/**
 * Cost of [DocumentTree.build] over n random documents: what opening a namespace from a Go
 * checkpoint, installing a snapshot, or serving a peer's snapshot of a fresh head pays once.
 *
 * Opt-in (KDB_BENCH=1; KDB_BENCH_SIZES to override the sizes):
 *
 *     KDB_BENCH=1 ./gradlew :kdb-document:jvmTest --tests '*DocumentTreeBuildBenchmark*' --rerun
 */
class DocumentTreeBuildBenchmark {
    @Test
    fun buildCostBySize() {
        if (System.getenv("KDB_BENCH") != "1") return
        val sizes = System.getenv("KDB_BENCH_SIZES")?.split(",")?.map { it.trim().toInt() } ?: listOf(100_000, 1_000_000)
        for (n in sizes) {
            val r = Random(n)
            val m = HashMap<KdbUuid, KdbHash>(n * 2)
            while (m.size < n) m[KdbUuid(r.nextLong(), r.nextLong())] = KdbHash(r.nextBytes(32))
            // The first build warms the JIT; report the best of the rest.
            val runs = if (n >= 1_000_000) 2 else 4
            var best = Long.MAX_VALUE
            var hash = ""
            repeat(runs) {
                val t = System.nanoTime()
                val tree = DocumentTree.build(m)
                val el = System.nanoTime() - t
                if (it > 0 || runs == 1) best = minOf(best, el)
                hash = tree.treeHash.toHex()
            }
            println("DocumentTreeBuildBenchmark n=$n build=${best / 1_000_000} ms (${best / n} ns/doc) hash=${hash.take(16)}")
        }
    }
}
