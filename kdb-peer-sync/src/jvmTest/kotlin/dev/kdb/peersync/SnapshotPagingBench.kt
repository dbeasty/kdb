package dev.kdb.peersync

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbDocument
import kotlinx.coroutines.runBlocking
import kotlin.random.Random
import kotlin.test.Test

/**
 * Cost of walking a whole snapshot page by page, sort-per-page (as SNAPSHOT_FETCH was) against the
 * trie seek. Off unless KDB_SNAPSHOT_BENCH is set (to a comma list of sizes, or 1 for 100k,1M):
 *
 *     KDB_SNAPSHOT_BENCH=1 ./gradlew :kdb-peer-sync:jvmTest --tests '*SnapshotPagingBench*' -i
 *
 * The old path's full walk is extrapolated from a few timed pages: at 1M documents and small pages
 * it would run for many minutes.
 */
class SnapshotPagingBench {
    private suspend fun oldPage(
        tree: DocumentTree,
        after: String,
        budget: Int,
        fetch: suspend (List<KdbUuid>) -> List<KdbDocument?>,
    ): Pair<List<SnapshotDoc>, Boolean> {
        val ids = tree.entries.keys.map { it to it.toString() }.sortedBy { it.second }.filter { it.second > after }
        val docs = ArrayList<SnapshotDoc>()
        var size = 0
        var i = 0
        while (i < ids.size) {
            val chunk = ids.subList(i, minOf(ids.size, i + 128))
            val bodies = fetch(chunk.map { it.first })
            var stop = false
            for ((j, d) in bodies.withIndex()) {
                val body = d!!.json
                if (docs.isNotEmpty() && size + body.length > budget) {
                    stop = true
                    break
                }
                size += body.length + 64
                docs += SnapshotDoc(chunk[j].second, body)
            }
            if (stop) break
            i += chunk.size
        }
        return docs to (docs.size == ids.size)
    }

    @Test
    fun walk() =
        runBlocking {
            val env = System.getenv("KDB_SNAPSHOT_BENCH") ?: return@runBlocking
            val sizes = if (env == "1") listOf(100_000, 1_000_000) else env.split(",").map { it.trim().toInt() }
            val body = "{\"v\":\"" + "x".repeat(90) + "\"}" // ~100 bytes, counted as ~164 per document
            val fetch: suspend (List<KdbUuid>) -> List<KdbDocument?> = { ids -> ids.map { KdbDocument(it, body) } }
            for (n in sizes) {
                val r = Random(n)
                val entries = HashMap<KdbUuid, KdbHash>(n * 2)
                val h = KdbHash(ByteArray(32) { 1 })
                while (entries.size < n) entries[KdbUuid(r.nextLong(), r.nextLong())] = h
                var t = System.nanoTime()
                val tree = DocumentTree.build(entries)
                println("n=$n build ${(System.nanoTime() - t) / 1_000_000} ms")
                for (budget in listOf(DEFAULT_PAGE_BYTES, 256 shl 10)) {
                    // New: the whole walk, twice (the first warms the JIT).
                    var pages = 0
                    var newMs = 0.0
                    repeat(2) {
                        pages = 0
                        var cursor = ""
                        t = System.nanoTime()
                        while (true) {
                            val (docs, done) = snapshotPageDocs(tree, cursor, budget, "h", fetch)
                            pages++
                            if (done) break
                            cursor = docs.last().docId
                        }
                        newMs = (System.nanoTime() - t) / 1e6
                    }
                    // Old: a warm-up page, then a few timed pages from spread-out cursors.
                    oldPage(tree, "", budget, fetch)
                    val sorted = entries.keys.map { it.toString() }.sorted()
                    val samples = 5
                    t = System.nanoTime()
                    for (s in 0 until samples) oldPage(tree, sorted[(n.toLong() * s / samples).toInt()], budget, fetch)
                    val oldPerPage = (System.nanoTime() - t) / 1e6 / samples
                    println(
                        "n=$n budget=${budget shr 10}KiB pages=$pages  new: walk ${"%.0f".format(newMs)} ms " +
                            "(${"%.2f".format(newMs / pages)} ms/page)  old: ${"%.1f".format(oldPerPage)} ms/page, " +
                            "walk ~${"%.1f".format(oldPerPage * pages / 1000)} s  speedup ~${"%.0f".format(oldPerPage * pages / newMs)}x",
                    )
                }
            }
        }
}
