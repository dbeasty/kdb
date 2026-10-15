package dev.kdb.peersync

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbDocument
import kotlinx.coroutines.test.runTest
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * SNAPSHOT_FETCH pages must be what they were before the host seeked through the trie: same
 * documents, same order, same done flag, for any cursor a peer can send.
 */
class SnapshotPagingTest {
    /** The page builder as it was, sorting every id per page (kept verbatim as the reference). */
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
                val id = chunk[j]
                val body = d!!.json
                if (docs.isNotEmpty() && size + body.length > budget) {
                    stop = true
                    break
                }
                size += body.length + 64
                docs += SnapshotDoc(id.second, body)
            }
            if (stop) break
            i += chunk.size
        }
        return docs to (docs.size == ids.size)
    }

    private fun corpus(n: Int, seed: Int): Map<KdbUuid, String> {
        val r = Random(seed)
        val out = LinkedHashMap<KdbUuid, String>()
        out[KdbUuid(-1, -1)] = "{}"
        out[KdbUuid(0, 0)] = "{\"z\":0}"
        while (out.size < n) out[KdbUuid(r.nextLong(), r.nextLong())] = "{\"v\":\"" + "x".repeat(r.nextInt(0, 400)) + "\"}"
        return out
    }

    private fun treeOf(bodies: Map<KdbUuid, String>): DocumentTree =
        DocumentTree.build(bodies.mapValues { (_, json) -> KdbHash(ByteArray(32) { json.length.toByte() }) })

    @Test
    fun everyPageOfAFullWalkMatches() =
        runTest {
            val bodies = corpus(1000, 1)
            val tree = treeOf(bodies)
            val fetch: suspend (List<KdbUuid>) -> List<KdbDocument?> = { ids -> ids.map { KdbDocument(it, bodies.getValue(it)) } }
            for (budget in listOf(1, 300, 4096, 64 * 1024, 1 shl 30)) {
                var cursor = ""
                var pages = 0
                var seen = 0
                while (true) {
                    val want = oldPage(tree, cursor, budget, fetch)
                    val got = snapshotPageDocs(tree, cursor, budget, "head", fetch)
                    assertEquals(want, got, "budget=$budget page=$pages cursor=$cursor")
                    seen += got.first.size
                    pages++
                    if (got.second) break
                    cursor = got.first.last().docId
                }
                assertEquals(bodies.size, seen, "budget=$budget")
                if (budget == 1) assertEquals(bodies.size, pages)
            }
        }

    @Test
    fun oddCursorsMatch() =
        runTest {
            val bodies = corpus(300, 2)
            val tree = treeOf(bodies)
            val fetch: suspend (List<KdbUuid>) -> List<KdbDocument?> = { ids -> ids.map { KdbDocument(it, bodies.getValue(it)) } }
            val some = bodies.keys.first { it.msb != 0L && it.msb != -1L }.toString()
            val cursors =
                listOf(
                    "", "0", "7", "8", "f", "g", "zzz", "-", "00000000-0000-0000-0000-000000000000",
                    "ffffffff-ffff-ffff-ffff-ffffffffffff", some, some.uppercase(), some.replace("-", ""),
                    some.dropLast(1), some + "0", " " + some,
                )
            for (c in cursors) {
                for (budget in listOf(1, 2048, 1 shl 30)) {
                    assertEquals(oldPage(tree, c, budget, fetch), snapshotPageDocs(tree, c, budget, "head", fetch), "cursor=\"$c\" budget=$budget")
                }
            }
        }

    @Test
    fun missingBodyStillFails() =
        runTest {
            val bodies = corpus(10, 3)
            val tree = treeOf(bodies)
            val e = runCatching { snapshotPageDocs(tree, "", 1 shl 20, "abc") { ids -> ids.map { null } } }.exceptionOrNull()
            assertTrue(e is PeerSyncException && e.message!!.contains("of abc has no body here"), "$e")
        }
}
