package dev.kdb.replica

import kotlinx.coroutines.runBlocking
import java.io.File
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** Commit hashes, content hashes and trie roots against Go's fixtures (packages/kdb-replica/test/golden). */
class GoldenTest {
    private val commits = golden("commits.json").arr.map { it.obj }

    @Test
    fun genesisIsGos() {
        for (ns in listOf("app/u/42", "orders")) {
            val want = commits.single { it.s("name") == "genesis:$ns" }
            val g = genesisCommit(ns)
            assertEquals(want.s("hash"), g.hash, "genesis of $ns")
            assertEquals(want.s("payloadHex"), g.payload.toHex(), "genesis payload of $ns")
        }
    }

    @Test
    fun commitsDecodeToGosHashAndReencodeByteForByte() {
        for (f in commits) {
            val payload = hexToBytes(f.s("payloadHex"))
            val c = ReplicaCommit.decode(payload)
            assertEquals(f.s("hash"), c.hash, f.s("name"))
            assertEquals(f.s("treeHash"), c.treeHash, f.s("name"))
            assertEquals(f["parents"]!!.arr.map { it.toString().trim('"') }, c.parents, f.s("name"))
            // kdb-document's codec re-encodes exactly what Go wrote.
            assertContentEquals(payload, c.commit.toPayloadBytes(), f.s("name"))
        }
    }

    @Test
    fun commitListRoundTrips() {
        val list = commits.map { ReplicaCommit.decode(hexToBytes(it.s("payloadHex"))) }
        assertEquals(list.map { it.hash }, decodeCommitList(encodeCommitList(list)).map { it.hash })
        assertEquals(emptyList(), decodeCommitList(ByteArray(0)))
        assertTrue(runCatching { decodeCommitList(byteArrayOf(-1, -1, -1, 0x0f)) }.isFailure)
    }

    @Test
    fun contentHashesMatchGos() {
        // the hashes.json case @limidus/kdb-client checks too
        val f = File(repoRoot, "packages/kdb-client/test/golden/hashes.json")
        for (h in wireJson.parseToJsonElement(f.readText()).arr.map { it.obj }) {
            assertEquals(h.s("contentHash"), contentHash(h.s("docId"), h.s("json")))
        }
    }

    @Test
    fun treeRootsAreGos() =
        runBlocking {
            for (f in golden("trees.json").arr.map { it.obj }) {
                val name = f.s("name")
                val entries = f["entries"]!!.arr.map { it.obj.s("id") to it.obj.s("contentHash") }
                val store = MemoryReplicaStore()
                val edit = TrieEdit(store, "ns")
                for ((id, h) in entries) edit.set(id, h)
                assertEquals(f.s("root"), edit.finish(), "tree $name")
                store.apply(Batch().also(edit::writeTo))
                val reader = TrieReader(store, "ns")
                assertEquals(f.s("root"), reader.rootHash(), "tree $name reread")
                val seen = ArrayList<String>()
                reader.walk { id, _ ->
                    seen += id
                    true
                }
                assertEquals(entries.map { it.first }, seen, "walk visits ids in order ($name)")
                for ((id, h) in entries.take(50)) assertEquals(h, reader.lookup(id))
            }
        }

    @Test
    fun treeEditStepsAreGos() =
        runBlocking {
            for (f in golden("tree-edits.json").arr.map { it.obj }) {
                val store = MemoryReplicaStore()
                for ((i, step) in f["steps"]!!.arr.map { it.obj }.withIndex()) {
                    val edit = TrieEdit(store, "ns")
                    for (e in step["edits"]!!.arr.map { it.obj }) {
                        if (e.s("contentHash").isNotEmpty()) edit.set(e.s("id"), e.s("contentHash")) else edit.delete(e.s("id"))
                    }
                    assertEquals(step.s("root"), edit.finish(), "${f.s("name")} step $i")
                    store.apply(Batch().also(edit::writeTo))
                    assertEquals(step.s("root"), TrieReader(store, "ns").rootHash(), "${f.s("name")} step $i reread")
                }
            }
        }

    @Test
    fun deletingEverythingLeavesNoRecords() =
        runBlocking {
            val f = golden("trees.json").arr.map { it.obj }.single { it.s("name") == "random300" }
            val ids = f["entries"]!!.arr.map { it.obj.s("id") to it.obj.s("contentHash") }
            val store = MemoryReplicaStore()
            var edit = TrieEdit(store, "ns")
            for ((id, h) in ids) edit.set(id, h)
            edit.finish()
            store.apply(Batch().also(edit::writeTo))
            edit = TrieEdit(store, "ns")
            for ((id, _) in ids) edit.delete(id)
            assertEquals("0".repeat(64), edit.finish())
            store.apply(Batch().also(edit::writeTo))
            assertEquals(emptyList(), store.scan("t/", prefixEnd("t/")))
        }

    @Test
    fun syncFramesRoundTrip() {
        val env = envelope("refsRequest", kotlinx.serialization.json.buildJsonObject { put("namespaces", stringArray(listOf("a/b"))) })
        val frame = encodeSyncFrame(SyncMsg.REFS_REQUEST, 7, env)
        assertEquals(frame.size, (frame[0].toInt() and 0xff) or ((frame[1].toInt() and 0xff) shl 8))
        val back = decodeSyncFrame(frame)
        assertEquals(SyncMsg.REFS_REQUEST, back.code)
        assertEquals(7, back.correlationId)
        assertEquals("refsRequest", back.kind)
        assertEquals(env, back.envelope)
    }

    @Test
    fun projectionNamesAreGos() {
        // peersync.ProjectionNamespace: the first 8 hex digits of SHA-256 of the trimmed filter.
        val name = projectionNamespace("shop/orders", "  region = 'EU' ")
        assertEquals(name, projectionNamespace("shop/orders", "region = 'EU'"))
        assertTrue(Regex("^shop/orders\\.projection-[0-9a-f]{8}$").matches(name))
    }
}
