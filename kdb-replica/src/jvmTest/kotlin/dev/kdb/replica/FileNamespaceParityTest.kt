package dev.kdb.replica

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.document.kdbSha256
import dev.kdb.document.KdbOp
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * The file-backed namespace ([FileReplicaNamespace]) and the store-backed one hold the same state the
 * same way: the same local commits hash the same, and every tree node a squash diff compares - the
 * document trie and the attachment trie - is identical, so a host sees no difference between them.
 * Then the file-backed one reopens with its pending commits. No Go needed.
 */
class FileNamespaceParityTest {
    private val ns = "app/parity"
    private val node = "5b2a3c4d-0000-4000-8000-00000000abcd"

    private fun ops(round: Int): List<KdbOp> =
        (0 until 40).map { i ->
            val id = KdbUuid(0x4000L + i * 7919L, (i * round).toLong())
            if (round > 1 && i % 9 == 0) KdbOp.Delete(KdbUuid(0x4000L + i * 7919L, (i * (round - 1)).toLong())) else KdbOp.Write(id, """{"i":$i,"r":$round}""")
        } + KdbOp.AttachmentPut(KdbUuid(0x4000L, 0), KdbUuid(1, round.toLong()), "a$round.txt", 1, KdbHash.fromBytes(kdbSha256(byteArrayOf(round.toByte()))), 10L + round, "text/plain")

    @Test
    fun sameCommitsAndTreeNodesAsTheStoreBackedNamespace() =
        runBlocking {
            val dir = Files.createTempDirectory("kdb-replica-parity").toString()
            val store = ReplicaNamespace.open(MemoryReplicaStore(), ns, node)
            val file = FileReplicaNamespace.open(dir, ns, node)
            for (round in 1..3) {
                val tx = KdbUuid(9, round.toLong())
                val a = store.commitLocal(ops(round), tx, "round $round", 1_760_000_000_000_000L + round)
                val b = file.commitLocal(ops(round), tx, "round $round", 1_760_000_000_000_000L + round)
                assertEquals(a.hash, b.hash, "round $round")
            }
            assertEquals(store.rootHash(), file.rootHash())
            assertEquals(store.attachmentRoot(), file.attachmentRoot())
            assertEquals(store.scan(), file.scan())
            for (kind in TreeKind.entries) {
                for (prefix in listOf("", "0", "00", "000", "0000", "00000000", "0000000000000000", "f", "7")) {
                    for (limit in listOf(0, 1, 8, 64)) {
                        assertEquals(store.treeNode(prefix, limit, kind), file.treeNode(prefix, limit, kind), "$kind \"$prefix\" limit $limit")
                    }
                }
            }
            val head = file.head
            file.close()
            val again = FileReplicaNamespace.open(dir, ns, node)
            assertEquals(head, again.head)
            assertEquals(store.pending, again.pending)
            assertEquals(store.pendingCommits().map { it.hash }, again.pendingCommits().map { it.hash })
            assertEquals(store.scan(), again.scan())
            again.close()
        }
}
