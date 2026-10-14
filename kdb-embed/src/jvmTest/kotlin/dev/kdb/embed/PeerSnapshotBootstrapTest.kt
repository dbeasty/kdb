package dev.kdb.embed

import dev.kdb.codec.KdbUuid
import dev.kdb.peersync.PeerClientConfig
import dev.kdb.peersync.PeerHostConfig
import dev.kdb.peersync.peerSyncClient
import dev.kdb.peersync.peerSyncHost
import dev.kdb.stream.InMemoryWireTransport
import dev.kdb.wire.defaultWireCodec
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Snapshot bootstrap over Kotlin peer sync v2, between memory runtimes: a fresh node takes a peer's
 * state at its head - documents and attachment bytes, the bytes checked before anything is
 * installed - then catches up on what the peer commits after; and a node bootstrapped so is itself
 * shallow, so a third fresh node pulling from it takes a snapshot too.
 */
class PeerSnapshotBootstrapTest {
    private val wire = defaultWireCodec()
    private val ns = "app/snap"

    private suspend fun host(
        rt: EmbeddedKdbRuntime,
        hub: String,
    ) = peerSyncHost(wire, rt.dag, rt.storage).also {
        it.start(PeerHostConfig(ns, "host-$hub", hub, blobs = rt.peerBlobStore(ns), snapshots = rt.peerSnapshots(ns)))
    }

    private suspend fun pull(
        rt: EmbeddedKdbRuntime,
        hub: String,
        prefer: Boolean,
    ) {
        val client = peerSyncClient(wire, InMemoryWireTransport(), rt.dag, rt.storage)
        client.connect(
            PeerClientConfig(
                ns, "node-$hub", "memory://$hub", blobs = rt.peerBlobStore(ns), snapshots = rt.peerSnapshots(ns),
                materializeCommit = { materializeCommit(rt, ns, it) }, preferSnapshot = prefer,
            ),
        ).pullMissing()
        client.disconnect()
    }

    @Test
    fun aFreshNodeBootstrapsFromASnapshotAndCatchesUp() =
        runBlocking {
            val a = openMemoryRuntime("app", ns)
            val ids = (1..30).map { KdbUuid.random() }
            for ((i, id) in ids.withIndex()) putJson(a, ns, """{"id":"$id","n":$i}""")
            val blob = ByteArray(3 shl 20) { (it * 31).toByte() }
            a.putAttachment(ns, ids[0], AttachmentSource.of(blob), PutAttachmentOptions(name = "big.bin"))
            val hostA = host(a, "snap-a")

            val b = openMemoryRuntime("app", ns)
            pull(b, "snap-a", prefer = true)
            assertEquals(a.dag.head(), b.dag.head())
            assertEquals(listOf(a.dag.head()), b.shallowRootsOf(ns), "bootstrapped from a snapshot, not history")
            suspend fun doc(rt: EmbeddedKdbRuntime, id: KdbUuid) = rt.storage.getDocument(ns, id, rt.dag.getCommitOrThrow(rt.dag.head()).documentTreeHash)?.json
            for (id in ids) {
                assertTrue(doc(a, id) != null)
                assertEquals(doc(a, id), doc(b, id))
            }
            val read = b.getAttachment(ns, ids[0], AttachmentSelector.ByName("big.bin"))
            assertContentEquals(blob, read.reader.readAll())
            read.reader.close()

            // What A commits next arrives as history on top of the root.
            putJson(a, ns, """{"id":"${KdbUuid.random()}","later":true}""")
            pull(b, "snap-a", prefer = true)
            assertEquals(a.dag.head(), b.dag.head())
            assertTrue(b.dag.getCommitOrThrow(b.dag.head()).parentHashes.isNotEmpty())

            // B is shallow: a fresh C pulling from it gets a snapshot without asking for one.
            val hostB = host(b, "snap-b")
            val c = openMemoryRuntime("app", ns)
            pull(c, "snap-b", prefer = false)
            assertEquals(b.dag.head(), c.dag.head())
            assertEquals(listOf(b.dag.head()), c.shallowRootsOf(ns))
            hostA.stop()
            hostB.stop()
        }
}
