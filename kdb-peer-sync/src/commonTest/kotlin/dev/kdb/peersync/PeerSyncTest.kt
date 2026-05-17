package dev.kdb.peersync

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.storage.mem.InMemoryStorageAdapter
import dev.kdb.stream.InMemoryWireTransport
import dev.kdb.wire.WireClientMode
import dev.kdb.wire.WireHeader
import dev.kdb.wire.WireMessage
import dev.kdb.wire.WireMessageType
import dev.kdb.wire.HandshakePayload
import dev.kdb.wire.defaultWireCodec
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class PeerSyncTest {
    private val wire = defaultWireCodec()

    @Test
    fun handshakeFullPeer() =
        runTest {
            val ns = "app/peer"
            val dag = inMemoryCommitDag(ns)
            val host = peerSyncHost(wire, dag, InMemoryStorageAdapter())
            host.start(PeerHostConfig(ns, "host", ns))
            val frame =
                wire.encode(
                    WireMessage.Handshake(
                        WireHeader(WireMessageType.HANDSHAKE, 1, 1, 0),
                        HandshakePayload(
                            nodeId = "client",
                            namespaces = listOf(ns),
                            localHeads = mapOf(ns to dag.head().toHex()),
                            clientMode = WireClientMode.FULL_PEER,
                        ),
                    ),
                )
            val ack = host.handleFrame(frame)
            assertTrue(ack != null)
            val decoded = wire.decode(ack!!) as WireMessage.HandshakeAck
            assertTrue(decoded.response.accepted)
            assertTrue(decoded.response.remoteHeads.containsKey(ns))
            host.stop()
        }

    @Test
    fun fetchLinearChain() =
        runTest {
            val ns = "app/fetch"
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val parent = dag.head()
            val c1 = appendCommit(dag, storage, ns, parent, "a")
            val c2 = appendCommit(dag, storage, ns, c1.hash, "b")
            val host = peerSyncHost(wire, dag, storage) as DefaultPeerSyncHost
            host.start(PeerHostConfig(ns, "test", ns))
            val fetched = host.fetchCommits(sinceHash = c1.hash, maxCommits = 10)
            host.stop()
            assertEquals(1, fetched.size)
            assertEquals(c2.hash, fetched.single().hash)
        }

    @Test
    fun pullMissing() =
        runTest {
            val ns = "app/pull"
            val hub = ns
            val localDag = inMemoryCommitDag(ns)
            val remoteDag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val parent = remoteDag.head()
            appendCommit(remoteDag, storage, ns, parent, "remote")
            val remoteHost = peerSyncHost(wire, remoteDag, storage)
            remoteHost.start(PeerHostConfig(ns, "remote-host", hub))
            val client = peerSyncClient(wire, InMemoryWireTransport(), localDag, storage)
            val session = client.connect(PeerClientConfig(ns, "local", "memory://$hub"))
            val result = session.pullMissing()
            assertEquals(1, result.appliedCommits)
            assertEquals(remoteDag.head(), localDag.head())
            client.disconnect()
            remoteHost.stop()
        }

    @Test
    fun computePlanFork() =
        runTest {
            val ns = "app/fork"
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val root = dag.head()
            val a = appendCommit(dag, storage, ns, root, "a")
            val b = appendCommit(dag, storage, ns, a.hash, "b")
            dag.setHead("main", a.hash)
            val c = appendCommit(dag, storage, ns, a.hash, "c")
            val plan = computeSyncPlan(dag, b.hash, c.hash)
            assertEquals(a.hash, plan.commonAncestor)
            assertTrue(plan.localOnly.contains(b.hash))
            assertTrue(plan.remoteOnly.contains(c.hash))
        }

    @Test
    fun namespaceMismatch() =
        runTest {
            val ns = "app/ns"
            val dag = inMemoryCommitDag(ns)
            val host = peerSyncHost(wire, dag, InMemoryStorageAdapter())
            host.start(PeerHostConfig(ns, "host", ns))
            val frame =
                wire.encode(
                    WireMessage.CommitFetch(
                        WireHeader(WireMessageType.COMMIT_FETCH, 1, 2, 0),
                        "other/ns",
                        null,
                        10,
                    ),
                )
            assertFailsWith<PeerSyncException> {
                host.handleFrame(frame)
            }
            host.stop()
        }

    private suspend fun appendCommit(
        dag: dev.kdb.dag.CommitDag,
        storage: InMemoryStorageAdapter,
        ns: String,
        parent: KdbHash,
        json: String,
    ): KdbCommit {
        val doc = KdbDocument(KdbUuid.random(), """{"v":"$json"}""")
        storage.putDocument(ns, doc)
        val tree = storage.commitTree(ns, dag.getCommitOrThrow(parent).documentTreeHash)
        val tx =
            KdbTransaction(
                KdbUuid.random(),
                parent,
                listOf(KdbOp.Write(doc.id, doc.json)),
                KdbTimestamp.now(),
                KdbUuid.random(),
            )
        return dag.appendCommit(tx, parent, tree, null)
    }
}
