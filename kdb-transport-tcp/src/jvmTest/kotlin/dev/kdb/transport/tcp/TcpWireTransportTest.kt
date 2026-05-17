package dev.kdb.transport.tcp

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.peersync.PeerClientConfig
import dev.kdb.peersync.PeerHostConfig
import dev.kdb.peersync.peerSyncClient
import dev.kdb.peersync.peerSyncHost
import dev.kdb.storage.mem.InMemoryStorageAdapter
import dev.kdb.wire.HandshakePayload
import dev.kdb.wire.WireClientMode
import dev.kdb.wire.WireHeader
import dev.kdb.wire.WireMessage
import dev.kdb.wire.WireMessageType
import dev.kdb.wire.defaultWireCodec
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class TcpWireTransportTest {
    private val wire = defaultWireCodec()
    private val transport = defaultTcpWireTransport()

    @Test
    fun tcpRoundtrip_loopback() =
        runBlocking {
            val server = TcpLoopbackServer()
            server.start { conn ->
                val frame = conn.incoming().first()
                conn.send(frame)
            }
            val client = transport.connect(server.connectUri)
            val ping = wire.encode(handshakeFrame("echo"))
            client.send(ping)
            val echoed = client.incoming().first()
            assertTrue(ping.contentEquals(echoed))
            client.close()
            server.stop()
        }

    @Test
    fun listenAccept_twoClients() =
        runBlocking {
            val server = TcpLoopbackServer()
            val seen = mutableListOf<String>()
            server.start { conn ->
                val frame = conn.incoming().first()
                val msg = wire.decode(frame)
                if (msg is WireMessage.Handshake) {
                    seen += msg.request.nodeId
                }
                conn.send(frame)
            }
            val c1 = transport.connect(server.connectUri)
            val c2 = transport.connect(server.connectUri)
            c1.send(wire.encode(handshakeFrame("a")))
            c2.send(wire.encode(handshakeFrame("b")))
            c1.incoming().first()
            c2.incoming().first()
            assertEquals(setOf("a", "b"), seen.toSet())
            c1.close()
            c2.close()
            server.stop()
        }

    @Test
    fun parse_kdbTcpUri() {
        val u = TcpTransportUri.parse("kdb-tcp://127.0.0.1:9")
        assertEquals("127.0.0.1", u.host)
        assertEquals(9, u.port)
    }

    @Test
    fun peerSyncOverTcp() =
        runBlocking {
            val ns = "app/tcp-sync"
            val remoteDag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val parent = remoteDag.head()
            appendCommit(remoteDag, storage, ns, parent, "remote")

            val server = TcpLoopbackServer()
            val host = peerSyncHost(wire, remoteDag, storage)
            host.start(PeerHostConfig(ns, "remote-host", "tcp-hub"))
            server.start { conn ->
                conn.incoming().collect { frame ->
                    val ack = host.handleFrame(frame)
                    if (ack != null) conn.send(ack)
                }
            }

            val localDag = inMemoryCommitDag(ns)
            val client = peerSyncClient(wire, transport, localDag, storage)
            val session =
                client.connect(
                    PeerClientConfig(namespaceId = ns, nodeId = "local", peerUri = server.connectUri),
                )
            val result = session.pullMissing()
            assertEquals(1, result.appliedCommits)
            assertEquals(remoteDag.head(), localDag.head())
            client.disconnect()
            host.stop()
            server.stop()
        }

    private fun handshakeFrame(nodeId: String): WireMessage.Handshake =
        WireMessage.Handshake(
            WireHeader(WireMessageType.HANDSHAKE, 1, 1, 0),
            HandshakePayload(
                nodeId = nodeId,
                namespaces = listOf("ns"),
                localHeads = emptyMap(),
                clientMode = WireClientMode.STREAM_READ_ONLY,
            ),
        )

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
