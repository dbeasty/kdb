package dev.kdb.peersync

import dev.kdb.auth.ConnectionContext
import dev.kdb.auth.static.StaticAuthConfig
import dev.kdb.auth.static.StaticUserConfig
import dev.kdb.auth.static.staticAuthEngine
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.storage.mem.InMemoryStorageAdapter
import dev.kdb.wire.HandshakePayload
import dev.kdb.wire.WireClientMode
import dev.kdb.wire.WireHeader
import dev.kdb.wire.WireMessage
import dev.kdb.wire.WireMessageType
import dev.kdb.wire.defaultWireCodec
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class PeerSyncAuthTest {
    private val ns = "demo/users"
    private val wire = defaultWireCodec()
    private val auth =
        staticAuthEngine(
            StaticAuthConfig(
                users =
                    mapOf(
                        "syncer" to StaticUserConfig(secret = "s-secret", roles = listOf("syncer")),
                        "reader" to StaticUserConfig(secret = "r-secret", roles = listOf("reader")),
                    ),
                roles =
                    mapOf(
                        "syncer" to listOf("sync:demo/*"),
                        "reader" to listOf("read:demo/*"),
                    ),
            ),
        )

    @Test
    fun handshake_withoutCredentials_rejected() =
        runTest {
            val (host, dag) = connectionHost(ConnectionContext.EMPTY)
            val ack = handshake(host, dag.head().toHex())
            assertFalse(ack.response.accepted)
        }

    @Test
    fun readerRole_cannotHandshakeAsPeer() =
        runTest {
            val (host, dag) =
                connectionHost(ConnectionContext(user = "reader", password = "r-secret"))
            val ack = handshake(host, dag.head().toHex())
            assertFalse(ack.response.accepted)
        }

    @Test
    fun syncerRole_accepted() =
        runTest {
            val (host, dag) =
                connectionHost(ConnectionContext(user = "syncer", password = "s-secret"))
            val ack = handshake(host, dag.head().toHex())
            assertTrue(ack.response.accepted)
            assertTrue(ack.response.remoteHeads.containsKey(ns))
        }

    private fun connectionHost(ctx: ConnectionContext): Pair<PeerSyncHost, dev.kdb.dag.CommitDag> {
        val dag = inMemoryCommitDag(ns)
        val host =
            peerSyncHostFactory(
                wire = wire,
                dag = dag,
                storage = InMemoryStorageAdapter(),
                config = PeerHostConfig(ns, "host", ns),
                auth = auth,
            )(ctx)
        return host to dag
    }

    private suspend fun handshake(
        host: PeerSyncHost,
        headHex: String,
    ): dev.kdb.wire.WireMessage.HandshakeAck {
        val frame =
            wire.encode(
                WireMessage.Handshake(
                    WireHeader(WireMessageType.HANDSHAKE, 1, 1, 0),
                    HandshakePayload(
                        nodeId = "client",
                        namespaces = listOf(ns),
                        localHeads = mapOf(ns to headHex),
                        clientMode = WireClientMode.FULL_PEER,
                    ),
                ),
            )
        return wire.decode(host.handleFrame(frame)!!) as dev.kdb.wire.WireMessage.HandshakeAck
    }
}
