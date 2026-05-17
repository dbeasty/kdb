package dev.kdb.integration

import dev.kdb.auth.ConnectionContext
import dev.kdb.auth.static.StaticAuthConfig
import dev.kdb.auth.static.StaticUserConfig
import dev.kdb.auth.static.staticAuthEngine
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.peersync.PeerClientConfig
import dev.kdb.peersync.PeerHostConfig
import dev.kdb.peersync.peerSyncClient
import dev.kdb.peersync.peerSyncHost
import dev.kdb.storage.mem.InMemoryStorageAdapter
import dev.kdb.stream.InMemoryWireTransport
import dev.kdb.wire.defaultWireCodec
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.test.fail

class PeerSyncAuthIntegrationTest {
    private val auth =
        staticAuthEngine(
            StaticAuthConfig(
                users = mapOf("syncer" to StaticUserConfig("s-secret", listOf("syncer"))),
                roles = mapOf("syncer" to listOf("sync:demo/*")),
            ),
        )

    @Test
    fun clientWithoutCredentials_rejectedOnConnect() =
        runTest {
            val ns = "demo/sync"
            val hub = "auth-hub"
            val wire = defaultWireCodec()
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val host =
                peerSyncHost(
                    wire,
                    dag,
                    storage,
                    auth = auth,
                    connectionContext = ConnectionContext.EMPTY,
                )
            host.start(PeerHostConfig(ns, "host", hub))
            val client = peerSyncClient(wire, InMemoryWireTransport(), dag, storage)
            try {
                client.connect(PeerClientConfig(ns, "local", "memory://$hub"))
                fail("expected handshake rejection")
            } catch (e: Exception) {
                assertTrue(
                    e is dev.kdb.peersync.PeerSyncException ||
                        e.message?.contains("authentication", ignoreCase = true) == true ||
                        e.message?.contains("rejected", ignoreCase = true) == true,
                    "unexpected: ${e::class.simpleName}: ${e.message}",
                )
            }
            host.stop()
        }

    @Test
    fun syncerCredentials_hostAcceptsInMemoryHub() =
        runTest {
            val ns = "demo/sync2"
            val hub = "auth-hub-2"
            val wire = defaultWireCodec()
            val remoteDag = inMemoryCommitDag(ns)
            val localDag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val host =
                peerSyncHost(
                    wire,
                    remoteDag,
                    storage,
                    auth = auth,
                    connectionContext = ConnectionContext(user = "syncer", password = "s-secret"),
                )
            host.start(PeerHostConfig(ns, "host", hub))
            val client = peerSyncClient(wire, InMemoryWireTransport(), localDag, storage)
            val session = client.connect(PeerClientConfig(ns, "local", "memory://$hub"))
            assertTrue(session.namespaceId == ns)
            client.disconnect()
            host.stop()
        }
}
