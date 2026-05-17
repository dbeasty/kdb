package dev.kdb.integration

import dev.kdb.integration.fixtures.integrationFixture
import dev.kdb.peersync.PeerClientConfig
import dev.kdb.peersync.peerSyncClient
import dev.kdb.peersync.peerSyncHost
import dev.kdb.stream.InMemoryWireTransport
import dev.kdb.transport.tcp.TcpLoopbackServer
import dev.kdb.transport.tcp.defaultTcpWireTransport
import dev.kdb.wire.defaultWireCodec
import dev.kdb.peersync.PeerHostConfig
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.runBlocking
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class FullStackIntegrationTest {
    init {
        dev.kdb.jdbc.KdbDriver
    }

    @Test
    fun layer3_writePath() =
        runBlocking {
            val fx = integrationFixture("integration/l3")
            val before = fx.head()
            fx.writeJson("""{"v":1}""")
            assertTrue(fx.head() != before)
        }

    @Test
    fun layer8_jdbcMemorySelect() {
        val url = "jdbc:kdb:memory:///it?namespace=integration/jdbc"
        DriverManager.getConnection(url).use { conn ->
            val rs = conn.metaData.getTables(null, null, null, null)
            assertTrue(rs.next())
            assertTrue(rs.getString("TABLE_CAT")?.contains("it") == true)
        }
    }

    @Test
    fun layer8_peerSyncInMemory() =
        runBlocking {
            val ns = "integration/peer"
            val wire = defaultWireCodec()
            val remote = integrationFixture(ns)
            remote.writeJson("""{"v":"remote"}""")
            val hub = "peer-hub"
            val host = peerSyncHost(wire, remote.runtime.dag, remote.runtime.storage)
            host.start(PeerHostConfig(ns, "host", hub))
            val local = integrationFixture(ns)
            val client = peerSyncClient(wire, InMemoryWireTransport(), local.runtime.dag, local.runtime.storage)
            val session = client.connect(PeerClientConfig(ns, "local", "memory://$hub"))
            val result = session.pullMissing()
            assertEquals(1, result.appliedCommits)
            host.stop()
        }

    @Test
    fun layer9_tcpPeerSync() =
        runBlocking {
            val ns = "integration/tcp"
            val wire = defaultWireCodec()
            val remote = integrationFixture(ns)
            remote.writeJson("""{"v":"tcp"}""")
            val host = peerSyncHost(wire, remote.runtime.dag, remote.runtime.storage)
            host.start(PeerHostConfig(ns, "host", "tcp"))
            val server = TcpLoopbackServer()
            server.start { conn ->
                conn.incoming().collect { frame ->
                    val ack = host.handleFrame(frame)
                    if (ack != null) conn.send(ack)
                }
            }
            val local = integrationFixture(ns)
            val client =
                peerSyncClient(
                    wire,
                    defaultTcpWireTransport(),
                    local.runtime.dag,
                    local.runtime.storage,
                )
            val session =
                client.connect(PeerClientConfig(ns, "local", server.connectUri))
            assertEquals(1, session.pullMissing().appliedCommits)
            host.stop()
            server.stop()
        }
}
