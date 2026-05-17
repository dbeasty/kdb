package dev.kdb.transport.ws

import dev.kdb.wire.HandshakePayload
import dev.kdb.wire.WireClientMode
import dev.kdb.wire.WireHeader
import dev.kdb.wire.WireMessage
import dev.kdb.wire.WireMessageType
import dev.kdb.wire.defaultWireCodec
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class WebSocketWireTransportTest {
    private val wire = defaultWireCodec()
    private val transport = defaultWebSocketWireTransport()

    @Test
    fun wsRoundtrip_inProcess() =
        runTest {
            val hub = "ws-echo"
            transport.listen("inproc-ws://$hub?bind=true") { conn ->
                val frame = conn.incoming().first()
                conn.send(frame)
            }
            val client = transport.connect("inproc-ws://$hub")
            val frame =
                wire.encode(
                    WireMessage.Handshake(
                        WireHeader(WireMessageType.HANDSHAKE, 1, 1, 0),
                        HandshakePayload(
                            nodeId = "c1",
                            namespaces = listOf("ns"),
                            localHeads = emptyMap(),
                            clientMode = WireClientMode.STREAM_READ_ONLY,
                        ),
                    ),
                )
            client.send(frame)
            val echoed = client.incoming().first()
            assertTrue(frame.contentEquals(echoed))
            client.close()
        }

    @Test
    fun parse_kdbWsUri() {
        val u = WebSocketTransportUriParser.parse("kdb-wss://localhost:7443/stream?namespace=app")
        assertTrue(u.secure)
        assertEquals(7443, u.port)
        assertEquals("/stream", u.path)
        assertEquals("app", u.query["namespace"])
    }

    @Test
    fun toWireUri() {
        val u = WebSocketTransportUriParser.parse("kdb-ws://127.0.0.1:80/")
        assertEquals("ws://127.0.0.1:80/", u.toWireUri())
    }
}
