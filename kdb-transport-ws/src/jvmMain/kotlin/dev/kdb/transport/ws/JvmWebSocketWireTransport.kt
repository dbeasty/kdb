package dev.kdb.transport.ws

import dev.kdb.stream.WireConnection
import dev.kdb.transport.core.FrameStreamWriter
import dev.kdb.transport.core.TransportConnectOptions
import dev.kdb.error.TransportException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.receiveAsFlow
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.launch

public class JvmWebSocketWireTransport : WebSocketWireTransport {
    private var networkServer: JvmNetworkWebSocketServer? = null
    private val listenScope = CoroutineScope(SupervisorJob())
    override suspend fun connect(uri: String): WireConnection {
        if (uri.startsWith("inproc-ws://")) {
            val name = uri.removePrefix("inproc-ws://").substringBefore('?')
            return InProcessWebSocketHub.connect(name)
        }
        val parsed = WebSocketTransportUriParser.parse(uri)
        return JvmRawSocketWebSocketConnection(
            host = parsed.host,
            port = parsed.port,
            path = parsed.path.ifEmpty { "/" },
            secure = parsed.secure,
            options = TransportConnectOptions(),
        )
    }

    override suspend fun listen(uri: String, handler: suspend (WireConnection) -> Unit) {
        if (uri.startsWith("inproc-ws://")) {
            val name = uri.removePrefix("inproc-ws://").substringBefore('?')
            InProcessWebSocketHub.hub(name).listen(handler)
            return
        }
        val parsed = WebSocketTransportUriParser.parse(uri)
        require(parsed.query["bind"] == "true") { "listen URI requires bind=true: $uri" }
        val server = JvmNetworkWebSocketServer()
        networkServer = server
        server.start(parsed.host, parsed.port, parsed.path) { conn ->
            listenScope.launch {
                try {
                    handler(conn)
                } finally {
                    conn.close()
                }
            }
        }
        try {
            awaitCancellation()
        } finally {
            server.stop()
        }
    }
}

internal object InProcessWebSocketHub {
    private val hubs = mutableMapOf<String, Hub>()

    fun hub(name: String): Hub =
        synchronized(hubs) {
            hubs.getOrPut(name) { Hub(name) }
        }

    fun connect(name: String): WireConnection {
        val h = hub(name)
        return h.createConnection()
    }

    class Hub(internal val name: String) {
        private val clients = mutableListOf<ClientLink>()
        private var listenHandler: (suspend (WireConnection) -> Unit)? = null

        fun listen(handler: suspend (WireConnection) -> Unit) {
            listenHandler = handler
        }

        fun createConnection(): ClientLink {
            val link = ClientLink(this)
            synchronized(clients) { clients += link }
            return link
        }

        suspend fun dispatchToServer(frame: ByteArray) {
            val handler = listenHandler
            if (handler != null) {
                handler(ServerLink(this, frame))
            }
        }

        suspend fun serverSend(frame: ByteArray) {
            val snapshot = synchronized(clients) { clients.toList() }
            for (c in snapshot) {
                c.deliverFromServer(frame)
            }
        }

        internal class ClientLink(private val hub: Hub) : WireConnection {
            private val serverToClient = Channel<ByteArray>(Channel.UNLIMITED)
            private val options = TransportConnectOptions()

            override suspend fun send(frame: ByteArray) {
                FrameStreamWriter.validateOutgoing(frame, options.maxFrameBytes)
                hub.dispatchToServer(frame)
            }

            override fun incoming(): Flow<ByteArray> = serverToClient.receiveAsFlow()

            override suspend fun close() {
                serverToClient.close()
            }

            internal suspend fun deliverFromServer(frame: ByteArray) {
                serverToClient.send(frame)
            }
        }

        internal class ServerLink(
            private val hub: Hub,
            initialFrame: ByteArray,
        ) : WireConnection {
            private val pending = Channel<ByteArray>(Channel.UNLIMITED)

            init {
                pending.trySend(initialFrame)
            }

            override suspend fun send(frame: ByteArray) {
                hub.serverSend(frame)
            }

            override fun incoming(): Flow<ByteArray> = pending.receiveAsFlow()

            override suspend fun close() {}
        }
    }
}

public class JvmInProcessWebSocketServer : WebSocketServer {
    private val active = AtomicInteger(0)
    private var hubName: String = "default"

    override val activeConnections: Int get() = active.get()

    override suspend fun start(bindUri: String) {
        hubName = bindUri.removePrefix("inproc-ws://").substringBefore('?')
    }

    override suspend fun stop() {
        InProcessWebSocketHub.hub(hubName).listen { }
    }

    public fun connectUri(): String = "inproc-ws://$hubName"
}

public actual fun defaultWebSocketWireTransport(): WebSocketWireTransport = JvmWebSocketWireTransport()

public actual fun inProcessWebSocketServer(): WebSocketServer = JvmInProcessWebSocketServer()
