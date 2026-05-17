package dev.kdb.transport.ws

import dev.kdb.error.ConnectionClosedException
import dev.kdb.error.TransportException
import dev.kdb.stream.WireConnection
import dev.kdb.transport.core.FrameStreamWriter
import dev.kdb.transport.core.TransportConnectOptions
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.receiveAsFlow
import java.net.URI
import java.net.http.HttpClient
import java.net.http.WebSocket
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

public class JvmWebSocketWireTransport : WebSocketWireTransport {
    override suspend fun connect(uri: String): WireConnection {
        if (uri.startsWith("inproc-ws://")) {
            val name = uri.removePrefix("inproc-ws://").substringBefore('?')
            return InProcessWebSocketHub.connect(name)
        }
        val parsed = WebSocketTransportUriParser.parse(uri)
        return JvmHttpWebSocketConnection(parsed.toWireUri(), TransportConnectOptions())
    }

    override suspend fun listen(uri: String, handler: suspend (WireConnection) -> Unit) {
        if (uri.startsWith("inproc-ws://")) {
            val name = uri.removePrefix("inproc-ws://").substringBefore('?')
            InProcessWebSocketHub.hub(name).listen(handler)
            return
        }
        throw TransportException("JVM WebSocket listen requires inproc-ws:// for v1 tests: $uri")
    }
}

internal class JvmHttpWebSocketConnection(
    private val wsUri: String,
    private val options: TransportConnectOptions,
) : WireConnection {
    private val closed = AtomicBoolean(false)
    private val incomingChannel = Channel<ByteArray>(Channel.BUFFERED)
    private val client = HttpClient.newBuilder().build()
    private val socket: WebSocket =
        client
            .newWebSocketBuilder()
            .buildAsync(URI.create(wsUri), object : WebSocket.Listener {
                override fun onBinary(
                    webSocket: WebSocket,
                    data: ByteBuffer,
                    last: Boolean,
                ): CompletionStage<*>? {
                    val bytes = ByteArray(data.remaining())
                    data.get(bytes)
                    incomingChannel.trySend(bytes)
                    return CompletableFuture.completedFuture(null)
                }

                override fun onClose(
                    webSocket: WebSocket,
                    statusCode: Int,
                    reason: String,
                ): CompletionStage<*>? {
                    incomingChannel.close(ConnectionClosedException(reason))
                    return CompletableFuture.completedFuture(null)
                }

                override fun onError(
                    webSocket: WebSocket,
                    error: Throwable,
                ) {
                    incomingChannel.close(TransportException(error.message ?: "websocket error", error))
                }
            }).join()

    override suspend fun send(frame: ByteArray) {
        if (closed.get()) throw ConnectionClosedException()
        FrameStreamWriter.validateOutgoing(frame, options.maxFrameBytes)
        socket.sendBinary(ByteBuffer.wrap(frame), true).join()
    }

    override fun incoming(): Flow<ByteArray> = incomingChannel.receiveAsFlow()

    override suspend fun close() {
        if (closed.compareAndSet(false, true)) {
            socket.sendClose(WebSocket.NORMAL_CLOSURE, "bye").join()
            incomingChannel.close(ConnectionClosedException())
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
