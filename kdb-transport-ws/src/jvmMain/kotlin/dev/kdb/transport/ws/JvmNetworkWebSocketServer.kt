package dev.kdb.transport.ws

import dev.kdb.error.ConnectionClosedException
import dev.kdb.error.TransportException
import dev.kdb.stream.WireConnection
import dev.kdb.transport.core.FrameStreamWriter
import dev.kdb.transport.core.TransportConnectOptions
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.net.ServerSocket
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

public class JvmNetworkWebSocketServer(
    private val options: TransportConnectOptions = TransportConnectOptions(),
    private val secure: Boolean = false,
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private var serverSocket: ServerSocket? = null
    private val active = AtomicInteger(0)
    var port: Int = 0
        private set
    var listenPath: String = "/"
        private set

    val listenUri: String
        get() {
            val scheme = if (secure) "kdb-wss" else "kdb-ws"
            return "$scheme://127.0.0.1:$port$listenPath?bind=true"
        }

    suspend fun start(
        host: String,
        portHint: Int,
        path: String,
        handler: suspend (WireConnection) -> Unit,
    ) {
        withContext(Dispatchers.IO) {
            val ss =
                if (secure) {
                    val tls =
                        JvmTransportTls.resolveTlsSettings(secure = true, tls = options.tls)
                            ?: error("unreachable")
                    JvmTransportTls.createServerSocket(host, portHint, tls)
                } else {
                    ServerSocket().also { plain ->
                        plain.bind(java.net.InetSocketAddress(host, portHint), 128)
                    }
                }
            serverSocket = ss
            port = ss.localPort
            listenPath = path.ifEmpty { "/" }
            scope.launch {
                while (isActive) {
                    val socket = ss.accept()
                    active.incrementAndGet()
                    scope.launch {
                        try {
                            val (conn, headers) = acceptWebSocket(socket, listenPath)
                            handler(
                                JvmAttributedWebSocketConnection(
                                    conn,
                                    dev.kdb.transport.core.WireConnectionAttributes(httpHeaders = headers),
                                ),
                            )
                        } catch (_: Exception) {
                            try {
                                socket.close()
                            } catch (_: Exception) {
                            }
                        } finally {
                            active.decrementAndGet()
                        }
                    }
                }
            }
        }
    }

    suspend fun stop() {
        scope.cancel()
        withContext(Dispatchers.IO) {
            serverSocket?.close()
            serverSocket = null
        }
    }

    private fun acceptWebSocket(
        socket: Socket,
        expectedPath: String,
    ): Pair<JvmSocketWebSocketConnection, Map<String, String>> {
        socket.tcpNoDelay = true
        val input = BufferedInputStream(socket.getInputStream())
        val output = BufferedOutputStream(socket.getOutputStream())
        val request = WebSocketFraming.readHttpHeaders(input)
        val firstLine = request.line(0)
        val path = firstLine.split(' ').getOrNull(1) ?: "/"
        if (path != expectedPath && path != expectedPath.trimEnd('/')) {
            throw TransportException("unexpected WebSocket path: $path")
        }
        val key =
            request.headers["sec-websocket-key"]
                ?: throw TransportException("missing Sec-WebSocket-Key")
        val accept = WebSocketFraming.websocketAccept(key)
        val response =
            "HTTP/1.1 101 Switching Protocols\r\n" +
                "Upgrade: websocket\r\n" +
                "Connection: Upgrade\r\n" +
                "Sec-WebSocket-Accept: $accept\r\n\r\n"
        output.write(response.toByteArray(StandardCharsets.US_ASCII))
        output.flush()
        val conn = JvmSocketWebSocketConnection(socket, input, output, options)
        return conn to request.headers
    }
}

internal class JvmAttributedWebSocketConnection(
    private val inner: WireConnection,
    override val attributes: dev.kdb.transport.core.WireConnectionAttributes,
) : WireConnection by inner,
    dev.kdb.transport.core.AttributedWireConnection

private class JvmSocketWebSocketConnection(
    private val socket: Socket,
    private val input: BufferedInputStream,
    private val output: BufferedOutputStream,
    private val options: TransportConnectOptions,
) : WireConnection {
    private val closed = AtomicBoolean(false)
    private val incomingChannel = Channel<ByteArray>(Channel.BUFFERED)
    private val reader =
        CoroutineScope(SupervisorJob() + Dispatchers.IO).launch {
            try {
                while (!closed.get()) {
                    val payload = WebSocketFraming.readFrame(input, output) ?: break
                    incomingChannel.send(payload)
                }
            } catch (_: Exception) {
            } finally {
                incomingChannel.close()
            }
        }

    override suspend fun send(frame: ByteArray) {
        if (closed.get()) throw ConnectionClosedException()
        FrameStreamWriter.validateOutgoing(frame, options.maxFrameBytes)
        WebSocketFraming.writeBinaryFrame(output, frame, maskOutbound = false)
        output.flush()
    }

    override fun incoming(): Flow<ByteArray> = incomingChannel.receiveAsFlow()

    override fun tryPoll(): ByteArray? = incomingChannel.tryReceive().getOrNull()

    override suspend fun close() {
        if (closed.compareAndSet(false, true)) {
            reader.cancel()
            withContext(Dispatchers.IO) {
                try {
                    socket.close()
                } catch (_: Exception) {
                }
            }
            incomingChannel.close()
        }
    }
}
