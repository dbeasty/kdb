package dev.kdb.transport.tcp

import dev.kdb.error.ConnectionClosedException
import dev.kdb.error.TransportException
import dev.kdb.stream.WireConnection
import dev.kdb.transport.core.FrameFramer
import dev.kdb.transport.core.FrameStreamReader
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
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketTimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

public class JvmTcpWireTransport : TcpWireTransport {
    override suspend fun connect(uri: String): WireConnection {
        val parsed = TcpTransportUri.parse(uri)
        require(!parsed.bind) { "connect URI must not use bind=true: $uri" }
        return withContext(Dispatchers.IO) {
            val socket = Socket()
            socket.connect(InetSocketAddress(parsed.host, parsed.port), 10_000)
            socket.tcpNoDelay = true
            TcpSocketConnection(socket, TransportConnectOptions())
        }
    }

    override suspend fun listen(uri: String, handler: suspend (WireConnection) -> Unit) {
        val parsed = TcpTransportUri.parse(uri)
        require(parsed.bind) { "listen URI requires bind=true query param: $uri" }
        withContext(Dispatchers.IO) {
            val server = ServerSocket()
            server.bind(InetSocketAddress(parsed.host, parsed.port), 128)
            try {
                while (true) {
                    val client = server.accept()
                    client.tcpNoDelay = true
                    val conn = TcpSocketConnection(client, TransportConnectOptions())
                    launchTcpHandler(conn, handler)
                }
            } finally {
                server.close()
            }
        }
    }
}

private fun launchTcpHandler(
    conn: TcpSocketConnection,
    handler: suspend (WireConnection) -> Unit,
) {
    val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    scope.launch {
        try {
            handler(conn)
        } finally {
            conn.close()
            scope.cancel()
        }
    }
}

internal class TcpSocketConnection(
    private val socket: Socket,
    private val options: TransportConnectOptions,
) : WireConnection {
    private val closed = AtomicBoolean(false)
    private val reader = FrameStreamReader(options.maxFrameBytes)
    private val incomingChannel = Channel<ByteArray>(Channel.BUFFERED)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    init {
        if (options.readTimeoutMs > 0) {
            socket.soTimeout = options.readTimeoutMs.toInt()
        }
        scope.launch { readLoop() }
    }

    private suspend fun readLoop() {
        val input = socket.getInputStream()
        val buf = ByteArray(8192)
        try {
            while (scope.isActive && !closed.get()) {
                val n =
                    try {
                        input.read(buf)
                    } catch (e: SocketTimeoutException) {
                        throw dev.kdb.error.TransportTimeoutException(options.readTimeoutMs)
                    }
                if (n < 0) break
                if (n == 0) continue
                for (frame in reader.feed(buf, 0, n)) {
                    incomingChannel.send(frame)
                }
            }
        } catch (e: TransportException) {
            incomingChannel.close(e)
        } catch (e: Exception) {
            if (!closed.get()) {
                incomingChannel.close(TransportException("TCP read failed: ${e.message}", e))
            }
        } finally {
            incomingChannel.close(ConnectionClosedException())
            try {
                socket.close()
            } catch (_: Exception) {
            }
        }
    }

    override suspend fun send(frame: ByteArray) {
        if (closed.get()) throw ConnectionClosedException()
        FrameStreamWriter.validateOutgoing(frame, options.maxFrameBytes)
        withContext(Dispatchers.IO) {
            socket.getOutputStream().write(frame)
            socket.getOutputStream().flush()
        }
    }

    override fun incoming(): Flow<ByteArray> = incomingChannel.receiveAsFlow()

    override suspend fun close() {
        if (closed.compareAndSet(false, true)) {
            scope.cancel()
            withContext(Dispatchers.IO) {
                try {
                    socket.close()
                } catch (_: Exception) {
                }
            }
            incomingChannel.close(ConnectionClosedException())
        }
    }

    override fun tryPoll(): ByteArray? = incomingChannel.tryReceive().getOrNull()
}

public actual fun defaultTcpWireTransport(): TcpWireTransport = JvmTcpWireTransport()

/** Loopback server for tests — binds ephemeral port on 127.0.0.1. */
public class TcpLoopbackServer(
    private val options: TransportConnectOptions = TransportConnectOptions(),
    scope: CoroutineScope? = null,
) {
    private val active = AtomicInteger(0)
    private var serverSocket: ServerSocket? = null
    private var port: Int = 0
    private val scope = scope ?: CoroutineScope(SupervisorJob() + Dispatchers.IO)

    public val connectUri: String
        get() = "kdb-tcp://127.0.0.1:$port"

    public val listenUri: String
        get() = "kdb-tcp://127.0.0.1:$port?bind=true"

    public suspend fun start(handler: suspend (WireConnection) -> Unit) {
        withContext(Dispatchers.IO) {
            val ss = ServerSocket()
            ss.bind(InetSocketAddress("127.0.0.1", 0), 128)
            serverSocket = ss
            port = ss.localPort
            scope.launch {
                while (scope.isActive) {
                    val client = ss.accept()
                    client.tcpNoDelay = true
                    active.incrementAndGet()
                    val conn = TcpSocketConnection(client, options)
                    scope.launch {
                        try {
                            handler(conn)
                        } finally {
                            active.decrementAndGet()
                        }
                    }
                }
            }
        }
    }

    public suspend fun stop() {
        scope.cancel()
        withContext(Dispatchers.IO) {
            serverSocket?.close()
            serverSocket = null
        }
    }
}
