package dev.kdb.transport.ws

import dev.kdb.error.ConnectionClosedException
import dev.kdb.stream.WireConnection
import dev.kdb.transport.core.FrameStreamWriter
import dev.kdb.transport.core.TransportConnectOptions
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.security.SecureRandom
import java.util.Base64
import java.util.concurrent.atomic.AtomicBoolean

internal class JvmRawSocketWebSocketConnection(
    host: String,
    port: Int,
    path: String,
    secure: Boolean,
    private val options: TransportConnectOptions,
) : WireConnection {
    private val socket: Socket =
        openSocket(host, if (port > 0) port else if (secure) 443 else 80, secure, options)
    private val input = BufferedInputStream(socket.getInputStream())
    private val output = BufferedOutputStream(socket.getOutputStream())
    private val closed = AtomicBoolean(false)
    private val incomingChannel = Channel<ByteArray>(Channel.BUFFERED)
    private val reader: kotlinx.coroutines.Job

    init {
        socket.tcpNoDelay = true
        performClientHandshake(host, port, path)
        reader =
            CoroutineScope(SupervisorJob() + Dispatchers.IO).launch {
                try {
                    while (isActive && !closed.get()) {
                        val payload = WebSocketFraming.readFrame(input, output) ?: break
                        incomingChannel.send(payload)
                    }
                } catch (_: Exception) {
                } finally {
                    incomingChannel.close()
                }
            }
    }

    private fun performClientHandshake(
        host: String,
        port: Int,
        path: String,
    ) {
        val keyBytes = ByteArray(16).also { SecureRandom().nextBytes(it) }
        val key = Base64.getEncoder().encodeToString(keyBytes)
        val portSuffix = if ((port > 0 && port != 80 && port != 443)) ":$port" else ""
        val extraHeaders =
            options.connectHeaders.entries.joinToString("") { (name, value) ->
                "$name: $value\r\n"
            }
        val request =
            "GET $path HTTP/1.1\r\n" +
                "Host: $host$portSuffix\r\n" +
                "Upgrade: websocket\r\n" +
                "Connection: Upgrade\r\n" +
                extraHeaders +
                "Sec-WebSocket-Key: $key\r\n" +
                "Sec-WebSocket-Version: 13\r\n\r\n"
        output.write(request.toByteArray(StandardCharsets.US_ASCII))
        output.flush()
        val response = WebSocketFraming.readHttpHeaders(input)
        val status = response.line(0)
        if (!status.contains("101")) {
            throw dev.kdb.error.TransportException("WebSocket upgrade failed: $status")
        }
        val accept = response.headers["sec-websocket-accept"]
        val expected = WebSocketFraming.websocketAccept(key)
        if (accept != expected) {
            throw dev.kdb.error.TransportException("invalid Sec-WebSocket-Accept")
        }
    }

    override suspend fun send(frame: ByteArray) {
        if (closed.get()) throw ConnectionClosedException()
        withContext(Dispatchers.IO) {
            FrameStreamWriter.validateOutgoing(frame, options.maxFrameBytes)
            WebSocketFraming.writeBinaryFrame(output, frame, maskOutbound = true)
            output.flush()
        }
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
            incomingChannel.close(ConnectionClosedException())
        }
    }

    companion object {
        private fun openSocket(
            host: String,
            port: Int,
            secure: Boolean,
            options: TransportConnectOptions,
        ): Socket {
            if (!secure) {
                return Socket(host, port)
            }
            val tls =
                JvmTransportTls.resolveTlsSettings(secure = true, tls = options.tls)
                    ?: error("unreachable")
            return JvmTransportTls.createClientSocket(
                host = host,
                port = port,
                settings = tls,
                connectTimeoutMs = options.connectTimeoutMs,
            )
        }
    }
}
