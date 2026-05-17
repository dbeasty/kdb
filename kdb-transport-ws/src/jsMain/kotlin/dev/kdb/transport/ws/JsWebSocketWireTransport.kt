package dev.kdb.transport.ws

import dev.kdb.error.ConnectionClosedException
import dev.kdb.error.TransportException
import dev.kdb.stream.WireConnection
import dev.kdb.transport.core.FrameStreamWriter
import dev.kdb.transport.core.TransportConnectOptions
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.suspendCancellableCoroutine
import org.khronos.webgl.ArrayBuffer
import org.khronos.webgl.Int8Array
import org.w3c.dom.BinaryType
import org.w3c.dom.MessageEvent
import org.w3c.dom.WebSocket
import org.w3c.dom.events.Event
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

public class JsWebSocketWireTransport : WebSocketWireTransport {
    override suspend fun connect(uri: String): WireConnection {
        val parsed = WebSocketTransportUriParser.parse(uri)
        return JsBrowserWebSocketConnection(parsed.toWireUri(), TransportConnectOptions())
    }

    override suspend fun listen(uri: String, handler: suspend (WireConnection) -> Unit) {
        throw TransportException("WebSocket listen is JVM-only in v1: $uri")
    }
}

internal class JsBrowserWebSocketConnection(
    private val wsUri: String,
    private val options: TransportConnectOptions,
) : WireConnection {
    private val incomingChannel = Channel<ByteArray>(Channel.BUFFERED)
    private val socket: WebSocket

    init {
        socket = WebSocket(wsUri)
        socket.binaryType = BinaryType.ARRAYBUFFER
        socket.onmessage = { event: MessageEvent ->
            val data = event.data
            when (data) {
                is ArrayBuffer -> {
                    val arr = Int8Array(data)
                    val bytes = ByteArray(arr.length)
                    for (i in 0 until arr.length) {
                        bytes[i] = arr[i]
                    }
                    incomingChannel.trySend(bytes)
                }
                is String -> {
                    incomingChannel.close(TransportException("text frames not supported"))
                }
                else -> {
                    incomingChannel.close(TransportException("unsupported WebSocket message type"))
                }
            }
        }
        socket.onerror = {
            incomingChannel.close(TransportException("WebSocket error"))
        }
        socket.onclose = {
            incomingChannel.close(ConnectionClosedException())
        }
    }

    override suspend fun send(frame: ByteArray) {
        FrameStreamWriter.validateOutgoing(frame, options.maxFrameBytes)
        if (socket.readyState != WebSocket.OPEN) {
            awaitOpen()
        }
        val buffer = frame.toArrayBuffer()
        socket.send(buffer)
    }

    override fun incoming(): Flow<ByteArray> = incomingChannel.receiveAsFlow()

    override suspend fun close() {
        socket.close()
        incomingChannel.close(ConnectionClosedException())
    }

    private suspend fun awaitOpen() =
        suspendCancellableCoroutine { cont ->
            if (socket.readyState == WebSocket.OPEN) {
                cont.resume(Unit)
                return@suspendCancellableCoroutine
            }
            val prevOpen = socket.onopen
            socket.onopen = { _: Event ->
                socket.onopen = prevOpen
                cont.resume(Unit)
            }
            val prevError = socket.onerror
            socket.onerror = { _: Event ->
                socket.onerror = prevError
                cont.resumeWithException(TransportException("WebSocket failed to open"))
            }
        }
}

private fun ByteArray.toArrayBuffer(): ArrayBuffer {
    val buffer = ArrayBuffer(size)
    val view = Int8Array(buffer)
    for (i in indices) {
        view[i] = this[i]
    }
    return buffer
}

public actual fun defaultWebSocketWireTransport(): WebSocketWireTransport = JsWebSocketWireTransport()

public actual fun inProcessWebSocketServer(): WebSocketServer =
    object : WebSocketServer {
        override suspend fun start(bindUri: String) {
            throw TransportException("in-process WebSocket server is JVM-only")
        }

        override suspend fun stop() {}

        override val activeConnections: Int = 0
    }
