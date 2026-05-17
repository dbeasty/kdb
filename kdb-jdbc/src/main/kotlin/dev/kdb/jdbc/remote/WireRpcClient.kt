package dev.kdb.jdbc.remote

import dev.kdb.stream.WireConnection
import dev.kdb.wire.KDB_WIRE_PROTOCOL_VERSION
import dev.kdb.wire.WireCodec
import dev.kdb.wire.WireHeader
import dev.kdb.wire.WireMessage
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeout
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

internal class WireRpcClient(
    private val wire: WireCodec,
    private val connection: WireConnection,
    private val timeoutMs: Long = 30_000,
) {
    private val correlation = AtomicInteger(1)
    private val pending = ConcurrentHashMap<Int, CompletableDeferred<WireMessage>>()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val sendLock = Mutex()

    init {
        scope.launch {
            connection.incoming().collect { frame ->
                val message = wire.decode(frame)
                pending.remove(message.header.correlationId)?.complete(message)
            }
        }
    }

    suspend fun request(message: WireMessage): WireMessage {
        val id = correlation.getAndIncrement()
        val framed =
            wire.encode(
                withCorrelation(message, id),
            )
        val deferred = CompletableDeferred<WireMessage>()
        pending[id] = deferred
        sendLock.withLock {
            connection.send(framed)
        }
        return withTimeout(timeoutMs) { deferred.await() }
    }

    fun close() {
        scope.cancel()
    }

    private fun withCorrelation(
        message: WireMessage,
        correlationId: Int,
    ): WireMessage {
        val header =
            WireHeader(
                messageType = message.header.messageType,
                protocolVersion = KDB_WIRE_PROTOCOL_VERSION,
                correlationId = correlationId,
                payloadLength = message.header.payloadLength,
            )
        return when (message) {
            is WireMessage.Handshake -> message.copy(header = header)
            is WireMessage.SessionBegin -> message.copy(header = header)
            is WireMessage.SqlExec -> message.copy(header = header)
            is WireMessage.TxCommit -> message.copy(header = header)
            is WireMessage.TxRollback -> message.copy(header = header)
            is WireMessage.TransactionReplay -> message.copy(header = header)
            else -> message
        }
    }
}
