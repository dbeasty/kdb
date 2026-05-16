package dev.kdb.stream

import dev.kdb.codec.KdbHash
import dev.kdb.document.KdbTransaction
import dev.kdb.error.CompactionBoundaryException
import dev.kdb.index.IndexManager
import dev.kdb.transaction.TransactionEngine
import dev.kdb.wire.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.launch

public interface StreamSubscriber {
    public suspend fun connect(config: StreamSubscriberConfig): StreamConnection
    public suspend fun disconnect()
    public val events: kotlinx.coroutines.flow.SharedFlow<StreamEvent>
}

public fun streamSubscriber(
    wire: WireCodec,
    transport: WireTransport,
    indexManager: IndexManager,
    transactionEngine: TransactionEngine? = null,
    hintApplier: IndexHintApplier = defaultIndexHintApplier(indexManager),
): StreamSubscriber =
    DefaultStreamSubscriber(wire, transport, transactionEngine, hintApplier)

internal class DefaultStreamSubscriber(
    private val wire: WireCodec,
    private val transport: WireTransport,
    private val transactionEngine: TransactionEngine?,
    private val hintApplier: IndexHintApplier,
) : StreamSubscriber {
    private val _events = MutableSharedFlow<StreamEvent>(replay = 16, extraBufferCapacity = 32)
    override val events = _events.asSharedFlow()

    private var connection: WireConnection? = null
    private var scope: CoroutineScope? = null
    private var position: KdbHash? = null
    private var config: StreamSubscriberConfig? = null
    private var correlation = 1

    override suspend fun connect(cfg: StreamSubscriberConfig): StreamConnection {
        if (cfg.mode == StreamClientMode.WRITE_BACK && transactionEngine == null) {
            throw IllegalArgumentException("WRITE_BACK requires TransactionEngine")
        }
        config = cfg
        position = cfg.resumeFrom
        val conn = transport.connect(cfg.coordinatorUri)
        connection = conn
        val sc = CoroutineScope(kotlinx.coroutines.Dispatchers.Unconfined + Job())
        scope = sc
        val wireMode =
            when (cfg.mode) {
                StreamClientMode.READ_ONLY -> WireClientMode.STREAM_READ_ONLY
                StreamClientMode.WRITE_BACK -> WireClientMode.STREAM_WRITE_BACK
            }
        val hs =
            WireMessage.Handshake(
                WireHeader(WireMessageType.HANDSHAKE, KDB_WIRE_PROTOCOL_VERSION, correlation++, 0),
                HandshakePayload(
                    nodeId = cfg.nodeId,
                    namespaces = listOf(cfg.namespaceId),
                    localHeads = cfg.resumeFrom?.let { mapOf(cfg.namespaceId to it.toHex()) } ?: emptyMap(),
                    clientMode = wireMode,
                ),
            )
        sc.launch(start = kotlinx.coroutines.CoroutineStart.UNDISPATCHED) {
            conn.incoming().collect { frame ->
                handleFrame(cfg, frame)
            }
        }
        conn.send(wire.encode(hs))
        return StreamConnection(
            namespaceId = cfg.namespaceId,
            mode = cfg.mode,
            position = { position },
            submitTransaction = { tx -> submitTransaction(cfg, tx) },
            tryPoll = { conn.tryPoll() },
        )
    }

    override suspend fun disconnect() {
        connection?.close()
        scope?.cancel()
        connection = null
        scope = null
        _events.emit(StreamEvent.Disconnected(null))
    }

    private suspend fun handleFrame(cfg: StreamSubscriberConfig, frame: ByteArray) {
        try {
            when (val msg = wire.decode(frame)) {
                is WireMessage.HandshakeAck -> {
                    if (!msg.response.accepted) {
                        _events.emit(StreamEvent.Error(IllegalStateException(msg.response.rejectionReason)))
                        return
                    }
                    _events.emit(StreamEvent.Connected(msg.response.negotiatedEncoding))
                }

                is WireMessage.DeltaCommit -> {
                    val p = msg.payload
                    if (p.namespace != cfg.namespaceId) return
                    val expectedParent = position
                    if (expectedParent != null && p.parentHash != expectedParent) {
                        throw StreamDesyncException(expectedParent, p.parentHash)
                    }
                    hintApplier.apply(cfg.namespaceId, p.indexHints)
                    position = p.commitHash
                    _events.emit(StreamEvent.DeltaReceived(p.commitHash, p.indexHints.size))
                    _events.emit(StreamEvent.PositionUpdated(p.commitHash))
                    val ack =
                        WireMessage.PositionAck(
                            WireHeader(
                                WireMessageType.POSITION_ACK,
                                KDB_WIRE_PROTOCOL_VERSION,
                                msg.header.correlationId,
                                0,
                            ),
                            cfg.namespaceId,
                            p.commitHash,
                        )
                    connection?.send(wire.encode(ack))
                }

                is WireMessage.CompactionNotice -> {
                    val boundary = msg.intent.boundary
                    _events.emit(StreamEvent.CompactionWarning(boundary))
                    val pos = position
                    if (pos != null && !isAtOrAbove(pos, boundary)) {
                        throw CompactionBoundaryException(
                            "position below compaction boundary",
                            cfg.namespaceId,
                            pos.toHex(),
                            boundary.toHex(),
                        )
                    }
                }

                is WireMessage.IceArchiveNotice -> {
                    _events.emit(StreamEvent.IceArchived(msg.originalHash, msg.archiveLocation))
                }

                is WireMessage.ConflictReport -> {
                    _events.emit(StreamEvent.Error(IllegalStateException("conflict reported")))
                }

                else -> {}
            }
        } catch (e: Throwable) {
            _events.emit(StreamEvent.Error(e))
        }
    }

    private suspend fun submitTransaction(
        cfg: StreamSubscriberConfig,
        tx: KdbTransaction,
    ): ReplayResult {
        if (connection == null) throw StreamNotConnectedException()
        val base = position ?: tx.baseVersion
        val payload =
            WireMessage.TransactionReplay(
                WireHeader(WireMessageType.TRANSACTION_REPLAY, KDB_WIRE_PROTOCOL_VERSION, correlation++, 0),
                cfg.namespaceId,
                base,
                tx.id.toString().encodeToByteArray(),
            )
        connection?.send(wire.encode(payload))
        return ReplayResult.Rejected("async replay not awaited in v1")
    }

    /** v1: lexicographic compare on full hash bytes as stand-in for DAG ancestry. */
    private fun isAtOrAbove(pos: KdbHash, boundary: KdbHash): Boolean {
        val a = pos.bytes
        val b = boundary.bytes
        for (i in a.indices) {
            val cmp = (a[i].toInt() and 0xFF) - (b[i].toInt() and 0xFF)
            if (cmp != 0) return cmp >= 0
        }
        return true
    }
}
