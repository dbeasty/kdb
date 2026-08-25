package dev.kdb.stream

import dev.kdb.codec.KdbHash
import dev.kdb.document.KdbTransaction
import dev.kdb.error.CompactionBoundaryException
import dev.kdb.error.ConflictReport
import dev.kdb.index.IndexManager
import dev.kdb.transaction.TransactionEngine
import dev.kdb.wire.*
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeout

/** How long [DefaultStreamSubscriber.submitTransaction] waits for a correlated response before
 * treating the replay as rejected - component 46's fix must actually await *something*, and an
 * unbounded wait would hang forever if the coordinator never responds (e.g. it doesn't understand
 * TransactionReplay at all, the exact bug being fixed). */
private const val REPLAY_TIMEOUT_MILLIS = 10_000L

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

    // Component 46: correlation id -> the caller awaiting submitTransaction's response.
    // Guarded by a mutex since handleFrame (the dedicated reader coroutine) and
    // submitTransaction (called from any caller coroutine) both touch this map concurrently.
    private val pendingReplaysMutex = Mutex()
    private val pendingReplays = mutableMapOf<Int, CompletableDeferred<ReplayResult>>()

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
        // Don't leave any in-flight submitTransaction call hanging until its timeout - the
        // connection it was waiting on is gone.
        val stale = pendingReplaysMutex.withLock { val s = pendingReplays.toMap(); pendingReplays.clear(); s }
        stale.values.forEach { it.complete(ReplayResult.Rejected("disconnected while awaiting replay response")) }
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
                    val deferred = pendingReplaysMutex.withLock { pendingReplays.remove(msg.header.correlationId) }
                    if (deferred != null) {
                        deferred.complete(ReplayResult.Conflict(decodeConflictReport(msg.reportBytes)))
                    } else {
                        // Not a reply to a pending submitTransaction call (e.g. delivered
                        // out-of-band by some other flow) - preserve the prior behavior.
                        _events.emit(StreamEvent.Error(IllegalStateException("conflict reported")))
                    }
                }

                is WireMessage.SqlResult -> {
                    // Component 46: this is the response shape SqlWireHost.handleTransactionReplay
                    // actually sends back for TransactionReplay (see kdb-server's SqlWireHost.kt) -
                    // not a dedicated ack type, so this subscriber must recognize it as one.
                    val deferred = pendingReplaysMutex.withLock { pendingReplays.remove(msg.header.correlationId) }
                    if (deferred != null) {
                        val error = msg.error
                        if (error != null) {
                            deferred.complete(ReplayResult.Rejected(error))
                        } else {
                            deferred.complete(ReplayResult.Applied(KdbHash.fromHex(msg.resolvedCommitHex)))
                        }
                    }
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
        val conn = connection ?: throw StreamNotConnectedException()
        val base = position ?: tx.baseVersion
        val correlationId = correlation++
        val payload =
            WireMessage.TransactionReplay(
                WireHeader(WireMessageType.TRANSACTION_REPLAY, KDB_WIRE_PROTOCOL_VERSION, correlationId, 0),
                cfg.namespaceId,
                base,
                // Component 46 fix: the actual transaction, not just its id - a UUID string alone
                // gives the coordinator nothing to replay. TransactionWireCodec is the same codec
                // the SQL wire client/server already use for TxCommit's transactionBytes.
                TransactionWireCodec.encode(tx),
            )
        val deferred = CompletableDeferred<ReplayResult>()
        pendingReplaysMutex.withLock { pendingReplays[correlationId] = deferred }
        conn.send(wire.encode(payload))
        return try {
            withTimeout(REPLAY_TIMEOUT_MILLIS) { deferred.await() }
        } catch (e: TimeoutCancellationException) {
            pendingReplaysMutex.withLock { pendingReplays.remove(correlationId) }
            ReplayResult.Rejected("timed out waiting for replay response")
        }
    }

    /** Mirrors SqlWireHost.encodeConflictReport's wire shape exactly (kdb-server) - that encoder
     * only writes transactionId/baseHash/targetHash, no conflicts array, so this only reads
     * those three fields. Hand-rolled rather than kotlinx.serialization since kdb-stream has no
     * JSON library dependency today and ConflictReport itself isn't @Serializable. */
    private fun decodeConflictReport(reportBytes: ByteArray): ConflictReport {
        val text = reportBytes.decodeToString()
        fun field(name: String): String {
            val marker = "\"$name\":\""
            val start = text.indexOf(marker)
            if (start < 0) return ""
            val from = start + marker.length
            val end = text.indexOf('"', from)
            return if (end < 0) "" else text.substring(from, end)
        }
        return ConflictReport(
            transactionId = field("transactionId"),
            baseHash = field("baseHash"),
            targetHash = field("targetHash"),
            conflicts = emptyList(),
        )
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
