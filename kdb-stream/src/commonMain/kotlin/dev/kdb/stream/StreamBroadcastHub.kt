package dev.kdb.stream

import dev.kdb.codec.KdbHash
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbOp
import dev.kdb.wire.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * Fan-out [WireMessage.DeltaCommit] to WebSocket (or other) subscribers for one namespace.
 */
public class StreamBroadcastHub(
    private val wire: WireCodec,
    private val namespaceId: String,
    private val headProvider: suspend () -> KdbHash,
    /**
     * Component 46: routes an incoming [WireMessage.TransactionReplay] to the same
     * TransactionEngine/commit path the SQL host uses, returning the wire response to send back
     * (a [WireMessage.SqlResult] on success/error or [WireMessage.ConflictReport] on conflict,
     * matching kdb-server's SqlWireHost.handleTransactionReplay exactly - reused shapes, no new
     * wire type). Null (the default) means Mode 2 write-back isn't wired up for this hub;
     * TransactionReplay is then rejected explicitly rather than silently dropped, which is still
     * strictly better than today's silent fall-through.
     *
     * A plain callback rather than a direct KdbServerRuntime dependency, since kdb-server already
     * depends on kdb-stream - kdb-stream depending back on kdb-server would be circular. Whoever
     * wires both together (kdb-service's KdbServiceMain.kt) supplies the closure.
     */
    private val transactionReplayer: (suspend (WireMessage.TransactionReplay) -> WireMessage)? = null,
) {
    private val mutex = Mutex()
    private val subscribers = mutableListOf<RegisteredSubscriber>()
    private var correlation = 5000

    private data class RegisteredSubscriber(
        val nodeId: String,
        val connection: WireConnection,
        var lastAck: KdbHash?,
    )

    public suspend fun handleFrame(
        connection: WireConnection,
        frame: ByteArray,
    ): ByteArray? =
        when (val msg = wire.decode(frame)) {
            is WireMessage.Handshake -> handleHandshake(connection, msg)
            is WireMessage.PositionAck -> {
                updateLastAck(connection, msg.commitHash)
                null
            }
            is WireMessage.TransactionReplay -> {
                if (msg.namespace != namespaceId) {
                    null
                } else {
                    val replayer = transactionReplayer
                    val response =
                        if (replayer != null) {
                            replayer(msg)
                        } else {
                            WireMessage.SqlResult(
                                WireHeader(WireMessageType.SQL_RESULT, KDB_WIRE_PROTOCOL_VERSION, msg.header.correlationId, 0),
                                namespace = msg.namespace,
                                sessionId = "",
                                columns = emptyList(),
                                rows = emptyList(),
                                rowsAffected = 0,
                                resolvedCommitHex = "",
                                readOnly = false,
                                error = "write-back replay is not enabled on this stream host",
                            )
                        }
                    wire.encode(response)
                }
            }
            else -> null
        }

    public suspend fun unregister(connection: WireConnection) {
        mutex.withLock {
            subscribers.removeAll { it.connection === connection }
        }
    }

    public suspend fun publish(commit: PublishedCommit) {
        val encoded = wire.encode(deltaMessage(commit))
        val targets =
            mutex.withLock {
                subscribers.toList()
            }
        for (sub in targets) {
            try {
                sub.connection.send(encoded)
            } catch (_: Exception) {
                unregister(sub.connection)
            }
        }
    }

    private suspend fun handleHandshake(
        connection: WireConnection,
        msg: WireMessage.Handshake,
    ): ByteArray {
        val mode = msg.request.clientMode
        if (mode != WireClientMode.STREAM_READ_ONLY && mode != WireClientMode.STREAM_WRITE_BACK) {
            return wire.encode(
                handshakeAck(
                    msg,
                    accepted = false,
                    heads = emptyMap(),
                    rejectionReason = "STREAM_READ_ONLY or STREAM_WRITE_BACK required",
                ),
            )
        }
        if (namespaceId !in msg.request.namespaces) {
            return wire.encode(
                handshakeAck(
                    msg,
                    accepted = false,
                    heads = emptyMap(),
                    rejectionReason = "namespace mismatch",
                ),
            )
        }
        val head = headProvider()
        val resumeHex = msg.request.localHeads[namespaceId]
        val resume = resumeHex?.let { KdbHash.fromHex(it) }
        mutex.withLock {
            subscribers.removeAll { it.connection === connection }
            subscribers +=
                RegisteredSubscriber(
                    nodeId = msg.request.nodeId,
                    connection = connection,
                    lastAck = resume,
                )
        }
        return wire.encode(
            handshakeAck(
                msg,
                accepted = true,
                heads = mapOf(namespaceId to head.toHex()),
                rejectionReason = null,
            ),
        )
    }

    private suspend fun updateLastAck(
        connection: WireConnection,
        commitHash: KdbHash,
    ) {
        mutex.withLock {
            subscribers.filter { it.connection === connection }.forEach { it.lastAck = commitHash }
        }
    }

    private fun handshakeAck(
        msg: WireMessage.Handshake,
        accepted: Boolean,
        heads: Map<String, String>,
        rejectionReason: String?,
    ): WireMessage.HandshakeAck =
        WireMessage.HandshakeAck(
            WireHeader(
                WireMessageType.HANDSHAKE,
                KDB_WIRE_PROTOCOL_VERSION,
                msg.header.correlationId,
                0,
            ),
            HandshakeAckPayload(
                accepted = accepted,
                negotiatedEncoding = PayloadEncoding.KDB_BINARY,
                protocolVersion = KDB_WIRE_PROTOCOL_VERSION,
                remoteHeads = heads,
                rejectionReason = rejectionReason,
            ),
        )

    private fun deltaMessage(commit: PublishedCommit): WireMessage.DeltaCommit =
        WireMessage.DeltaCommit(
            WireHeader(
                WireMessageType.DELTA_COMMIT,
                KDB_WIRE_PROTOCOL_VERSION,
                correlation++,
                0,
            ),
            DeltaCommitPayload(
                namespace = namespaceId,
                commitHash = commit.commitHash,
                parentHash = commit.parentHash,
                timestampMicros = commit.timestampMicros,
                operations = commit.operations,
                indexHints = commit.indexHints,
            ),
        )
}

/** Sentinel used when a commit has no parent (a namespace's root commit) - DeltaCommitPayload's
 * wire-level parentHash is non-nullable, so there's no "null" to send instead; an all-zero hash
 * is the common DAG convention for "no parent" and lets a root commit publish without crashing
 * (it used to `error(...)` here - only reachable once Component 44 started calling this for real
 * SQL-write commits, since peer-sync's own use of it never happened to hit a namespace's very
 * first commit in practice). */
private val ZERO_PARENT_HASH: KdbHash = KdbHash.fromHex("00".repeat(32))

public fun publishedCommitFrom(commit: KdbCommit): PublishedCommit {
    val parent = commit.parentHashes.firstOrNull() ?: ZERO_PARENT_HASH
    return PublishedCommit(
        commitHash = commit.hash,
        parentHash = parent,
        operations = commit.operations,
        indexHints = emptyList(),
        timestampMicros = commit.timestamp.toEpochMicros(),
    )
}

/** Derives stream coordinator URI from peer-sync URI (`/kdb` → `/kdb/stream`). */
public fun streamUriFromPeerUri(peerUri: String): String {
    val trimmed = peerUri.trim()
    val pathEnd = trimmed.indexOf('?')
    val base = if (pathEnd >= 0) trimmed.substring(0, pathEnd) else trimmed
    val query = if (pathEnd >= 0) trimmed.substring(pathEnd) else ""
    val streamBase =
        when {
            base.endsWith("/kdb") -> base + "/stream"
            base.endsWith("/") -> base + "kdb/stream"
            else -> base.trimEnd('/') + "/stream"
        }
    return streamBase + query
}
