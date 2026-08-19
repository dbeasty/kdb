package dev.kdb.server

import dev.kdb.auth.AllowAllAuth
import dev.kdb.auth.AuthEngine
import dev.kdb.auth.ConnectionContext
import dev.kdb.auth.store.RoleStore
import dev.kdb.auth.store.UserStore
import dev.kdb.stream.WireConnection
import dev.kdb.transport.core.TransportConnectOptions
import dev.kdb.transport.ws.WebSocketWireTransport
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

public fun sqlWireHostFactory(
    wire: dev.kdb.wire.WireCodec,
    server: KdbServerRuntime,
    defaultNamespace: String,
    auth: AuthEngine = AllowAllAuth,
    userStore: UserStore? = null,
    roleStore: RoleStore? = null,
): (ConnectionContext) -> SqlWireHost =
    { ctx ->
        SqlWireHost(wire, server, defaultNamespace, auth, ctx, userStore, roleStore)
    }

/**
 * Reads frames off [conn] and dispatches each to [host] without waiting
 * for the previous one's response: a client can have multiple requests
 * in flight on a single connection instead of one round trip at a time.
 * SqlWireHost serializes same-session requests internally (see its
 * sessionLockFor), so ordering correctness doesn't depend on this loop -
 * different sessions (and independent requests like Handshake/
 * TransactionReplay) run fully concurrently. conn.send() itself is
 * serialized here via sendMutex since WireConnection doesn't guarantee
 * concurrent-call safety and interleaved partial frames would corrupt
 * the stream.
 */
public suspend fun pipelinedPerConnection(conn: WireConnection, host: SqlWireHost) {
    val sendMutex = Mutex()
    coroutineScope {
        conn.incoming().collect { frame ->
            launch {
                val response = host.handleFrame(frame)
                if (response != null) {
                    sendMutex.withLock { conn.send(response) }
                }
            }
        }
    }
}

public suspend fun runSqlWireListen(
    transport: WebSocketWireTransport,
    listenUri: String,
    host: SqlWireHost,
    transportOptions: TransportConnectOptions = TransportConnectOptions(),
    perConnection: suspend (WireConnection, SqlWireHost) -> Unit = ::pipelinedPerConnection,
) {
    transport.listen(listenUri, transportOptions) { conn ->
        perConnection(conn, host)
    }
}

public suspend fun runSqlWireListen(
    transport: WebSocketWireTransport,
    listenUri: String,
    hostFactory: (ConnectionContext) -> SqlWireHost,
    transportOptions: TransportConnectOptions = TransportConnectOptions(),
    perConnection: suspend (WireConnection, SqlWireHost) -> Unit = ::pipelinedPerConnection,
) {
    transport.listen(listenUri, transportOptions) { conn ->
        val ctx = connectionContextFor(conn)
        perConnection(conn, hostFactory(ctx))
    }
}
