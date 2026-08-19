package dev.kdb.server

import dev.kdb.auth.AllowAllAuth
import dev.kdb.auth.AuthAction
import dev.kdb.auth.AuthEngine
import dev.kdb.auth.ConnectionContext
import dev.kdb.auth.store.RoleAlreadyExistsException
import dev.kdb.auth.store.RoleNotFoundException
import dev.kdb.auth.store.RoleStore
import dev.kdb.auth.store.UserAlreadyExistsException
import dev.kdb.auth.store.UserNotFoundException
import dev.kdb.auth.store.UserStore
import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbTransaction
import dev.kdb.embed.toQueryResultJson
import dev.kdb.error.ConflictException
import dev.kdb.error.ConflictReport
import dev.kdb.error.DocumentLockedException
import dev.kdb.query.hybrid.HybridQueryRequest
import dev.kdb.query.hybrid.ReadConsistency
import dev.kdb.sql.GrantSpec
import dev.kdb.sql.SqlCell
import dev.kdb.sql.decodeSqlParameters
import dev.kdb.sql.defaultSqlParser
import dev.kdb.sql.isAdminStatement
import dev.kdb.sql.isDdlStatement
import dev.kdb.sql.isTransactionControlStatement
import dev.kdb.sql.SqlStatement
import dev.kdb.transaction.TransactionBuilder
import dev.kdb.transaction.TransactionResult
import dev.kdb.wire.KDB_WIRE_PROTOCOL_VERSION
import dev.kdb.wire.TransactionWireCodec
import dev.kdb.wire.WireCodec
import dev.kdb.wire.WireHeader
import dev.kdb.wire.WireMessage
import dev.kdb.wire.WireMessageType
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonPrimitive

public class SqlWireHost(
    private val wire: WireCodec,
    public val server: KdbServerRuntime,
    private val defaultNamespace: String,
    auth: AuthEngine = AllowAllAuth,
    connectionContext: ConnectionContext = ConnectionContext.EMPTY,
    /** RBAC admin surface (CREATE/DROP ROLE, GRANT/REVOKE, CREATE/DROP USER, see
     * docs/kdb-rbac-plan.md phase 4). Null means those statements are rejected — a server that
     * hasn't been configured with a [dev.kdb.auth.store.RegistryAuthStore] has no durable place
     * to put them. */
    private val userStore: UserStore? = null,
    private val roleStore: RoleStore? = null,
) {
    private val sessions = SessionManager(server)
    private val sqlAuth = SqlAuthSupport(auth, connectionContext)
    private var correlation = 1

    // Per-session ordering lock: SqlExec/TxCommit/TxRollback for the same
    // sessionId must still execute one at a time (transaction semantics
    // depend on it - e.g. a Commit must see the effects of every prior
    // statement in that session), but different sessions - and therefore
    // different connections, or multiple in-flight requests pipelined on
    // one connection - now run fully concurrently instead of being
    // serialized by a single blocking read loop. See Phase 6 of
    // docs/benchmarks/phase0-baseline.md: the wire protocol already
    // carries a correlationId for exactly this, but the default
    // perConnection handler (SqlWireListen.kt) awaited each response
    // before reading the next frame, so it went unused.
    private val sessionLocksGuard = Mutex()
    private val sessionLocks = mutableMapOf<String, Mutex>()

    private suspend fun sessionLockFor(sessionId: String): Mutex =
        sessionLocksGuard.withLock { sessionLocks.getOrPut(sessionId) { Mutex() } }

    public suspend fun handleFrame(frame: ByteArray): ByteArray? {
        val message = wire.decode(frame)
        val sessionId = sessionIdOf(message)
        val reply =
            if (sessionId != null) {
                sessionLockFor(sessionId).withLock { dispatch(message) }
            } else {
                dispatch(message)
            }
        return reply?.let { wire.encode(it) }
    }

    private fun sessionIdOf(message: WireMessage): String? =
        when (message) {
            is WireMessage.SqlExec -> message.sessionId
            is WireMessage.TxCommit -> message.sessionId
            is WireMessage.TxRollback -> message.sessionId
            else -> null
        }

    private suspend fun dispatch(message: WireMessage): WireMessage? =
        when (message) {
            is WireMessage.Handshake -> handleHandshake(message)
            is WireMessage.SessionBegin -> handleSessionBegin(message)
            is WireMessage.SqlExec -> handleSqlExec(message)
            is WireMessage.TxCommit -> handleTxCommit(message)
            is WireMessage.TxRollback -> handleTxRollback(message)
            is WireMessage.TransactionReplay -> handleTransactionReplay(message)
            else -> null
        }

    private suspend fun handleHandshake(msg: WireMessage.Handshake): WireMessage.HandshakeAck {
        val modeOk = msg.request.clientMode == dev.kdb.wire.WireClientMode.SQL_CLIENT
        val head = server.runtime.dag.head().toHex()
        if (!modeOk) {
            return handshakeAck(msg, accepted = false, head, "SQL_CLIENT mode required")
        }
        val principal =
            try {
                sqlAuth.authenticateConnection()
            } catch (e: Throwable) {
                return handshakeAck(msg, accepted = false, head, sqlAuth.authFailureMessage(e))
            }
        val ns =
            if (msg.request.namespaces.isNotEmpty()) {
                msg.request.namespaces.first()
            } else {
                defaultNamespace
            }
        try {
            sqlAuth.authorize(principal, AuthAction.SessionBegin(ns))
        } catch (e: Throwable) {
            return handshakeAck(msg, accepted = false, head, sqlAuth.authFailureMessage(e))
        }
        sessions.begin(ns, ReadConsistency.READ_COMMITTED, principal = principal)
        return handshakeAck(msg, accepted = true, head, rejectionReason = null)
    }

    private fun handshakeAck(
        msg: WireMessage.Handshake,
        accepted: Boolean,
        headHex: String,
        rejectionReason: String?,
    ): WireMessage.HandshakeAck =
        WireMessage.HandshakeAck(
            header(msg.header.correlationId, WireMessageType.HANDSHAKE),
            dev.kdb.wire.HandshakeAckPayload(
                accepted = accepted,
                negotiatedEncoding = wire.encoding,
                protocolVersion = KDB_WIRE_PROTOCOL_VERSION,
                remoteHeads = mapOf(defaultNamespace to headHex),
                rejectionReason = rejectionReason,
            ),
        )

    private suspend fun handleSessionBegin(msg: WireMessage.SessionBegin): WireMessage.SessionBeginAck {
        val consistency = ReadConsistency.valueOf(msg.readConsistency)
        val principal =
            try {
                sqlAuth.authenticateConnection()
            } catch (e: Throwable) {
                return sessionBeginAuthError(msg, sqlAuth.authFailureMessage(e))
            }
        try {
            sqlAuth.authorize(principal, AuthAction.SessionBegin(msg.namespace))
        } catch (e: Throwable) {
            return sessionBeginAuthError(msg, sqlAuth.authFailureMessage(e))
        }
        val session =
            sessions.begin(
                namespaceId = msg.namespace,
                readConsistency = consistency,
                baseVersionHex = msg.baseVersionHex,
                sessionId = msg.sessionId,
                principal = principal,
            )
        return WireMessage.SessionBeginAck(
            header(msg.header.correlationId, WireMessageType.SESSION_BEGIN_ACK),
            namespace = msg.namespace,
            sessionId = session.id.value,
            headHex = server.runtime.dag.head().toHex(),
            readConsistency = session.readConsistency.name,
        )
    }

    private suspend fun handleSqlExec(msg: WireMessage.SqlExec): WireMessage {
        val session =
            sessions.get(msg.sessionId)
                ?: return sqlError(msg, "unknown session: ${msg.sessionId}")
        val parsed = defaultSqlParser().parse(msg.sql.trim())
        if (isTransactionControlStatement(parsed)) {
            return handleTransactionControlSql(msg, session, parsed)
        }
        if (isAdminStatement(parsed)) {
            return handleAdminSql(msg, session, parsed)
        }
        if (!session.autoCommit && isDdlStatement(parsed)) {
            return sqlError(msg, "DDL not allowed inside a transaction")
        }
        try {
            sqlAuth.authorize(
                session.principal,
                AuthAction.SqlExec(msg.namespace, readOnly = !sqlRequiresWrite(parsed)),
            )
        } catch (e: Throwable) {
            return sqlError(msg, sqlAuth.authFailureMessage(e))
        }
        return try {
            val deferCommit = !session.autoCommit
            val parameters = decodeSqlParameters(msg.parametersJson)
            val result =
                server.runtime.hybrid.execute(
                    msg.sql,
                    HybridQueryRequest(
                        namespaceId = session.namespaceId,
                        schema = server.runtime.schema,
                        parameters = parameters,
                        readConsistency = session.readConsistency,
                        readPin = session.readPin,
                        sessionCheckout = session.sessionCheckout,
                        writeSessionId = session.id.value,
                        documentLocks = server.documentLocks,
                        deferCommit = deferCommit,
                        transactionBase = if (deferCommit) session.baseVersion else null,
                        bufferOps =
                            if (deferCommit) {
                                { ops ->
                                    appendPendingOps(sessions.pendingBuilder(session), ops)
                                }
                            } else {
                                null
                            },
                    ),
                )
            val json = result.toQueryResultJson()
            WireMessage.SqlResult(
                header(msg.header.correlationId, WireMessageType.SQL_RESULT),
                namespace = msg.namespace,
                sessionId = msg.sessionId,
                columns = json.columns,
                rows = json.rows.map { row -> row.map { cellToString(it) } },
                rowsAffected = result.result.rowsAffected,
                resolvedCommitHex = json.resolvedCommit,
                readOnly = json.readOnly,
                error = null,
                generatedIds = result.result.generatedIds,
            )
        } catch (e: Throwable) {
            sqlError(msg, e.message ?: e.toString())
        }
    }

    /** CREATE/DROP ROLE, GRANT/REVOKE, CREATE/DROP USER — see docs/kdb-rbac-plan.md phase 4.
     * Gated behind [AuthAction.Admin] rather than the ordinary write check, and executed
     * directly against [userStore]/[roleStore] rather than going through
     * [dev.kdb.query.hybrid.HybridQueryEngine] — those stores live in `kdb-auth-store`, which
     * `kdb-sql`/`kdb-embed` have no dependency on. */
    private suspend fun handleAdminSql(
        msg: WireMessage.SqlExec,
        session: KdbSession,
        stmt: SqlStatement,
    ): WireMessage {
        try {
            sqlAuth.authorize(session.principal, AuthAction.Admin())
        } catch (e: Throwable) {
            return sqlError(msg, sqlAuth.authFailureMessage(e))
        }
        val users = userStore
        val roles = roleStore
        if (users == null || roles == null) {
            return sqlError(msg, "RBAC admin statements are not enabled on this server")
        }
        return try {
            when (stmt) {
                is SqlStatement.CreateRole -> {
                    roles.createRole(stmt.name)
                    sqlSuccess(msg, session, resolvedCommitHex = "", rowsAffected = 1)
                }
                is SqlStatement.DropRole -> {
                    roles.deleteRole(stmt.name)
                    sqlSuccess(msg, session, resolvedCommitHex = "", rowsAffected = 1)
                }
                is SqlStatement.Grant -> {
                    applyGrant(roles, stmt.grant, add = true)
                    sqlSuccess(msg, session, resolvedCommitHex = "", rowsAffected = 1)
                }
                is SqlStatement.Revoke -> {
                    applyGrant(roles, stmt.grant, add = false)
                    sqlSuccess(msg, session, resolvedCommitHex = "", rowsAffected = 1)
                }
                is SqlStatement.CreateUser -> {
                    users.createUser(stmt.id, stmt.password, stmt.roles.toSet())
                    sqlSuccess(msg, session, resolvedCommitHex = "", rowsAffected = 1)
                }
                is SqlStatement.DropUser -> {
                    users.deleteUser(stmt.id)
                    sqlSuccess(msg, session, resolvedCommitHex = "", rowsAffected = 1)
                }
                else -> sqlError(msg, "not an admin statement: $stmt")
            }
        } catch (e: UserAlreadyExistsException) {
            sqlError(msg, e.message ?: "user already exists")
        } catch (e: UserNotFoundException) {
            sqlError(msg, e.message ?: "user not found")
        } catch (e: RoleAlreadyExistsException) {
            sqlError(msg, e.message ?: "role already exists")
        } catch (e: RoleNotFoundException) {
            sqlError(msg, e.message ?: "role not found")
        }
    }

    private suspend fun applyGrant(
        roles: RoleStore,
        spec: GrantSpec,
        add: Boolean,
    ) {
        val existing = roles.getRole(spec.role) ?: throw RoleNotFoundException(spec.role)
        val grantString =
            buildString {
                append(spec.kind)
                append(':')
                append(spec.database)
                if (spec.collection != null) {
                    append('/')
                    append(spec.collection)
                    if (spec.documentId != null) {
                        append('/')
                        append(spec.documentId)
                    }
                }
            }
        val updated = if (add) existing.grants + grantString else existing.grants - grantString
        roles.updateGrants(spec.role, updated)
    }

    private suspend fun handleTransactionControlSql(
        msg: WireMessage.SqlExec,
        session: KdbSession,
        stmt: SqlStatement,
    ): WireMessage =
        when (stmt) {
            SqlStatement.BeginTransaction ->
                when {
                    session.pending != null ->
                        sqlError(msg, "transaction already in progress")
                    !session.autoCommit ->
                        sqlSuccess(
                            msg,
                            session,
                            resolvedCommitHex = session.baseVersion.toHex(),
                        )
                    else -> {
                        session.autoCommit = false
                        sqlSuccess(
                            msg,
                            session,
                            resolvedCommitHex = session.baseVersion.toHex(),
                        )
                    }
                }
            SqlStatement.Commit ->
                commitSession(
                    correlationId = msg.header.correlationId,
                    namespace = msg.namespace,
                    session = session,
                ) { err -> sqlError(msg, err) }
            SqlStatement.Rollback -> {
                rollbackSession(session)
                sqlSuccess(
                    msg,
                    session,
                    resolvedCommitHex = server.runtime.dag.head().toHex(),
                )
            }
            else -> sqlError(msg, "unsupported transaction statement")
        }

    private suspend fun handleTxCommit(msg: WireMessage.TxCommit): WireMessage {
        val session =
            sessions.get(msg.sessionId)
                ?: return sqlError(msg, "unknown session: ${msg.sessionId}")
        try {
            sqlAuth.authorize(session.principal, AuthAction.TxCommit(msg.namespace))
        } catch (e: Throwable) {
            return sqlError(msg, sqlAuth.authFailureMessage(e))
        }
        if (msg.transactionBytes.isNotEmpty()) {
            return commitEncodedTransaction(msg, session)
        }
        return commitSession(
            correlationId = msg.header.correlationId,
            namespace = msg.namespace,
            session = session,
        ) { err -> sqlError(msg, err) }
    }

    private suspend fun commitEncodedTransaction(
        msg: WireMessage.TxCommit,
        session: KdbSession,
    ): WireMessage =
        try {
            val tx = TransactionWireCodec.decode(msg.transactionBytes)
            val effective =
                if (tx.operations.isEmpty() && session.pending != null) {
                    session.pending!!.build()
                } else {
                    tx
                }
            val commit =
                server.commit(
                    session.namespaceId,
                    effective,
                    sessionId = session.id.value,
                    authorizer = sqlAuth.writeAuthorizerFor(session.principal),
                )
            finishCommittedSession(session, commit.hash, effective.operations.size)
            WireMessage.SqlResult(
                header(msg.header.correlationId, WireMessageType.SQL_RESULT),
                namespace = msg.namespace,
                sessionId = msg.sessionId,
                columns = emptyList(),
                rows = emptyList(),
                rowsAffected = effective.operations.size,
                resolvedCommitHex = commit.hash.toHex(),
                readOnly = false,
            )
        } catch (e: ConflictException) {
            abortSessionAfterFailedCommit(session)
            WireMessage.ConflictReport(
                header(msg.header.correlationId, WireMessageType.CONFLICT_REPORT),
                namespace = msg.namespace,
                reportBytes = encodeConflictReport(e.report),
            )
        } catch (e: DocumentLockedException) {
            abortSessionAfterFailedCommit(session)
            sqlError(msg, e.message ?: "document locked")
        } catch (e: Throwable) {
            abortSessionAfterFailedCommit(session)
            sqlError(msg, e.message ?: e.toString())
        }

    private suspend fun commitSession(
        correlationId: Int,
        namespace: String,
        session: KdbSession,
        onError: (String) -> WireMessage,
    ): WireMessage =
        try {
            val effective =
                session.pending?.build()
                    ?: return onError("no pending transaction to commit")
            val commit =
                server.commit(
                    session.namespaceId,
                    effective,
                    sessionId = session.id.value,
                    authorizer = sqlAuth.writeAuthorizerFor(session.principal),
                )
            finishCommittedSession(session, commit.hash, effective.operations.size)
            WireMessage.SqlResult(
                header(correlationId, WireMessageType.SQL_RESULT),
                namespace = namespace,
                sessionId = session.id.value,
                columns = emptyList(),
                rows = emptyList(),
                rowsAffected = effective.operations.size,
                resolvedCommitHex = commit.hash.toHex(),
                readOnly = false,
            )
        } catch (e: ConflictException) {
            abortSessionAfterFailedCommit(session)
            WireMessage.ConflictReport(
                header(correlationId, WireMessageType.CONFLICT_REPORT),
                namespace = namespace,
                reportBytes = encodeConflictReport(e.report),
            )
        } catch (e: DocumentLockedException) {
            abortSessionAfterFailedCommit(session)
            onError(e.message ?: "document locked")
        } catch (e: Throwable) {
            abortSessionAfterFailedCommit(session)
            onError(e.message ?: e.toString())
        }

    private suspend fun finishCommittedSession(
        session: KdbSession,
        commitHash: KdbHash,
        @Suppress("UNUSED_PARAMETER") rowsAffected: Int,
    ) {
        sessions.clearPending(session)
        server.documentLocks.releaseAll(session.id.value)
        session.baseVersion = commitHash
        if (session.readConsistency == ReadConsistency.SNAPSHOT) {
            session.readPin = commitHash
        }
    }

    private suspend fun abortSessionAfterFailedCommit(session: KdbSession) {
        server.documentLocks.releaseAll(session.id.value)
        sessions.clearPending(session)
    }

    private suspend fun rollbackSession(session: KdbSession) {
        server.documentLocks.releaseAll(session.id.value)
        sessions.clearPending(session)
    }

    private suspend fun handleTxRollback(msg: WireMessage.TxRollback): WireMessage.SqlResult {
        val session = sessions.get(msg.sessionId)
        if (session != null) {
            rollbackSession(session)
        }
        return WireMessage.SqlResult(
            header(msg.header.correlationId, WireMessageType.SQL_RESULT),
            namespace = msg.namespace,
            sessionId = msg.sessionId,
            columns = emptyList(),
            rows = emptyList(),
            rowsAffected = 0,
            resolvedCommitHex = server.runtime.dag.head().toHex(),
            readOnly = false,
        )
    }

    private fun sqlSuccess(
        msg: WireMessage.SqlExec,
        session: KdbSession,
        resolvedCommitHex: String,
        rowsAffected: Int = 0,
    ): WireMessage.SqlResult =
        WireMessage.SqlResult(
            header(msg.header.correlationId, WireMessageType.SQL_RESULT),
            namespace = msg.namespace,
            sessionId = session.id.value,
            columns = emptyList(),
            rows = emptyList(),
            rowsAffected = rowsAffected,
            resolvedCommitHex = resolvedCommitHex,
            readOnly = false,
        )

    private suspend fun handleTransactionReplay(msg: WireMessage.TransactionReplay): WireMessage {
        try {
            sqlAuth.authorize(
                sqlAuth.connectionPrincipal,
                AuthAction.TxCommit(msg.namespace),
            )
        } catch (e: Throwable) {
            return WireMessage.SqlResult(
                header(msg.header.correlationId, WireMessageType.SQL_RESULT),
                namespace = msg.namespace,
                sessionId = "",
                columns = emptyList(),
                rows = emptyList(),
                rowsAffected = 0,
                resolvedCommitHex = "",
                readOnly = false,
                error = sqlAuth.authFailureMessage(e),
            )
        }
        val tx = TransactionWireCodec.decode(msg.transactionBytes)
        val replayTarget = server.runtime.dag.head()
        val authorizer = sqlAuth.writeAuthorizerFor(sqlAuth.connectionPrincipal)
        return when (val result = server.replay(msg.namespace, tx, replayTarget, authorizer = authorizer)) {
            is TransactionResult.Success ->
                WireMessage.SqlResult(
                    header(msg.header.correlationId, WireMessageType.SQL_RESULT),
                    namespace = msg.namespace,
                    sessionId = "",
                    columns = emptyList(),
                    rows = emptyList(),
                    rowsAffected = tx.operations.size,
                    resolvedCommitHex = result.commit.hash.toHex(),
                    readOnly = false,
                )
            is TransactionResult.Conflict ->
                WireMessage.ConflictReport(
                    header(msg.header.correlationId, WireMessageType.CONFLICT_REPORT),
                    namespace = msg.namespace,
                    reportBytes = encodeConflictReport(result.report),
                )
            is TransactionResult.SchemaError ->
                WireMessage.SqlResult(
                    header(msg.header.correlationId, WireMessageType.SQL_RESULT),
                    namespace = msg.namespace,
                    sessionId = "",
                    columns = emptyList(),
                    rows = emptyList(),
                    rowsAffected = 0,
                    resolvedCommitHex = replayTarget.toHex(),
                    readOnly = false,
                    error = "schema rejection",
                )
            is TransactionResult.Aborted ->
                WireMessage.SqlResult(
                    header(msg.header.correlationId, WireMessageType.SQL_RESULT),
                    namespace = msg.namespace,
                    sessionId = "",
                    columns = emptyList(),
                    rows = emptyList(),
                    rowsAffected = 0,
                    resolvedCommitHex = replayTarget.toHex(),
                    readOnly = false,
                    error = "transaction aborted: ${result.cause.message ?: result.cause.toString()}",
                )
        }
    }

    private fun sqlError(
        msg: WireMessage.SqlExec,
        error: String,
    ): WireMessage.SqlResult =
        WireMessage.SqlResult(
            header(msg.header.correlationId, WireMessageType.SQL_RESULT),
            namespace = msg.namespace,
            sessionId = msg.sessionId,
            columns = emptyList(),
            rows = emptyList(),
            rowsAffected = 0,
            resolvedCommitHex = "",
            readOnly = true,
            error = error,
        )

    private fun sqlError(
        msg: WireMessage.TxCommit,
        error: String,
    ): WireMessage.SqlResult =
        WireMessage.SqlResult(
            header(msg.header.correlationId, WireMessageType.SQL_RESULT),
            namespace = msg.namespace,
            sessionId = msg.sessionId,
            columns = emptyList(),
            rows = emptyList(),
            rowsAffected = 0,
            resolvedCommitHex = "",
            readOnly = false,
            error = error,
        )

    private fun header(
        correlationId: Int,
        type: WireMessageType,
    ): WireHeader = WireHeader(type, KDB_WIRE_PROTOCOL_VERSION, correlationId, 0)

    private fun cellToString(cell: kotlinx.serialization.json.JsonElement): String =
        when {
            cell is JsonPrimitive && cell.isString -> cell.content
            else -> cell.toString()
        }

    private fun encodeConflictReport(report: ConflictReport): ByteArray =
        (
            """{"transactionId":"${report.transactionId}","baseHash":"${report.baseHash}","targetHash":"${report.targetHash}"}"""
        ).encodeToByteArray()

    private fun sessionBeginAuthError(
        msg: WireMessage.SessionBegin,
        @Suppress("UNUSED_PARAMETER") error: String,
    ): WireMessage.SessionBeginAck =
        WireMessage.SessionBeginAck(
            header(msg.header.correlationId, WireMessageType.SESSION_BEGIN_ACK),
            namespace = msg.namespace,
            sessionId = "",
            headHex = "",
            readConsistency = msg.readConsistency,
        )
}
