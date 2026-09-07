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
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.error.ViolationType
import dev.kdb.index.IndexDescriptor
import dev.kdb.index.IndexType
import dev.kdb.index.RankedResult
import dev.kdb.index.fusion.FusionArm
import dev.kdb.index.fusion.FusionMode
import dev.kdb.index.fusion.fuseRankings
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
import dev.kdb.sql.isDmlStatement
import dev.kdb.sql.isTransactionControlStatement
import dev.kdb.sql.SqlStatement
import dev.kdb.transaction.TransactionBuilder
import dev.kdb.transaction.TransactionResult
import dev.kdb.transaction.UniqueConstraintViolationException
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

    /** Test-only seam (Component 74 §12): invoked with the session id at the start of every SqlExec,
     * inside the per-session lock, so a test can hold one session's statement open and prove that
     * sessionless frames (DocumentGet, Search) and other sessions on the same connection still
     * complete, while a second statement on the same session waits its turn. */
    internal var sqlExecHook: (suspend (sessionId: String) -> Unit)? = null

    /** Component 45: releases every session (and its document locks) this connection's
     * SqlWireHost holds - called from the connection's teardown path (pipelinedPerConnection's
     * finally), not from any wire message. Idempotent (a second call just ends zero sessions). */
    public suspend fun endSession() {
        sessions.endAll()
    }

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
            is WireMessage.DocumentGet -> handleDocumentGet(message)
            is WireMessage.Upsert -> handleUpsert(message)
            is WireMessage.Search -> handleSearch(message)
            else -> null
        }

    /**
     * Component 40 direct point lookup by document id - no session, no read-consistency
     * tracking, matching Go's handleDocumentGet exactly (wire codes 0x14/0x15 are the Go
     * side's; this is the Kotlin half of the coordinated port).
     */
    private suspend fun handleDocumentGet(msg: WireMessage.DocumentGet): WireMessage.DocumentGetResult {
        val principal =
            try {
                sqlAuth.authenticateConnection()
            } catch (e: Throwable) {
                return documentGetError(msg, sqlAuth.authFailureMessage(e))
            }
        val docId =
            try {
                KdbUuid.fromString(msg.docId)
            } catch (e: IllegalArgumentException) {
                return documentGetError(msg, "invalid docId: ${e.message}")
            }
        try {
            sqlAuth.authorize(principal, AuthAction.DocumentRead(msg.namespace, msg.docId))
        } catch (e: Throwable) {
            return documentGetError(msg, sqlAuth.authFailureMessage(e))
        }
        val (json, headHex) = server.getDocument(msg.namespace, docId)
        return WireMessage.DocumentGetResult(
            header(msg.header.correlationId, WireMessageType.DOCUMENT_GET_RESULT),
            namespace = msg.namespace,
            docId = msg.docId,
            json = json,
            commitHex = headHex,
        )
    }

    private fun documentGetError(
        msg: WireMessage.DocumentGet,
        error: String,
    ): WireMessage.DocumentGetResult =
        WireMessage.DocumentGetResult(
            header(msg.header.correlationId, WireMessageType.DOCUMENT_GET_RESULT),
            namespace = msg.namespace,
            docId = msg.docId,
            json = null,
            commitHex = "",
            error = error,
        )

    /** Component 40 unconditional upsert - Go's handleUpsert, ported (wire codes 0x16/0x17). */
    private suspend fun handleUpsert(msg: WireMessage.Upsert): WireMessage.UpsertResult {
        val principal =
            try {
                sqlAuth.authenticateConnection()
            } catch (e: Throwable) {
                return upsertError(msg, sqlAuth.authFailureMessage(e))
            }
        val docId =
            try {
                KdbUuid.fromString(msg.docId)
            } catch (e: IllegalArgumentException) {
                return upsertError(msg, "invalid docId: ${e.message}")
            }
        try {
            sqlAuth.authorize(principal, AuthAction.DocumentWrite(msg.namespace, msg.docId))
        } catch (e: Throwable) {
            return upsertError(msg, sqlAuth.authFailureMessage(e))
        }
        val commit =
            try {
                server.upsert(msg.namespace, docId, msg.json, sqlAuth.writeAuthorizerFor(principal))
            } catch (e: Throwable) {
                if (e is kotlinx.coroutines.CancellationException) throw e
                return upsertError(msg, e.message ?: e.toString(), errorCodeFor(e))
            }
        return WireMessage.UpsertResult(
            header(msg.header.correlationId, WireMessageType.UPSERT_RESULT),
            namespace = msg.namespace,
            commitHex = commit.hash.toHex(),
        )
    }

    private fun upsertError(
        msg: WireMessage.Upsert,
        error: String,
        errorCode: String? = null,
    ): WireMessage.UpsertResult =
        WireMessage.UpsertResult(
            header(msg.header.correlationId, WireMessageType.UPSERT_RESULT),
            namespace = msg.namespace,
            commitHex = "",
            error = error,
            errorCode = errorCode,
        )

    /**
     * Maps a failed write to the wire's typed error code where the Go server does the same
     * (go/kdb/server/error_classification.go): UNIQUE_VIOLATION for a unique-constraint collision
     * (Layer 16 §9.6), SCHEMA_VIOLATION for any other schema rejection. Null for everything else -
     * the message alone, as before.
     */
    private fun errorCodeFor(e: Throwable): String? =
        when {
            e is UniqueConstraintViolationException -> "UNIQUE_VIOLATION"
            e is dev.kdb.transaction.TransactionSchemaException -> "SCHEMA_VIOLATION"
            e is dev.kdb.error.SchemaViolationException -> "SCHEMA_VIOLATION"
            e is IllegalArgumentException && e.message?.startsWith("schema rejection") == true -> "SCHEMA_VIOLATION"
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
        sqlExecHook?.invoke(msg.sessionId)
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
            // Authorize against session.namespaceId, not msg.namespace: msg.namespace is
            // client-supplied and unvalidated, while execution below always runs against
            // session.namespaceId (fixed at SessionBegin, when it was actually authorized). A
            // client whose session was legitimately opened against namespace A but who only
            // holds an exec/write grant on namespace B could otherwise pass B in msg.namespace
            // and have this check wrongly authorize a statement that actually executes against
            // A - not every write op gets a secondary per-document check (writeAuthorizerFor
            // treats KdbOp.SchemaMigration as unauthorizable-here, so DDL like CREATE TABLE in
            // autocommit mode has no other gate at all).
            sqlAuth.authorize(
                session.principal,
                AuthAction.SqlExec(session.namespaceId, readOnly = !sqlRequiresWrite(parsed)),
            )
        } catch (e: Throwable) {
            return sqlError(msg, sqlAuth.authFailureMessage(e))
        }
        return try {
            val deferCommit = !session.autoCommit
            // Autocommit DML is lowered to ops by the hybrid engine but committed here through
            // server.commit rather than the engine's own private commit path, so it goes through
            // the namespace's write coordinator and the engine that enforces unique constraints
            // (Layer 16 §9.6) exactly like TxCommit and Upsert do. The hybrid engine's deferred
            // mode already exposes the ops for this.
            val autocommitDml = session.autoCommit && isDmlStatement(parsed)
            val collectedOps = mutableListOf<KdbOp>()
            val parameters = decodeSqlParameters(msg.parametersJson)
            val result =
                server.hybridFor(session.namespaceId).execute(
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
                        deferCommit = deferCommit || autocommitDml,
                        transactionBase = if (deferCommit) session.baseVersion else null,
                        bufferOps =
                            when {
                                deferCommit -> { ops -> appendPendingOps(sessions.pendingBuilder(session), ops) }
                                autocommitDml -> { ops -> collectedOps += ops }
                                else -> null
                            },
                    ),
                )
            val json = result.toQueryResultJson()
            var resolvedCommitHex = json.resolvedCommit
            if (autocommitDml) {
                resolvedCommitHex =
                    if (collectedOps.isEmpty()) {
                        server.runtime.dag.head().toHex()
                    } else {
                        val tx =
                            KdbTransaction(
                                KdbUuid.random(),
                                server.runtime.dag.head(),
                                collectedOps.toList(),
                                KdbTimestamp.now(),
                                KdbUuid.random(),
                            )
                        val commit =
                            server.commit(
                                session.namespaceId,
                                tx,
                                sessionId = session.id.value,
                                authorizer = sqlAuth.writeAuthorizerFor(session.principal),
                            )
                        session.baseVersion = commit.hash
                        commit.hash.toHex()
                    }
            }
            WireMessage.SqlResult(
                header(msg.header.correlationId, WireMessageType.SQL_RESULT),
                namespace = msg.namespace,
                sessionId = msg.sessionId,
                columns = json.columns,
                rows = json.rows.map { row -> row.map { cellToString(it) } },
                rowsAffected = result.result.rowsAffected,
                resolvedCommitHex = resolvedCommitHex,
                readOnly = json.readOnly,
                error = null,
                generatedIds = result.result.generatedIds,
            )
        } catch (e: ConflictException) {
            conflictReport(msg.header.correlationId, session.namespaceId, e.report, msg.namespace)
        } catch (e: Throwable) {
            if (e is kotlinx.coroutines.CancellationException) throw e
            sqlError(msg, e.message ?: e.toString(), errorCodeFor(e))
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
                ) { err, code -> sqlError(msg, err, code) }
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
            // Same reasoning as handleSqlExec above: authorize the namespace actually being
            // committed to (session.namespaceId), not the client-supplied msg.namespace.
            sqlAuth.authorize(session.principal, AuthAction.TxCommit(session.namespaceId))
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
        ) { err, code -> sqlError(msg, err, code) }
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
            conflictReport(msg.header.correlationId, session.namespaceId, e.report, msg.namespace)
        } catch (e: DocumentLockedException) {
            abortSessionAfterFailedCommit(session)
            sqlError(msg, e.message ?: "document locked")
        } catch (e: Throwable) {
            abortSessionAfterFailedCommit(session)
            sqlError(msg, e.message ?: e.toString(), errorCodeFor(e))
        }

    private suspend fun commitSession(
        correlationId: Int,
        namespace: String,
        session: KdbSession,
        onError: (String, String?) -> WireMessage,
    ): WireMessage =
        try {
            val effective =
                session.pending?.build()
                    ?: return onError("no pending transaction to commit", null)
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
            conflictReport(correlationId, session.namespaceId, e.report, namespace)
        } catch (e: DocumentLockedException) {
            abortSessionAfterFailedCommit(session)
            onError(e.message ?: "document locked", null)
        } catch (e: Throwable) {
            abortSessionAfterFailedCommit(session)
            onError(e.message ?: e.toString(), errorCodeFor(e))
        }

    private suspend fun finishCommittedSession(
        session: KdbSession,
        commitHash: KdbHash,
        @Suppress("UNUSED_PARAMETER") rowsAffected: Int,
    ) {
        sessions.clearPending(session)
        server.documentLocks.releaseAll(session.id.value)
        session.baseVersion = commitHash
        // The transaction is over: the next one anchors on - and, for a SNAPSHOT session, reads
        // at - the commit just produced. The old pin (if any) has to be released here, not left
        // for session end: without this, a long-lived SNAPSHOT session accumulates one pin per
        // commit and compaction never reclaims anything it ever read at. See CommitDag.pin.
        session.pinRelease?.invoke()
        session.pinRelease = null
        if (session.readConsistency == ReadConsistency.SNAPSHOT) {
            session.readPin = commitHash
            session.pinRelease = server.runtime.dag.pin(commitHash)
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
                conflictReport(msg.header.correlationId, msg.namespace, result.report)
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
                    errorCode =
                        if (result.violations.any { v -> v.violations.any { it.violationType == ViolationType.UNIQUE_CONSTRAINT } }) {
                            "UNIQUE_VIOLATION"
                        } else {
                            "SCHEMA_VIOLATION"
                        },
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
        errorCode: String? = null,
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
            errorCode = errorCode,
        )

    private fun sqlError(
        msg: WireMessage.TxCommit,
        error: String,
        errorCode: String? = null,
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
            errorCode = errorCode,
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

    /**
     * Shapes a lost optimistic-concurrency race, carrying the structured report a client needs
     * to decide *what* to do and the code/retry-after it needs to decide *when* - mirrors Go's
     * conflictReport (server/wire_listen.go). Before these fields existed a conflict was the one
     * refusal the server could not pace: the message carried a report and nothing else, so a
     * client's only option was to retry instantly and collide again. See conflictRetryAfterMs.
     */
    private fun conflictReport(
        correlationId: Int,
        namespace: String,
        report: ConflictReport,
        /** Namespace echoed on the wire when it differs from the one whose gate paced the hint. */
        replyNamespace: String = namespace,
    ): WireMessage.ConflictReport =
        WireMessage.ConflictReport(
            header(correlationId, WireMessageType.CONFLICT_REPORT),
            namespace = replyNamespace,
            reportBytes = encodeConflictReport(report),
            errorCode = "CONFLICT",
            // Component 74: paced by the namespace's own write gate, not a runtime-wide one.
            retryAfterMs = server.conflictRetryAfterMs(namespace),
        )

    // ---- Layer 16 Component 69 (§11): SEARCH over the wire ------------------------------------

    /**
     * Sessionless like DocumentGet, authorized as DocumentRead on the namespace. Each present arm
     * runs through the runtime's IndexManager for the namespace - `index` resolves to a FULLTEXT /
     * VECTOR index by its SQL name (`options["index_name"]`) or by its first field. One arm returns
     * that arm's ranking (after its own minScore/depth); both arms are fused per §8 (`rrf` default,
     * `weighted`). Expired documents (§9.5) are dropped from head results; `includeJson` fetches
     * bodies at the resolved commit and drops hits whose document is gone at that commit.
     */
    private suspend fun handleSearch(msg: WireMessage.Search): WireMessage.SearchResult {
        val principal =
            try {
                sqlAuth.authenticateConnection()
            } catch (e: Throwable) {
                return searchError(msg, sqlAuth.authFailureMessage(e), "UNAUTHORIZED")
            }
        try {
            sqlAuth.authorize(principal, AuthAction.DocumentRead(msg.namespace, ""))
        } catch (e: Throwable) {
            return searchError(msg, sqlAuth.authFailureMessage(e), "UNAUTHORIZED")
        }
        if (msg.text == null && msg.vector == null) {
            return searchError(msg, "search needs a text and/or a vector arm", "SCHEMA_VIOLATION")
        }
        if (msg.limit <= 0) {
            return searchError(msg, "limit must be > 0", "SCHEMA_VIOLATION")
        }
        val mode =
            when (msg.fusion?.lowercase()) {
                null, "", "rrf" -> FusionMode.RRF
                "weighted" -> FusionMode.WEIGHTED_SUM
                else -> return searchError(msg, "unknown fusion mode: ${msg.fusion}", "SCHEMA_VIOLATION")
            }
        val head = server.runtime.dag.head()
        val requestedCommitHex = msg.atCommitHex
        val resolved =
            if (requestedCommitHex.isNullOrEmpty()) {
                head
            } else {
                val h = runCatching { KdbHash.fromHex(requestedCommitHex) }.getOrNull()
                if (h == null || !server.runtime.dag.hasCommit(h)) {
                    return searchError(msg, "unknown commit: $requestedCommitHex", "SCHEMA_VIOLATION")
                }
                h
            }
        val atHead = resolved == head
        val registry = server.runtime.indexManager.registryFor(msg.namespace)
        val reader = server.runtime.indexManager.reader
        val fused = msg.text != null && msg.vector != null

        val arms = mutableListOf<FusionArm>()
        try {
            msg.text?.let { arm ->
                resolveIndex(registry.indexes.map { it.descriptor }, IndexType.FULLTEXT, arm.index)
                    ?: return searchError(msg, "no FULLTEXT index for ${arm.index}", "SCHEMA_VIOLATION")
                val depth = arm.depth ?: 0
                val fetch = if (depth > 0) depth else if (fused) Int.MAX_VALUE else msg.limit
                // The reader resolves an index by SQL name as well as by field, so the client's own
                // spelling goes through unchanged; the descriptor lookup above exists to turn "no
                // such index" into a message naming what was asked for rather than an exception.
                val results = reader.lookupFullText(registry, arm.index, arm.query, resolved, fetch)
                arms += FusionArm(results, arm.weight ?: 1.0, depth, arm.minScore)
            }
            msg.vector?.let { arm ->
                resolveIndex(registry.indexes.map { it.descriptor }, IndexType.VECTOR, arm.index)
                    ?: return searchError(msg, "no VECTOR index for ${arm.index}", "SCHEMA_VIOLATION")
                val depth = arm.depth ?: 0
                val k = if (depth > 0) depth else if (fused) Int.MAX_VALUE else msg.limit
                val query = FloatArray(arm.vector.size) { arm.vector[it].toFloat() }
                val results = reader.lookupVector(registry, arm.index, query, k, resolved)
                arms += FusionArm(results, arm.weight ?: 1.0, depth, arm.minScore)
            }
        } catch (e: Throwable) {
            if (e is kotlinx.coroutines.CancellationException) throw e
            return searchError(msg, e.message ?: e.toString(), "INTERNAL")
        }

        val ranked: List<RankedResult> =
            if (fused) {
                fuseRankings(arms, mode, Int.MAX_VALUE)
            } else {
                val arm = arms.single()
                var list = arm.results
                arm.minScore?.let { floor -> list = list.filter { it.score >= floor } }
                if (arm.depth > 0 && list.size > arm.depth) list = list.take(arm.depth)
                list
            }

        // Materialise bodies when asked for, or when expiry must be applied at head (§9.5); a hit
        // whose document is absent at the resolved commit is dropped either way.
        val expiry = if (atHead) server.expiryPolicyFor(msg.namespace) else null
        val tree = server.runtime.dag.getCommitOrThrow(resolved).documentTreeHash
        val hits = ArrayList<WireMessage.SearchHit>(minOf(ranked.size, msg.limit))
        val needBody = msg.includeJson || expiry != null
        val now = server.nowMillis()
        for (r in ranked) {
            if (hits.size >= msg.limit) break
            if (!needBody) {
                hits += WireMessage.SearchHit(r.docId.toString(), r.score)
                continue
            }
            val doc = server.runtime.storage.getDocument(msg.namespace, r.docId, tree) ?: continue
            if (expiry != null && isDocumentExpired(doc.json, expiry, now)) continue
            hits += WireMessage.SearchHit(r.docId.toString(), r.score, if (msg.includeJson) doc.json else null)
        }
        return WireMessage.SearchResult(
            header(msg.header.correlationId, WireMessageType.SEARCH_RESULT),
            namespace = msg.namespace,
            hits = hits,
            resolvedCommitHex = resolved.toHex(),
        )
    }

    /** `index` is a SQL index name (descriptor option `index_name`) or the index's first field. */
    private fun resolveIndex(
        descriptors: List<IndexDescriptor>,
        type: IndexType,
        nameOrField: String,
    ): IndexDescriptor? {
        val ofType = descriptors.filter { it.type == type }
        return ofType.firstOrNull { it.options["index_name"] == nameOrField }
            ?: ofType.firstOrNull { it.fieldName == nameOrField }
    }

    private fun searchError(
        msg: WireMessage.Search,
        error: String,
        errorCode: String?,
    ): WireMessage.SearchResult =
        WireMessage.SearchResult(
            header(msg.header.correlationId, WireMessageType.SEARCH_RESULT),
            namespace = msg.namespace,
            hits = emptyList(),
            resolvedCommitHex = "",
            error = error,
            errorCode = errorCode,
        )

    private fun sessionBeginAuthError(
        msg: WireMessage.SessionBegin,
        error: String,
    ): WireMessage.SessionBeginAck =
        WireMessage.SessionBeginAck(
            header(msg.header.correlationId, WireMessageType.SESSION_BEGIN_ACK),
            namespace = msg.namespace,
            sessionId = "",
            headHex = "",
            readConsistency = msg.readConsistency,
            error = error,
        )
}
