package dev.kdb.server

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.query.hybrid.ReadConsistency
import dev.kdb.transaction.TransactionBuilder
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.atomic.AtomicInteger

public class SessionManager(
    private val server: KdbServerRuntime,
) {
    private val mutex = Mutex()
    private val sessions = mutableMapOf<String, KdbSession>()
    private val idSeq = AtomicInteger()

    public suspend fun begin(
        namespaceId: String,
        readConsistency: ReadConsistency,
        baseVersionHex: String? = null,
        sessionId: String? = null,
        principal: dev.kdb.auth.Principal? = null,
    ): KdbSession {
        val head =
            baseVersionHex?.let { KdbHash.fromHex(it) }
                ?: server.runtime.dag.head()
        require(server.runtime.dag.hasCommit(head)) { "unknown base version: $baseVersionHex" }
        val id = SessionId(sessionId ?: "sess-${idSeq.incrementAndGet()}")
        val readPin =
            when (readConsistency) {
                ReadConsistency.SNAPSHOT -> head
                ReadConsistency.READ_COMMITTED, ReadConsistency.READ_YOUR_WRITES -> null
            }
        val session =
            KdbSession(
                id = id,
                namespaceId = namespaceId,
                baseVersion = head,
                readPin = readPin,
                readConsistency = readConsistency,
                pending = null,
                principal = principal,
            )
        mutex.withLock { sessions[id.value] = session }
        return session
    }

    public suspend fun get(sessionId: String): KdbSession? = mutex.withLock { sessions[sessionId] }

    public suspend fun end(sessionId: String) {
        mutex.withLock {
            sessions.remove(sessionId)?.let {
                server.documentLocks.releaseAll(sessionId)
            }
        }
    }

    /**
     * Ends every session this manager currently holds, releasing each one's document locks.
     * Component 45: one [SessionManager] is 1:1 with one connection ([SqlWireHost] is created
     * per connection by [sqlWireHostFactory]), and a connection can hold more than one session
     * (a client may open several via [WireMessage.SessionBegin]) - so connection teardown needs
     * "end all", not a single [end] call for one guessed session id.
     */
    public suspend fun endAll() {
        val ids = mutex.withLock { sessions.keys.toList() }
        for (id in ids) {
            end(id)
        }
    }

    public suspend fun pendingBuilder(session: KdbSession): LockingTransactionBuilder {
        if (session.pending == null) {
            val inner =
                TransactionBuilder(
                    namespaceId = session.namespaceId,
                    baseVersion = session.baseVersion,
                    authorNodeId = KdbUuid.random(),
                    schema = server.runtime.schema,
                )
            session.pending = inner
        }
        return LockingTransactionBuilder(
            inner = session.pending!!,
            locks = server.documentLocks,
            sessionId = session.id.value,
        )
    }

    public suspend fun clearPending(session: KdbSession) {
        session.pending = null
    }
}
