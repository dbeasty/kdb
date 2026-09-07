package dev.kdb.server

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.query.hybrid.ReadConsistency
import dev.kdb.transaction.TransactionBuilder
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

public class SessionManager(
    private val server: KdbServerRuntime,
) {
    private val mutex = Mutex()
    private val sessions = mutableMapOf<String, KdbSession>()

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
        // Minted from the *runtime's* counter, not this manager's. Each connection gets its own
        // SessionManager, so a per-manager counter handed every connection its own "sess-1" -
        // harmless while session ids were only ever looked up within their own connection, but
        // documentLocks is runtime-global and keys ownership by session id. Two connections both
        // calling themselves "sess-1" were therefore treated as one holder: each could take locks
        // the other held, and either could release the other's. Ports the same fix Go's
        // SessionManager.Begin already carries (KdbServerRuntime.nextSessionOrdinal).
        val id = SessionId(sessionId ?: "sess-${server.nextSessionOrdinal()}")
        val readPin =
            when (readConsistency) {
                ReadConsistency.SNAPSHOT -> head
                ReadConsistency.READ_COMMITTED, ReadConsistency.READ_YOUR_WRITES -> null
            }
        // Pin before publishing the session: an unpinned readPin is a window in which the
        // commit it names could be reclaimed before anything protects it. See CommitDag.pin.
        val pinRelease = readPin?.let { server.runtime.dag.pin(it) }
        val session =
            KdbSession(
                id = id,
                namespaceId = namespaceId,
                baseVersion = head,
                readPin = readPin,
                readConsistency = readConsistency,
                pending = null,
                principal = principal,
                pinRelease = pinRelease,
            )
        mutex.withLock { sessions[id.value] = session }
        return session
    }

    public suspend fun get(sessionId: String): KdbSession? = mutex.withLock { sessions[sessionId] }

    public suspend fun end(sessionId: String) {
        val removed = mutex.withLock { sessions.remove(sessionId) }
        if (removed != null) {
            server.documentLocks.releaseAll(sessionId)
            // Outside mutex, deliberately - dag.pin's release takes the DAG's own lock, and
            // holding the session map's lock across that would nest one lock inside the other
            // for no reason. Safe unordered: the map remove above is what claims the right to
            // release, so a concurrent end() for the same id finds nothing and releases nothing.
            // Without this a session whose connection dropped mid-transaction would hold a
            // commit against compaction for the process lifetime - the read-side twin of the
            // document locks released just above.
            removed.pinRelease?.invoke()
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

    /**
     * Returns the session's in-flight transaction builder, opening one anchored at the session's
     * base version if this is the first buffered write.
     *
     * Opening that transaction is also where a non-SNAPSHOT session re-anchors its write base to
     * the live head, because this - not session begin - is the moment its transaction actually
     * starts. READ_COMMITTED and READ_YOUR_WRITES sessions take no read pin, so their statements
     * read at the live head while [KdbSession.baseVersion] stayed frozen at the last transaction
     * boundary; any commit landing in between left the session writing against a version older
     * than the one its own statement had just read, and conflict detection - per-document and
     * content-addressed - then reported a conflict against a change that statement had already
     * seen.
     *
     * Only the *first* buffered write re-anchors. Every later statement in the same transaction
     * keeps that anchor, so a genuinely concurrent writer arriving mid-transaction still
     * conflicts, which is the whole point of the base version. A SNAPSHOT session never
     * re-anchors: its writes stay pinned to the snapshot its reads see, and a conflict there is
     * real by definition.
     *
     * Mirrors Go's SessionManager.PendingBuilder, which carries the same rule.
     */
    public suspend fun pendingBuilder(session: KdbSession): LockingTransactionBuilder {
        if (session.pending == null) {
            if (session.readConsistency != ReadConsistency.SNAPSHOT) {
                session.baseVersion = server.runtime.dag.head()
            }
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
