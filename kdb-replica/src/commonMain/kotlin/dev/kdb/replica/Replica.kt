package dev.kdb.replica

import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbOp
import dev.kdb.transport.ws.defaultWebSocketWireTransport
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext

/** Which namespaces a replica holds, and how (plan §4). */
public sealed class NamespaceSpec {
    public abstract val ns: String

    /** The whole namespace with real commits; needs a pull grant on it (plan §4.1). */
    public data class Full(override val ns: String) : NamespaceSpec()

    /**
     * What matches [filter] and the token may read (plan §4.2); read-only unless [writeBack],
     * which sends local writes to the source by PROJECT_WRITE.
     */
    public data class Filtered(override val ns: String, val filter: String, val writeBack: Boolean = false) : NamespaceSpec()
}

/** Opens a WebSocket to the host with kdb-transport-ws's JVM client. */
public val defaultPeerConnector: PeerConnector = { url ->
    // The JVM client connects (TCP and the HTTP upgrade) in its constructor, blocking.
    withContext(Dispatchers.Default) { defaultWebSocketWireTransport().connect(url) }
}

public data class ReplicaOptions(
    val store: ReplicaStore,
    /** The host's peer sync endpoint, e.g. `ws://host:port/kdb/sync`. */
    val url: String,
    val namespaces: List<NamespaceSpec>,
    /** A fixed bearer token; [tokenProvider] wins when both are set. */
    val token: String? = null,
    /** Asked on every connect, so a refreshed token is used. */
    val tokenProvider: (suspend () -> String?)? = null,
    /** Opens the transport; WebSocket by default. */
    val connect: PeerConnector = defaultPeerConnector,
    val catchUp: CatchUp = CatchUp.AUTO,
    /** Bytes of trie nodes and document bodies kept in memory; the rest is read from the store. */
    val cacheBytes: Long = 32L shl 20,
    /**
     * Milliseconds between sync rounds; 0 syncs only on writes, host reports and [Replica.syncNow].
     * Null: 5000, or 60000 while the host is reporting changes (live).
     */
    val pollMs: Long? = null,
    /**
     * Keep the connection open between rounds and ask the host to report changes (REFS_WATCH), so
     * remote writes arrive without waiting for a poll. A host without the watch capability is polled.
     */
    val live: Boolean = true,
    /** Page size asked of the host, in bytes. */
    val pageBytes: Int = DEFAULT_PAGE_BYTES,
    /** Start syncing at once and on every local write; false syncs only on [Replica.syncNow]. */
    val autoSync: Boolean = true,
)

public enum class ChangeSource { LOCAL, REMOTE }

public data class ChangeEvent(val namespace: String, val ids: List<String>, val source: ChangeSource)

public data class ConflictEvent(
    val namespace: String,
    val report: ConflictReport?,
    /** Full mode: the local commits the host refused, still held. Filtered: none (the write was undone). */
    val pending: List<String>,
    /** Filtered mode: the source's decision ("conflict" or "refused"), and why. */
    val outcome: String? = null,
    val reason: String? = null,
)

public enum class NamespaceMode { FULL, FILTERED }

public data class NamespaceStatus(
    val mode: NamespaceMode,
    val head: String?,
    val pending: Int,
    val converging: Boolean,
    val lastSource: String? = null,
)

public data class ReplicaStatus(
    val connected: Boolean,
    /** Epoch milliseconds of the last completed round, or null. */
    val lastSyncAt: Long?,
    val lastError: String?,
    val namespaces: Map<String, NamespaceStatus>,
    /** Bytes the read cache holds now; never more than [ReplicaOptions.cacheBytes]. */
    val residentBytes: Long,
)

/** A sync round failed for some namespaces; the others synced. */
public class ReplicaSyncException(message: String, public val results: List<NamespaceSyncResult>) : RuntimeException(message)

/**
 * Namespaces held locally and kept in sync with a KDB node (docs/kdb-client-replica-plan.md §3.3;
 * a port of packages/kdb-replica/src/replica.ts).
 *
 * Reads and writes are local and never wait for the network. Sync runs in rounds - connect, hello,
 * each namespace pushed then pulled - on a poll interval, right after a local write, when the host
 * reports a change (REFS_WATCH), and on demand. A failed round backs off and retries; nothing local
 * is lost by a failure, because local writes are durable commits before they are acknowledged.
 *
 * Threading: everything is suspend and safe to call from any coroutine; rounds run one at a time
 * on the replica's own scope (kotlinx.coroutines, as kdb-peer-sync uses), and listeners are called
 * from it, so they must not block.
 */
public class Replica private constructor(
    private val opts: ReplicaOptions,
    private val store: CachedReplicaStore,
    private val nodeId: String,
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val ns = LinkedHashMap<String, ReplicaNamespace>()
    private val proj = LinkedHashMap<String, ProjectionNamespace>()
    private val outboxSizes = MutableStateFlow<Map<String, Int>>(emptyMap())
    private val changeListeners = MutableStateFlow<List<(ChangeEvent) -> Unit>>(emptyList())
    private val conflictListeners = MutableStateFlow<List<(ConflictEvent) -> Unit>>(emptyList())
    private val syncListeners = MutableStateFlow<List<(List<NamespaceSyncResult>) -> Unit>>(emptyList())

    // Round scheduling; guarded by gate.
    private val gate = Mutex()
    private var running: Deferred<List<NamespaceSyncResult>>? = null
    private var timer: Job? = null
    private var again = false

    @kotlin.concurrent.Volatile private var closed = false

    @kotlin.concurrent.Volatile private var failures = 0

    @kotlin.concurrent.Volatile private var connected = false

    @kotlin.concurrent.Volatile private var lastSyncAt: Long? = null

    @kotlin.concurrent.Volatile private var lastError: String? = null

    /** The open connection, reused across rounds while live; touched only inside a round or close. */
    @kotlin.concurrent.Volatile private var conn: PeerConnection? = null

    @kotlin.concurrent.Volatile private var watching = false

    /** What the last round did to the filtered namespaces. */
    @kotlin.concurrent.Volatile
    public var lastProjectionResults: List<ProjectionSyncResult> = emptyList()
        private set

    public companion object {
        public suspend fun open(options: ReplicaOptions): Replica {
            val store = CachedReplicaStore(options.store, options.cacheBytes)
            var nodeId = store.get("m/node")?.decodeToString()
            if (nodeId == null) {
                nodeId = randomUuid()
                store.apply(Batch().apply { put("m/node", nodeId.encodeToByteArray()) })
            }
            val r = Replica(options, store, nodeId)
            for (spec in options.namespaces) {
                require(spec.ns !in r.ns && spec.ns !in r.proj) { "kdb replica: ${spec.ns} is listed twice" }
                when (spec) {
                    is NamespaceSpec.Filtered -> {
                        val p = ProjectionNamespace.open(store, spec.ns, spec.filter, spec.writeBack)
                        r.proj[spec.ns] = p
                        val size = p.outboxSize()
                        r.outboxSizes.update { it + (spec.ns to size) }
                    }
                    is NamespaceSpec.Full -> r.ns[spec.ns] = ReplicaNamespace.open(store, spec.ns, nodeId)
                }
            }
            if (options.autoSync) r.schedule(0)
            return r
        }
    }

    /** A full-mode namespace. */
    public fun namespace(name: String): ReplicaNamespace =
        ns[name] ?: throw IllegalArgumentException("kdb replica: $name is not one of this replica's full namespaces")

    /** A filtered namespace. */
    public fun projection(name: String): ProjectionNamespace =
        proj[name] ?: throw IllegalArgumentException("kdb replica: $name is not one of this replica's filtered namespaces")

    private fun requireHeld(name: String) {
        require(name in ns || name in proj) { "kdb replica: $name is not one of this replica's namespaces" }
    }

    // --- local reads and writes ------------------------------------------------------------------

    public suspend fun get(namespace: String, id: String): String? {
        requireHeld(namespace)
        return ns[namespace]?.get(id) ?: proj[namespace]?.get(id)
    }

    public suspend fun scan(namespace: String, after: String? = null, before: String? = null, limit: Int = Int.MAX_VALUE): List<ReplicaDocument> {
        requireHeld(namespace)
        return ns[namespace]?.scan(after, before, limit) ?: proj.getValue(namespace).scan(after, before, limit)
    }

    /**
     * Writes documents locally, durable before it returns, and schedules a sync. Full mode: one
     * local commit, pushed as is. Filtered with write-back: one write-back transaction. Returns the
     * commit hash or the write-back transaction id.
     */
    public suspend fun write(namespace: String, ops: List<KdbOp>, message: String = ""): String {
        val p = proj[namespace]
        if (p != null) {
            val docs =
                ops.map { o ->
                    when (o) {
                        is KdbOp.Write -> o.docId.toString() to o.patch
                        is KdbOp.Delete -> o.docId.toString() to null
                        else -> throw IllegalArgumentException("kdb replica: a filtered namespace takes document writes and deletes only, not $o")
                    }
                }
            val entry = p.write(docs, randomUuid())
            outboxSizes.update { it + (namespace to (it[namespace] ?: 0) + 1) }
            emitChange(ChangeEvent(namespace, docs.map { it.first }, ChangeSource.LOCAL))
            schedule(50)
            return entry.txId
        }
        val c = namespace(namespace).commitLocal(ops, KdbUuid.random(), message)
        val ids =
            ops.mapNotNull {
                when (it) {
                    is KdbOp.Write -> it.docId.toString()
                    is KdbOp.Delete -> it.docId.toString()
                    else -> null
                }
            }
        emitChange(ChangeEvent(namespace, ids, ChangeSource.LOCAL))
        schedule(50)
        return c.hash
    }

    /** Writes the whole document [json] under [id]. */
    public suspend fun put(namespace: String, id: String, json: String): String =
        write(namespace, listOf(KdbOp.Write(parseUuid(id), json)))

    public suspend fun delete(namespace: String, id: String): String = write(namespace, listOf(KdbOp.Delete(parseUuid(id))))

    // --- events and status -----------------------------------------------------------------------

    /** Subscribes to document changes; returns the unsubscribe function. */
    public fun onChange(listener: (ChangeEvent) -> Unit): () -> Unit = subscribe(changeListeners, listener)

    /** Subscribes to conflicts (a refused push, or a write-back the source did not apply). */
    public fun onConflict(listener: (ConflictEvent) -> Unit): () -> Unit = subscribe(conflictListeners, listener)

    /** Subscribes to completed rounds. */
    public fun onSync(listener: (List<NamespaceSyncResult>) -> Unit): () -> Unit = subscribe(syncListeners, listener)

    private fun <T> subscribe(set: MutableStateFlow<List<T>>, l: T): () -> Unit {
        set.update { it + l }
        return { set.update { list -> list.filter { it !== l } } }
    }

    private fun <E> emit(set: MutableStateFlow<List<(E) -> Unit>>, e: E) {
        for (l in set.value) {
            try {
                l(e)
            } catch (_: Exception) {
                // A listener's failure is the listener's.
            }
        }
    }

    private fun emitChange(e: ChangeEvent) = emit(changeListeners, e)

    public fun status(): ReplicaStatus {
        val namespaces = LinkedHashMap<String, NamespaceStatus>()
        for ((name, n) in ns) namespaces[name] = NamespaceStatus(NamespaceMode.FULL, n.head, n.pending.size, n.target != null)
        for ((name, p) in proj) {
            namespaces[name] =
                NamespaceStatus(NamespaceMode.FILTERED, null, outboxSizes.value[name] ?: 0, p.transfer != null, p.lastSource)
        }
        return ReplicaStatus(connected, lastSyncAt, lastError, namespaces, store.residentBytes)
    }

    // --- sync ------------------------------------------------------------------------------------

    /** Runs a sync round now (or joins the one running) and returns what it did. */
    public suspend fun syncNow(): List<NamespaceSyncResult> = startRound().await()

    private suspend fun startRound(): Deferred<List<NamespaceSyncResult>> =
        gate.withLock {
            check(!closed) { "kdb replica: the replica is closed" }
            running?.let {
                again = true
                return@withLock it
            }
            val t = timer
            timer = null
            // A round started by the timer must not cancel the timer's own coroutine.
            if (t != null && t !== kotlin.coroutines.coroutineContext[Job]) t.cancel()
            val d =
                scope.async(start = CoroutineStart.LAZY) {
                    try {
                        round()
                    } finally {
                        afterRound()
                    }
                }
            running = d
            d.start()
            d
        }

    private suspend fun afterRound() {
        gate.withLock {
            running = null
            if (closed) return@withLock
            if (again) {
                again = false
                scheduleLocked(0)
            } else {
                val poll = opts.pollMs ?: if (watching) 60_000L else 5_000L
                val backoff = if (failures > 0) minOf(60_000L, 500L shl minOf(failures, 7)) else 0L
                if (backoff > 0) scheduleLocked(backoff) else if (poll > 0) scheduleLocked(poll)
            }
        }
    }

    private fun schedule(ms: Long) {
        if (closed || !opts.autoSync) return
        scope.launch { gate.withLock { scheduleLocked(ms) } }
    }

    private fun scheduleLocked(ms: Long) {
        if (closed || !opts.autoSync) return
        if (running != null) {
            again = true
            return
        }
        timer?.cancel()
        timer =
            scope.launch {
                delay(ms)
                try {
                    syncNow()
                } catch (e: CancellationException) {
                    throw e
                } catch (_: Exception) {
                    // Recorded in status(); the next round is already scheduled with backoff.
                }
            }
    }

    private suspend fun connect(): Pair<PeerConnection, Boolean> {
        conn?.let { if (!it.closed) return it to false }
        val token = opts.tokenProvider?.invoke() ?: opts.token
        // Filtered namespaces' sources are named too: where the token may also sync one, the host
        // grants it, and the replica can watch it.
        val names = (ns.keys + proj.keys).distinct()
        val c = PeerConnection.open(opts.url, nodeId, token, names, opts.connect)
        c.onClose {
            if (conn === c) {
                conn = null
                watching = false
                connected = false
                schedule(if (closed) 0 else 1000)
            }
        }
        return c to true
    }

    private val live: Boolean get() = opts.live && opts.autoSync

    private suspend fun round(): List<NamespaceSyncResult> {
        var c: PeerConnection? = null
        var keep = false
        try {
            val (opened, fresh) = connect()
            c = opened
            // A reused connection's hello refs are stale: ask again.
            if (!fresh) c.refreshRefs(c.refs.map { it.namespace })
            connected = true
            val results = ArrayList<NamespaceSyncResult>()
            val projections = ArrayList<ProjectionSyncResult>()
            val errors = ArrayList<String>()
            val syncOpts = SyncOptions(opts.catchUp, opts.pageBytes)
            for ((name, n) in ns) {
                try {
                    val r = syncNamespace(c, n, c.refs.find { it.namespace == name }, syncOpts)
                    results += r
                    if (r.changed.isNotEmpty()) emitChange(ChangeEvent(name, r.changed, ChangeSource.REMOTE))
                    if (r.pushOutcome == "conflict") emit(conflictListeners, ConflictEvent(name, r.conflict, n.pending))
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    errors += "$name: ${e.message}"
                }
            }
            for ((name, p) in proj) {
                try {
                    val r = syncProjection(c, p, SyncOptions(CatchUp.AUTO, opts.pageBytes))
                    projections += r
                    val size = p.outboxSize()
                    outboxSizes.update { it + (name to size) }
                    if (r.changed.isNotEmpty()) emitChange(ChangeEvent(name, r.changed.distinct(), ChangeSource.REMOTE))
                    for (rej in r.rejected) emit(conflictListeners, conflictEventOf(name, rej))
                    if (r.deferred != null) errors += "$name: write-back deferred: ${r.deferred}"
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    errors += "$name: ${e.message}"
                }
            }
            lastSyncAt = KdbTimestamp.now().epochMillis
            lastError = if (errors.isEmpty()) null else errors.joinToString("; ")
            failures = if (errors.isEmpty()) 0 else failures + 1
            lastProjectionResults = projections
            if (live && c.has("watch")) {
                keep = true
                conn = c
                if (!watching && c.refs.isNotEmpty()) {
                    watching = true
                    c.watch(c.refs.map { it.namespace }) { schedule(0) }
                }
            }
            emit(syncListeners, results)
            if (errors.isNotEmpty()) throw ReplicaSyncException("kdb replica: sync failed for ${errors.joinToString("; ")}", results)
            return results
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            lastError = e.message ?: e.toString()
            if (e !is ReplicaSyncException) failures++
            throw e
        } finally {
            if (!keep) {
                if (conn === c) conn = null
                watching = false
                connected = false
                c?.close()
            }
        }
    }

    public suspend fun close() {
        closed = true
        val r =
            gate.withLock {
                timer?.cancel()
                timer = null
                running
            }
        try {
            r?.await()
        } catch (_: Exception) {
        }
        val c = conn
        conn = null
        c?.close()
        scope.cancel()
        store.close()
    }
}

/** A write-back the source did not apply, in the same ConflictReport shape a full-mode push conflict has. */
private fun conflictEventOf(namespace: String, rej: RejectedWrite): ConflictEvent =
    ConflictEvent(
        namespace = namespace,
        pending = emptyList(),
        outcome = rej.outcome,
        reason = rej.reason,
        report =
            ConflictReport(
                transactionId = rej.txId,
                baseHash = "",
                targetHash = "",
                conflicts =
                    rej.docs.map { d ->
                        val cur = rej.current.find { it.docId == d.docId }
                        ConflictItem(
                            documentId = d.docId,
                            operationType = "CONCURRENT_WRITE",
                            localDoc = if (d.deleted) null else d.body,
                            incomingDoc = if (cur != null && !cur.absent) cur.body else null,
                        )
                    },
            ),
    )
