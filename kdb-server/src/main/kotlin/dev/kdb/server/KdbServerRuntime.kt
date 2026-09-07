package dev.kdb.server

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.embed.EmbeddedKdbRuntime
import dev.kdb.embed.commitViaEngine
import dev.kdb.policy.DocumentExpiryPolicy
import dev.kdb.query.hybrid.HybridQueryEngine
import dev.kdb.query.hybrid.hybridQueryEngine
import dev.kdb.schema.KdbSchema
import dev.kdb.sql.sqlEngine
import dev.kdb.transaction.ConflictPolicy
import dev.kdb.transaction.DocumentLockManager
import dev.kdb.transaction.TransactionEngine
import dev.kdb.transaction.TransactionResult
import dev.kdb.transaction.UniqueKeyRegistry
import dev.kdb.transaction.WriteAuthorizer
import dev.kdb.transaction.authorizingTransactionEngine
import dev.kdb.transaction.transactionEngine
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/** Author node id stamped on commits the server makes on its own behalf (the expiry sweeper). */
public val SERVER_SYSTEM_NODE_ID: KdbUuid = KdbUuid.fromString("00000000-0000-8000-8000-00000000cdb5")

/** §9.5: at most this many DeleteOps per expiry-sweep commit. */
public const val EXPIRY_SWEEP_BATCH: Int = 500

public class KdbServerRuntime(
    public val runtime: EmbeddedKdbRuntime,
    /** Seed coordinator for [runtime]'s default namespace - see [writeCoordinatorFor]. */
    writeCoordinator: WriteCoordinator = WriteCoordinator(),
    public val documentLocks: DocumentLockManager = DocumentLockManager(),
    /** Layer 16 §9.5: a read-only runtime never runs the expiry sweeper. */
    public val readOnly: Boolean = false,
    /** Wall clock for the expiry predicate and sweeper (injectable for tests). */
    public val nowMillis: () -> Long = { System.currentTimeMillis() },
    /** Owns background jobs (the expiry sweeper); cancelled by [close]. */
    private val backgroundScope: CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.Default),
) {
    // internal (not private): ServerRuntimeRegistry.release, in the same module, needs to read
    // the post-release count to decide whether to evict its map entry - see its doc comment.
    internal val refCount = AtomicInteger(1)

    /**
     * Mints session identities unique across every connection this runtime serves. It has to live
     * here rather than on [SessionManager] because a SessionManager is created per connection
     * (see `sqlWireHostFactory`) while [documentLocks] is runtime-global and keys lock ownership
     * by session identity: a per-manager counter handed every connection's first session the same
     * `sess-1`, so two unrelated connections were one lock holder - each able to take locks the
     * other already held, and each `releaseAll` dropping the other's mid-transaction. Mirrors Go's
     * KdbServerRuntime.sessionSeq / nextSessionOrdinal.
     */
    private val sessionOrdinal = AtomicInteger()

    internal fun nextSessionOrdinal(): Int = sessionOrdinal.incrementAndGet()
    private val closeMutex = Mutex()
    private val engineMutex = Mutex()
    private val engines = mutableMapOf<String, TransactionEngine>()
    private val upsertEngines = mutableMapOf<String, TransactionEngine>()

    // ---- Component 74 (§12): one write gate per namespace, not one per runtime -------------------

    private val writeCoordinators = ConcurrentHashMap<String, WriteCoordinator>()

    init {
        writeCoordinators[runtime.defaultNamespace] = writeCoordinator
    }

    /** The write gate serialising commits, replays and upserts for [namespaceId] - and the
     * contention sensor behind [conflictRetryAfterMs] for that namespace. Namespaces never wait on
     * each other's commits. */
    public fun writeCoordinatorFor(namespaceId: String): WriteCoordinator =
        writeCoordinators.computeIfAbsent(namespaceId) { WriteCoordinator() }

    /** The default namespace's coordinator - kept for callers that predate per-namespace gates. */
    public val writeCoordinator: WriteCoordinator
        get() = writeCoordinatorFor(runtime.defaultNamespace)

    // ---- Component 73 (§9.6): unique-key registry per namespace --------------------------------

    private val uniqueRegistries = mutableMapOf<String, UniqueKeyRegistry>()

    /** The registry every engine for [namespaceId] enforces against. Built (and rebuilt from the
     * head document tree under [runtime.schema]) on first use, so a runtime opened over existing
     * data enforces against what is already there; [rebuildUniqueKeys] redoes that explicitly. */
    public suspend fun uniqueKeysFor(namespaceId: String): UniqueKeyRegistry =
        engineMutex.withLock { uniqueKeysLocked(namespaceId) }

    private suspend fun uniqueKeysLocked(namespaceId: String): UniqueKeyRegistry {
        uniqueRegistries[namespaceId]?.let { return it }
        val registry = UniqueKeyRegistry()
        rebuildInto(registry, namespaceId, runtime.schema)
        uniqueRegistries[namespaceId] = registry
        return registry
    }

    private suspend fun rebuildInto(
        registry: UniqueKeyRegistry,
        namespaceId: String,
        schema: KdbSchema,
    ) {
        val head = runtime.dag.head()
        val tree = runtime.dag.getCommitOrThrow(head).documentTreeHash
        registry.rebuild(namespaceId, runtime.storage, tree, schema)
    }

    /** Rebuilds [namespaceId]'s unique-key registry from the current head under [schema] - call
     * after the namespace schema changes outside a transaction. Throws
     * [dev.kdb.transaction.UniqueConstraintViolationException] if the data already violates it. */
    public suspend fun rebuildUniqueKeys(
        namespaceId: String,
        schema: KdbSchema = runtime.schema,
    ) {
        val registry = uniqueKeysFor(namespaceId)
        writeCoordinatorFor(namespaceId).run { rebuildInto(registry, namespaceId, schema) }
    }

    public suspend fun engineFor(namespaceId: String): TransactionEngine =
        engineMutex.withLock {
            engines.getOrPut(namespaceId) {
                transactionEngine(
                    runtime.policyRegistry.get(namespaceId).conflict,
                    uniqueKeys = uniqueKeysLocked(namespaceId),
                )
            }
        }

    // A separate LAST_WRITE engine per namespace rather than a per-call policy override, because
    // the engine bakes its conflict policy in at construction - same shape as Go's UpsertEngine.
    // Shares the namespace's unique registry with the STRICT engine, as Go's does.
    private suspend fun upsertEngineFor(namespaceId: String): TransactionEngine =
        engineMutex.withLock {
            upsertEngines.getOrPut(namespaceId) {
                transactionEngine(ConflictPolicy.LAST_WRITE, uniqueKeys = uniqueKeysLocked(namespaceId))
            }
        }

    public suspend fun commit(
        namespaceId: String,
        transaction: KdbTransaction,
        schema: KdbSchema = runtime.schema,
        sessionId: String? = null,
        authorizer: WriteAuthorizer? = null,
        message: String = "",
    ): KdbCommit {
        // The base version has to survive the queue, not just the commit. It was resolved by the
        // caller before this call and is not consulted until the engine's commit runs at the
        // front of the namespace's write coordinator, which can be a long way behind other callers
        // - and commit throws if it has been reclaimed by then. Nothing else roots it: a base
        // version is not a branch head. Pinned before entering the coordinator, mirroring Go's
        // runTransaction (replay needs no equivalent - it ignores the transaction's base version
        // and targets the live head, which is a branch head already).
        val release = runtime.dag.pin(transaction.baseVersion)
        try {
            val engine = effectiveEngine(namespaceId, authorizer)
            return writeCoordinatorFor(namespaceId).run {
                commitViaEngine(
                    runtime,
                    namespaceId,
                    transaction,
                    schema,
                    engine,
                    documentLocks = documentLocks,
                    sessionId = sessionId,
                    message = message,
                )
            }
        } finally {
            release()
        }
    }

    public suspend fun replay(
        namespaceId: String,
        transaction: KdbTransaction,
        replayTarget: KdbHash,
        schema: KdbSchema = runtime.schema,
        authorizer: WriteAuthorizer? = null,
    ): TransactionResult {
        val engine = effectiveEngine(namespaceId, authorizer)
        return writeCoordinatorFor(namespaceId).run {
            engine.replay(transaction, runtime.dag, runtime.storage, schema, replayTarget)
        }
    }

    /** The cached per-namespace engine, wrapped per call with [authorizer] when the caller
     * (the wire layer) has a principal to check writes against. Wrapping happens outside the
     * cache since the authorizer is bound to one request's principal, not to the namespace. */
    private suspend fun effectiveEngine(
        namespaceId: String,
        authorizer: WriteAuthorizer?,
    ): TransactionEngine {
        val engine = engineFor(namespaceId)
        return if (authorizer != null) authorizingTransactionEngine(engine, namespaceId, authorizer) else engine
    }

    // ---- Component 73 (§9.5): document expiry -------------------------------------------------

    public suspend fun expiryPolicyFor(namespaceId: String): DocumentExpiryPolicy? =
        runtime.policyRegistry.get(namespaceId).documentExpiry

    private val hybridMutex = Mutex()
    private val filteredHybrids = mutableMapOf<Pair<String, DocumentExpiryPolicy>, HybridQueryEngine>()

    /**
     * The SQL/hybrid engine wire SqlExec runs against for [namespaceId]. Without an expiry policy
     * this is the runtime's own engine; with one it is an engine of the same shape built over an
     * [ExpiryFilteringStorageAdapter], so head scans, index-scan materialisation and point reads
     * skip expired documents while historical (`AT COMMIT`) reads stay untouched. Cached per
     * (namespace, policy) so a policy change picks up a fresh engine.
     */
    public suspend fun hybridFor(namespaceId: String): HybridQueryEngine {
        val expiry = expiryPolicyFor(namespaceId) ?: return runtime.hybrid
        return hybridMutex.withLock {
            filteredHybrids.getOrPut(namespaceId to expiry) {
                val filtered = ExpiryFilteringStorageAdapter(runtime.storage, runtime.dag, expiry, nowMillis)
                // The runtime's own store factory, so a CREATE INDEX issued through this engine
                // shares the runtime's single IndexBlobStore (Layer 16 §6.5/§9.2).
                val sql =
                    sqlEngine(
                        runtime.indexManager,
                        filtered,
                        runtime.dag,
                        indexStoreFactory = runtime.indexStoreFactory,
                    )
                hybridQueryEngine(sql, runtime.dag, runtime.policyRegistry, runtime.indexManager, filtered)
            }
        }
    }

    /** Applies the namespace's expiry predicate to a head read. */
    public suspend fun isExpiredAtHead(
        namespaceId: String,
        json: String,
    ): Boolean {
        val expiry = expiryPolicyFor(namespaceId) ?: return false
        return isDocumentExpired(json, expiry, nowMillis())
    }

    private var sweeperJob: Job? = null

    /** Whether the expiry sweeper coroutine is currently running (tests, diagnostics). */
    public val expirySweeperActive: Boolean
        get() = sweeperJob?.isActive == true

    /**
     * Starts the runtime's background work: the §9.5 expiry sweeper for [namespaceId] when its
     * policy declares `documentExpiry` and the runtime is not [readOnly], and an explicit unique-key
     * rebuild for the namespace (§9.6 "rebuild at open"). Idempotent; [close] stops it.
     */
    public suspend fun start(namespaceId: String = runtime.defaultNamespace) {
        uniqueKeysFor(namespaceId)
        val expiry = expiryPolicyFor(namespaceId) ?: return
        if (readOnly) return
        closeMutex.withLock {
            if (sweeperJob?.isActive == true) return
            sweeperJob =
                backgroundScope.launch {
                    while (isActive) {
                        delay(expiry.sweepIntervalMillis)
                        try {
                            sweepExpiredOnce(namespaceId)
                        } catch (e: CancellationException) {
                            throw e
                        } catch (_: Throwable) {
                            // A failed sweep is retried next interval; reads still hide expired docs.
                        }
                    }
                }
        }
    }

    /**
     * One sweeper pass (§9.5): scans head, and commits the expired documents' DeleteOps in batches of
     * at most [EXPIRY_SWEEP_BATCH] per commit under the LAST_WRITE engine as the system principal
     * with message `expiry sweep`. Returns the number of documents deleted. Public so tests and
     * operators can run a pass deterministically.
     */
    public suspend fun sweepExpiredOnce(namespaceId: String = runtime.defaultNamespace): Int {
        val expiry = expiryPolicyFor(namespaceId) ?: return 0
        if (readOnly) return 0
        val now = nowMillis()
        val head = runtime.dag.head()
        val tree = runtime.dag.getCommitOrThrow(head).documentTreeHash
        val expired = mutableListOf<KdbUuid>()
        runtime.storage.scanDocuments(namespaceId, tree, 256) { batch ->
            for (doc in batch) {
                if (isDocumentExpired(doc.json, expiry, now)) expired += doc.id
            }
        }
        if (expired.isEmpty()) return 0
        var deleted = 0
        for (chunk in expired.chunked(EXPIRY_SWEEP_BATCH)) {
            val base = runtime.dag.head()
            val tx =
                KdbTransaction(
                    KdbUuid.random(),
                    base,
                    chunk.map { KdbOp.Delete(it) },
                    KdbTimestamp.now(),
                    SERVER_SYSTEM_NODE_ID,
                )
            val engine = upsertEngineFor(namespaceId)
            val release = runtime.dag.pin(base)
            try {
                writeCoordinatorFor(namespaceId).run {
                    commitViaEngine(
                        runtime,
                        namespaceId,
                        tx,
                        runtime.schema,
                        engine,
                        documentLocks = documentLocks,
                        sessionId = "expiry-sweep",
                        message = "expiry sweep",
                    )
                }
            } finally {
                release()
            }
            deleted += chunk.size
        }
        return deleted
    }

    /** Stops background work (the expiry sweeper) and flushes index snapshots so a restart reloads
     * them instead of rebuilding by scan (Layer 16 §6.5). Safe to call more than once. */
    public suspend fun close(namespaceId: String = runtime.defaultNamespace) {
        closeMutex.withLock {
            sweeperJob?.cancel()
            sweeperJob = null
        }
        runtime.indexManager.registryFor(namespaceId).flushAll()
    }

    /**
     * Direct point lookup by document id at the current head (component 40's GetJSON) -
     * mirrors Go's KdbServerRuntime.GetDocument exactly: returns (json-or-null, headHex). An
     * expired document (§9.5) reads as absent.
     */
    public suspend fun getDocument(
        namespaceId: String,
        docId: KdbUuid,
    ): Pair<String?, String> {
        val head = runtime.dag.head()
        val commit = runtime.dag.getCommitOrThrow(head)
        val doc = runtime.storage.getDocument(namespaceId, docId, commit.documentTreeHash)
        if (doc != null && isExpiredAtHead(namespaceId, doc.json)) {
            return null to head.toHex()
        }
        return doc?.json to head.toHex()
    }

    /**
     * Writes [json] at [docId] unconditionally - create if absent, replace if present - with a
     * LAST_WRITE-policy engine anchored on the current head (component 40 spec §3/§5: "Upsert
     * never conflicts and never needs a BaseVersion"). Mirrors Go's KdbServerRuntime.Upsert.
     */
    public suspend fun upsert(
        namespaceId: String,
        docId: KdbUuid,
        json: String,
        authorizer: WriteAuthorizer? = null,
    ): KdbCommit {
        val head = runtime.dag.head()
        val tx =
            KdbTransaction(
                KdbUuid.random(),
                head,
                listOf(KdbOp.Write(docId, json)),
                KdbTimestamp.now(),
                KdbUuid.random(),
            )
        // See commit()'s pin comment - resolved outside the coordinator here too, since head is
        // this call's own base version and must stay resolvable for the same reason.
        val release = runtime.dag.pin(head)
        try {
            var engine: TransactionEngine = upsertEngineFor(namespaceId)
            if (authorizer != null) {
                engine = authorizingTransactionEngine(engine, namespaceId, authorizer)
            }
            return writeCoordinatorFor(namespaceId).run {
                commitViaEngine(
                    runtime,
                    namespaceId,
                    tx,
                    runtime.schema,
                    engine,
                    documentLocks = documentLocks,
                )
            }
        } finally {
            release()
        }
    }

    public fun retain() {
        refCount.incrementAndGet()
    }

    public suspend fun release() {
        if (refCount.decrementAndGet() > 0) return
        closeMutex.withLock {
            if (refCount.get() > 0) return
            sweeperJob?.cancel()
            sweeperJob = null
            // v1: in-memory/file engines rely on process exit; no explicit storage close yet.
        }
        runtime.indexManager.registryFor(runtime.defaultNamespace).flushAll()
    }
}

public object ServerRuntimeRegistry {
    private val mutex = Mutex()
    private val runtimes = mutableMapOf<String, KdbServerRuntime>()

    /** Returns an existing runtime or opens a new one, retaining a reference for the caller
     * either way - every successful call must be balanced by exactly one [release] of the same
     * key. */
    public suspend fun getOrOpen(key: String, open: suspend () -> KdbServerRuntime): KdbServerRuntime =
        mutex.withLock {
            val existing = runtimes[key]
            if (existing != null) {
                existing.retain()
                return@withLock existing
            }
            val rt = open()
            // KdbServerRuntime already starts refCount at 1 - that implicit reference is this
            // first caller's own, so no additional retain() belongs here. This used to retain an
            // extra time on top of it (refCount 3 for the first caller, 2 for every later
            // cache-hit caller, since the outer .also{it.retain()} applied unconditionally to
            // both the hit and miss paths), so release() could never actually bring a fresh
            // runtime's refCount down to zero.
            runtimes[key] = rt
            rt
        }

    /** Releases one reference obtained from [getOrOpen]. Once the last outstanding reference is
     * released (refCount reaches zero - see [KdbServerRuntime.release]), the entry is removed
     * from the registry so a later [getOrOpen] for the same key reopens fresh rather than
     * reusing an already-released, zero-refCount instance. */
    public suspend fun release(key: String) {
        mutex.withLock {
            val rt = runtimes[key] ?: return@withLock
            rt.release()
            if (rt.refCount.get() <= 0) {
                runtimes.remove(key)
            }
        }
    }
}
