package dev.kdb.server

import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.embed.EmbeddedKdbRuntime
import dev.kdb.embed.commitViaEngine
import dev.kdb.error.ConflictException
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.isNone
import dev.kdb.transaction.ConflictPolicy
import dev.kdb.transaction.DocumentLockManager
import dev.kdb.transaction.TransactionEngine
import dev.kdb.transaction.TransactionResult
import dev.kdb.transaction.WriteAuthorizer
import dev.kdb.transaction.authorizingTransactionEngine
import dev.kdb.transaction.transactionEngine
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.atomic.AtomicInteger

public class KdbServerRuntime(
    public val runtime: EmbeddedKdbRuntime,
    public val writeCoordinator: WriteCoordinator = WriteCoordinator(),
    public val documentLocks: DocumentLockManager = DocumentLockManager(),
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

    public suspend fun engineFor(namespaceId: String): TransactionEngine =
        engineMutex.withLock {
            engines.getOrPut(namespaceId) {
                transactionEngine(runtime.policyRegistry.get(namespaceId).conflict)
            }
        }

    public suspend fun commit(
        namespaceId: String,
        transaction: KdbTransaction,
        schema: KdbSchema = runtime.schema,
        sessionId: String? = null,
        authorizer: WriteAuthorizer? = null,
    ): KdbCommit =
        writeCoordinator.run {
            commitViaEngine(
                runtime,
                namespaceId,
                transaction,
                schema,
                effectiveEngine(namespaceId, authorizer),
                documentLocks = documentLocks,
                sessionId = sessionId,
            )
        }

    public suspend fun replay(
        namespaceId: String,
        transaction: KdbTransaction,
        replayTarget: dev.kdb.codec.KdbHash,
        schema: KdbSchema = runtime.schema,
        authorizer: WriteAuthorizer? = null,
    ): TransactionResult =
        writeCoordinator.run {
            effectiveEngine(namespaceId, authorizer)
                .replay(transaction, runtime.dag, runtime.storage, schema, replayTarget)
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

    /**
     * Direct point lookup by document id at the current head (component 40's GetJSON) -
     * mirrors Go's KdbServerRuntime.GetDocument exactly: returns (json-or-null, headHex).
     */
    public suspend fun getDocument(
        namespaceId: String,
        docId: KdbUuid,
    ): Pair<String?, String> {
        val head = runtime.dag.head()
        val commit = runtime.dag.getCommitOrThrow(head)
        val doc = runtime.storage.getDocument(namespaceId, docId, commit.documentTreeHash)
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
    ): KdbCommit =
        writeCoordinator.run {
            val head = runtime.dag.head()
            val tx =
                KdbTransaction(
                    KdbUuid.random(),
                    head,
                    listOf(KdbOp.Write(docId, json)),
                    KdbTimestamp.now(),
                    KdbUuid.random(),
                )
            var engine: TransactionEngine = upsertEngine
            if (authorizer != null) {
                engine = authorizingTransactionEngine(engine, namespaceId, authorizer)
            }
            commitViaEngine(
                runtime,
                namespaceId,
                tx,
                runtime.schema,
                engine,
                documentLocks = documentLocks,
            )
        }

    // A separate LAST_WRITE engine instance rather than a per-call policy override, because the
    // engine bakes its conflict policy in at construction - same shape as Go's UpsertEngine.
    private val upsertEngine: TransactionEngine = transactionEngine(ConflictPolicy.LAST_WRITE)

    public fun retain() {
        refCount.incrementAndGet()
    }

    public suspend fun release() {
        if (refCount.decrementAndGet() > 0) return
        closeMutex.withLock {
            if (refCount.get() > 0) return
            // v1: in-memory/file engines rely on process exit; no explicit storage close yet.
        }
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
