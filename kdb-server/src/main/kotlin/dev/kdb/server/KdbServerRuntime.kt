package dev.kdb.server

import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbTransaction
import dev.kdb.embed.EmbeddedKdbRuntime
import dev.kdb.embed.commitViaEngine
import dev.kdb.error.ConflictException
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.isNone
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
    private val refCount = AtomicInteger(1)
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

    public suspend fun getOrOpen(key: String, open: suspend () -> KdbServerRuntime): KdbServerRuntime =
        mutex.withLock {
            runtimes.getOrPut(key) {
                open().also { it.retain() }
            }.also { it.retain() }
        }

    public suspend fun release(key: String) {
        mutex.withLock {
            runtimes[key]?.release()
            if (runtimes[key]?.let { true } == true) {
                // keep entry until refCount zero — simplified v1: never evict
            }
        }
    }
}
