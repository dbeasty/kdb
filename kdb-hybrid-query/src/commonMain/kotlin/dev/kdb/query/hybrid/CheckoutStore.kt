package dev.kdb.query.hybrid

import dev.kdb.dag.CommitDag
import dev.kdb.dag.CommitRef
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

public class CheckoutStore {
    private val mutex = Mutex()
    private val checkouts = mutableMapOf<String, CheckoutHandle>()

    public suspend fun checkout(
        dag: CommitDag,
        namespaceId: String,
        ref: CommitRef,
    ): CheckoutHandle {
        val hash = dag.resolveRefOrThrow(ref)
        val handle = CheckoutHandle(namespaceId, hash, readOnly = true)
        mutex.withLock { checkouts[namespaceId] = handle }
        return handle
    }

    public suspend fun get(namespaceId: String): CheckoutHandle? = mutex.withLock { checkouts[namespaceId] }

    public suspend fun reset(namespaceId: String) {
        mutex.withLock { checkouts.remove(namespaceId) }
    }
}
