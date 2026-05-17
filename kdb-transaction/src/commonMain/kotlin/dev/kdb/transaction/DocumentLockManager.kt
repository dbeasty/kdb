package dev.kdb.transaction

import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.error.DocumentLockedException
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/** Exclusive write lock per `(namespaceId, docId)` held by a session until release. */
public class DocumentLockManager {
    private val mutex = Mutex()
    private val locks = mutableMapOf<LockKey, String>()

    public suspend fun tryAcquire(
        namespaceId: String,
        docId: KdbUuid,
        sessionId: String,
    ) {
        mutex.withLock {
            val key = LockKey(namespaceId, docId)
            val owner = locks[key]
            when {
                owner == null -> locks[key] = sessionId
                owner == sessionId -> Unit
                else ->
                    throw DocumentLockedException(
                        "document ${docId.toString()} is locked by session $owner",
                        namespaceId,
                        docId.toString(),
                        owner,
                    )
            }
        }
    }

    public suspend fun release(
        namespaceId: String,
        docId: KdbUuid,
        sessionId: String,
    ) {
        mutex.withLock {
            val key = LockKey(namespaceId, docId)
            if (locks[key] == sessionId) {
                locks.remove(key)
            }
        }
    }

    public suspend fun releaseAll(sessionId: String) {
        mutex.withLock {
            locks.entries.removeAll { (_, owner) -> owner == sessionId }
        }
    }

    public suspend fun assertHeld(
        namespaceId: String,
        docId: KdbUuid,
        sessionId: String,
    ) {
        mutex.withLock {
            val owner = locks[LockKey(namespaceId, docId)]
            if (owner != sessionId) {
                throw DocumentLockedException(
                    "session $sessionId does not hold lock on document ${docId.toString()}",
                    namespaceId,
                    docId.toString(),
                    owner ?: "",
                )
            }
        }
    }

    public suspend fun acquireAllForTransaction(
        namespaceId: String,
        sessionId: String,
        transaction: KdbTransaction,
    ) {
        for (docId in documentIdsIn(transaction)) {
            tryAcquire(namespaceId, docId, sessionId)
        }
    }

    private data class LockKey(
        val namespaceId: String,
        val docId: KdbUuid,
    )
}

public fun documentIdsIn(transaction: KdbTransaction): Set<KdbUuid> =
    documentIdsIn(transaction.operations)

public fun documentIdsIn(operations: List<KdbOp>): Set<KdbUuid> =
    operations.mapNotNull { op ->
        when (op) {
            is KdbOp.Write -> op.docId
            is KdbOp.Delete -> op.docId
            is KdbOp.FileWrite, is KdbOp.SchemaMigration -> null
        }
    }.toSet()
