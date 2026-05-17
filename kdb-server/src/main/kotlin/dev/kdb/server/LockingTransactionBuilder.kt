package dev.kdb.server

import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbDocument
import dev.kdb.transaction.DocumentLockManager
import dev.kdb.transaction.TransactionBuilder

/** Acquires document locks when write/delete ops are buffered in a session transaction. */
public class LockingTransactionBuilder(
    private val inner: TransactionBuilder,
    private val locks: DocumentLockManager,
    private val sessionId: String,
) {
    public suspend fun write(
        docId: KdbUuid,
        patchJson: String,
    ): LockingTransactionBuilder {
        locks.tryAcquire(inner.namespaceId, docId, sessionId)
        inner.write(docId, patchJson)
        return this
    }

    public suspend fun writeDocument(document: KdbDocument): LockingTransactionBuilder =
        write(document.id, document.json)

    public suspend fun delete(docId: KdbUuid): LockingTransactionBuilder {
        locks.tryAcquire(inner.namespaceId, docId, sessionId)
        inner.delete(docId)
        return this
    }

    public suspend fun build() = inner.build()

    public val namespaceId: String get() = inner.namespaceId
}
