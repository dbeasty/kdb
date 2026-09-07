package dev.kdb.index

import dev.kdb.codec.KdbHash
import dev.kdb.storage.StorageAdapter
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * Keyed blob store for index snapshots and the index catalog (Layer 16, §6.5 / §9.2).
 *
 * [StorageAdapter]'s blob API is content-addressed (`writeBlob` returns the SHA-256 of the
 * bytes), so a snapshot written there cannot be found again from a name alone. This interface
 * adds the missing name → bytes mapping. The keys used by the index layer are:
 *
 * - `index/<indexId>/snapshot` — one FULLTEXT / VECTOR snapshot per index
 * - `index/catalog/<namespaceId>` — the namespace's [IndexCatalog]
 */
public interface IndexBlobStore {
    public suspend fun write(
        key: String,
        bytes: ByteArray,
    )

    public suspend fun read(key: String): ByteArray?

    public suspend fun delete(key: String)
}

/** Snapshot blob key for one index. */
public fun indexSnapshotBlobKey(indexId: dev.kdb.codec.KdbUuid): String = "index/$indexId/snapshot"

/** Catalog blob key for one namespace. */
public fun indexCatalogBlobKey(namespaceId: String): String = "index/catalog/$namespaceId"

/** Purely in-memory store — what memory runtimes use (they never persist, §6.5). */
public class InMemoryIndexBlobStore : IndexBlobStore {
    private val mutex = Mutex()
    private val blobs = mutableMapOf<String, ByteArray>()

    override suspend fun write(
        key: String,
        bytes: ByteArray,
    ) {
        mutex.withLock { blobs[key] = bytes.copyOf() }
    }

    override suspend fun read(key: String): ByteArray? = mutex.withLock { blobs[key]?.copyOf() }

    override suspend fun delete(key: String) {
        mutex.withLock { blobs.remove(key) }
    }
}

/**
 * Durable key → content-hash pointers for [StorageAdapterIndexBlobStore]. The default is an
 * in-memory table; a runtime with a keyed metadata file (the JDBC file runtime, the server's
 * catalog directory) supplies its own so pointers survive a restart.
 */
public interface IndexBlobPointers {
    public suspend fun get(key: String): KdbHash?

    public suspend fun put(
        key: String,
        hash: KdbHash,
    )

    public suspend fun remove(key: String)
}

public class InMemoryIndexBlobPointers : IndexBlobPointers {
    private val mutex = Mutex()
    private val map = mutableMapOf<String, KdbHash>()

    override suspend fun get(key: String): KdbHash? = mutex.withLock { map[key] }

    override suspend fun put(
        key: String,
        hash: KdbHash,
    ) {
        mutex.withLock { map[key] = hash }
    }

    override suspend fun remove(key: String) {
        mutex.withLock { map.remove(key) }
    }
}

/**
 * Writes every blob through [StorageAdapter.writeBlob] (content-addressed, so the bytes live in
 * the namespace's ordinary blob store and are covered by its durability guarantees) and keeps the
 * key → hash pointer in [pointers]. Reads resolve the pointer and then [StorageAdapter.readBlob].
 */
public class StorageAdapterIndexBlobStore(
    private val storage: StorageAdapter,
    private val pointers: IndexBlobPointers = InMemoryIndexBlobPointers(),
) : IndexBlobStore {
    override suspend fun write(
        key: String,
        bytes: ByteArray,
    ) {
        val hash = storage.writeBlob(bytes)
        pointers.put(key, hash)
    }

    override suspend fun read(key: String): ByteArray? {
        val hash = pointers.get(key) ?: return null
        return storage.readBlob(hash)
    }

    override suspend fun delete(key: String) {
        pointers.remove(key)
    }
}

public fun storageAdapterIndexBlobStore(
    storage: StorageAdapter,
    pointers: IndexBlobPointers = InMemoryIndexBlobPointers(),
): IndexBlobStore = StorageAdapterIndexBlobStore(storage, pointers)
