package dev.kdb.storage.engine

import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbDocument
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * Holds writes staged by putDocument/deleteDocument but not yet visible
 * via getDocument/commitTree, sharded like ShardedDocStore so different
 * documents' staging doesn't serialize. ServerStorageEngine is
 * constructed per-namespace, so unlike a namespace-keyed store this
 * needs no outer namespace map. Mirrors Go's pending_shard.go
 * (go/kdb/storage/engine). A document is staged in at most one of
 * puts/deletes at a time: put always clears any pending delete for the
 * same id and vice versa.
 */
public class ShardedPendingStore {
    private companion object {
        const val SHARD_COUNT = 64
    }

    private class Shard {
        val mutex = Mutex()
        val puts = mutableMapOf<KdbUuid, KdbDocument>()
        val deletes = mutableSetOf<KdbUuid>()
    }

    private val shards = Array(SHARD_COUNT) { Shard() }

    private fun shardFor(id: KdbUuid): Shard {
        val idx = (id.lsb.toInt() and Int.MAX_VALUE) % SHARD_COUNT
        return shards[idx]
    }

    public suspend fun put(document: KdbDocument) {
        val sh = shardFor(document.id)
        sh.mutex.withLock {
            sh.deletes.remove(document.id)
            sh.puts[document.id] = document
        }
    }

    public suspend fun delete(id: KdbUuid) {
        val sh = shardFor(id)
        sh.mutex.withLock {
            sh.puts.remove(id)
            sh.deletes.add(id)
        }
    }

    /** Atomically (per shard) returns and clears every staged put/delete across all shards. */
    public suspend fun takeAllAndClear(): Pair<List<KdbDocument>, List<KdbUuid>> {
        val puts = mutableListOf<KdbDocument>()
        val deletes = mutableListOf<KdbUuid>()
        for (sh in shards) {
            sh.mutex.withLock {
                puts.addAll(sh.puts.values)
                deletes.addAll(sh.deletes)
                sh.puts.clear()
                sh.deletes.clear()
            }
        }
        return puts to deletes
    }

    /** Clears every staged put/delete without applying them, restoring the last-committed visible state. */
    public suspend fun discardAll() {
        for (sh in shards) {
            sh.mutex.withLock {
                sh.puts.clear()
                sh.deletes.clear()
            }
        }
    }
}
