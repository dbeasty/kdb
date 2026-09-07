package dev.kdb.index

import dev.kdb.dag.CommitDag
import dev.kdb.storage.StorageAdapter

/**
 * Node bootstrap: Layer 5 index stores wired for production. One [IndexBlobStore] is shared by the
 * store factory (snapshots) and the manager (catalog); pass [blobs] to make both durable across a
 * restart (see [StorageAdapterIndexBlobStore] / [IndexBlobPointers]).
 */
public fun productionIndexManager(
    dag: CommitDag,
    storage: StorageAdapter,
    vectorDimensions: Int = 128,
    blobs: IndexBlobStore = storageAdapterIndexBlobStore(storage),
): IndexManager = indexManager(compositeIndexStoreFactory(dag, storage, vectorDimensions, blobs), blobs)
