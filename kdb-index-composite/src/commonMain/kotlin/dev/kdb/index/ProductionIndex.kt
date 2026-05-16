package dev.kdb.index

import dev.kdb.dag.CommitDag
import dev.kdb.storage.StorageAdapter

/** Node bootstrap: Layer 5 index stores wired for production. */
public fun productionIndexManager(
    dag: CommitDag,
    storage: StorageAdapter,
    vectorDimensions: Int = 128,
): IndexManager = indexManager(compositeIndexStoreFactory(dag, storage, vectorDimensions))
